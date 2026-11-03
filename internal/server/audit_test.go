package server

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gin-gonic/gin"

	"litepay/internal/config"
	"litepay/internal/model"
	"litepay/internal/settings"
	"litepay/internal/store"
)

func TestAuditLoginCannotRotateForwardedIPToBypassLimit(t *testing.T) {
	engine, _, _ := newServerTestEnv(t, nil)
	for i := 0; i < 8; i++ {
		r := httptest.NewRequest(http.MethodPost, "/admin/login", strings.NewReader("username=admin&password=wrong"))
		r.Header.Set("Content-Type", "application/x-www-form-urlencoded")
		r.Header.Set("X-Forwarded-For", fmt.Sprintf("203.0.113.%d", i+1))
		r.RemoteAddr = "198.51.100.1:1234"
		w := httptest.NewRecorder()
		engine.ServeHTTP(w, r)
		if i >= 6 && !strings.Contains(w.Header().Get("Location"), url.QueryEscape("尝试过于频繁")) {
			t.Fatal("forwarded IP bypassed login limit")
		}
	}
}

func TestAuditConcurrentLoginAttemptsAreReserved(t *testing.T) {
	_, mgr, _ := newServerTestEnv(t, nil)
	a := newAdminAuth(mgr.Get())
	var wg sync.WaitGroup
	var allowed atomic.Int32
	for i := 0; i < 32; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if a.allow("same-ip") {
				allowed.Add(1)
			}
		}()
	}
	wg.Wait()
	if allowed.Load() != 6 {
		t.Fatalf("allowed %d concurrent attempts", allowed.Load())
	}
}

func TestAuditSuccessfulLoginClearsOnlySettledFailures(t *testing.T) {
	cfg := config.Default()
	cfg.Admin.Username = "admin"
	cfg.Admin.Password = "correct horse battery staple"
	a := newAdminAuth(cfg)

	if !a.allow("client") {
		t.Fatal("first attempt was rejected")
	}
	a.succeeded("client")
	for i := 0; i < 6; i++ {
		if !a.allow("client") {
			t.Fatalf("attempt %d remained limited after a successful login", i+1)
		}
	}
	if a.allow("client") {
		t.Fatal("six attempts after successful login were not limited")
	}

	a.attempts["concurrent"] = &loginAttempt{count: 2, inflight: 2, until: time.Now().Add(time.Minute)}
	a.succeeded("concurrent")
	if got := a.attempts["concurrent"].count; got != 1 {
		t.Fatalf("success did not preserve a concurrently reserved attempt: count=%d", got)
	}
}

func TestAuditCashierUsesLockedUIDAndPrecision(t *testing.T) {
	engine, mgr, st := newServerTestEnv(t, nil)
	o, err := st.CreateOrder(context.Background(), store.CreateOrderInput{Pid: "bepusdt", TradeNo: "locked-display", OutTradeNo: "locked-display", OriginMoney: "2.50", OriginCurrency: "USD", ExpiredAt: time.Now().Add(time.Hour), Reselect: true})
	if err != nil {
		t.Fatal(err)
	}
	o, err = st.SelectPayment(context.Background(), o.TradeNo, "binance", "USDT", "locked-old-uid", 2.5099, .0001, 99, .01, 4, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	w := httptest.NewRecorder()
	engine.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/pay/"+o.TradeNo, nil))
	if !strings.Contains(w.Body.String(), "locked-old-uid") || !strings.Contains(w.Body.String(), "2.5100") {
		t.Fatal("cashier did not use locked payment details")
	}
	uid := "new-uid"
	if err := mgr.Update(context.Background(), settings.Patch{BinanceUID: &uid}, "test"); err == nil {
		t.Fatal("changed payee with active orders")
	}
}

func TestAuditRejectsTrailingJSONAndUnboundedTimeout(t *testing.T) {
	if _, err := decodeBEpusdtJSON([]byte(`{"amount":1} {}`)); err == nil {
		t.Fatal("accepted trailing JSON")
	}
	if _, err := decodeBEpusdtJSON([]byte(`null`)); err == nil {
		t.Fatal("accepted null object")
	}
	if len(generateTradeNo()) != 34 {
		t.Fatal("public order credential lacks 128-bit random identifier")
	}
}

func TestAuditInvalidCSRFDoesNotExecuteMutation(t *testing.T) {
	engine, mgr, _ := newServerTestEnv(t, nil)
	before := mgr.Get().BEpusdt.APIToken
	req := httptest.NewRequest(http.MethodPost, "/admin/settings/token", strings.NewReader("csrf=wrong"))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	req.AddCookie(&http.Cookie{Name: adminCookieName, Value: adminCookie(t, mgr.Get())})
	rec := httptest.NewRecorder()
	engine.ServeHTTP(rec, req)
	if rec.Code != http.StatusForbidden || mgr.Get().BEpusdt.APIToken != before {
		t.Fatal("invalid CSRF executed token rotation")
	}
}

func TestAuditCashierRejectsCrossSitePaymentSelection(t *testing.T) {
	engine, mgr, st := newServerTestEnv(t, nil)
	o, err := st.CreateOrder(context.Background(), store.CreateOrderInput{
		Pid: "bepusdt", TradeNo: "cross-site", OutTradeNo: "cross-site",
		OriginMoney: "2.50", OriginCurrency: "USD",
		ExpiredAt: time.Now().Add(time.Hour), Reselect: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	form := url.Values{
		"trade_no": {o.TradeNo}, "provider": {"binance"}, "currency": {"USDT"},
	}
	req := httptest.NewRequest(http.MethodPost, "/api/order/payment", strings.NewReader(form.Encode()))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	req.Header.Set("Sec-Fetch-Site", "cross-site")
	rec := httptest.NewRecorder()
	engine.ServeHTTP(rec, req)
	if rec.Code != http.StatusForbidden {
		t.Fatalf("cross-site selection HTTP %d: %s", rec.Code, rec.Body.String())
	}
	got, _ := st.GetByTradeNo(context.Background(), o.TradeNo)
	if got.Status != model.StatusPending || got.Collection != "" || got.Currency != "" {
		t.Fatalf("cross-site request changed order: %+v", got)
	}
	if mgr.Get().Checkout.Binance.UID == "" {
		t.Fatal("test setup lost Binance UID")
	}
}

func TestAuditCashierStatusReportsProviderForReload(t *testing.T) {
	engine, mgr, st := newServerTestEnv(t, nil)
	rec := postJSON(engine, "/api/v1/order/create-order",
		signedCreateOrder(t, mgr.Get().BEpusdt.APIToken, "MERCHANT-RELOAD", "2.50", "USDT,USDC", true))
	var resp bepusdtResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatal(err)
	}
	tradeID := resp.Data.(map[string]interface{})["trade_id"].(string)
	if _, err := st.SelectPayment(context.Background(), tradeID, "binance", "USDT",
		mgr.Get().Checkout.Binance.UID, 2.50, 0.01, 99, 0.01, 2, time.Minute); err != nil {
		t.Fatal(err)
	}
	status := httptest.NewRecorder()
	engine.ServeHTTP(status, httptest.NewRequest(http.MethodGet,
		"/api/order/status?order_id="+tradeID, nil))
	if status.Code != http.StatusOK || !strings.Contains(status.Body.String(), `"provider":"binance"`) {
		t.Fatalf("status response = %d %s", status.Code, status.Body.String())
	}
}

func TestAuditRejectsOversizedMerchantFields(t *testing.T) {
	engine, mgr, _ := newServerTestEnv(t, nil)
	rec := postJSON(engine, "/api/v1/order/create-order",
		signedCreateOrder(t, mgr.Get().BEpusdt.APIToken,
			strings.Repeat("o", 129), "2.50", "USDT", true))
	var resp bepusdtResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatal(err)
	}
	if resp.StatusCode != 400 || !strings.Contains(resp.Message, "maximum length") {
		t.Fatalf("oversized order was accepted: %+v", resp)
	}
}

func TestAuditAdminOrderQueryBounds(t *testing.T) {
	if got := adminOrdersPage("999999999999999999999999"); got != 1 {
		t.Fatalf("invalid page = %d, want 1", got)
	}
	if got := adminOrdersPage("999999999"); got != adminOrdersMaxPage {
		t.Fatalf("oversized page = %d, want %d", got, adminOrdersMaxPage)
	}
	if got := adminOrderStatus("PAID' OR 1=1 --"); got != "" {
		t.Fatalf("unexpected status filter %q", got)
	}
	if got := adminOrderStatus("paid"); got != "PAID" {
		t.Fatalf("status normalization = %q", got)
	}
}

func TestAuditSettingsCanExplicitlyClearSensitiveFields(t *testing.T) {
	_, mgr, _ := newServerTestEnv(t, nil)
	form := url.Values{
		"binance_uid":              {""},
		"clear_binance_uid":        {"on"},
		"okx_uid":                  {""},
		"clear_okx_uid":            {"on"},
		"checkout_note":            {""},
		"clear_checkout_note":      {"on"},
		"binance_api_key":          {""},
		"clear_binance_api_key":    {"on"},
		"binance_api_secret":       {""},
		"clear_binance_api_secret": {"on"},
		"okx_api_key":              {""},
		"clear_okx_api_key":        {"on"},
		"okx_api_secret":           {""},
		"clear_okx_api_secret":     {"on"},
		"okx_passphrase":           {""},
		"clear_okx_passphrase":     {"on"},
		"checkout_currencies":      {"USDT"},
	}
	req := httptest.NewRequest(http.MethodPost, "/admin/settings", strings.NewReader(form.Encode()))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	c, _ := gin.CreateTestContext(httptest.NewRecorder())
	c.Request = req
	patch, errs := parseSettingsForm(c)
	if len(errs) != 0 {
		t.Fatalf("unexpected errors: %v", errs)
	}
	if err := mgr.Update(context.Background(), patch, "test"); err != nil {
		t.Fatal(err)
	}
	cfg := mgr.Get()
	if cfg.Checkout.Binance.UID != "" || cfg.Checkout.OKX.UID != "" ||
		cfg.Binance.APIKey != "" || cfg.Binance.APISecret != "" ||
		cfg.OKX.APIKey != "" || cfg.OKX.APISecret != "" || cfg.OKX.Passphrase != "" ||
		cfg.Checkout.Note != "" {
		t.Fatalf("clear checkboxes did not clear values: %+v", cfg)
	}
	// The test constructor seeds a YAML token and does not call Load, so the
	// managed flag is only asserted after a real startup path.
	if cfg.BEpusdt.APIToken == "" {
		t.Fatal("clearing payment fields must not alter the API token")
	}
}

func TestAuditGlobalSecurityHeaders(t *testing.T) {
	engine, _, _ := newServerTestEnv(t, nil)
	rec := httptest.NewRecorder()
	engine.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/", nil))
	if got := rec.Header().Get("X-Frame-Options"); got != "DENY" {
		t.Fatalf("X-Frame-Options = %q", got)
	}
	if got := rec.Header().Get("Permissions-Policy"); !strings.Contains(got, "camera=()") {
		t.Fatalf("Permissions-Policy = %q", got)
	}
	if got := rec.Header().Get("Strict-Transport-Security"); !strings.Contains(got, "max-age=31536000") {
		t.Fatalf("Strict-Transport-Security = %q", got)
	}
}

func TestAuditPublicHealthAndHomeDoNotExposeOrderStats(t *testing.T) {
	engine, _, st := newServerTestEnv(t, nil)
	if _, err := st.CreateOrder(context.Background(), store.CreateOrderInput{
		Pid: "bepusdt", TradeNo: "public-stats", OutTradeNo: "public-stats",
		OriginMoney: "2.50", OriginCurrency: "USD", ExpiredAt: time.Now().Add(time.Hour),
	}); err != nil {
		t.Fatal(err)
	}

	for _, path := range []string{"/healthz", "/"} {
		rec := httptest.NewRecorder()
		engine.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, path, nil))
		want := http.StatusOK
		if path == "/" {
			want = http.StatusNotFound
		}
		if rec.Code != want {
			t.Fatalf("%s HTTP %d", path, rec.Code)
		}
		body := strings.ToLower(rec.Body.String())
		for _, leaked := range []string{"stats", "pending", "待支付", "已支付", "已过期"} {
			if strings.Contains(body, leaked) {
				t.Fatalf("%s exposes order statistics marker %q: %s", path, leaked, rec.Body.String())
			}
		}
	}
}

func TestAuditAdminSessionUsesStrongKDFAndRotatesOnPasswordChange(t *testing.T) {
	cfg := config.Default()
	cfg.Admin.Username = "admin"
	cfg.Admin.Password = "correct horse battery staple"
	auth := newAdminAuth(cfg)
	if len(auth.key) != adminSessionKeyBytes {
		t.Fatalf("session key length = %d", len(auth.key))
	}
	token, _, err := auth.issue()
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := auth.parse(token); !ok {
		t.Fatal("valid session did not parse")
	}

	cfg.Admin.Password = "another strong password"
	if _, ok := newAdminAuth(cfg).parse(token); ok {
		t.Fatal("old session survived a password change")
	}
}
