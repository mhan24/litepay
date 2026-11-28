package server

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"

	"litepay/internal/config"
	"litepay/internal/model"
	"litepay/internal/settings"
	"litepay/internal/sign"
	"litepay/internal/store"
)

func TestSignedCashierAndAdminPages(t *testing.T) {
	st, e := store.Open(":memory:")
	if e != nil {
		t.Fatal(e)
	}
	defer st.Close()
	cfg := config.Default()
	cfg.Server.Production = false
	cfg.Admin.Password = "test-password"
	cfg.Server.BaseURL = "https://pay.example"
	cfg.Server.NotifyHosts = []string{"shop.example"}
	cfg.Binance.APIKey = "key"
	cfg.Binance.APISecret = "secret"
	cfg.Checkout.Binance.UID = "receiver"
	mgr := settings.New(cfg, st)
	if e = mgr.Load(context.Background()); e != nil {
		t.Fatal(e)
	}
	engine, e := New(mgr, st, verifiedPoller(t, mgr, st))
	if e != nil {
		t.Fatal(e)
	}
	payload := map[string]any{"order_id": "shop-order", "amount": json.Number("10.001"), "notify_url": "https://shop.example/notify", "redirect_url": "https://shop.example/return", "fiat": "USD", "reselect": true}
	payload["signature"] = sign.BEpusdtSignGeneric(payload, mgr.Get().BEpusdt.APIToken)
	body, _ := json.Marshal(payload)
	w := httptest.NewRecorder()
	r := httptest.NewRequest("POST", "/api/v1/order/create-order", bytes.NewReader(body))
	r.Header.Set("Content-Type", "application/json")
	engine.ServeHTTP(w, r)
	var reply struct {
		Status int `json:"status_code"`
		Data   struct {
			ID string `json:"trade_id"`
		} `json:"data"`
	}
	if e = json.Unmarshal(w.Body.Bytes(), &reply); e != nil || reply.Status != 200 || reply.Data.ID == "" {
		t.Fatal(w.Body.String(), e)
	}
	no := reply.Data.ID
	for _, path := range []string{"/pay/" + no, "/pay/" + no + "?provider=binance"} {
		w = httptest.NewRecorder()
		engine.ServeHTTP(w, httptest.NewRequest("GET", path, nil))
		if w.Code != 200 || strings.Contains(w.Body.String(), "ZgotmplZ") {
			t.Fatal(path, w.Code, w.Body.String())
		}
	}
	form := url.Values{"trade_no": {no}, "provider": {"binance"}, "currency": {"USDT"}}
	w = httptest.NewRecorder()
	r = httptest.NewRequest("POST", "/api/order/payment", strings.NewReader(form.Encode()))
	r.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	engine.ServeHTTP(w, r)
	if w.Code != 200 {
		t.Fatal(w.Body.String())
	}
	o, e := st.GetByTradeNo(context.Background(), no)
	if e != nil || o.PayAmount() != "10.01" {
		t.Fatal(o, e)
	}
	retry := signedCreateRequest(t, engine, mgr.Get(), payload)
	var retried struct {
		Data struct {
			TradeID string `json:"trade_id"`
			Network []struct {
				Network      string `json:"network"`
				Currency     string `json:"currency"`
				ActualAmount string `json:"actual_amount"`
			} `json:"network"`
		} `json:"data"`
	}
	if err := json.Unmarshal(retry.Body.Bytes(), &retried); err != nil || retried.Data.TradeID != no {
		t.Fatalf("retry did not return the original order: %s", retry.Body.String())
	}
	var selectedAmount string
	for _, method := range retried.Data.Network {
		if method.Network == "Binance" && method.Currency == "USDT" {
			selectedAmount = method.ActualAmount
		}
	}
	if selectedAmount != o.PayAmount() {
		t.Fatalf("retry returned %q for the reserved payment amount %q", selectedAmount, o.PayAmount())
	}
	w = httptest.NewRecorder()
	engine.ServeHTTP(w, httptest.NewRequest("GET", "/pay/"+no, nil))
	if !strings.Contains(w.Body.String(), "10.01") || strings.Contains(w.Body.String(), "四舍五入") {
		t.Fatal("wrong payment display")
	}
	auth := newAdminAuth(mgr.Get())
	token, _, e := auth.issue()
	if e != nil {
		t.Fatal(e)
	}
	for _, path := range []string{"/admin", "/admin/orders", "/admin/orders/" + no, "/admin/settings", "/admin/integration", "/admin/login", "/api/order/status?order_id=" + no} {
		w = httptest.NewRecorder()
		r = httptest.NewRequest("GET", path, nil)
		r.AddCookie(&http.Cookie{Name: adminCookieName, Value: token})
		engine.ServeHTTP(w, r)
		if w.Code != 200 && path != "/admin/login" {
			t.Fatal(path, w.Code, w.Body.String())
		}
		if strings.Contains(w.Body.String(), "<no value>") {
			t.Fatal(fmt.Sprint(path, " missing view data"))
		}
	}
}

// 上游商城（dujiao-next BEpusdt 适配器）回调验签用 %v(float64) 重算金额：
// 金额 >= 1000000 或 < 0.0001 会被渲染成指数形式（1e+06 / 1e-05），与网关按十进制
// 字符串生成的签名永远不一致，回调会被商城静默拒绝。创建订单时必须显式拒绝。
func TestBEpusdtCreateRejectsInteroperabilityRange(t *testing.T) {
	st, e := store.Open(":memory:")
	if e != nil {
		t.Fatal(e)
	}
	defer st.Close()
	cfg := config.Default()
	cfg.Server.Production = false
	cfg.Binance.APIKey = "key"
	cfg.Binance.APISecret = "secret"
	cfg.Checkout.Binance.UID = "receiver"
	mgr := settings.New(cfg, st)
	if e = mgr.Load(context.Background()); e != nil {
		t.Fatal(e)
	}
	engine, e := New(mgr, st, verifiedPoller(t, mgr, st))
	if e != nil {
		t.Fatal(e)
	}
	token := mgr.Get().BEpusdt.APIToken
	for name, tc := range map[string]struct {
		amount string
		ok     bool
	}{
		"upper_edge_ok": {"999999.99", true},
		"one_million":   {"1000000", false},
		"lower_edge_ok": {"0.0001", true},
		"too_small":     {"0.000001", false},
	} {
		t.Run(name, func(t *testing.T) {
			payload := map[string]any{"order_id": "shop-" + name, "amount": json.Number(tc.amount), "notify_url": "https://shop.example/notify", "redirect_url": "https://shop.example/return", "fiat": "USD"}
			payload["signature"] = sign.BEpusdtSignGeneric(payload, token)
			body, _ := json.Marshal(payload)
			r := httptest.NewRequest("POST", "/api/v1/order/create-order", bytes.NewReader(body))
			r.Header.Set("Content-Type", "application/json")
			w := httptest.NewRecorder()
			engine.ServeHTTP(w, r)
			var reply struct {
				Status  int    `json:"status_code"`
				Message string `json:"message"`
				Data    struct {
					ID string `json:"trade_id"`
				} `json:"data"`
			}
			if e := json.Unmarshal(w.Body.Bytes(), &reply); e != nil {
				t.Fatal(w.Body.String(), e)
			}
			if tc.ok && (reply.Status != 200 || reply.Data.ID == "") {
				t.Fatal("expected acceptance", w.Body.String())
			}
			if !tc.ok && (reply.Status == 200 || !strings.Contains(reply.Message, "interoperable")) {
				t.Fatal("expected explicit interoperability rejection", w.Body.String())
			}
		})
	}
}

// 创建订单接受的原始金额，向上对齐到两位小数后可能正好越过商城验签上限：
// 选择付款方式必须整单拒绝，且失败不能留下金额档位占用。
func TestPaymentSelectionRejectsAlignmentOverflow(t *testing.T) {
	st, e := store.Open(":memory:")
	if e != nil {
		t.Fatal(e)
	}
	defer st.Close()
	cfg := config.Default()
	cfg.Server.Production = false
	cfg.Binance.APIKey = "key"
	cfg.Binance.APISecret = "secret"
	cfg.Checkout.Binance.UID = "receiver"
	mgr := settings.New(cfg, st)
	if e = mgr.Load(context.Background()); e != nil {
		t.Fatal(e)
	}
	engine, e := New(mgr, st, verifiedPoller(t, mgr, st))
	if e != nil {
		t.Fatal(e)
	}
	payload := map[string]any{"order_id": "shop-overflow", "amount": json.Number("999999.999999"), "notify_url": "https://shop.example/notify", "redirect_url": "https://shop.example/return", "fiat": "USD"}
	payload["signature"] = sign.BEpusdtSignGeneric(payload, mgr.Get().BEpusdt.APIToken)
	body, _ := json.Marshal(payload)
	r := httptest.NewRequest("POST", "/api/v1/order/create-order", bytes.NewReader(body))
	r.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	engine.ServeHTTP(w, r)
	var reply struct {
		Status int `json:"status_code"`
		Data   struct {
			ID string `json:"trade_id"`
		} `json:"data"`
	}
	if e = json.Unmarshal(w.Body.Bytes(), &reply); e != nil || reply.Status != 200 || reply.Data.ID == "" {
		t.Fatal(w.Body.String(), e)
	}
	no := reply.Data.ID
	form := url.Values{"trade_no": {no}, "provider": {"binance"}, "currency": {"USDT"}}
	w = httptest.NewRecorder()
	r = httptest.NewRequest("POST", "/api/order/payment", strings.NewReader(form.Encode()))
	r.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	engine.ServeHTTP(w, r)
	if w.Code != 400 || !strings.Contains(w.Body.String(), "可核销范围") {
		t.Fatal("alignment overflow accepted", w.Code, w.Body.String())
	}
	o, e := st.GetByTradeNo(context.Background(), no)
	if e != nil || o.Status != model.Pending || o.PayUnits != 0 || o.SelectedAt != nil {
		t.Fatal("rejected selection mutated the order", o, e)
	}
	var slots int64
	st.DB().Model(&model.Slot{}).Count(&slots)
	if slots != 0 {
		t.Fatalf("rejected selection reserved %d tiers", slots)
	}
}

func TestExpiredPendingOrderPhaseClosedAndURLHostValidation(t *testing.T) {
	ctx := context.Background()
	st, err := store.Open(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()

	cfg := config.Default()
	cfg.Server.Production = false
	cfg.Binance.APIKey = "key"
	cfg.Binance.APISecret = "secret"
	cfg.Checkout.Binance.UID = "receiver"
	mgr := settings.New(cfg, st)
	if err := mgr.Load(ctx); err != nil {
		t.Fatal(err)
	}
	engine, err := New(mgr, st, verifiedPoller(t, mgr, st))
	if err != nil {
		t.Fatal(err)
	}

	if _, err := st.Create(ctx, model.Order{
		TradeNo:     "expired-pending-phase",
		OutTradeNo:  "expired-pending-phase",
		OriginUnits: 5_000_000,
		ExpiresAt:   time.Now().Add(-time.Minute),
	}); err != nil {
		t.Fatal(err)
	}
	w := getRequest(engine, "/api/order/status?order_id=expired-pending-phase")
	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", w.Code, w.Body.String())
	}
	if !strings.Contains(w.Body.String(), `"payment_phase":"closed"`) || strings.Contains(w.Body.String(), `"payment_phase":"settling"`) {
		t.Fatalf("expired PENDING order reported wrong payment_phase: %s", w.Body.String())
	}

	// externalURL must reject backslashes in host authority as well as userinfo and non-HTTP schemes.
	for _, bad := range []string{
		"https://evil.com\\@shop.example/return",
		"https://user:pass@shop.example/return",
		"javascript:alert(1)",
		"https://shop.example\r\n/return",
	} {
		if got := externalURL(bad); got != "" {
			t.Fatalf("externalURL accepted unsafe URL %q -> %q", bad, got)
		}
	}
	if got := externalURL("https://shop.example/return?ok=1"); got != "https://shop.example/return?ok=1" {
		t.Fatalf("externalURL rejected valid URL: %q", got)
	}

	// Cashier page must check cancellation_review_status in onPaid so reviewed orders reload.
	if _, err := st.Create(ctx, model.Order{
		TradeNo:     "cashier-review-js",
		OutTradeNo:  "cashier-review-js",
		OriginUnits: 5_000_000,
		ExpiresAt:   time.Now().Add(time.Hour),
	}); err != nil {
		t.Fatal(err)
	}
	selectPayment(t, engine, "cashier-review-js", "binance")
	w = getRequest(engine, "/pay/cashier-review-js")
	if w.Code != http.StatusOK || !strings.Contains(w.Body.String(), "d.cancellation_review_status") {
		t.Fatalf("cashier missing cancellation_review_status guard: %d", w.Code)
	}
}

func TestOrderStatusAndCashierReturn503OnDatabaseFailure(t *testing.T) {
	ctx := context.Background()
	st, err := store.Open(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	cfg := config.Default()
	cfg.Server.Production = false
	cfg.Binance.APIKey = "key"
	cfg.Binance.APISecret = "secret"
	cfg.Checkout.Binance.UID = "receiver"
	mgr := settings.New(cfg, st)
	if err := mgr.Load(ctx); err != nil {
		t.Fatal(err)
	}
	engine, err := New(mgr, st, verifiedPoller(t, mgr, st))
	if err != nil {
		t.Fatal(err)
	}
	if err := st.Close(); err != nil {
		t.Fatal(err)
	}

	if w := getRequest(engine, "/api/order/status?order_id=any"); w.Code != http.StatusServiceUnavailable {
		t.Fatalf("expected 503 on DB error for OrderStatus, got %d: %s", w.Code, w.Body.String())
	}
	if w := getRequest(engine, "/pay/any"); w.Code != http.StatusServiceUnavailable {
		t.Fatalf("expected 503 on DB error for CashierPage, got %d: %s", w.Code, w.Body.String())
	}
}
