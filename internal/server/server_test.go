package server

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/gin-gonic/gin"

	"litepay/internal/amount"
	"litepay/internal/config"
	"litepay/internal/model"
	"litepay/internal/settings"
	"litepay/internal/sign"
	"litepay/internal/store"
	"litepay/internal/worker"
)

func newServerTestEnv(t *testing.T, mutate func(*config.Config)) (*gin.Engine, *settings.Manager, *store.Store) {
	t.Helper()
	cfg := config.Default()
	cfg.Server.BaseURL = "https://pay.example.test"
	cfg.BEpusdt.APIToken = "0123456789abcdef0123456789abcdef"
	cfg.Admin.Password = "admin-test-password"
	cfg.Database.Path = filepath.Join(t.TempDir(), "litepay.db")
	cfg.Checkout.Binance.UID = "987654321"
	cfg.Checkout.Currencies = []string{"USDT", "USDC"}
	cfg.Binance.APIKey = "binance-read-key"
	cfg.Binance.APISecret = "binance-read-secret"
	cfg.OKX.APIKey = "okx-read-key"
	cfg.OKX.APISecret = "okx-read-secret"
	cfg.OKX.Passphrase = "okx-pass"
	cfg.Checkout.OKX.UID = "okx-receiver-uid"
	if mutate != nil {
		mutate(cfg)
	}
	if err := settings.Validate(cfg); err != nil {
		t.Fatalf("invalid test config: %v", err)
	}
	st, err := store.Open(cfg.Database.Path)
	if err != nil {
		t.Fatal(err)
	}
	mgr := settings.New(cfg, st)
	poller := worker.NewPoller(mgr, st)
	engine, err := New(mgr, st, poller)
	if err != nil {
		t.Fatal(err)
	}
	return engine, mgr, st
}

func signedCreateOrder(t *testing.T, token, orderID, amount, currencies string, reselect bool) []byte {
	t.Helper()
	payload := map[string]interface{}{
		"order_id":     orderID,
		"notify_url":   "https://merchant.example.test/notify",
		"redirect_url": "https://merchant.example.test/return",
		"amount":       mustFloat(t, amount),
		"fiat":         "USD",
		"currencies":   currencies,
		"reselect":     reselect,
	}
	payload["signature"] = sign.BEpusdtSignGeneric(payload, token)
	b, err := json.Marshal(payload)
	if err != nil {
		t.Fatal(err)
	}
	return b
}

func mustFloat(t *testing.T, raw string) float64 {
	t.Helper()
	var v float64
	if _, err := fmt.Sscanf(raw, "%f", &v); err != nil {
		t.Fatal(err)
	}
	return v
}

func postJSON(engine http.Handler, path string, body []byte) *httptest.ResponseRecorder {
	req := httptest.NewRequest(http.MethodPost, path, strings.NewReader(string(body)))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	engine.ServeHTTP(rec, req)
	return rec
}

func adminCookie(t *testing.T, cfg *config.Config) string {
	t.Helper()
	auth := newAdminAuth(cfg)
	token, _, err := auth.issue()
	if err != nil {
		t.Fatal(err)
	}
	return token
}

func requestAsAdmin(t *testing.T, engine http.Handler, cfg *config.Config, path string) *httptest.ResponseRecorder {
	t.Helper()
	token := adminCookie(t, cfg)
	req := httptest.NewRequest(http.MethodGet, path, nil)
	req.AddCookie(&http.Cookie{Name: adminCookieName, Value: token})
	rec := httptest.NewRecorder()
	engine.ServeHTTP(rec, req)
	return rec
}

func TestParseSettingsFormOnlyExposesUSDTAndUSDC(t *testing.T) {
	form := url.Values{}
	req := httptest.NewRequest(http.MethodPost, "/admin/settings", strings.NewReader(form.Encode()))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	c, _ := gin.CreateTestContext(httptest.NewRecorder())
	c.Request = req

	_, errs := parseSettingsForm(c)
	if len(errs) != 0 {
		t.Fatalf("unexpected validation errors: %v", errs)
	}
}

func TestParseSettingsFormKeepsExistingTextWhenFieldsAreBlank(t *testing.T) {
	form := url.Values{
		"binance_uid":            {""},
		"okx_uid":                {""},
		"checkout_note":          {""},
		"checkout_currencies":    {"USDT", "USDC"},
		"order_expire_minutes":   {"10"},
		"order_amount_step":      {"0.01"},
		"order_max_offset":       {"99"},
		"order_min_amount":       {"0.01"},
		"order_slot_grace":       {"120"},
		"order_amount_precision": {"2"},
		"binance_poll_interval":  {"4"},
		"binance_lookback":       {"2"},
		"okx_poll_interval":      {"4"},
		"okx_lookback":           {"2"},
		"notify_retry_delays":    {"5,15,30,60,180"},
		"notify_max_attempts":    {"5"},
		"notify_timeout":         {"10"},
	}
	req := httptest.NewRequest(http.MethodPost, "/admin/settings", strings.NewReader(form.Encode()))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	c, _ := gin.CreateTestContext(httptest.NewRecorder())
	c.Request = req

	patch, errs := parseSettingsForm(c)
	if len(errs) != 0 {
		t.Fatalf("unexpected validation errors: %v", errs)
	}
	for name, ptr := range map[string]*string{
		"binance_uid": patch.BinanceUID, "okx_uid": patch.OKXUID,
		"checkout_note": patch.CheckoutNote,
	} {
		if ptr != nil {
			t.Fatalf("blank %s unexpectedly produced a patch value", name)
		}
	}
}

func TestBEpusdtCreateOrderReturnsCashierAndDoesNotAllocateSlot(t *testing.T) {
	engine, mgr, st := newServerTestEnv(t, nil)
	rec := postJSON(engine, "/api/v1/order/create-order",
		signedCreateOrder(t, mgr.Get().BEpusdt.APIToken, "MERCHANT-1", "7.30", "USDT,USDC", true))
	if rec.Code != http.StatusOK {
		t.Fatalf("HTTP %d: %s", rec.Code, rec.Body.String())
	}
	var resp bepusdtResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatal(err)
	}
	if resp.StatusCode != 200 {
		t.Fatalf("status_code = %d, message = %s", resp.StatusCode, resp.Message)
	}
	data := resp.Data.(map[string]interface{})
	paymentURL, _ := data["payment_url"].(string)
	if !strings.HasPrefix(paymentURL, "https://pay.example.test/pay/LP") {
		t.Fatalf("payment_url = %q", paymentURL)
	}
	if got := slotCount(t, st); got != 0 {
		t.Fatalf("slots = %d, want 0 before selecting payment", got)
	}
	networks, ok := data["network"].([]interface{})
	if !ok || len(networks) != 4 {
		t.Fatalf("network = %#v, want four cashier payment methods", data["network"])
	}
	for _, raw := range networks {
		method, ok := raw.(map[string]interface{})
		if !ok || method["amount"] != "7.3" || method["actual_amount"] != "7.3" ||
			method["fiat"] != "USD" || method["exchange_rate"] != "1" ||
			method["token_net_name"] == "" || method["network"] == "" ||
			(method["currency"] != "USDT" && method["currency"] != "USDC") {
			t.Fatalf("incomplete payment method: %#v", raw)
		}
	}
}

func TestBEpusdtGlobalReselectSwitchOverridesMerchantRequest(t *testing.T) {
	engine, mgr, st := newServerTestEnv(t, func(cfg *config.Config) {
		cfg.BEpusdt.Reselect = false
	})
	rec := postJSON(engine, "/api/v1/order/create-order",
		signedCreateOrder(t, mgr.Get().BEpusdt.APIToken, "MERCHANT-NO-RESELECT", "2.50", "USDT,USDC", true))
	var resp bepusdtResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatal(err)
	}
	data := resp.Data.(map[string]interface{})
	if got := data["reselect"]; got != false {
		t.Fatalf("response reselect = %#v, want false from the global switch", got)
	}
	tradeID := data["trade_id"].(string)
	order, err := st.GetByTradeNo(context.Background(), tradeID)
	if err != nil || order == nil {
		t.Fatalf("order query failed: %v", err)
	}
	if order.Reselect {
		t.Fatal("merchant reselect=true bypassed the disabled global switch")
	}

	selectForm := func(provider string) *httptest.ResponseRecorder {
		form := url.Values{
			"trade_no": {tradeID},
			"provider": {provider},
			"currency": {"USDT"},
		}
		req := httptest.NewRequest(http.MethodPost, "/api/order/payment", strings.NewReader(form.Encode()))
		req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
		out := httptest.NewRecorder()
		engine.ServeHTTP(out, req)
		return out
	}
	if first := selectForm("binance"); first.Code != http.StatusOK {
		t.Fatalf("initial payment selection HTTP %d: %s", first.Code, first.Body.String())
	}
	if second := selectForm("okx"); second.Code != http.StatusConflict {
		t.Fatalf("reselect HTTP %d, want 409: %s", second.Code, second.Body.String())
	}
	page := httptest.NewRecorder()
	engine.ServeHTTP(page, httptest.NewRequest(http.MethodGet, "/pay/"+tradeID, nil))
	if strings.Contains(page.Body.String(), "更换收款平台或币种") {
		t.Fatal("cashier still offers reselect while the global switch is disabled")
	}
}

func TestBEpusdtMerchantCanDisableReselectWhenGlobalSwitchIsEnabled(t *testing.T) {
	engine, mgr, st := newServerTestEnv(t, func(cfg *config.Config) {
		cfg.BEpusdt.Reselect = true
	})
	rec := postJSON(engine, "/api/v1/order/create-order",
		signedCreateOrder(t, mgr.Get().BEpusdt.APIToken, "MERCHANT-OPT-OUT", "2.50", "USDT,USDC", false))
	var resp bepusdtResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatal(err)
	}
	data := resp.Data.(map[string]interface{})
	if got := data["reselect"]; got != false {
		t.Fatalf("response reselect = %#v, want false from the merchant request", got)
	}
	order, err := st.GetByTradeNo(context.Background(), data["trade_id"].(string))
	if err != nil || order == nil {
		t.Fatalf("order query failed: %v", err)
	}
	if order.Reselect {
		t.Fatal("merchant reselect=false was ignored")
	}
}

func TestBEpusdtCreateOrderAcceptsJSONNumberPrecision(t *testing.T) {
	engine, mgr, _ := newServerTestEnv(t, nil)
	payload := `{"order_id":"MERCHANT-DECIMAL","notify_url":"https://merchant.example.test/notify",` +
		`"redirect_url":"https://merchant.example.test/return","amount":2.5100,"fiat":"USD",` +
		`"currencies":"USDT,USDC","reselect":true}`
	var generic map[string]interface{}
	decoder := json.NewDecoder(strings.NewReader(payload))
	decoder.UseNumber()
	if err := decoder.Decode(&generic); err != nil {
		t.Fatal(err)
	}
	sig := sign.BEpusdtSignGeneric(generic, mgr.Get().BEpusdt.APIToken)
	body := strings.TrimSuffix(payload, "}") + `,"signature":"` + sig + `"}`
	rec := postJSON(engine, "/api/v1/order/create-order", []byte(body))
	var resp bepusdtResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatal(err)
	}
	if resp.StatusCode != 200 {
		t.Fatalf("status_code = %d, message = %s", resp.StatusCode, resp.Message)
	}
	data := resp.Data.(map[string]interface{})
	if data["amount"] != "2.5100" {
		t.Fatalf("amount = %#v, want exact input 2.5100", data["amount"])
	}
}

func TestBEpusdtCreateOrderRetryReusesSameTradeAndExpiry(t *testing.T) {
	engine, mgr, st := newServerTestEnv(t, nil)
	body := signedCreateOrder(t, mgr.Get().BEpusdt.APIToken, "MERCHANT-RETRY", "2.50", "USDT,USDC", true)
	var first, second bepusdtResponse
	for i, target := range []*bepusdtResponse{&first, &second} {
		if i == 1 {
			time.Sleep(1100 * time.Millisecond)
		}
		rec := postJSON(engine, "/api/v1/order/create-order", body)
		if rec.Code != http.StatusOK {
			t.Fatalf("request %d HTTP %d: %s", i+1, rec.Code, rec.Body.String())
		}
		if err := json.Unmarshal(rec.Body.Bytes(), &target); err != nil {
			t.Fatal(err)
		}
	}
	a := first.Data.(map[string]interface{})
	b := second.Data.(map[string]interface{})
	if a["trade_id"] != b["trade_id"] || a["payment_url"] != b["payment_url"] {
		t.Fatalf("retry created a new order: first=%#v second=%#v", a, b)
	}
	if a["expiration_time"] == b["expiration_time"] {
		t.Fatal("retry reset the order expiry")
	}
	var count int64
	if err := st.DB().Model(&model.Order{}).Where("out_trade_no = ?", "MERCHANT-RETRY").Count(&count).Error; err != nil {
		t.Fatal(err)
	}
	if count != 1 {
		t.Fatalf("orders = %d, want 1", count)
	}
}

func slotCount(t *testing.T, st *store.Store) int64 {
	t.Helper()
	var n int64
	if err := st.DB().Model(&model.AmountSlot{}).Count(&n).Error; err != nil {
		t.Fatal(err)
	}
	return n
}

func TestCashierRendersTwoStepSelectionAndBothCurrencies(t *testing.T) {
	engine, mgr, st := newServerTestEnv(t, nil)
	rec := postJSON(engine, "/api/v1/order/create-order",
		signedCreateOrder(t, mgr.Get().BEpusdt.APIToken, "MERCHANT-2", "7.30", "USDT,USDC", true))
	var resp bepusdtResponse
	_ = json.Unmarshal(rec.Body.Bytes(), &resp)
	paymentURL := resp.Data.(map[string]interface{})["payment_url"].(string)
	path := strings.TrimPrefix(paymentURL, "https://pay.example.test")

	page := httptest.NewRecorder()
	engine.ServeHTTP(page, httptest.NewRequest(http.MethodGet, path, nil))
	if page.Code != http.StatusOK || !strings.Contains(page.Body.String(), "选择收款平台") {
		t.Fatalf("provider page failed: HTTP %d", page.Code)
	}
	if !strings.Contains(page.Body.String(), "Binance") || !strings.Contains(page.Body.String(), "OKX") {
		t.Fatal("provider page must offer Binance and OKX")
	}
	for _, stale := range []string{"Binance Pay", "选择交易所", "选择支付方式"} {
		if strings.Contains(page.Body.String(), stale) {
			t.Fatalf("provider page contains stale copy %q", stale)
		}
	}
	if !strings.Contains(page.Body.String(), ".overlay{") ||
		!strings.Contains(page.Body.String(), "position:fixed;inset:0;z-index:50;display:none") {
		t.Fatal("payment overlay must be hidden until the order reaches a terminal state")
	}
	for _, icon := range []string{"/assets/brand/binance.svg", "/assets/brand/okx.svg"} {
		if !strings.Contains(page.Body.String(), icon) {
			t.Fatalf("provider page missing brand icon %s", icon)
		}
	}
	if !strings.Contains(page.Body.String(), "<title>选择收款平台 · 7.3 USD</title>") ||
		strings.Contains(page.Body.String(), "<title>选择收款平台 · 0.00") {
		t.Fatal("provider page must show the original fiat amount before a currency is selected")
	}

	second := httptest.NewRecorder()
	engine.ServeHTTP(second, httptest.NewRequest(http.MethodGet, path+"?provider=okx", nil))
	body := second.Body.String()
	if second.Code != http.StatusOK || !strings.Contains(body, "选择币种") {
		t.Fatalf("currency page failed: HTTP %d", second.Code)
	}
	for _, want := range []string{"USDT", "USDC", "站内转账"} {
		if !strings.Contains(body, want) {
			t.Fatalf("currency page missing %s", want)
		}
	}
	for _, icon := range []string{"/assets/brand/usdt.svg", "/assets/brand/usdc.svg"} {
		if !strings.Contains(body, icon) {
			t.Fatalf("currency page missing brand icon %s", icon)
		}
	}
	if got := slotCount(t, st); got != 0 {
		t.Fatalf("slots = %d before selection", got)
	}
}

func TestBrandAssetsAreEmbeddedAndServed(t *testing.T) {
	engine, _, _ := newServerTestEnv(t, nil)
	for _, name := range []string{"binance.svg", "okx.svg", "usdt.svg", "usdc.svg"} {
		req := httptest.NewRequest(http.MethodGet, "/assets/brand/"+name, nil)
		rec := httptest.NewRecorder()
		engine.ServeHTTP(rec, req)
		if rec.Code != http.StatusOK {
			t.Fatalf("%s HTTP %d", name, rec.Code)
		}
		if got := rec.Header().Get("Content-Type"); !strings.HasPrefix(got, "image/svg+xml") {
			t.Fatalf("%s Content-Type = %q", name, got)
		}
		if !strings.Contains(rec.Body.String(), "<svg") {
			t.Fatalf("%s is not SVG", name)
		}
	}

	rec := httptest.NewRecorder()
	engine.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/assets/brand/../server.go", nil))
	if rec.Code == http.StatusOK {
		t.Fatal("path traversal request must not succeed")
	}
}

func TestSelectPaymentLocksOKXReceiverUIDAndPrecision(t *testing.T) {
	engine, mgr, st := newServerTestEnv(t, func(cfg *config.Config) {
		cfg.Order.AmountPrecision = 4
		cfg.Checkout.OKX.UID = "okx-test-uid"
	})
	rec := postJSON(engine, "/api/v1/order/create-order",
		signedCreateOrder(t, mgr.Get().BEpusdt.APIToken, "MERCHANT-3", "2.50", "USDT,USDC", true))
	var resp bepusdtResponse
	_ = json.Unmarshal(rec.Body.Bytes(), &resp)
	tradeID := resp.Data.(map[string]interface{})["trade_id"].(string)

	form := url.Values{
		"trade_no": {tradeID},
		"provider": {"okx"},
		"currency": {"USDC"},
	}
	req := httptest.NewRequest(http.MethodPost, "/api/order/payment", strings.NewReader(form.Encode()))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	rec = httptest.NewRecorder()
	engine.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("unexpected HTTP %d: %s", rec.Code, rec.Body.String())
	}

	o, _ := st.GetByTradeNo(context.Background(), tradeID)
	if o == nil {
		t.Fatal("order disappeared")
	}
	if o.Currency != "USDC" || o.Collection != "okx" {
		t.Fatalf("wrong locked selection: %+v", o)
	}
	if o.PayAddress != "okx-test-uid" {
		t.Fatalf("pay address = %q", o.PayAddress)
	}
	if !amount.MatchRounded(amount.FormatDecimal(o.PayAmount, 4), o.PayAmount) {
		t.Fatalf("amount %.8f is not locked at precision 4", o.PayAmount)
	}
}

func TestCashierPollsCanceledState(t *testing.T) {
	engine, mgr, st := newServerTestEnv(t, nil)
	rec := postJSON(engine, "/api/v1/order/create-order",
		signedCreateOrder(t, mgr.Get().BEpusdt.APIToken, "MERCHANT-CANCELED-POLL", "2.50", "USDT,USDC", true))
	var resp bepusdtResponse
	_ = json.Unmarshal(rec.Body.Bytes(), &resp)
	tradeID := resp.Data.(map[string]interface{})["trade_id"].(string)

	form := url.Values{
		"trade_no": {tradeID},
		"provider": {"binance"},
		"currency": {"USDT"},
	}
	req := httptest.NewRequest(http.MethodPost, "/api/order/payment", strings.NewReader(form.Encode()))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	selectRec := httptest.NewRecorder()
	engine.ServeHTTP(selectRec, req)
	if selectRec.Code != http.StatusOK {
		t.Fatalf("selection HTTP %d: %s", selectRec.Code, selectRec.Body.String())
	}

	page := httptest.NewRecorder()
	engine.ServeHTTP(page, httptest.NewRequest(http.MethodGet, "/pay/"+tradeID, nil))
	if !strings.Contains(page.Body.String(), "onCanceled()") {
		t.Fatal("cashier polling does not handle CANCELED")
	}
	if err := st.CancelOrder(context.Background(), tradeID); err != nil {
		t.Fatal(err)
	}
	status := httptest.NewRecorder()
	engine.ServeHTTP(status, httptest.NewRequest(http.MethodGet, "/api/order/status?order_id="+tradeID, nil))
	if !strings.Contains(status.Body.String(), `"status":"CANCELED"`) {
		t.Fatalf("status response does not expose CANCELED: %s", status.Body.String())
	}
}

func TestExpiredOrderPageDoesNotClaimMerchantCanceled(t *testing.T) {
	engine, mgr, st := newServerTestEnv(t, nil)
	rec := postJSON(engine, "/api/v1/order/create-order",
		signedCreateOrder(t, mgr.Get().BEpusdt.APIToken, "MERCHANT-EXPIRED-PAGE", "2.50", "USDT,USDC", true))
	var resp bepusdtResponse
	_ = json.Unmarshal(rec.Body.Bytes(), &resp)
	tradeID := resp.Data.(map[string]interface{})["trade_id"].(string)

	form := url.Values{
		"trade_no": {tradeID},
		"provider": {"binance"},
		"currency": {"USDT"},
	}
	req := httptest.NewRequest(http.MethodPost, "/api/order/payment", strings.NewReader(form.Encode()))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	selectRec := httptest.NewRecorder()
	engine.ServeHTTP(selectRec, req)
	if selectRec.Code != http.StatusOK {
		t.Fatalf("selection HTTP %d: %s", selectRec.Code, selectRec.Body.String())
	}
	if err := st.ForceExpire(context.Background(), tradeID); err != nil {
		t.Fatal(err)
	}

	page := httptest.NewRecorder()
	engine.ServeHTTP(page, httptest.NewRequest(http.MethodGet, "/pay/"+tradeID, nil))
	if page.Code != http.StatusOK {
		t.Fatalf("expired result HTTP %d", page.Code)
	}
	if strings.Contains(page.Body.String(), "商户已取消该订单") {
		t.Fatal("a locally expired order must not be described as merchant-canceled")
	}
	if !strings.Contains(page.Body.String(), "订单已超时") {
		t.Fatal("expired order page is missing the timeout result")
	}
}

func TestCashierRejectsUnsafeReturnURL(t *testing.T) {
	engine, mgr, st := newServerTestEnv(t, nil)
	rec := postJSON(engine, "/api/v1/order/create-order",
		signedCreateOrder(t, mgr.Get().BEpusdt.APIToken, "MERCHANT-SAFE-RETURN", "2.50", "USDT,USDC", true))
	var resp bepusdtResponse
	_ = json.Unmarshal(rec.Body.Bytes(), &resp)
	tradeID := resp.Data.(map[string]interface{})["trade_id"].(string)

	if err := st.DB().Model(&model.Order{}).Where("trade_no = ?", tradeID).
		Update("return_url", "javascript:alert(1)").Error; err != nil {
		t.Fatal(err)
	}
	form := url.Values{
		"trade_no": {tradeID},
		"provider": {"binance"},
		"currency": {"USDT"},
	}
	req := httptest.NewRequest(http.MethodPost, "/api/order/payment", strings.NewReader(form.Encode()))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	selectRec := httptest.NewRecorder()
	engine.ServeHTTP(selectRec, req)
	if selectRec.Code != http.StatusOK {
		t.Fatalf("selection HTTP %d: %s", selectRec.Code, selectRec.Body.String())
	}

	page := httptest.NewRecorder()
	engine.ServeHTTP(page, httptest.NewRequest(http.MethodGet, "/pay/"+tradeID, nil))
	if page.Code != http.StatusOK {
		t.Fatalf("cashier HTTP %d", page.Code)
	}
	if strings.Contains(page.Body.String(), "javascript:alert(1)") {
		t.Fatal("unsafe return URL was rendered into the cashier")
	}

	status := httptest.NewRecorder()
	engine.ServeHTTP(status, httptest.NewRequest(http.MethodGet, "/api/order/status?order_id="+tradeID, nil))
	if strings.Contains(status.Body.String(), "javascript:alert(1)") {
		t.Fatal("unsafe return URL was exposed by the status API")
	}
}

func TestCreateOrderRejectsUnsafeMerchantURLs(t *testing.T) {
	engine, mgr, _ := newServerTestEnv(t, nil)
	for name, mutate := range map[string]func(map[string]interface{}){
		"notify_url": func(payload map[string]interface{}) {
			payload["notify_url"] = "javascript:alert(1)"
		},
		"redirect_url": func(payload map[string]interface{}) {
			payload["redirect_url"] = "data:text/html,unsafe"
		},
		"credentials": func(payload map[string]interface{}) {
			payload["redirect_url"] = "https://user:password@merchant.example.test/return"
		},
	} {
		t.Run(name, func(t *testing.T) {
			payload := map[string]interface{}{
				"order_id":     "MERCHANT-UNSAFE-" + name,
				"notify_url":   "https://merchant.example.test/notify",
				"redirect_url": "https://merchant.example.test/return",
				"amount":       2.5,
				"fiat":         "USD",
				"currencies":   "USDT,USDC",
				"reselect":     true,
			}
			mutate(payload)
			payload["signature"] = sign.BEpusdtSignGeneric(payload, mgr.Get().BEpusdt.APIToken)
			body, err := json.Marshal(payload)
			if err != nil {
				t.Fatal(err)
			}
			rec := postJSON(engine, "/api/v1/order/create-order", body)
			var resp bepusdtResponse
			if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
				t.Fatal(err)
			}
			if resp.StatusCode != 400 {
				t.Fatalf("status_code = %d, want 400", resp.StatusCode)
			}
		})
	}
}

func TestSelectPaymentUSDOrderUsesOneToOneRate(t *testing.T) {
	engine, mgr, st := newServerTestEnv(t, nil)
	payload := map[string]interface{}{
		"order_id":     "MERCHANT-USD",
		"notify_url":   "https://merchant.example.test/notify",
		"redirect_url": "https://merchant.example.test/return",
		"amount":       2.5,
		"fiat":         "USD",
		"currencies":   "USDT,USDC",
		"reselect":     true,
	}
	payload["signature"] = sign.BEpusdtSignGeneric(payload, mgr.Get().BEpusdt.APIToken)
	raw, err := json.Marshal(payload)
	if err != nil {
		t.Fatal(err)
	}
	rec := postJSON(engine, "/api/v1/order/create-order", raw)
	var resp bepusdtResponse
	_ = json.Unmarshal(rec.Body.Bytes(), &resp)
	tradeID := resp.Data.(map[string]interface{})["trade_id"].(string)

	form := url.Values{
		"trade_no": {tradeID},
		"provider": {"binance"},
		"currency": {"USDT"},
	}
	req := httptest.NewRequest(http.MethodPost, "/api/order/payment", strings.NewReader(form.Encode()))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	rec = httptest.NewRecorder()
	engine.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("selection HTTP %d: %s", rec.Code, rec.Body.String())
	}
	o, err := st.GetByTradeNo(context.Background(), tradeID)
	if err != nil || o == nil {
		t.Fatalf("order query failed: %v", err)
	}
	if o.PayAmount < 2.5 || o.PayAmount >= 3.5 {
		t.Fatalf("USD order amount = %v, want about 2.5", o.PayAmount)
	}
}

func TestSelectPaymentLocksBinanceReceiverUID(t *testing.T) {
	engine, mgr, st := newServerTestEnv(t, nil)
	rec := postJSON(engine, "/api/v1/order/create-order",
		signedCreateOrder(t, mgr.Get().BEpusdt.APIToken, "MERCHANT-BINANCE", "2.50", "USDT,USDC", true))
	var resp bepusdtResponse
	_ = json.Unmarshal(rec.Body.Bytes(), &resp)
	tradeID := resp.Data.(map[string]interface{})["trade_id"].(string)

	form := url.Values{
		"trade_no": {tradeID},
		"provider": {"binance"},
		"currency": {"USDT"},
	}
	req := httptest.NewRequest(http.MethodPost, "/api/order/payment", strings.NewReader(form.Encode()))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	rec = httptest.NewRecorder()
	engine.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("unexpected HTTP %d: %s", rec.Code, rec.Body.String())
	}
	o, _ := st.GetByTradeNo(context.Background(), tradeID)
	if o == nil || o.PayAddress != "987654321" {
		t.Fatalf("Binance pay address = %#v, want receiver UID", o)
	}

	page := httptest.NewRecorder()
	engine.ServeHTTP(page, httptest.NewRequest(http.MethodGet, "/pay/"+tradeID, nil))
	if page.Code != http.StatusOK {
		t.Fatalf("cashier HTTP %d", page.Code)
	}
	body := page.Body.String()
	if !strings.Contains(body, "收款 UID") || !strings.Contains(body, "987654321") {
		t.Fatal("Binance cashier must show the receiver UID")
	}
}

func TestCashierUsesProviderThemeAndFocusesAmountCurrencyUID(t *testing.T) {
	engine, mgr, _ := newServerTestEnv(t, nil)
	rec := postJSON(engine, "/api/v1/order/create-order",
		signedCreateOrder(t, mgr.Get().BEpusdt.APIToken, "MERCHANT-THEME", "2.50", "USDT,USDC", true))
	var resp bepusdtResponse
	_ = json.Unmarshal(rec.Body.Bytes(), &resp)
	tradeID := resp.Data.(map[string]interface{})["trade_id"].(string)

	for _, provider := range []string{"binance", "okx"} {
		form := url.Values{
			"trade_no": {tradeID},
			"provider": {provider},
			"currency": {"USDT"},
		}
		req := httptest.NewRequest(http.MethodPost, "/api/order/payment", strings.NewReader(form.Encode()))
		req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
		selectRec := httptest.NewRecorder()
		engine.ServeHTTP(selectRec, req)
		if selectRec.Code != http.StatusOK {
			t.Fatalf("%s selection HTTP %d: %s", provider, selectRec.Code, selectRec.Body.String())
		}

		page := httptest.NewRecorder()
		engine.ServeHTTP(page, httptest.NewRequest(http.MethodGet, "/pay/"+tradeID, nil))
		body := page.Body.String()
		if page.Code != http.StatusOK {
			t.Fatalf("%s cashier HTTP %d", provider, page.Code)
		}
		if !strings.Contains(body, `<body class="brand-`+provider+`">`) {
			t.Fatalf("%s cashier does not apply its provider theme", provider)
		}
		if strings.Contains(body, "/api/qrcode") || strings.Contains(body, "二维码") {
			t.Fatalf("%s cashier still exposes a QR code", provider)
		}
		for _, want := range []string{`id="amount"`, "收款币种", "收款 UID", "站内转账"} {
			if !strings.Contains(body, want) {
				t.Fatalf("%s cashier missing focus field %q", provider, want)
			}
		}
		if !strings.Contains(body, "data-theme-toggle") || !strings.Contains(body, "litepay-theme") {
			t.Fatalf("%s cashier is missing light/dark theme switching", provider)
		}
		if strings.Contains(body, "打开 Binance App 付款") || strings.Contains(body, `name="payee_link"`) {
			t.Fatalf("%s cashier still contains the removed Binance app-link flow", provider)
		}
	}
}

func TestAdminSettingsOnlyExposesUIDAndManagedToken(t *testing.T) {
	engine, mgr, _ := newServerTestEnv(t, nil)
	rec := requestAsAdmin(t, engine, mgr.Get(), "/admin/settings")
	body := rec.Body.String()
	if rec.Code != http.StatusOK {
		t.Fatalf("settings HTTP %d", rec.Code)
	}
	for _, want := range []string{`name="binance_uid"`, `name="okx_uid"`, "/admin/settings/token"} {
		if !strings.Contains(body, want) {
			t.Fatalf("settings page missing %q", want)
		}
	}
	for _, gone := range []string{`name="bepusdt_api_token"`, "二维码", "/api/qrcode"} {
		if strings.Contains(body, gone) {
			t.Fatalf("settings page still exposes %q", gone)
		}
	}

	form := url.Values{
		"binance_uid":            {mgr.Get().Checkout.Binance.UID},
		"okx_uid":                {mgr.Get().Checkout.OKX.UID},
		"checkout_currencies":    {"USDT", "USDC"},
		"order_expire_minutes":   {"10"},
		"order_amount_step":      {"0.01"},
		"order_max_offset":       {"99"},
		"order_min_amount":       {"0.01"},
		"order_slot_grace":       {"120"},
		"order_amount_precision": {"2"},
		"binance_poll_interval":  {"4"},
		"binance_lookback":       {"2"},
		"okx_poll_interval":      {"4"},
		"okx_lookback":           {"2"},
		"notify_retry_delays":    {"5,15,30,60,180"},
		"notify_max_attempts":    {"5"},
		"notify_timeout":         {"10"},
		"bepusdt_reselect":       {"on"},
	}
	token := adminCookie(t, mgr.Get())
	// 使用实际会话中的 CSRF token 提交设置。
	csrf := ""
	{
		parsed, ok := newAdminAuth(mgr.Get()).parse(token)
		if !ok {
			t.Fatal("failed to parse admin session")
		}
		csrf = parsed.CSRF
	}
	form.Set("csrf", csrf)
	req := httptest.NewRequest(http.MethodPost, "/admin/settings", strings.NewReader(form.Encode()))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	req.AddCookie(&http.Cookie{Name: adminCookieName, Value: token})
	save := httptest.NewRecorder()
	engine.ServeHTTP(save, req)
	if save.Code != http.StatusSeeOther {
		t.Fatalf("save HTTP %d: %s", save.Code, save.Body.String())
	}
	if got := mgr.Get().Checkout.Binance.UID; got == "" {
		t.Fatal("Binance UID unexpectedly cleared")
	}
}

func TestAdminTokenRotationReplacesTokenAndRevealsOnlyNewValue(t *testing.T) {
	engine, mgr, _ := newServerTestEnv(t, nil)
	oldToken := mgr.Get().BEpusdt.APIToken
	token := adminCookie(t, mgr.Get())
	parsed, ok := newAdminAuth(mgr.Get()).parse(token)
	if !ok {
		t.Fatal("failed to parse admin session")
	}
	form := url.Values{"csrf": {parsed.CSRF}}
	req := httptest.NewRequest(http.MethodPost, "/admin/settings/token",
		strings.NewReader(form.Encode()))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	req.AddCookie(&http.Cookie{Name: adminCookieName, Value: token})
	rec := httptest.NewRecorder()
	engine.ServeHTTP(rec, req)
	if rec.Code != http.StatusSeeOther {
		t.Fatalf("rotate HTTP %d: %s", rec.Code, rec.Body.String())
	}
	newToken := mgr.Get().BEpusdt.APIToken
	if newToken == oldToken || len(newToken) != 48 {
		t.Fatalf("new token length = %d, unchanged = %v", len(newToken), newToken == oldToken)
	}
	if strings.Contains(rec.Header().Get("Location"), newToken) || !strings.Contains(rec.Header().Get("Location"), "token_rotated=1") {
		t.Fatal("rotation redirect must not place the secret in the URL")
	}
	stored, err := mgr.Store().LoadCredential(context.Background(), "bepusdt_api_token")
	if err != nil || stored == nil || stored.Value != newToken {
		t.Fatal("rotated token was not persisted")
	}
}

func TestQRCodeRouteIsRemoved(t *testing.T) {
	engine, _, _ := newServerTestEnv(t, nil)
	rec := httptest.NewRecorder()
	engine.ServeHTTP(rec, httptest.NewRequest(http.MethodGet,
		"/api/qrcode?text=anything", nil))
	if rec.Code != http.StatusNotFound {
		t.Fatalf("QR route HTTP %d, want 404", rec.Code)
	}
}

func TestCancelTransactionStopsOrderAndReturnsBEpusdtSuccess(t *testing.T) {
	engine, mgr, st := newServerTestEnv(t, nil)
	rec := postJSON(engine, "/api/v1/order/create-order",
		signedCreateOrder(t, mgr.Get().BEpusdt.APIToken, "MERCHANT-4", "7.30", "USDT,USDC", true))
	var resp bepusdtResponse
	_ = json.Unmarshal(rec.Body.Bytes(), &resp)
	tradeID := resp.Data.(map[string]interface{})["trade_id"].(string)
	payload := map[string]interface{}{"trade_id": tradeID}
	payload["signature"] = sign.BEpusdtSignGeneric(payload, mgr.Get().BEpusdt.APIToken)
	body, _ := json.Marshal(payload)
	cancelResp := postJSON(engine, "/api/v1/order/cancel-transaction", body)
	if cancelResp.Code != http.StatusOK {
		t.Fatalf("cancel HTTP %d", cancelResp.Code)
	}
	var cancelData bepusdtResponse
	_ = json.Unmarshal(cancelResp.Body.Bytes(), &cancelData)
	if cancelData.StatusCode != 200 {
		t.Fatalf("cancel status_code = %d, message = %s", cancelData.StatusCode, cancelData.Message)
	}
	o, _ := st.GetByTradeNo(context.Background(), tradeID)
	if o.Status != model.StatusCanceled {
		t.Fatalf("status = %s, want CANCELED", o.Status)
	}
	if got := orderStatusNumber(o); got != 4 {
		t.Fatalf("canceled response status = %d, want 4", got)
	}
}

func TestAllAdminTemplatesRender(t *testing.T) {
	engine, mgr, st := newServerTestEnv(t, nil)
	now := time.Now()
	o, err := st.CreateOrder(context.Background(), store.CreateOrderInput{
		Protocol: "bepusdt", Pid: "bepusdt", TradeNo: "LP-ADMIN", OutTradeNo: "MERCHANT-ADMIN",
		OriginMoney: "7.30", OriginCurrency: "USD", NotifyURL: "https://merchant.example/notify",
		ReturnURL: "https://merchant.example/return", ExpiredAt: now.Add(10 * time.Minute), Reselect: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := st.SelectPayment(context.Background(), o.TradeNo, "binance", "USDT",
		"123456789", 1, 0.01, 99, 0.01, 4, 2*time.Minute); err != nil {
		t.Fatal(err)
	}
	cfg := mgr.Get()
	staleCopy := []string{"Binance Pay", "选择交易所", "待选支付方式"}
	for _, path := range []string{
		"/admin", "/admin/orders", "/admin/orders/" + o.TradeNo,
		"/admin/settings", "/admin/integration",
	} {
		rec := requestAsAdmin(t, engine, cfg, path)
		if rec.Code != http.StatusOK {
			t.Fatalf("%s HTTP %d: %s", path, rec.Code, rec.Body.String())
		}
		for _, stale := range staleCopy {
			if strings.Contains(rec.Body.String(), stale) {
				t.Fatalf("%s contains stale copy %q", path, stale)
			}
		}
	}
}

func TestNotifierSigningTracksSentJSONPayload(t *testing.T) {
	token := "0123456789abcdef0123456789abcdef"
	o := &model.Order{
		TradeNo: "LP-SIGN", OutTradeNo: "MERCHANT-SIGN", OriginMoney: "2.50",
		PayAmount: 2.51, Currency: "USDT", PayAddress: "TPay",
	}
	params := worker.BuildNotifyParams(o)
	jsonPayload := map[string]interface{}{
		"trade_id":             params["trade_id"],
		"order_id":             params["order_id"],
		"amount":               mustFloat(t, params["amount"]),
		"actual_amount":        params["actual_amount"],
		"token":                params["token"],
		"block_transaction_id": params["block_transaction_id"],
		"status":               2,
	}
	jsonPayload["signature"] = sign.BEpusdtSignGeneric(jsonPayload, token)
	if !sign.BEpusdtVerifyGeneric(jsonPayload, token) {
		t.Fatal("signature does not verify against the exact JSON payload")
	}
}
