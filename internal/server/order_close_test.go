package server

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"litepay/internal/config"
	"litepay/internal/model"
	"litepay/internal/settings"
	"litepay/internal/store"
	"litepay/internal/worker"
)

func TestSettingsFormScopeAndSectionClear(t *testing.T) {
	parse := func(v url.Values) settings.Patch {
		t.Helper()
		c, _ := gin.CreateTestContext(httptest.NewRecorder())
		c.Request = httptest.NewRequest("POST", "/", strings.NewReader(v.Encode()))
		c.Request.Header.Set("Content-Type", "application/x-www-form-urlencoded")
		p, errs := parseSettingsForm(c)
		if len(errs) > 0 {
			t.Fatal(errs)
		}
		return p
	}
	p := parse(url.Values{"settings_scope": {"okx"}, "okx_uid": {"new"}, "okx_api_secret": {""}, "okx_currencies": {"USDC"}, "binance_uid": {"injected"}, "binance_currencies": {"USDT"}, "verify_accounts": {"on"}})
	if p.BinanceUID != nil || p.BinanceCurrencies != nil || p.OKXAPISecret != nil || p.OKXUID == nil || *p.OKXUID != "new" || p.VerifyProvider != "okx" || !p.VerifyAccounts {
		t.Fatalf("wrong scope: %+v", p)
	}
	cfg := config.Default()
	cfg.Server.Production = false
	cfg.Checkout.Binance.UID = "keep"
	cfg.Binance.APISecret = "keep-secret"
	cfg.OKX.APISecret = "clear"
	cfg.Checkout.OKX.UID = "clear"
	p = parse(url.Values{"settings_scope": {"okx"}, "clear_section": {"on"}, "verify_accounts": {"on"}, "okx_poll_interval": {"bad"}})
	p.Apply(cfg)
	cfg.NormalizePaymentCurrencies()
	if cfg.OKX.APISecret != "" || cfg.Checkout.OKX.UID != "" || len(cfg.ProviderCurrencies("okx")) != 0 || cfg.Checkout.Binance.UID != "keep" || cfg.Binance.APISecret != "keep-secret" || p.VerifyAccounts {
		t.Fatal("section clear leaked or retained fields")
	}
}

func TestOrderCloseAuthenticationSignaturesAndPaidProtection(t *testing.T) {
	ctx := context.Background()
	st, err := store.Open(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	cfg := config.Default()
	cfg.Server.Production = false
	cfg.Admin.Password = "local-test-password"
	mgr := settings.New(cfg, st)
	if err = mgr.Load(ctx); err != nil {
		t.Fatal(err)
	}
	engine, err := New(mgr, st, worker.NewPoller(mgr, st))
	if err != nil {
		t.Fatal(err)
	}
	create := func(no string) {
		t.Helper()
		_, err := st.Create(ctx, model.Order{TradeNo: no, OutTradeNo: "merchant-" + no, OriginUnits: 10_000_000, ExpiresAt: time.Now().Add(time.Minute)})
		if err != nil {
			t.Fatal(err)
		}
	}
	create("manual")
	create("signed")
	create("paid")
	token, csrf, err := newAdminAuth(mgr.Get()).issue()
	if err != nil {
		t.Fatal(err)
	}
	postAdmin := func(no, cookie, csrf string) *httptest.ResponseRecorder {
		v := url.Values{"csrf": {csrf}}
		req := httptest.NewRequest("POST", "/admin/orders/"+no+"/close", strings.NewReader(v.Encode()))
		req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
		if cookie != "" {
			req.AddCookie(&http.Cookie{Name: adminCookieName, Value: cookie})
		}
		w := httptest.NewRecorder()
		engine.ServeHTTP(w, req)
		return w
	}
	if w := postAdmin("manual", "", csrf); w.Code != 303 {
		t.Fatal(w.Code)
	}
	if w := postAdmin("manual", token, ""); w.Code != 403 {
		t.Fatal(w.Code)
	}
	o, _ := st.GetByTradeNo(ctx, "manual")
	if o.Status != model.Pending {
		t.Fatal("unauthenticated mutation")
	}
	if w := postAdmin("manual", token, csrf); w.Code != 303 {
		t.Fatal(w.Code, w.Body.String())
	}
	o, _ = st.GetByTradeNo(ctx, "manual")
	if o.Status != model.Canceled {
		t.Fatal(o.Status)
	}
	cancel := func(fields map[string]any, valid bool) int {
		t.Helper()
		fields["signature"] = dujiaoSign(fields, mgr.Get().BEpusdt.APIToken)
		if !valid {
			fields["signature"] = "invalid"
		}
		b, _ := json.Marshal(fields)
		w := httptest.NewRecorder()
		req := httptest.NewRequest("POST", "/api/v1/order/cancel-transaction", bytes.NewReader(b))
		req.Header.Set("Content-Type", "application/json")
		engine.ServeHTTP(w, req)
		var result bepusdtResponse
		if err := json.Unmarshal(w.Body.Bytes(), &result); err != nil {
			t.Fatal(err, w.Body.String())
		}
		return result.StatusCode
	}
	if cancel(map[string]any{"trade_id": "signed", "order_id": "merchant-manual"}, true) == 200 {
		t.Fatal("mismatched references accepted")
	}
	if cancel(map[string]any{"order_id": "merchant-signed"}, false) == 200 {
		t.Fatal("unsigned close accepted")
	}
	o, _ = st.GetByTradeNo(ctx, "signed")
	if o.Status != model.Pending {
		t.Fatal("invalid cancel mutated order")
	}
	for i := 0; i < 2; i++ {
		if cancel(map[string]any{"order_id": "merchant-signed"}, true) != 200 {
			t.Fatal("signed merchant reference cancellation failed")
		}
	}
	if err := st.DB().Model(&model.Order{}).Where("trade_no = ?", "paid").Update("status", model.Paid).Error; err != nil {
		t.Fatal(err)
	}
	postAdmin("paid", token, csrf)
	if cancel(map[string]any{"trade_id": "paid", "order_id": "merchant-paid"}, true) == 200 {
		t.Fatal("paid order closed")
	}
	o, _ = st.GetByTradeNo(ctx, "paid")
	if o.Status != model.Paid {
		t.Fatal(o.Status)
	}
}

func TestCashierUsesProviderCurrenciesAndMerchantLimits(t *testing.T) {
	cfg := config.Default()
	cfg.Server.Production = false
	cfg.Binance.APIKey = "key"
	cfg.Binance.APISecret = "secret"
	cfg.Checkout.Binance.UID = "receiver"
	cfg.OKX.APIKey = "key"
	cfg.OKX.APISecret = "secret"
	cfg.OKX.Passphrase = "phrase"
	cfg.Checkout.OKX.UID = "receiver"
	cfg.Checkout.BinanceCurrencies = []string{"USDC"}
	cfg.Checkout.OKXCurrencies = []string{"USDT"}
	cfg.NormalizePaymentCurrencies()
	o := &model.Order{CurrencyLimit: "USDT"}
	options := providerOptions(cfg, o)
	if options[0].Enabled || !options[1].Enabled || len(currencyOptionsFor(cfg, o, "binance")) != 0 {
		t.Fatal("merchant limit ignored")
	}
	o.CurrencyLimit = ""
	currencies := currencyOptionsFor(cfg, o, "binance")
	if len(currencies) != 1 || currencies[0].Currency != "USDC" {
		t.Fatal(currencies)
	}
}
