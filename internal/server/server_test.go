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

	"litepay/internal/config"
	"litepay/internal/settings"
	"litepay/internal/sign"
	"litepay/internal/store"
	"litepay/internal/worker"
)

func TestSignedCashierAndAdminPages(t *testing.T) {
	st, e := store.Open(":memory:")
	if e != nil {
		t.Fatal(e)
	}
	defer st.Close()
	cfg := config.Default()
	cfg.Admin.Password = "test-password"
	cfg.Server.BaseURL = "https://pay.example"
	cfg.Binance.APIKey = "key"
	cfg.Binance.APISecret = "secret"
	cfg.Checkout.Binance.UID = "receiver"
	mgr := settings.New(cfg, st)
	if e = mgr.Load(context.Background()); e != nil {
		t.Fatal(e)
	}
	engine, e := New(mgr, st, worker.NewPoller(mgr, st))
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
