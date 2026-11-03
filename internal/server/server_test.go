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
