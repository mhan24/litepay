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
	"litepay/internal/store"
	"litepay/internal/worker"
)

func TestMerchantLongTimeoutsAndAdminNotificationRecovery(t *testing.T) {
	ctx := context.Background()
	st, err := store.Open(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	cfg := config.Default()
	cfg.Admin.Password = "local-test-password"
	cfg.Server.BaseURL = "https://pay.example"
	cfg.Server.NotifyHosts = []string{"shop.example"}
	cfg.Binance.APIKey = "key"
	cfg.Binance.APISecret = "secret"
	cfg.Checkout.Binance.UID = "receiver"
	mgr := settings.New(cfg, st)
	if err = mgr.Load(ctx); err != nil {
		t.Fatal(err)
	}
	engine, err := New(mgr, st, worker.NewPoller(mgr, st))
	if err != nil {
		t.Fatal(err)
	}
	var first string
	for _, hours := range []int{1, 6, 24} {
		body := map[string]any{"order_id": fmt.Sprintf("timeout-%d", hours), "amount": 1, "timeout": hours * 3600, "notify_url": "https://shop.example/notify", "redirect_url": "https://shop.example/return"}
		body["signature"] = dujiaoSign(body, mgr.Get().BEpusdt.APIToken)
		raw, _ := json.Marshal(body)
		w := httptest.NewRecorder()
		req := httptest.NewRequest("POST", "/api/v1/order/create-order", bytes.NewReader(raw))
		engine.ServeHTTP(w, req)
		var response struct {
			Status int `json:"status_code"`
			Data   struct {
				TradeID string `json:"trade_id"`
			} `json:"data"`
		}
		if err = json.Unmarshal(w.Body.Bytes(), &response); err != nil || response.Status != 200 {
			t.Fatal(w.Body.String(), err)
		}
		o, err := st.GetByTradeNo(ctx, response.Data.TradeID)
		if err != nil || time.Until(o.ExpiresAt) < time.Duration(hours)*time.Hour-time.Second {
			t.Fatal(o, err)
		}
		if first == "" {
			first = o.TradeNo
		}
	}
	o, err := st.Select(ctx, first, "binance", "USDT", "receiver", 2, 10000, 10000, 99, 120)
	if err != nil {
		t.Fatal(err)
	}
	tx := model.Transaction{Provider: "binance", EventID: "paid-retry", Reference: "reference", Currency: "USDT", Receiver: "receiver", Units: o.PayUnits, OccurredAt: time.Now(), State: model.Confirmed}
	if _, err = st.Process(ctx, tx, ""); err != nil {
		t.Fatal(err)
	}
	task, _, err := st.ReserveCallback(ctx, 1, 1)
	if err != nil {
		t.Fatal(err)
	}
	if err = st.FinishCallback(ctx, *task, 503, false, "temporary", 0); err != nil {
		t.Fatal(err)
	}
	token, csrf, err := newAdminAuth(mgr.Get()).issue()
	if err != nil {
		t.Fatal(err)
	}
	post := func(cookie, csrf, reason string) *httptest.ResponseRecorder {
		req := httptest.NewRequest("POST", "/admin/orders/"+first+"/retry-notify", strings.NewReader(url.Values{"csrf": {csrf}, "reason": {reason}}.Encode()))
		req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
		if cookie != "" {
			req.AddCookie(&http.Cookie{Name: adminCookieName, Value: cookie})
		}
		w := httptest.NewRecorder()
		engine.ServeHTTP(w, req)
		return w
	}
	if w := post("", csrf, "checked"); w.Code != 303 {
		t.Fatal(w.Code)
	}
	if w := post(token, "", "checked"); w.Code != 403 {
		t.Fatal(w.Code)
	}
	if w := post(token, csrf, ""); !strings.Contains(w.Header().Get("Location"), "t=err") {
		t.Fatal("blank reason accepted")
	}
	if w := post(token, csrf, "商城未交付，已恢复服务"); w.Code != 303 || strings.Contains(w.Header().Get("Location"), "t=err") {
		t.Fatal(w.Code, w.Header())
	}
	for _, path := range []string{"/admin/orders/" + first, "/admin"} {
		w := httptest.NewRecorder()
		req := httptest.NewRequest("GET", path, nil)
		req.AddCookie(&http.Cookie{Name: adminCookieName, Value: token})
		engine.ServeHTTP(w, req)
		if w.Code != 200 {
			t.Fatal(w.Code, w.Body.String())
		}
	}
	var audits int64
	st.DB().Model(&model.CallbackRecovery{}).Count(&audits)
	if audits != 1 {
		t.Fatal(audits)
	}
}
