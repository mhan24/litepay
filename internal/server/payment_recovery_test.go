package server

import (
	"context"
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

func TestManualPaymentRecoveryAndHistoricalScanAuthorization(t *testing.T) {
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
	_, err = st.Create(ctx, model.Order{TradeNo: "late-recovery", OutTradeNo: "late-recovery", OriginUnits: 1000000, ExpiresAt: time.Now().Add(time.Minute)})
	if err != nil {
		t.Fatal(err)
	}
	o, err := st.Select(ctx, "late-recovery", "binance", "USDT", "receiver", 2, 10000, 10000, 99, 120)
	if err != nil {
		t.Fatal(err)
	}
	st.DB().Model(&model.PaymentAttempt{}).Where("order_id = ?", o.ID).Update("settle_until", time.Now().Add(-time.Second))
	event := model.Transaction{Provider: "binance", EventID: "verified-late", Reference: "reference-late", Currency: "USDT", Receiver: "receiver", Units: o.PayUnits, OccurredAt: time.Now(), State: model.Confirmed}
	if _, err = st.Process(ctx, event, ""); err != nil {
		t.Fatal(err)
	}
	st.DB().Model(o).Update("status", model.Expired)
	token, csrf, err := newAdminAuth(mgr.Get()).issue()
	if err != nil {
		t.Fatal(err)
	}
	post := func(action, cookie, csrf, eventID string) *httptest.ResponseRecorder {
		values := url.Values{"csrf": {csrf}, "provider": {"binance"}, "event_id": {eventID}, "reason": {"核对平台流水，商城确认尚未交付"}}
		req := httptest.NewRequest("POST", "/admin/orders/late-recovery/"+action, strings.NewReader(values.Encode()))
		req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
		if cookie != "" {
			req.AddCookie(&http.Cookie{Name: adminCookieName, Value: cookie})
		}
		w := httptest.NewRecorder()
		engine.ServeHTTP(w, req)
		return w
	}
	for _, action := range []string{"reconcile", "rescan"} {
		if w := post(action, "", csrf, event.EventID); w.Code != 303 {
			t.Fatal(w.Code)
		}
		if w := post(action, token, "bad", event.EventID); w.Code != 403 {
			t.Fatal(w.Code)
		}
	}
	if w := post("rescan", token, csrf, ""); w.Code != 303 || strings.Contains(w.Header().Get("Location"), "t=err") {
		t.Fatal(w.Code, w.Header())
	}
	if w := post("reconcile", token, csrf, "invented"); !strings.Contains(w.Header().Get("Location"), "t=err") {
		t.Fatal("invented evidence accepted")
	}
	if w := post("reconcile", token, csrf, event.EventID); w.Code != 303 || strings.Contains(w.Header().Get("Location"), "t=err") {
		t.Fatal(w.Code, w.Header())
	}
	if w := post("reconcile", token, csrf, event.EventID); !strings.Contains(w.Header().Get("Location"), "t=err") {
		t.Fatal("same event claimed twice")
	}
	var callbacks, audits int64
	st.DB().Model(&model.Callback{}).Count(&callbacks)
	st.DB().Model(&model.PaymentRecovery{}).Count(&audits)
	if callbacks != 1 || audits != 1 {
		t.Fatal(callbacks, audits)
	}
}
