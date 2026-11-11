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

func TestCancellationRefundReviewRequiresAdminCSRFAndFreshSignedStatus(t *testing.T) {
	ctx := context.Background()
	st, err := store.Open(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	cfg := config.Default()
	cfg.Server.Production = false
	cfg.Admin.Password = "local-refund-review"
	mgr := settings.New(cfg, st)
	if err = mgr.Load(ctx); err != nil {
		t.Fatal(err)
	}
	engine, err := New(mgr, st, verifiedPoller(t, mgr, st))
	if err != nil {
		t.Fatal(err)
	}
	_, err = st.Create(ctx, model.Order{TradeNo: "refund-test", OutTradeNo: "merchant-refund", OriginUnits: 10000000, ExpiresAt: time.Now().Add(time.Hour)})
	if err != nil {
		t.Fatal(err)
	}
	o, err := st.Select(ctx, "refund-test", "binance", "USDT", "receiver", 2, 10000, 10000, 99, 120)
	if err != nil {
		t.Fatal(err)
	}
	if err = st.Cancel(ctx, o.TradeNo); err != nil {
		t.Fatal(err)
	}
	_, err = st.Process(ctx, model.Transaction{Provider: "binance", EventID: "canceled-funds", Receiver: "receiver", Currency: "USDT", Units: o.PayUnits, OccurredAt: time.Now(), State: model.Confirmed}, "")
	if err != nil {
		t.Fatal(err)
	}
	cookie, csrf, err := newAdminAuth(mgr.Get()).issue()
	if err != nil {
		t.Fatal(err)
	}
	post := func(cookie, csrf string) *httptest.ResponseRecorder {
		v := url.Values{"csrf": {csrf}, "provider": {"binance"}, "event_id": {"canceled-funds"}, "reference": {"external-refund-proof"}, "reason": {"verified original funds and actual refund"}}
		r := httptest.NewRequest("POST", "/admin/orders/refund-test/refund-record", strings.NewReader(v.Encode()))
		r.Header.Set("Content-Type", "application/x-www-form-urlencoded")
		if cookie != "" {
			r.AddCookie(&http.Cookie{Name: adminCookieName, Value: cookie})
		}
		w := httptest.NewRecorder()
		engine.ServeHTTP(w, r)
		return w
	}
	if w := post("", csrf); w.Code != 303 {
		t.Fatal(w.Code)
	}
	if w := post(cookie, ""); w.Code != 403 {
		t.Fatal(w.Code)
	}
	if reviewed, _ := st.CancellationRefundReviewed(ctx, o.TradeNo, o.OutTradeNo); reviewed {
		t.Fatal("unauthorized refund record")
	}
	if w := getRequest(engine, "/pay/refund-test"); w.Code != 200 || !strings.Contains(w.Body.String(), "商城已取消，到账核对中") {
		t.Fatal(w.Code, w.Body.String())
	}
	if w := post(cookie, csrf); w.Code != 303 || strings.Contains(w.Header().Get("Location"), "t=err") {
		t.Fatal(w.Code, w.Header().Get("Location"))
	}
	if w := getRequest(engine, "/pay/refund-test"); !strings.Contains(w.Body.String(), "退款凭证已登记") || strings.Contains(w.Body.String(), "external-refund-proof") {
		t.Fatal("public page lost status or leaked private reference", w.Body.String())
	}
	r := httptest.NewRequest("GET", "/admin/orders/refund-test", nil)
	r.AddCookie(&http.Cookie{Name: adminCookieName, Value: cookie})
	w := httptest.NewRecorder()
	engine.ServeHTTP(w, r)
	if w.Code != 200 || !strings.Contains(w.Body.String(), "实际到账 10 USDT") || !strings.Contains(w.Body.String(), "退款核对审计") {
		t.Fatal(w.Code, w.Body.String())
	}
	query := map[string]any{"trade_id": o.TradeNo, "order_id": o.OutTradeNo, "timestamp": fmt.Sprint(time.Now().Unix())}
	send := func(valid bool, stamp int64) *httptest.ResponseRecorder {
		query["timestamp"] = fmt.Sprint(stamp)
		delete(query, "signature")
		query["signature"] = sign.BEpusdtSignGeneric(query, mgr.Get().BEpusdt.APIToken)
		if !valid {
			query["signature"] = "fake"
		}
		raw, _ := json.Marshal(query)
		w := httptest.NewRecorder()
		engine.ServeHTTP(w, httptest.NewRequest("POST", "/api/v1/order/cancellation-review", bytes.NewReader(raw)))
		return w
	}
	if w := send(false, time.Now().Unix()); strings.Contains(w.Body.String(), `"refund_recorded":true`) {
		t.Fatal(w.Body.String())
	}
	if w := send(true, time.Now().Add(-2*time.Minute).Unix()); strings.Contains(w.Body.String(), `"refund_recorded":true`) {
		t.Fatal("stale review accepted")
	}
	if w := send(true, time.Now().Unix()); !strings.Contains(w.Body.String(), `"refund_recorded":true`) {
		t.Fatal(w.Body.String())
	}
	if err = st.BeginRecovery(ctx, "recovered snapshot"); err != nil {
		t.Fatal(err)
	}
	if w := send(true, time.Now().Unix()); !strings.Contains(w.Body.String(), `"refund_recorded":false`) {
		t.Fatal("old-generation refund review accepted", w.Body.String())
	}
}
