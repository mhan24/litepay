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

func TestPaidCancellationStatusTracksEachRefundAndRecoveryGeneration(t *testing.T) {
	ctx := context.Background()
	st, err := store.Open(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	cfg := config.Default()
	cfg.Server.Production = false
	cfg.Admin.Password = "local-refund-status"
	mgr := settings.New(cfg, st)
	if err = mgr.Load(ctx); err != nil {
		t.Fatal(err)
	}
	engine, err := New(mgr, st, verifiedPoller(t, mgr, st))
	if err != nil {
		t.Fatal(err)
	}
	if _, err = st.Create(ctx, model.Order{TradeNo: "paid-refunds", OutTradeNo: "merchant-paid-refunds", OriginUnits: 10000000, ExpiresAt: time.Now().Add(time.Hour)}); err != nil {
		t.Fatal(err)
	}
	o, err := st.Select(ctx, "paid-refunds", "binance", "USDT", "receiver", 2, 10000, 10000, 99, 120)
	if err != nil {
		t.Fatal(err)
	}
	for _, id := range []string{"payment-A", "payment-B"} {
		if _, err = st.Process(ctx, model.Transaction{Provider: "binance", EventID: id, Receiver: "receiver", Currency: "USDT", Units: o.PayUnits, OccurredAt: time.Now(), State: model.Confirmed}, ""); err != nil {
			t.Fatal(err)
		}
	}
	assertStatus := func(want string, current bool) {
		t.Helper()
		w := getRequest(engine, "/api/order/status?order_id="+o.TradeNo)
		var response struct {
			Data struct {
				Status    string `json:"status"`
				Phase     string `json:"payment_phase"`
				Review    string `json:"cancellation_review_status"`
				Current   bool   `json:"refund_review_generation_current"`
				Accepting bool   `json:"accepting_payment"`
			}
		}
		if err := json.Unmarshal(w.Body.Bytes(), &response); err != nil || w.Code != 200 {
			t.Fatal(w.Code, w.Body.String(), err)
		}
		if response.Data.Status != model.Paid || response.Data.Phase != "paid" || response.Data.Review != want || response.Data.Current != current || response.Data.Accepting {
			t.Fatal(response, w.Body.String())
		}
		if strings.Contains(w.Body.String(), "private-refund") || strings.Contains(w.Body.String(), "review_generation\"") {
			t.Fatal("public status leaked private data", w.Body.String())
		}
		query := map[string]any{"trade_id": o.TradeNo, "order_id": o.OutTradeNo, "timestamp": fmt.Sprint(time.Now().Unix())}
		query["signature"] = sign.BEpusdtSignGeneric(query, mgr.Get().BEpusdt.APIToken)
		raw, _ := json.Marshal(query)
		signed := httptest.NewRecorder()
		engine.ServeHTTP(signed, httptest.NewRequest("POST", "/api/v1/order/cancellation-review", bytes.NewReader(raw)))
		if !strings.Contains(signed.Body.String(), fmt.Sprintf(`"refund_recorded":%t`, current)) {
			t.Fatal("signed and public status disagree", signed.Body.String())
		}
	}
	assertStatus("none", false)
	if _, err = st.CancelReference(ctx, o.TradeNo, o.OutTradeNo); err == nil {
		t.Fatal("paid order canceled")
	}
	assertStatus("pending", false)
	refund := func(id string) {
		t.Helper()
		if err := st.RecordCancellationRefund(ctx, o.TradeNo, "binance", id, "operator", "private-refund-"+id, "actual external refund verified"); err != nil {
			t.Fatal(err)
		}
	}
	refund("payment-A")
	assertStatus("pending", false)
	if w := getRequest(engine, "/pay/"+o.TradeNo); strings.Contains(w.Body.String(), "<h1>退款凭证已登记</h1>") || !strings.Contains(w.Body.String(), "商城已取消，到账核对中") {
		t.Fatal("partial refund shown complete", w.Body.String())
	}
	refund("payment-B")
	assertStatus("refund_recorded", true)
	if w := getRequest(engine, "/pay/"+o.TradeNo); !strings.Contains(w.Body.String(), "<h1>退款凭证已登记</h1>") || strings.Contains(w.Body.String(), "private-refund") {
		t.Fatal(w.Body.String())
	}
	if err = st.BeginRecovery(ctx, "restored payment database"); err != nil {
		t.Fatal(err)
	}
	assertStatus("recheck_required", false)
	if w := getRequest(engine, "/pay/"+o.TradeNo); !strings.Contains(w.Body.String(), "退款记录待重新核对") || strings.Contains(w.Body.String(), "<h1>退款凭证已登记</h1>") {
		t.Fatal("result page trusted stale generation", w.Body.String())
	}
	cookie, _, err := newAdminAuth(mgr.Get()).issue()
	if err != nil {
		t.Fatal(err)
	}
	r := httptest.NewRequest("GET", "/admin/orders/"+o.TradeNo, nil)
	r.AddCookie(&http.Cookie{Name: adminCookieName, Value: cookie})
	w := httptest.NewRecorder()
	engine.ServeHTTP(w, r)
	if w.Code != 200 || !strings.Contains(w.Body.String(), "历史退款凭证，待重新核对") || !strings.Contains(w.Body.String(), "payment-B") {
		t.Fatal("admin disagrees with result status", w.Code, w.Body.String())
	}
	refund("payment-A")
	assertStatus("recheck_required", false)
	refund("payment-B")
	assertStatus("refund_recorded", true)
}
