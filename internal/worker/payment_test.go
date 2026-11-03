package worker

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"litepay/internal/config"
	"litepay/internal/httpx"
	"litepay/internal/model"
	"litepay/internal/settings"
	"litepay/internal/sign"
	"litepay/internal/store"
)

type fixture struct {
	rows []model.Transaction
	err  error
}

func (f *fixture) AccountUID(context.Context) (string, error) { return "receiver", nil }
func (f *fixture) History(context.Context, time.Time, time.Time) ([]model.Transaction, error) {
	return f.rows, f.err
}
func (f *fixture) Lookup(context.Context, string, time.Time, time.Time) ([]model.Transaction, error) {
	return f.rows, f.err
}
func TestScanInquiryAndSignedCallback(t *testing.T) {
	ctx := context.Background()
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
	if e = mgr.Load(ctx); e != nil {
		t.Fatal(e)
	}
	received, fulfilled := 0, 0
	seen := map[string]bool{}
	merchant := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var payload map[string]any
		d := json.NewDecoder(r.Body)
		d.UseNumber()
		if d.Decode(&payload) != nil || !sign.BEpusdtVerifyGeneric(payload, mgr.Get().BEpusdt.APIToken) {
			t.Error("invalid merchant signature")
			w.WriteHeader(400)
			return
		}
		if payload["block_transaction_id"] != "parent-event" || payload["status"] != json.Number("2") || payload["actual_amount"] != json.Number("10") {
			t.Error("incorrect callback contract", payload)
		}
		key := payload["order_id"].(string)
		if !seen[key] {
			seen[key] = true
			fulfilled++
		}
		received++
		if received == 1 {
			w.Write([]byte("not yet"))
		} else {
			w.Write([]byte("success"))
		}
	}))
	defer merchant.Close()
	for _, no := range []string{"a", "b"} {
		if _, e = st.Create(ctx, model.Order{TradeNo: no, OutTradeNo: no, OriginUnits: 10000000, NotifyURL: merchant.URL, ExpiresAt: time.Now().Add(time.Minute)}); e != nil {
			t.Fatal(e)
		}
		if _, e = st.Select(ctx, no, "binance", "USDT", "receiver", 2, 10000, 10000, 99, 120); e != nil {
			t.Fatal(e)
		}
	}
	selected := time.Now().Add(-2 * time.Minute)
	st.DB().Model(&model.Order{}).Where("trade_no IN ?", []string{"a", "b"}).Update("selected_at", selected)
	f := &fixture{rows: []model.Transaction{{Provider: "binance", EventID: "parent-event", Reference: "app-order", Receiver: "receiver", Currency: "USDT", Units: 10000000, OccurredAt: time.Now(), State: model.Confirmed}}}
	p := NewPoller(mgr, st)
	p.clients["binance"] = &connection{config: *mgr.Get(), source: f}
	msg, e := p.Inquiry(ctx, "b", "app-order")
	if e != nil || !strings.Contains(msg, "不属于") {
		t.Fatal(msg, e)
	}
	// Failed/partial exchange reads never advance the cursor or pay an order.
	f.err = errors.New("incomplete history")
	if e = p.Scan(ctx, "binance"); e == nil {
		t.Fatal("failed query accepted")
	}
	cursor, _ := st.Cursor(ctx, "binance")
	if !cursor.ScannedUntil.IsZero() {
		t.Fatal("failed scan advanced cursor")
	}
	f.err = nil
	if e = p.Scan(ctx, "binance"); e != nil {
		t.Fatal(e)
	}
	a, _ := st.GetByTradeNo(ctx, "a")
	b, _ := st.GetByTradeNo(ctx, "b")
	if a.Status != model.Paid || b.Status != model.Ready {
		t.Fatal("wrong ownership", a.Status, b.Status)
	}
	if e = p.Scan(ctx, "binance"); e != nil {
		t.Fatal(e)
	}
	n := NewNotifier(mgr, st)
	n.http = httpx.NewClient(time.Second, true)
	if _, e = n.Deliver(ctx); e != nil {
		t.Fatal(e)
	}
	var cb model.Callback
	st.DB().First(&cb, a.ID)
	if cb.DeliveredAt != nil {
		t.Fatal("HTTP 200 mistaken for merchant acknowledgement")
	}
	st.DB().Model(&model.Callback{}).Where("order_id = ?", a.ID).Update("next_at", time.Now().Add(-time.Second))
	if _, e = n.Deliver(ctx); e != nil {
		t.Fatal(e)
	}
	st.DB().First(&cb, a.ID)
	if cb.DeliveredAt == nil || fulfilled != 1 || received != 2 {
		t.Fatal("callback retry/ACK failure", cb, fulfilled, received)
	}
	if did, e := n.Deliver(ctx); e != nil || did {
		t.Fatal("delivered callback sent again", e)
	}
}
