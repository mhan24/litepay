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
	rows  []model.Transaction
	err   error
	calls int
	from  time.Time
	to    time.Time
}

func (f *fixture) AccountUID(context.Context) (string, error) { return "receiver", nil }
func (f *fixture) History(_ context.Context, start, end time.Time) ([]model.Transaction, error) {
	f.calls++
	f.from, f.to = start, end
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

// 空闲期不请求交易所：既没有等待付款的订单，也没有待确认到账时，历史查询不可能改变任何
// 订单状态，扫描游标保持原位，等下一次真正需要补查时从该位置继续。
func TestScanSkipsIdleExchangeQueries(t *testing.T) {
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
	f := &fixture{}
	p := NewPoller(mgr, st)
	p.clients["binance"] = &connection{config: *mgr.Get(), source: f}

	// 首次查询同时核验只读凭据，即使还没有订单。
	if e = p.Scan(ctx, "binance"); e != nil {
		t.Fatal(e)
	}
	if f.calls != 1 {
		t.Fatal("missing verification scan", f.calls)
	}
	before, e := st.Cursor(ctx, "binance")
	if e != nil {
		t.Fatal(e)
	}
	if e = p.Scan(ctx, "binance"); e != nil {
		t.Fatal(e)
	}
	after, e := st.Cursor(ctx, "binance")
	if e != nil {
		t.Fatal(e)
	}
	if f.calls != 1 {
		t.Fatal("idle scan queried the exchange", f.calls)
	}
	if !after.ScannedUntil.Equal(before.ScannedUntil) {
		t.Fatal("idle scan moved the cursor")
	}

	// 等待付款的订单必须触发查询。
	if _, e = st.Create(ctx, model.Order{TradeNo: "a", OutTradeNo: "a", OriginUnits: 10000000, NotifyURL: "https://merchant.example/callback", ExpiresAt: time.Now().Add(time.Minute)}); e != nil {
		t.Fatal(e)
	}
	if _, e = st.Select(ctx, "a", "binance", "USDT", "receiver", 2, 10000, 10000, 99, 120); e != nil {
		t.Fatal(e)
	}
	if e = p.Scan(ctx, "binance"); e != nil {
		t.Fatal(e)
	}
	if f.calls != 2 {
		t.Fatal("ready order did not trigger a scan", f.calls)
	}

	// 订单关闭后重新空闲，但仍存在的未确认到账需要继续补查。
	if e = st.Cancel(ctx, "a"); e != nil {
		t.Fatal(e)
	}
	if e = p.Scan(ctx, "binance"); e != nil {
		t.Fatal(e)
	}
	if f.calls != 2 {
		t.Fatal("closed order still queried the exchange", f.calls)
	}
	if e = st.DB().Create(&model.Transaction{Provider: "binance", EventID: "confirming", Reference: "app", Receiver: "receiver", Currency: "USDT", Units: 10000000, OccurredAt: time.Now(), State: model.Unconfirmed}).Error; e != nil {
		t.Fatal(e)
	}
	if e = p.Scan(ctx, "binance"); e != nil {
		t.Fatal(e)
	}
	if f.calls != 3 {
		t.Fatal("unconfirmed deposit was not rechecked", f.calls)
	}

	// 空闲可能让游标落后数小时：补查从最早一张待付款订单的窗口开始，不重读空白历史。
	if _, e = st.Create(ctx, model.Order{TradeNo: "c", OutTradeNo: "c", OriginUnits: 10000000, NotifyURL: "https://merchant.example/callback", ExpiresAt: time.Now().Add(time.Minute)}); e != nil {
		t.Fatal(e)
	}
	if _, e = st.Select(ctx, "c", "binance", "USDT", "receiver", 2, 10000, 10000, 99, 120); e != nil {
		t.Fatal(e)
	}
	if e = st.DB().Model(&model.ScanCursor{}).Where("provider = ?", "binance").Update("scanned_until", time.Now().Add(-3*time.Hour)).Error; e != nil {
		t.Fatal(e)
	}
	if e = p.Scan(ctx, "binance"); e != nil {
		t.Fatal(e)
	}
	if f.calls != 4 {
		t.Fatal("ready order did not trigger a scan", f.calls)
	}
	if time.Since(f.from) > 10*time.Minute || f.to.Before(f.from) {
		t.Fatal("scan did not resume near the open payment window", f.from, f.to)
	}
}
