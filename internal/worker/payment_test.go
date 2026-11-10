package worker

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
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
	rows      []model.Transaction
	err       error
	calls     int
	failAfter int // 大于 0 时，第 N 次之后的查询失败
	from      time.Time
	to        time.Time
	windows   [][2]time.Time
}

func (f *fixture) AccountUID(context.Context) (string, error) { return "receiver", nil }
func (f *fixture) History(_ context.Context, start, end time.Time) ([]model.Transaction, error) {
	f.calls++
	f.from, f.to = start, end
	f.windows = append(f.windows, [2]time.Time{start, end})
	if f.failAfter > 0 && f.calls > f.failAfter {
		return nil, errors.New("history incomplete")
	}
	return f.inWindow(start, end), f.err
}

// inWindow 模拟交易所按查询时间窗口返回记录：窗口外的流水不会被读到。
func (f *fixture) inWindow(start, end time.Time) []model.Transaction {
	out := []model.Transaction{}
	for _, t := range f.rows {
		if !t.OccurredAt.Before(start) && !t.OccurredAt.After(end) {
			out = append(out, t)
		}
	}
	return out
}
func TestScanAndSignedCallback(t *testing.T) {
	ctx := context.Background()
	st, e := store.Open(":memory:")
	if e != nil {
		t.Fatal(e)
	}
	defer st.Close()
	cfg := config.Default()
	cfg.Server.Production = false
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
	syncAttemptFixture(t, st)
	f := &fixture{rows: []model.Transaction{{Provider: "binance", EventID: "parent-event", Reference: "app-order", Receiver: "receiver", Currency: "USDT", Units: 10000000, OccurredAt: time.Now(), State: model.Confirmed}}}
	p := NewPoller(mgr, st)
	p.clients["binance"] = &connection{config: *mgr.Get(), source: f}
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
	cfg.Server.Production = false
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
	if _, e = st.Process(ctx, model.Transaction{Provider: "binance", EventID: "confirming", Reference: "app", Receiver: "receiver", Currency: "USDT", Units: 10000000, OccurredAt: time.Now(), State: model.Unconfirmed}, ""); e != nil {
		t.Fatal(e)
	}
	if e = p.Scan(ctx, "binance"); e != nil {
		t.Fatal(e)
	}
	// 只有增量扫描一次：这条刚发生的未确认到账已经落在增量窗口里，不重复补查。
	if f.calls != 3 {
		t.Fatal("fresh pending deposit caused a duplicate query", f.calls)
	}
	if e = st.DB().Where("event_id = ?", "confirming").Delete(&model.Transaction{}).Error; e != nil {
		t.Fatal(e)
	}

	// 空闲或停机后仍需覆盖游标后的全部历史，不能跳过取消后的对账窗口。
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
	if time.Since(f.from) < 2*time.Hour || f.to.Before(f.from) {
		t.Fatal("scan skipped unscanned reconciliation history", f.from, f.to)
	}
}

// 首次成功扫描发生在订单创建很久之后：游标还没有任何覆盖，扫描必须回到最早一张待付款
// 订单的完整付款窗口，而不能从默认补查期限起步后把那张订单的付款漏掉。
func TestFirstScanCoversOldPaymentWindow(t *testing.T) {
	ctx := context.Background()
	st, e := store.Open(":memory:")
	if e != nil {
		t.Fatal(e)
	}
	defer st.Close()
	cfg := config.Default()
	cfg.Server.Production = false
	cfg.Binance.APIKey = "key"
	cfg.Binance.APISecret = "secret"
	cfg.Checkout.Binance.UID = "receiver"
	mgr := settings.New(cfg, st)
	if e = mgr.Load(ctx); e != nil {
		t.Fatal(e)
	}
	if _, e = st.Create(ctx, model.Order{TradeNo: "old", OutTradeNo: "old", OriginUnits: 10000000, NotifyURL: "https://merchant.example/callback", ExpiresAt: time.Now().Add(10 * time.Minute)}); e != nil {
		t.Fatal(e)
	}
	if _, e = st.Select(ctx, "old", "binance", "USDT", "receiver", 2, 10000, 10000, 99, 120); e != nil {
		t.Fatal(e)
	}
	// 付款方式三小时前就已选定，但这期间没有任何一次成功扫描覆盖过它的窗口。
	selected := time.Now().Add(-3 * time.Hour)
	if e = st.DB().Model(&model.Order{}).Where("trade_no = ?", "old").Updates(map[string]any{"selected_at": selected, "expires_at": selected.Add(10 * time.Minute)}).Error; e != nil {
		t.Fatal(e)
	}
	syncAttemptFixture(t, st)
	paidAt := selected.Add(30 * time.Second)
	f := &fixture{rows: []model.Transaction{{Provider: "binance", EventID: "old-event", Reference: "app", Receiver: "receiver", Currency: "USDT", Units: 10000000, OccurredAt: paidAt, State: model.Confirmed}}}
	p := NewPoller(mgr, st)
	p.clients["binance"] = &connection{config: *mgr.Get(), source: f}

	if e = p.Scan(ctx, "binance"); e != nil {
		t.Fatal(e)
	}
	if !f.from.Before(paidAt) {
		t.Fatal("first scan started after the payment", f.from, paidAt)
	}
	o, e := st.GetByTradeNo(ctx, "old")
	if e != nil || o.Status != model.Paid {
		t.Fatal("first scan missed the on-time payment", o.Status, e)
	}
}

// OKX 充值历史的 ts 是记录创建时间，状态随后才变为成功。普通游标一旦越过该时间，
// 回看窗口就再也读不到这条记录，因此仍处于补查期的未确认到账必须保持可读，状态变为
// 成功后照常走统一核销。
func TestPendingDepositStaysReadableAfterCursorAdvances(t *testing.T) {
	ctx := context.Background()
	st, e := store.Open(":memory:")
	if e != nil {
		t.Fatal(e)
	}
	defer st.Close()
	cfg := config.Default()
	cfg.Server.Production = false
	cfg.OKX.APIKey = "key"
	cfg.OKX.APISecret = "secret"
	cfg.OKX.Passphrase = "pass"
	cfg.Checkout.OKX.UID = "receiver"
	mgr := settings.New(cfg, st)
	if e = mgr.Load(ctx); e != nil {
		t.Fatal(e)
	}
	if _, e = st.Create(ctx, model.Order{TradeNo: "okx-order", OutTradeNo: "okx-order", OriginUnits: 10000000, NotifyURL: "https://merchant.example/callback", ExpiresAt: time.Now().Add(10 * time.Minute)}); e != nil {
		t.Fatal(e)
	}
	if _, e = st.Select(ctx, "okx-order", "okx", "USDT", "receiver", 2, 10000, 10000, 99, 120); e != nil {
		t.Fatal(e)
	}
	selected := time.Now().Add(-5 * time.Minute)
	if e = st.DB().Model(&model.Order{}).Where("trade_no = ?", "okx-order").Updates(map[string]any{"selected_at": selected, "expires_at": selected.Add(10 * time.Minute)}).Error; e != nil {
		t.Fatal(e)
	}
	syncAttemptFixture(t, st)
	deposit := model.Transaction{Provider: "okx", EventID: "dep-1", Reference: "wd-1", Receiver: "receiver", Currency: "USDT", Units: 10000000, OccurredAt: selected.Add(20 * time.Second), State: model.Unconfirmed}
	f := &fixture{rows: []model.Transaction{deposit}}
	p := NewPoller(mgr, st)
	p.clients["okx"] = &connection{config: *mgr.Get(), source: f}

	// 未确认的充值不会被核销，证据先落库。
	if e = p.Scan(ctx, "okx"); e != nil {
		t.Fatal(e)
	}
	if o, _ := st.GetByTradeNo(ctx, "okx-order"); o.Status != model.Ready {
		t.Fatal("unconfirmed deposit paid the order", o.Status)
	}
	// 普通游标继续前进到这笔记录创建时间之外。
	f.rows = nil
	for i := 0; i < 3; i++ {
		if e = p.Scan(ctx, "okx"); e != nil {
			t.Fatal(e)
		}
	}
	cursor, e := st.Cursor(ctx, "okx")
	if e != nil {
		t.Fatal(e)
	}
	if !cursor.ScannedUntil.After(deposit.OccurredAt.Add(2 * time.Minute)) {
		t.Fatal("cursor did not move past the deposit", cursor.ScannedUntil, deposit.OccurredAt)
	}
	// 交易所随后把同一条记录改为成功，创建时间不变。
	deposit.State = model.Confirmed
	f.rows = []model.Transaction{deposit}
	if e = p.Scan(ctx, "okx"); e != nil {
		t.Fatal(e)
	}
	o, e := st.GetByTradeNo(ctx, "okx-order")
	if e != nil || o.Status != model.Paid {
		t.Fatal("confirmed deposit outside the lookback window was lost", o.Status, e)
	}
}

// 一笔比游标早一小时以上、仍在补查期内的未确认充值，不能把正常增量扫描拖回旧窗口：
// 新订单的到账必须继续推进，旧交易既不丢失也不阻塞。
func TestOldPendingDepositDoesNotBlockNewPayment(t *testing.T) {
	ctx := context.Background()
	st, e := store.Open(":memory:")
	if e != nil {
		t.Fatal(e)
	}
	defer st.Close()
	cfg := config.Default()
	cfg.Server.Production = false
	cfg.Binance.APIKey = "key"
	cfg.Binance.APISecret = "secret"
	cfg.Checkout.Binance.UID = "receiver"
	// 补查期限拉长到两小时以上，旧交易才会一直留在补查范围里。
	cfg.Order.ExpireMinutes = 120
	mgr := settings.New(cfg, st)
	if e = mgr.Load(ctx); e != nil {
		t.Fatal(e)
	}
	if _, e = st.Create(ctx, model.Order{TradeNo: "new", OutTradeNo: "new", OriginUnits: 10000000, NotifyURL: "https://merchant.example/callback", ExpiresAt: time.Now().Add(10 * time.Minute)}); e != nil {
		t.Fatal(e)
	}
	if _, e = st.Select(ctx, "new", "binance", "USDT", "receiver", 2, 10000, 10000, 99, 120); e != nil {
		t.Fatal(e)
	}
	selected := time.Now().Add(-4 * time.Minute)
	if e = st.DB().Model(&model.Order{}).Where("trade_no = ?", "new").Updates(map[string]any{"selected_at": selected, "expires_at": selected.Add(10 * time.Minute)}).Error; e != nil {
		t.Fatal(e)
	}
	syncAttemptFixture(t, st)
	// 90 分钟前创建、至今仍未确认的充值：远早于当前游标，但仍在补查期内。
	old := model.Transaction{Provider: "binance", EventID: "old-pending", Reference: "old", Receiver: "receiver", Currency: "USDT", Units: 5000000, OccurredAt: time.Now().Add(-90 * time.Minute), State: model.Unconfirmed}
	if _, e = st.Process(ctx, old, ""); e != nil {
		t.Fatal(e)
	}
	payment := model.Transaction{Provider: "binance", EventID: "new-paid", Reference: "app", Receiver: "receiver", Currency: "USDT", Units: 10000000, OccurredAt: time.Now().Add(-3 * time.Minute), State: model.Confirmed}
	f := &fixture{rows: []model.Transaction{old, payment}}
	p := NewPoller(mgr, st)
	p.clients["binance"] = &connection{config: *mgr.Get(), source: f}

	for tick := 0; tick < 4; tick++ {
		if e = p.Scan(ctx, "binance"); e != nil {
			t.Fatal(e)
		}
		if o, _ := st.GetByTradeNo(ctx, "new"); o.Status == model.Paid {
			cursor, _ := st.Cursor(ctx, "binance")
			if !cursor.ScannedUntil.Before(payment.OccurredAt) {
				break
			}
		}
	}
	o, e := st.GetByTradeNo(ctx, "new")
	if e != nil || o.Status != model.Paid {
		t.Fatal("old pending deposit blocked the new payment", o.Status, e)
	}
	cursor, e := st.Cursor(ctx, "binance")
	if e != nil {
		t.Fatal(e)
	}
	if cursor.ScannedUntil.Before(payment.OccurredAt) {
		t.Fatal("incremental cursor did not reach the new payment", cursor.ScannedUntil, payment.OccurredAt)
	}
	// 旧证据仍然保留、未被核销。
	var stored model.Transaction
	if e = st.DB().First(&stored, "provider = ? AND event_id = ?", "binance", "old-pending").Error; e != nil {
		t.Fatal(e)
	}
	if stored.State != model.Unconfirmed || stored.OrderID != nil {
		t.Fatal("old evidence was lost or claimed", stored.State, stored.OrderID)
	}
}

// 未确认到账的补查按轮转前进：超过单轮上限的记录在后续轮次补上，不会长期饥饿，
// 同时增量扫描游标照常推进。
func TestPendingRecheckRotatesAcrossRecords(t *testing.T) {
	ctx := context.Background()
	st, e := store.Open(":memory:")
	if e != nil {
		t.Fatal(e)
	}
	defer st.Close()
	cfg := config.Default()
	cfg.Server.Production = false
	cfg.Binance.APIKey = "key"
	cfg.Binance.APISecret = "secret"
	cfg.Checkout.Binance.UID = "receiver"
	cfg.Order.ExpireMinutes = 120 // 补查期限两小时以上，12 笔未确认到账都留在补查范围内
	mgr := settings.New(cfg, st)
	if e = mgr.Load(ctx); e != nil {
		t.Fatal(e)
	}
	var pending []time.Time
	for i := 0; i < 12; i++ {
		at := time.Now().Add(-time.Duration(105-5*i) * time.Minute)
		pending = append(pending, at)
		if _, e = st.Process(ctx, model.Transaction{Provider: "binance", EventID: fmt.Sprintf("pending-%d", i), Reference: fmt.Sprintf("ref-%d", i), Receiver: "receiver", Currency: "USDT", Units: 1000000, OccurredAt: at, State: model.Unconfirmed}, ""); e != nil {
			t.Fatal(e)
		}
	}
	// 游标停在当前时刻附近，增量扫描窗口不会覆盖这些旧记录。
	if e = st.ScanResult(ctx, "binance", time.Now().Add(-time.Minute), nil); e != nil {
		t.Fatal(e)
	}
	f := &fixture{}
	p := NewPoller(mgr, st)
	p.clients["binance"] = &connection{config: *mgr.Get(), source: f}

	for tick := 0; tick < 3; tick++ {
		if e = p.Scan(ctx, "binance"); e != nil {
			t.Fatal(e)
		}
	}
	covered := map[int]bool{}
	for _, w := range f.windows {
		if w[1].Sub(w[0]) != 4*time.Minute {
			continue // 只统计按单条记录取窗口的补查查询
		}
		for i, at := range pending {
			if !w[0].After(at) && !w[1].Before(at) {
				covered[i] = true
			}
		}
	}
	if len(covered) != len(pending) {
		t.Fatal("pending recheck starved records", len(covered), len(pending))
	}
	cursor, e := st.Cursor(ctx, "binance")
	if e != nil {
		t.Fatal(e)
	}
	if time.Since(cursor.ScannedUntil) > time.Minute {
		t.Fatal("old pending deposits blocked the incremental scan", cursor.ScannedUntil)
	}
}

// 补查失败单独记录：增量扫描与游标不受影响，后台能看到未完成的补查，恢复后自动清除。
func TestPendingRecheckFailureIsRecorded(t *testing.T) {
	ctx := context.Background()
	st, e := store.Open(":memory:")
	if e != nil {
		t.Fatal(e)
	}
	defer st.Close()
	cfg := config.Default()
	cfg.Server.Production = false
	cfg.Binance.APIKey = "key"
	cfg.Binance.APISecret = "secret"
	cfg.Checkout.Binance.UID = "receiver"
	mgr := settings.New(cfg, st)
	if e = mgr.Load(ctx); e != nil {
		t.Fatal(e)
	}
	if _, e = st.Create(ctx, model.Order{TradeNo: "d", OutTradeNo: "d", OriginUnits: 10000000, NotifyURL: "https://merchant.example/callback", ExpiresAt: time.Now().Add(10 * time.Minute)}); e != nil {
		t.Fatal(e)
	}
	if _, e = st.Select(ctx, "d", "binance", "USDT", "receiver", 2, 10000, 10000, 99, 120); e != nil {
		t.Fatal(e)
	}
	selected := time.Now().Add(-10 * time.Minute)
	if e = st.DB().Model(&model.Order{}).Where("trade_no = ?", "d").Updates(map[string]any{"selected_at": selected, "expires_at": selected.Add(12 * time.Minute)}).Error; e != nil {
		t.Fatal(e)
	}
	syncAttemptFixture(t, st)
	// 到账时间早于增量回看窗口，只能由补查读到。
	deposit := model.Transaction{Provider: "binance", EventID: "late-event", Reference: "app", Receiver: "receiver", Currency: "USDT", Units: 10000000, OccurredAt: selected.Add(2 * time.Minute), State: model.Unconfirmed}
	if _, e = st.Process(ctx, deposit, ""); e != nil {
		t.Fatal(e)
	}
	if e = st.ScanResult(ctx, "binance", time.Now().Add(-time.Minute), nil); e != nil {
		t.Fatal(e)
	}
	f := &fixture{rows: []model.Transaction{deposit}, failAfter: 1}
	p := NewPoller(mgr, st)
	p.clients["binance"] = &connection{config: *mgr.Get(), source: f}

	// 正常增量查询成功，按原始时间补查失败。
	if e = p.Scan(ctx, "binance"); e == nil {
		t.Fatal("failed recheck was not reported")
	}
	cursor, e := st.Cursor(ctx, "binance")
	if e != nil {
		t.Fatal(e)
	}
	if cursor.LastError != "" || time.Since(cursor.ScannedUntil) > time.Minute {
		t.Fatal("recheck failure touched the incremental scan state", cursor.LastError, cursor.ScannedUntil)
	}
	if cursor.PendingError == "" || cursor.PendingAt.IsZero() {
		t.Fatal("recheck failure left no diagnostic")
	}
	if o, _ := st.GetByTradeNo(ctx, "d"); o.Status != model.Ready {
		t.Fatal("failed recheck fabricated a payment", o.Status)
	}

	// 恢复查询后补查成功，订单核销、诊断清除。
	f.failAfter = 0
	deposit.State = model.Confirmed
	f.rows = []model.Transaction{deposit}
	if e = p.Scan(ctx, "binance"); e != nil {
		t.Fatal(e)
	}
	cursor, e = st.Cursor(ctx, "binance")
	if e != nil {
		t.Fatal(e)
	}
	if cursor.PendingError != "" {
		t.Fatal("recheck diagnostic was not cleared", cursor.PendingError)
	}
	if o, _ := st.GetByTradeNo(ctx, "d"); o.Status != model.Paid {
		t.Fatal("deposit was not settled after the recheck recovered", o.Status)
	}
}
