package store

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"gorm.io/gorm"

	"litepay/internal/amount"
	"litepay/internal/model"
)

func testStore(t *testing.T) *Store {
	t.Helper()
	s, e := Open(":memory:")
	if e != nil {
		t.Fatal(e)
	}
	t.Cleanup(func() { s.Close() })
	return s
}
func readyOrder(t *testing.T, s *Store, no string) *model.Order {
	t.Helper()
	ctx := context.Background()
	_, e := s.Create(ctx, model.Order{TradeNo: no, OutTradeNo: no, OriginUnits: 10 * amount.Scale, ExpiresAt: time.Now().Add(time.Minute), Reselect: true, NotifyURL: "https://merchant.example/callback"})
	if e != nil {
		t.Fatal(e)
	}
	o, e := s.Select(ctx, no, "binance", "USDT", "receiver", 2, 10000, 10000, 99, 120)
	if e != nil {
		t.Fatal(e)
	}
	return o
}
func evidence(o *model.Order, id string) model.Transaction {
	return model.Transaction{Provider: o.Provider, EventID: id, Reference: "app-" + id, Receiver: o.Receiver, Currency: o.Currency, Units: o.PayUnits, OccurredAt: time.Now(), State: model.Confirmed}
}
func TestAtomicClaimOwnershipAndCooldown(t *testing.T) {
	s := testStore(t)
	ctx := context.Background()
	a := readyOrder(t, s, "a")
	b := readyOrder(t, s, "b")
	tx := evidence(a, "event")
	tx.OccurredAt = time.Now()
	result, e := s.Process(ctx, tx, b.TradeNo)
	if e != nil || result != "other_order" {
		t.Fatalf("cross-order claim %q %v", result, e)
	}
	for name, change := range map[string]func(*model.Transaction){"underpaid": func(x *model.Transaction) { x.Units-- }, "wrong_uid": func(x *model.Transaction) { x.Receiver = "other" }, "wrong_currency": func(x *model.Transaction) { x.Currency = "USDC" }, "old_payment": func(x *model.Transaction) { x.OccurredAt = a.SelectedAt.Add(-time.Second) }, "multi_asset": func(x *model.Transaction) {
		x.State = model.Ambiguous
		x.Assets = `[{"currency":"USDT"},{"currency":"USDC"}]`
	}} {
		t.Run(name, func(t *testing.T) {
			x := tx
			x.EventID = name
			x.Reference = name
			change(&x)
			result, e := s.Process(ctx, x, a.TradeNo)
			if e != nil || result == "paid" {
				t.Fatalf("invalid payment %q %v", result, e)
			}
		})
	}
	var callbacks int64
	s.DB().Model(&model.Callback{}).Count(&callbacks)
	if callbacks != 0 {
		t.Fatal("invalid evidence created callback")
	}
	// Concurrent scans race on the exact same event.
	var wg sync.WaitGroup
	var mu sync.Mutex
	paid := 0
	for i := 0; i < 8; i++ {
		wg.Go(func() {
			result, e := s.Process(ctx, tx, a.TradeNo)
			if e != nil {
				t.Error(e)
			}
			if result == "paid" {
				mu.Lock()
				paid++
				mu.Unlock()
			}
		})
	}
	wg.Wait()
	if paid != 1 {
		t.Fatalf("paid %d times", paid)
	}
	s.DB().Model(&model.Callback{}).Count(&callbacks)
	if callbacks != 1 {
		t.Fatal("missing/duplicate atomic callback")
	}
	var slot model.Slot
	if e = s.DB().First(&slot, "provider = ? AND currency = ? AND units = ?", a.Provider, a.Currency, a.PayUnits).Error; e != nil || slot.OrderID != nil {
		t.Fatal("cooldown slot missing or still attached")
	}
	c := readyOrder(t, s, "c")
	if c.PayUnits == a.PayUnits {
		t.Fatal("paid amount prematurely reused")
	}
	result, e = s.Process(ctx, tx, c.TradeNo)
	if e != nil || result != "other_order" {
		t.Fatal("event reused", result, e)
	}
	a, e = s.GetByTradeNo(ctx, a.TradeNo)
	if e != nil || a.Status != model.Paid {
		t.Fatal("claim missing")
	}
}
func TestClaimRollsBackWhenCallbackCannotPersist(t *testing.T) {
	s := testStore(t)
	o := readyOrder(t, s, "a")
	if e := s.DB().Exec("CREATE TRIGGER reject_callback BEFORE INSERT ON callbacks BEGIN SELECT RAISE(ABORT, 'injected'); END").Error; e != nil {
		t.Fatal(e)
	}
	if _, e := s.Process(context.Background(), evidence(o, "event"), ""); e == nil {
		t.Fatal("expected transaction failure")
	}
	fresh, _ := s.GetByTradeNo(context.Background(), o.TradeNo)
	if fresh.Status != model.Ready {
		t.Fatal("order committed without callback")
	}
	var n int64
	s.DB().Model(&model.Transaction{}).Where("order_id IS NOT NULL").Count(&n)
	if n != 0 {
		t.Fatal("false ledger claim")
	}
}
func TestMultipleCandidatesAndClosedOrders(t *testing.T) {
	s := testStore(t)
	ctx := context.Background()
	a := readyOrder(t, s, "a")
	b := readyOrder(t, s, "b")
	if e := s.DB().Model(b).Update("pay_units", a.PayUnits).Error; e != nil {
		t.Fatal(e)
	}
	result, e := s.Process(ctx, evidence(a, "ambiguous"), "")
	if e != nil || result != "multiple_orders" {
		t.Fatal(result, e)
	}
	if e = s.Cancel(ctx, a.TradeNo); e != nil {
		t.Fatal(e)
	}
	if e = s.Cancel(ctx, b.TradeNo); e != nil {
		t.Fatal(e)
	}
	result, e = s.Process(ctx, evidence(a, "closed"), a.TradeNo)
	if e != nil || result == "paid" {
		t.Fatal("closed order paid")
	}
}
func TestPendingEvidenceAndFinalDeadline(t *testing.T) {
	s := testStore(t)
	ctx := context.Background()
	o := readyOrder(t, s, "pending")
	tx := evidence(o, "event")
	tx.State = model.Unconfirmed
	if result, e := s.Process(ctx, tx, ""); e != nil || result != "pending" {
		t.Fatal(result, e)
	}
	tx.State = model.Confirmed
	if result, e := s.Process(ctx, tx, ""); e != nil || result != "paid" {
		t.Fatal("pending deposit cannot complete", result, e)
	}
	// A closed order never takes a claim, even when the evidence arrived inside its window.
	closed := readyOrder(t, s, "closed")
	past := map[string]any{"selected_at": time.Now().Add(-6 * time.Minute), "expires_at": time.Now().Add(-time.Minute)}
	if e := s.DB().Model(closed).Updates(past).Error; e != nil {
		t.Fatal(e)
	}
	if e := s.ScanResult(ctx, "binance", time.Now(), nil); e != nil {
		t.Fatal(e)
	}
	if e := s.Expire(ctx, 1); e != nil {
		t.Fatal(e)
	}
	fresh, e := s.GetByTradeNo(ctx, closed.TradeNo)
	if e != nil || fresh.Status != model.Expired {
		t.Fatal("covered window did not close", fresh.Status, e)
	}
	late := evidence(closed, "late")
	late.OccurredAt = time.Now().Add(-2 * time.Minute)
	if result, e := s.Process(ctx, late, ""); e != nil || result == "paid" {
		t.Fatal("closed order paid", result, e)
	}
}

// A scan that only discovers an on-time payment after the grace deadline must still claim it:
// closure is driven by scan coverage, not by the wall clock.
func TestLateScanStillClaimsOnTimePayment(t *testing.T) {
	s := testStore(t)
	ctx := context.Background()
	o := readyOrder(t, s, "late-scan")
	past := map[string]any{"selected_at": time.Now().Add(-6 * time.Minute), "expires_at": time.Now().Add(-time.Minute)}
	if e := s.DB().Model(o).Updates(past).Error; e != nil {
		t.Fatal(e)
	}
	if e := s.DB().Model(&model.Slot{}).Where("order_id = ?", o.ID).Update("expires_at", time.Now().Add(-time.Minute)).Error; e != nil {
		t.Fatal(e)
	}
	if e := s.Expire(ctx, 1); e != nil {
		t.Fatal(e)
	}
	if fresh, _ := s.GetByTradeNo(ctx, o.TradeNo); fresh.Status != model.Ready {
		t.Fatal("order closed without scan coverage", fresh.Status)
	}
	tx := evidence(o, "late-scan-event")
	tx.OccurredAt = time.Now().Add(-2 * time.Minute)
	if result, e := s.Process(ctx, tx, ""); e != nil || result != "paid" {
		t.Fatal("on-time payment lost", result, e)
	}
}

func TestExpireRequiresSuccessfulCoverage(t *testing.T) {
	s := testStore(t)
	ctx := context.Background()
	o := readyOrder(t, s, "cover")
	windowEnd := time.Now().Add(-time.Minute)
	if e := s.DB().Model(o).Updates(map[string]any{"selected_at": time.Now().Add(-6 * time.Minute), "expires_at": windowEnd}).Error; e != nil {
		t.Fatal(e)
	}
	if e := s.DB().Model(&model.Slot{}).Where("order_id = ?", o.ID).Update("expires_at", windowEnd).Error; e != nil {
		t.Fatal(e)
	}
	// A scan that stopped before the payment window proves nothing about this order.
	if e := s.ScanResult(ctx, "binance", windowEnd.Add(-time.Minute), nil); e != nil {
		t.Fatal(e)
	}
	if e := s.Expire(ctx, 0); e != nil {
		t.Fatal(e)
	}
	if fresh, _ := s.GetByTradeNo(ctx, o.TradeNo); fresh.Status != model.Ready {
		t.Fatal("closed without coverage", fresh.Status)
	}
	var slots int64
	s.DB().Model(&model.Slot{}).Where("order_id IS NOT NULL").Count(&slots)
	if slots != 1 {
		t.Fatal("reservation released without coverage")
	}
	// The scan that does cover the window closes the order and frees the tier.
	if e := s.ScanResult(ctx, "binance", time.Now(), nil); e != nil {
		t.Fatal(e)
	}
	if e := s.Expire(ctx, 0); e != nil {
		t.Fatal(e)
	}
	if fresh, _ := s.GetByTradeNo(ctx, o.TradeNo); fresh.Status != model.Expired {
		t.Fatal("covered window did not close", fresh.Status)
	}
	s.DB().Model(&model.Slot{}).Where("order_id IS NOT NULL").Count(&slots)
	if slots != 0 {
		t.Fatal("tier not released after closure")
	}
}

// An exchange-unconfirmed deposit inside the window keeps the order open, but only for the
// bounded retention period.
func TestUnconfirmedDepositHoldsOrderOpen(t *testing.T) {
	s := testStore(t)
	ctx := context.Background()
	o := readyOrder(t, s, "held")
	windowEnd := time.Now().Add(-time.Minute)
	if e := s.DB().Model(o).Updates(map[string]any{"selected_at": time.Now().Add(-6 * time.Minute), "expires_at": windowEnd}).Error; e != nil {
		t.Fatal(e)
	}
	if e := s.DB().Model(&model.Slot{}).Where("order_id = ?", o.ID).Update("expires_at", windowEnd).Error; e != nil {
		t.Fatal(e)
	}
	tx := evidence(o, "still-confirming")
	tx.State = model.Unconfirmed
	tx.OccurredAt = time.Now().Add(-2 * time.Minute)
	if _, e := s.Process(ctx, tx, ""); e != nil {
		t.Fatal(e)
	}
	if e := s.ScanResult(ctx, "binance", time.Now(), nil); e != nil {
		t.Fatal(e)
	}
	if e := s.Expire(ctx, 0); e != nil {
		t.Fatal(e)
	}
	if fresh, _ := s.GetByTradeNo(ctx, o.TradeNo); fresh.Status != model.Ready {
		t.Fatal("unconfirmed deposit dropped", fresh.Status)
	}
	if e := s.DB().Model(&model.ConfirmationTask{}).Where("event_id = ?", tx.EventID).Update("deadline", time.Now().Add(-time.Second)).Error; e != nil {
		t.Fatal(e)
	}
	if e := s.Expire(ctx, 0); e != nil {
		t.Fatal(e)
	}
	if fresh, _ := s.GetByTradeNo(ctx, o.TradeNo); fresh.Status != model.Expired {
		t.Fatal("retention bound ignored", fresh.Status)
	}
}

func createOrder(t *testing.T, s *Store, no string, units int64) {
	t.Helper()
	if _, e := s.Create(context.Background(), model.Order{TradeNo: no, OutTradeNo: no, OriginUnits: units, ExpiresAt: time.Now().Add(time.Minute), Reselect: true, NotifyURL: "https://merchant.example/callback"}); e != nil {
		t.Fatal(e)
	}
}

func slotOwners(t *testing.T, s *Store, orderID uint) int64 {
	t.Helper()
	var n int64
	if e := s.DB().Model(&model.Slot{}).Where("order_id = ?", orderID).Count(&n).Error; e != nil {
		t.Fatal(e)
	}
	return n
}

// 创建订单只约束原始金额；应付金额是向上对齐后再按档位递增算出来的，同样必须落在
// 商城能用 float64 验签的 [0.0001, 1000000) 区间内，否则付款后回调永远验签失败。
func TestSelectRejectsAmountsAboveMallLimit(t *testing.T) {
	s := testStore(t)
	ctx := context.Background()
	// 两位精度把 999999.999999 向上对齐成 1000000，越界必须整单拒绝。
	createOrder(t, s, "align", 999_999_999_999)
	if _, e := s.Select(ctx, "align", "binance", "USDT", "receiver", 2, 10000, 10000, 99, 120); e == nil {
		t.Fatal("precision alignment above the mall limit accepted")
	}
	align, e := s.GetByTradeNo(ctx, "align")
	if e != nil || align.Status != model.Pending || align.PayUnits != 0 || slotOwners(t, s, align.ID) != 0 {
		t.Fatal("rejected selection left the order or a tier reserved", align, e)
	}
	// 首档合法、冲突递增后越界：只能拒绝，且不能占用任何档位。
	createOrder(t, s, "first", 999_999_990_000)
	createOrder(t, s, "second", 999_999_990_000)
	if _, e = s.Select(ctx, "first", "binance", "USDT", "receiver", 2, 10000, 10000, 1, 120); e != nil {
		t.Fatal(e)
	}
	if _, e = s.Select(ctx, "second", "binance", "USDT", "receiver", 2, 10000, 10000, 1, 120); e == nil {
		t.Fatal("conflict increment above the mall limit accepted")
	}
	second, e := s.GetByTradeNo(ctx, "second")
	if e != nil || second.Status != model.Pending || second.PayUnits != 0 || slotOwners(t, s, second.ID) != 0 {
		t.Fatal("failed increment left the order or a tier reserved", second, e)
	}
	var slots int64
	s.DB().Model(&model.Slot{}).Count(&slots)
	if slots != 1 {
		t.Fatalf("failed selection created %d tiers", slots)
	}
	// 上限内侧的金额仍然可用：不做过度收紧。
	createOrder(t, s, "top", 999_999_999_999)
	top, e := s.Select(ctx, "top", "binance", "USDT", "receiver", 6, 1, 1, 99, 120)
	if e != nil || top.PayAmount() != "999999.999999" || !amount.MallCompatible(top.PayUnits) {
		t.Fatal("top of the interoperable range refused", top, e)
	}
}

// 低于商城最小金额的旧订单只能向上递增到合法档位，绝不能截断或调低。
func TestSelectRaisesAmountsBelowMallMinimum(t *testing.T) {
	s := testStore(t)
	ctx := context.Background()
	createOrder(t, s, "tiny", 50) // 0.00005：创建校验之前写入的原始金额
	if _, e := s.Select(ctx, "tiny", "binance", "USDT", "receiver", 6, 10, 1, 4, 120); e == nil {
		t.Fatal("amount below the mall minimum accepted")
	}
	o, e := s.Select(ctx, "tiny", "binance", "USDT", "receiver", 6, 10, 1, 5, 120)
	if e != nil || o.PayUnits != amount.MinMall || o.PayAmount() != "0.000100" {
		t.Fatal("lowest interoperable tier not reached by incrementing", o, e)
	}
}

// 重选同一付款方式复用已占用档位，但修复前写入的越界金额不能原样交回给用户。
func TestReselectRejectsOutOfRangeSelectedAmount(t *testing.T) {
	s := testStore(t)
	ctx := context.Background()
	o := readyOrder(t, s, "a")
	again, e := s.Select(ctx, "a", "binance", "USDT", "receiver", 2, 10000, 10000, 99, 120)
	if e != nil || again.PayUnits != o.PayUnits {
		t.Fatal("same method reselect changed the amount", again, e)
	}
	if e = s.DB().Model(o).Update("pay_units", amount.MaxMall).Error; e != nil {
		t.Fatal(e)
	}
	if _, e = s.Select(ctx, "a", "binance", "USDT", "receiver", 2, 10000, 10000, 99, 120); e == nil {
		t.Fatal("out-of-range selected amount handed back to the payer")
	}
	fresh, e := s.GetByTradeNo(ctx, "a")
	if e != nil || fresh.PayUnits != amount.MaxMall || fresh.Status != model.Ready {
		t.Fatal("rejected reselect mutated the order", fresh, e)
	}
}

func TestCallbackAttemptFencesStaleResult(t *testing.T) {
	s := testStore(t)
	ctx := context.Background()
	o := readyOrder(t, s, "a")
	if _, e := s.Process(ctx, evidence(o, "event"), ""); e != nil {
		t.Fatal(e)
	}
	first, _, e := s.ReserveCallback(ctx, 5, 3)
	if e != nil {
		t.Fatal(e)
	}
	s.DB().Model(&model.Callback{}).Where("order_id = ?", o.ID).Update("next_at", time.Now().Add(-time.Second))
	second, _, e := s.ReserveCallback(ctx, 5, 3)
	if e != nil {
		t.Fatal(e)
	}
	if e = s.FinishCallback(ctx, *first, 200, true, "", 0); e != nil {
		t.Fatal(e)
	}
	var cb model.Callback
	s.DB().First(&cb, o.ID)
	if cb.DeliveredAt != nil {
		t.Fatal("stale result accepted")
	}
	if e = s.FinishCallback(ctx, *second, 200, true, "", 0); e != nil {
		t.Fatal(e)
	}
	if _, _, e = s.ReserveCallback(ctx, 5, 3); !errors.Is(e, gorm.ErrRecordNotFound) {
		t.Fatal("acknowledged callback reserved again", e)
	}
}

func TestCanceledOrderReleasesOwnershipButPreservesCooldown(t *testing.T) {
	s := testStore(t)
	ctx := context.Background()
	o := readyOrder(t, s, "close-ready")
	var before model.Slot
	if err := s.DB().Where("order_id = ?", o.ID).First(&before).Error; err != nil {
		t.Fatal(err)
	}
	if _, err := s.CancelReference(ctx, o.TradeNo, o.OutTradeNo); err != nil {
		t.Fatal(err)
	}
	var after model.Slot
	if err := s.DB().Where("provider = ? AND currency = ? AND units = ?", before.Provider, before.Currency, before.Units).First(&after).Error; err != nil {
		t.Fatal(err)
	}
	if after.OrderID != nil || !after.ExpiresAt.Equal(before.ExpiresAt) {
		t.Fatal("cooldown changed", before, after)
	}
	if next := readyOrder(t, s, "after-close"); next.PayUnits == o.PayUnits {
		t.Fatal("canceled amount reused before cooldown")
	}
	if result, err := s.Process(ctx, evidence(o, "late-payment"), o.TradeNo); err != nil || result == "paid" {
		t.Fatal(result, err)
	}
}
