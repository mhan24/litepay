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
	// Automatic scanning and inquiry race on the exact same event.
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
	closed := readyOrder(t, s, "closed")
	expired := time.Now().Add(-time.Second)
	if e := s.DB().Model(&model.Slot{}).Where("order_id = ?", closed.ID).Update("expires_at", expired).Error; e != nil {
		t.Fatal(e)
	}
	if result, e := s.Process(ctx, evidence(closed, "late"), ""); e != nil || result != "confirmation_window_closed" {
		t.Fatal("late claim before expiration sweep", result, e)
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
