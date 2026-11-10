package store

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"litepay/internal/model"
)

func TestPaymentAttemptReselectionKeepsOriginalClaim(t *testing.T) {
	for _, mode := range []string{"pending_before_reselect", "unseen_before_reselect", "cross_currency"} {
		t.Run(mode, func(t *testing.T) {
			s := testStore(t)
			ctx := context.Background()
			_, err := s.Create(ctx, model.Order{TradeNo: "attempts", OutTradeNo: "attempts", OriginUnits: 10_000_000, ExpiresAt: time.Now().Add(time.Hour), Reselect: true})
			if err != nil {
				t.Fatal(err)
			}
			old, err := s.Select(ctx, "attempts", "okx", "USDT", "old-receiver", 2, 10000, 10000, 99, 120)
			if err != nil {
				t.Fatal(err)
			}
			tx := evidence(old, "old-payment")
			tx.OccurredAt = time.Now()
			tx.State = model.Unconfirmed
			if mode == "pending_before_reselect" {
				if _, err = s.Process(ctx, tx, ""); err != nil {
					t.Fatal(err)
				}
			}
			currency := "USDT"
			if mode == "cross_currency" {
				currency = "USDC"
			}
			allowSelectionFixture(t, s, "attempts")
			next, err := s.Select(ctx, "attempts", "binance", currency, "new-receiver", 2, 10000, 10000, 99, 120)
			if err != nil {
				t.Fatal(err)
			}
			if next.Provider != "binance" {
				t.Fatal(next)
			}
			tx.State = model.Confirmed
			result, err := s.Process(ctx, tx, "")
			if err != nil || result != "paid" {
				t.Fatal(result, err)
			}
			paid, _ := s.GetByTradeNo(ctx, "attempts")
			if paid.Provider != "okx" || paid.Receiver != old.Receiver || paid.PayUnits != old.PayUnits {
				t.Fatal(paid)
			}
			var attempts []model.PaymentAttempt
			s.DB().Where("order_id = ?", paid.ID).Order("id").Find(&attempts)
			if len(attempts) != 2 || attempts[0].Provider != "okx" || attempts[1].Provider != "binance" {
				t.Fatal(attempts)
			}
			var ledger model.Transaction
			s.DB().First(&ledger, "event_id = ?", tx.EventID)
			if ledger.AttemptID == nil || *ledger.AttemptID != attempts[0].ID {
				t.Fatal(ledger)
			}
		})
	}
}

func TestOldAndNewPaymentsOnlySettleOneOrder(t *testing.T) {
	s := testStore(t)
	ctx := context.Background()
	old := readyOrder(t, s, "double-payment")
	first := evidence(old, "old-deposit")
	first.OccurredAt = time.Now()
	allowSelectionFixture(t, s, old.TradeNo)
	next, err := s.Select(ctx, old.TradeNo, "okx", "USDC", "other-receiver", 2, 10000, 10000, 99, 120)
	if err != nil {
		t.Fatal(err)
	}
	second := evidence(next, "new-deposit")
	second.OccurredAt = time.Now()
	var wg sync.WaitGroup
	var mu sync.Mutex
	paid, additional := 0, 0
	for _, tx := range []model.Transaction{first, second} {
		wg.Go(func() {
			result, err := s.Process(ctx, tx, "")
			if err != nil {
				t.Error(err)
				return
			}
			mu.Lock()
			defer mu.Unlock()
			switch result {
			case "paid":
				paid++
			case "additional_payment":
				additional++
			default:
				t.Error(result)
			}
		})
	}
	wg.Wait()
	var claims, callbacks int64
	s.DB().Model(&model.Transaction{}).Where("order_id = ?", old.ID).Count(&claims)
	s.DB().Model(&model.Callback{}).Count(&callbacks)
	if paid != 1 || additional != 1 || claims != 1 || callbacks != 1 {
		t.Fatal(paid, additional, claims, callbacks)
	}
}

func TestFirstVisibleLatePaymentAndAuditedRecovery(t *testing.T) {
	ctx := context.Background()
	s := testStore(t)
	o := readyOrder(t, s, "late-index")
	selected, expires := time.Now().Add(-20*time.Minute), time.Now().Add(-10*time.Minute)
	s.DB().Model(o).Updates(map[string]any{"selected_at": selected, "expires_at": expires})
	syncAttemptFixture(t, s)
	s.DB().Model(&model.PaymentAttempt{}).Where("order_id = ?", o.ID).Update("settle_until", expires.Add(LateVisibilityWindow))
	if err := s.ScanResult(ctx, "binance", time.Now(), nil); err != nil {
		t.Fatal(err)
	}
	if err := s.Expire(ctx, 120); err != nil {
		t.Fatal(err)
	}
	ready, _ := s.GetByTradeNo(ctx, o.TradeNo)
	if ready.Status != model.Ready {
		t.Fatal("empty successful query closed settlement window")
	}
	tx := evidence(o, "late-first-visible")
	tx.OccurredAt = selected.Add(time.Minute)
	if result, err := s.Process(ctx, tx, ""); err != nil || result != "paid" {
		t.Fatal(result, err)
	}
	// Beyond the independent bound a fresh event is recorded but cannot auto-deliver.
	other := readyOrder(t, s, "manual-late")
	at := time.Now()
	s.DB().Model(&model.PaymentAttempt{}).Where("order_id = ?", other.ID).Update("settle_until", at.Add(-time.Second))
	tx = evidence(other, "manual-visible")
	tx.OccurredAt = at
	if result, err := s.Process(ctx, tx, ""); err != nil || result != "late_payment_review_required" {
		t.Fatal(result, err)
	}
	s.DB().Model(other).Update("status", model.Expired)
	if err := s.ReconcilePayment(ctx, other.TradeNo, "binance", tx.EventID, "operator", ""); err == nil {
		t.Fatal("unaudited recovery")
	}
	if err := s.ReconcilePayment(ctx, other.TradeNo, "binance", tx.EventID, "operator", "verified merchant unfulfilled and agreed"); err != nil {
		t.Fatal(err)
	}
	if err := s.ReconcilePayment(ctx, other.TradeNo, "binance", tx.EventID, "operator", "duplicate"); err == nil {
		t.Fatal("double claim")
	}
	var n int64
	s.DB().Model(&model.PaymentRecovery{}).Count(&n)
	if n != 1 {
		t.Fatal(n)
	}
	for _, state := range []string{model.Canceled, model.Ready} {
		no := fmt.Sprint("reject-", state)
		v := readyOrder(t, s, no)
		s.DB().Model(v).Update("status", state)
		bad := evidence(v, "wrong-"+state)
		bad.Units++
		if _, err := s.Process(ctx, bad, ""); err != nil {
			t.Fatal(err)
		}
		if err := s.ReconcilePayment(ctx, no, bad.Provider, bad.EventID, "operator", "attempt mismatch"); err == nil {
			t.Fatal("invalid recovery", state)
		}
	}
}

func TestNaturalExpiryRequiresPostBoundFullWindowRescan(t *testing.T) {
	ctx := context.Background()
	s := testStore(t)
	o := readyOrder(t, s, "final-pass")
	boundary := time.Now().Add(-time.Minute)
	s.DB().Model(&model.PaymentAttempt{}).Where("order_id = ?", o.ID).Update("settle_until", boundary)
	s.ScanResult(ctx, "binance", time.Now(), nil)
	var a model.PaymentAttempt
	s.DB().Where("order_id = ?", o.ID).First(&a)
	old := model.HistoricalScan{Automatic: true, OrderID: o.ID, AttemptID: a.ID, Provider: a.Provider, Receiver: a.Receiver, From: a.SelectedAt, To: a.ExpiresAt, Cursor: a.ExpiresAt, State: "completed", Operator: "system", Reason: "started before bound", CreatedAt: boundary.Add(-time.Minute)}
	if err := s.DB().Create(&old).Error; err != nil {
		t.Fatal(err)
	}
	if err := s.Expire(ctx, 0); err != nil {
		t.Fatal(err)
	}
	fresh, _ := s.GetByTradeNo(ctx, o.TradeNo)
	if fresh.Status != model.Ready {
		t.Fatal("pre-bound pass was treated as stable visibility")
	}
	completedSettlementFixture(t, s, o.ID)
	if err := s.Expire(ctx, 0); err != nil {
		t.Fatal(err)
	}
	fresh, _ = s.GetByTradeNo(ctx, o.TradeNo)
	if fresh.Status != model.Expired {
		t.Fatal("final historical coverage did not release order")
	}
}
