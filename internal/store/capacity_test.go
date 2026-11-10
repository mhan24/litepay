package store

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"litepay/internal/amount"
	"litepay/internal/model"
)

func TestContinuousSamePriceCapacity(t *testing.T) {
	for _, n := range []int{100, 500, 1000} {
		t.Run(fmt.Sprint(n), func(t *testing.T) {
			s := testStore(t)
			ctx := context.Background()
			used := map[int64]bool{}
			fine := 0
			start := time.Now()
			for i := 0; i < n; i++ {
				no := fmt.Sprintf("capacity-%d", i)
				o := readyOrder(t, s, no)
				if used[o.PayUnits] || o.PayUnits < 10*amount.Scale || o.PayUnits > 10*amount.Scale+990000 {
					t.Fatal("reused or enlarged price envelope", o.PayUnits)
				}
				used[o.PayUnits] = true
				if o.Precision == 6 {
					fine++
				}
				if _, err := s.Process(ctx, evidence(o, fmt.Sprintf("capacity-deposit-%d", i)), ""); err != nil {
					t.Fatal(err)
				}
			}
			var slots, claims, callbacks int64
			s.DB().Model(&model.Slot{}).Count(&slots)
			s.DB().Model(&model.Transaction{}).Where("order_id IS NOT NULL").Count(&claims)
			s.DB().Model(&model.Callback{}).Count(&callbacks)
			if slots != int64(n) || claims != int64(n) || callbacks != int64(n) || n > 100 && fine == 0 {
				t.Fatal(slots, claims, callbacks, fine)
			}
			t.Logf("%d paid same-price orders; %.1f ms/order; fine instructions=%d; 48-hour tiers retained=%d", n, float64(time.Since(start).Microseconds())/1000/float64(n), fine, slots)
		})
	}
}

func TestReselectionAbuseCannotGrowPool(t *testing.T) {
	ctx := context.Background()
	s := testStore(t)
	o := readyOrder(t, s, "abuse")
	for range 1000 {
		if _, err := s.Select(ctx, o.TradeNo, "binance", "USDC", "receiver", 2, 10000, 10000, 99, 120); !errors.Is(err, ErrSelectionThrottled) {
			t.Fatal(err)
		}
	}
	var attempts, slots int64
	s.DB().Model(&model.PaymentAttempt{}).Count(&attempts)
	s.DB().Model(&model.Slot{}).Count(&slots)
	if attempts != 1 || slots != 1 {
		t.Fatal("burst consumed slots", attempts, slots)
	}
	var usdc int64
	for change := 1; change < MaxSelectionChanges; change++ {
		allowSelectionFixture(t, s, o.TradeNo)
		currency := "USDC"
		if change%2 == 0 {
			currency = "USDT"
		}
		next, err := s.Select(ctx, o.TradeNo, "binance", currency, "receiver", 2, 10000, 10000, 99, 120)
		if err != nil {
			t.Fatal(err)
		}
		if currency == "USDT" && next.PayUnits != o.PayUnits {
			t.Fatal("return to old method allocated again")
		}
		if currency == "USDC" {
			if usdc != 0 && usdc != next.PayUnits {
				t.Fatal("USDC instruction changed")
			}
			usdc = next.PayUnits
		}
	}
	allowSelectionFixture(t, s, o.TradeNo)
	if _, err := s.Select(ctx, o.TradeNo, "binance", "USDT", "receiver", 2, 10000, 10000, 99, 120); !errors.Is(err, ErrSelectionLimit) {
		t.Fatal("lifetime switch limit missing", err)
	}
	s.DB().Model(&model.PaymentAttempt{}).Count(&attempts)
	s.DB().Model(&model.Slot{}).Count(&slots)
	if attempts != 2 || slots != 2 {
		t.Fatal("switch loop consumed tiers", attempts, slots)
	}
	// Idempotent retries of the current choice remain valid after hitting the switch limit.
	current, _ := s.GetByTradeNo(ctx, o.TradeNo)
	if _, err := s.Select(ctx, o.TradeNo, current.Provider, current.Currency, current.Receiver, 2, 10000, 10000, 99, 120); err != nil {
		t.Fatal(err)
	}
}

func TestPerOrderInstructionLimit(t *testing.T) {
	s := testStore(t)
	ctx := context.Background()
	o := readyOrder(t, s, "unique-limit")
	for i := 1; i < MaxPaymentAttempts; i++ {
		allowSelectionFixture(t, s, o.TradeNo)
		if _, err := s.Select(ctx, o.TradeNo, "binance", "USDT", fmt.Sprint("receiver-", i), 2, 10000, 10000, 99, 120); err != nil {
			t.Fatal(err)
		}
	}
	allowSelectionFixture(t, s, o.TradeNo)
	if _, err := s.Select(ctx, o.TradeNo, "binance", "USDT", "excess-receiver", 2, 10000, 10000, 99, 120); err == nil {
		t.Fatal("unique instruction cap missing")
	}
	var n int64
	s.DB().Model(&model.Slot{}).Where("order_id = ?", o.ID).Count(&n)
	if n != MaxPaymentAttempts {
		t.Fatal(n)
	}
}

func TestFiniteAmountEnvelopeFailsWithoutPartialReservation(t *testing.T) {
	s := testStore(t)
	ctx := context.Background()
	for i := range 3 {
		no := fmt.Sprint("finite-", i)
		_, err := s.Create(ctx, model.Order{TradeNo: no, OutTradeNo: no, OriginUnits: 10000000, ExpiresAt: time.Now().Add(time.Hour)})
		if err != nil {
			t.Fatal(err)
		}
		_, err = s.Select(ctx, no, "binance", "USDT", "receiver", 6, 1, 1, 1, 120)
		if (i < 2 && err != nil) || (i == 2 && err == nil) {
			t.Fatal(i, err)
		}
	}
	var slots, attempts int64
	s.DB().Model(&model.Slot{}).Count(&slots)
	s.DB().Model(&model.PaymentAttempt{}).Count(&attempts)
	o, err := s.GetByTradeNo(ctx, "finite-2")
	if err != nil || slots != 2 || attempts != 2 || o.Status != model.Pending || o.SelectionChanges != 0 {
		t.Fatal(slots, attempts, o, err)
	}
}
