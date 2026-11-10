package store

import (
	"context"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"gorm.io/gorm"
	"gorm.io/gorm/logger"
	"litepay/internal/model"
)

type queryCounter struct {
	logger.Interface
	count atomic.Int64
}

func (q *queryCounter) Trace(ctx context.Context, begin time.Time, sql func() (string, int64), err error) {
	q.count.Add(1)
}
func seedFutureOrders(tb testing.TB, s *Store, n int) {
	tb.Helper()
	now := time.Now()
	orders := make([]model.Order, n)
	for i := range orders {
		orders[i] = model.Order{TradeNo: fmt.Sprint("future-", i), OutTradeNo: fmt.Sprint("future-", i), OriginUnits: 10000000, Provider: "binance", Currency: "USDT", Receiver: "receiver", PayUnits: 10000000 + int64(i), Precision: 6, Status: model.Ready, SelectedAt: &now, ExpiresAt: now.Add(time.Hour), NextExpiryCheckAt: now.Add(49 * time.Hour)}
	}
	if err := s.DB().CreateInBatches(&orders, 200).Error; err != nil {
		tb.Fatal(err)
	}
}
func TestExpiryFutureBacklogHasBoundedQueryCount(t *testing.T) {
	for _, n := range []int{100, 1000, 10000} {
		t.Run(fmt.Sprint(n), func(t *testing.T) {
			s := testStore(t)
			seedFutureOrders(t, s, n)
			counter := &queryCounter{Interface: logger.Default.LogMode(logger.Silent)}
			s.db = s.db.Session(&gorm.Session{Logger: counter})
			if err := s.Expire(context.Background(), 120); err != nil {
				t.Fatal(err)
			}
			if count := counter.count.Load(); count > 6 {
				t.Fatal("expiry query count grew with backlog", count)
			} else {
				t.Logf("future orders=%d, expiry statements=%d", n, count)
			}
		})
	}
}
func BenchmarkExpireFutureBacklog(b *testing.B) {
	for _, n := range []int{100, 1000, 10000} {
		b.Run(fmt.Sprint(n), func(b *testing.B) {
			s, err := Open(":memory:")
			if err != nil {
				b.Fatal(err)
			}
			defer s.Close()
			seedFutureOrders(b, s, n)
			sqlDB, _ := s.DB().DB()
			before := sqlDB.Stats()
			b.ResetTimer()
			b.ReportAllocs()
			for range b.N {
				if err = s.Expire(context.Background(), 120); err != nil {
					b.Fatal(err)
				}
			}
			b.StopTimer()
			after := sqlDB.Stats()
			b.ReportMetric(float64(after.WaitDuration-before.WaitDuration)/float64(b.N), "dbwait-ns/op")
		})
	}
}

func seedDueOrders(tb testing.TB, s *Store, n int) {
	tb.Helper()
	now := time.Now()
	orders := make([]model.Order, n)
	selected, expires, deadline := now.Add(-50*time.Hour), now.Add(-49*time.Hour), now.Add(-time.Hour)
	for i := range orders {
		orders[i] = model.Order{TradeNo: fmt.Sprint("due-", i), OutTradeNo: fmt.Sprint("due-", i), OriginUnits: 30000000, Provider: "binance", Currency: "USDT", Receiver: "receiver", PayUnits: 30000000 + int64(i), Precision: 6, Status: model.Ready, SelectedAt: &selected, ExpiresAt: expires, NextExpiryCheckAt: deadline}
	}
	if err := s.DB().CreateInBatches(&orders, 200).Error; err != nil {
		tb.Fatal(err)
	}
	attempts := make([]model.PaymentAttempt, n)
	for i, o := range orders {
		attempts[i] = model.PaymentAttempt{OrderID: o.ID, Provider: o.Provider, Receiver: o.Receiver, Currency: o.Currency, Units: o.PayUnits, Precision: 6, SelectedAt: selected, ExpiresAt: expires, SettleUntil: deadline}
	}
	if err := s.DB().CreateInBatches(&attempts, 200).Error; err != nil {
		tb.Fatal(err)
	}
}
func TestExpiryDueBatchIsBoundedAndFair(t *testing.T) {
	s := testStore(t)
	seedDueOrders(t, s, 1000)
	counter := &queryCounter{Interface: logger.Default.LogMode(logger.Silent)}
	s.db = s.db.Session(&gorm.Session{Logger: counter})
	now := time.Now()
	for batch := 1; batch <= 2; batch++ {
		before := counter.count.Load()
		if err := s.Expire(context.Background(), 120); err != nil {
			t.Fatal(err)
		}
		if count := counter.count.Load() - before; count > 9 {
			t.Fatal("per-order SQL returned", count)
		}
		var postponed int64
		s.DB().Model(&model.Order{}).Where("next_expiry_check_at > ?", now).Count(&postponed)
		if postponed != int64(batch*ExpiryBatchSize) {
			t.Fatal("due orders starved or unbounded batch", postponed)
		}
	}
}
func BenchmarkExpireDueBacklog(b *testing.B) {
	for _, n := range []int{100, 1000, 10000} {
		b.Run(fmt.Sprint(n), func(b *testing.B) {
			s, err := Open(":memory:")
			if err != nil {
				b.Fatal(err)
			}
			defer s.Close()
			seedDueOrders(b, s, n)
			b.ReportAllocs()
			b.ResetTimer()
			for range b.N {
				b.StopTimer()
				s.DB().Model(&model.Order{}).Where("status = ?", model.Ready).Update("next_expiry_check_at", time.Now().Add(-time.Minute))
				b.StartTimer()
				if err = s.Expire(context.Background(), 120); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
