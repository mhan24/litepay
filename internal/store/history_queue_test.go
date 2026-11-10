package store

import (
	"context"
	"errors"
	"fmt"
	"gorm.io/gorm"
	"litepay/internal/model"
	"testing"
	"time"
)

func TestSettlementCohortQueue(t *testing.T) {
	s := testStore(t)
	o := readyOrder(t, s, "cohort")
	selected := time.Now().Add(-20 * time.Minute)
	expires := time.Now().Add(-10 * time.Minute)
	s.DB().Model(&model.PaymentAttempt{}).Where("order_id=?", o.ID).Updates(map[string]any{"selected_at": selected, "expires_at": expires})
	if err := s.QueueSettlementScan(context.Background(), "binance", "receiver", time.Minute); err != nil {
		t.Fatal(err)
	}
}

func TestHistoricalCoverageIsSharedAndSkipsEmptyGaps(t *testing.T) {
	s := testStore(t)
	ctx := context.Background()
	now := time.Now()
	var orders []model.Order
	start := now.Truncate(time.Hour).Add(-150 * time.Hour)
	for i := range 960 {
		selected := start.Add(time.Duration(i/20)*2*time.Hour + 5*time.Minute)
		expires := selected.Add(10 * time.Minute)
		orders = append(orders, model.Order{TradeNo: fmt.Sprint("history-", i), OutTradeNo: fmt.Sprint("history-", i), OriginUnits: 1000000, Status: model.Ready, Provider: "binance", Receiver: "receiver", Currency: "USDT", PayUnits: 1000000 + int64(i), Precision: 6, SelectedAt: &selected, ExpiresAt: expires, NextExpiryCheckAt: expires.Add(LateVisibilityWindow)})
	}
	if err := s.DB().CreateInBatches(&orders, 100).Error; err != nil {
		t.Fatal(err)
	}
	attempts := make([]model.PaymentAttempt, len(orders))
	for i, o := range orders {
		attempts[i] = model.PaymentAttempt{OrderID: o.ID, Provider: o.Provider, Receiver: o.Receiver, Currency: o.Currency, Units: o.PayUnits, SelectedAt: *o.SelectedAt, ExpiresAt: o.ExpiresAt, SettleUntil: o.ExpiresAt.Add(LateVisibilityWindow)}
	}
	if err := s.DB().CreateInBatches(&attempts, 100).Error; err != nil {
		t.Fatal(err)
	}
	for range 48 {
		if err := s.QueueSettlementScan(ctx, "binance", "receiver", time.Minute); err != nil {
			t.Fatal(err)
		}
		job, err := s.NextHistoricalScan(ctx, "binance")
		if err != nil {
			t.Fatal(err)
		}
		if job.Priority != 2 || job.To.Sub(job.From) > 15*time.Minute {
			t.Fatal("empty historical gap queried", job)
		}
		after := job
		after.Cursor = job.To
		after.State = "completed"
		if err = s.SaveHistoricalScan(ctx, job, after, nil); err != nil {
			t.Fatal(err)
		}
	}
	if err := s.QueueSettlementScan(ctx, "binance", "receiver", time.Minute); err != nil {
		t.Fatal(err)
	}
	if _, err := s.NextHistoricalScan(ctx, "binance"); !errors.Is(err, gorm.ErrRecordNotFound) {
		t.Fatal("completed coverage requeued", err)
	}
	s.ScanResult(ctx, "binance", now, nil)
	for range 10 {
		if err := s.Expire(ctx, 0); err != nil {
			t.Fatal(err)
		}
	}
	var ready int64
	s.DB().Model(&model.Order{}).Where("status=?", model.Ready).Count(&ready)
	if ready != 0 {
		t.Fatal("shared final coverage failed expiry", ready)
	}
	t.Log("960 orders across 48 occupied hourly cohorts: 48 shared final passes, no requests across empty gaps")
}

func TestHistoricalShrinkPreservesCoverageCheckpoint(t *testing.T) {
	s := testStore(t)
	o := readyOrder(t, s, "shrink")
	var a model.PaymentAttempt
	s.DB().Where("order_id=?", o.ID).First(&a)
	job := model.HistoricalScan{OrderID: o.ID, AttemptID: a.ID, Provider: "binance", Receiver: "receiver", From: *o.SelectedAt, To: o.ExpiresAt, Cursor: *o.SelectedAt, State: "pending", Operator: "test", Reason: "dense window"}
	if err := s.DB().Create(&job).Error; err != nil {
		t.Fatal(err)
	}
	if err := s.ShrinkHistoricalChunk(context.Background(), job, job.Cursor.Add(time.Hour)); err != nil {
		t.Fatal(err)
	}
	var next model.HistoricalScan
	s.DB().First(&next, job.ID)
	if !next.Cursor.Equal(job.Cursor) || next.ChunkMillis != 1800000 || next.State != "pending" {
		t.Fatal(next)
	}
}

func TestQueueWaitingMetrics(t *testing.T) {
	s := testStore(t)
	ctx := context.Background()
	o := readyOrder(t, s, "queue-age")
	if _, err := s.Process(ctx, evidence(o, "queue-age-event"), ""); err != nil {
		t.Fatal(err)
	}
	s.DB().Model(&model.Order{}).Where("id=?", o.ID).Update("paid_at", time.Now().Add(-3*time.Minute))
	stats, err := s.QueueStats(ctx)
	if err != nil || stats.CallbackWaiting != 1 || stats.OldestCallbackSeconds < 179 || stats.OldestCallbackSeconds > 182 {
		t.Fatal(stats, err)
	}
	s.DB().Model(&model.Callback{}).Where("order_id=?", o.ID).Update("state", "merchant_review")
	stats, err = s.QueueStats(ctx)
	if err != nil || stats.CallbackWaiting != 0 || stats.CallbackReview != 1 {
		t.Fatal(stats, err)
	}
}
