package store

import (
	"context"
	"litepay/internal/model"
	"testing"
	"time"
)

func TestSharedCoverageSkipsOverlapWithoutRefreshingEvidence(t *testing.T) {
	s := testStore(t)
	ctx := context.Background()
	o := readyOrder(t, s, "proof")
	from := time.Now().Add(-30 * time.Hour)
	to := from.Add(24 * time.Hour)
	observed := time.Now().Add(-time.Minute)
	proofJob := model.HistoricalScan{Automatic: true, OrderID: o.ID, Provider: "binance", Receiver: "receiver", From: from, To: to, Cursor: from, CreatedAt: observed, State: "pending", Operator: "system", Reason: "proof"}
	if err := s.DB().Create(&proofJob).Error; err != nil {
		t.Fatal(err)
	}
	done := proofJob
	done.Cursor = to
	done.State = "completed"
	if err := s.SaveHistoricalScan(ctx, proofJob, done, nil); err != nil {
		t.Fatal(err)
	}
	next := model.HistoricalScan{Automatic: true, OrderID: o.ID, Provider: proofJob.Provider, Receiver: proofJob.Receiver, From: from.Add(time.Hour), To: to.Add(time.Hour), Cursor: from.Add(time.Hour), CoverageAfter: time.Now(), RequiredAfter: observed.Add(-time.Minute), State: "pending", Operator: "system", Reason: "overlap"}
	s.DB().Create(&next)
	reused, err := s.ReuseHistoricalCoverage(ctx, next)
	if err != nil || !reused.Cursor.Equal(to) || reused.State != "pending" || !reused.CoverageAfter.Equal(observed) {
		t.Fatal(reused, err)
	}
	var proofs int64
	s.DB().Model(&model.HistoryCoverage{}).Count(&proofs)
	if proofs != 1 {
		t.Fatal("reuse manufactured fresh proof", proofs)
	}
	newer := next
	newer.ID = 0
	newer.Cursor = next.From
	newer.RequiredAfter = observed.Add(time.Second)
	s.DB().Create(&newer)
	unchanged, err := s.ReuseHistoricalCoverage(ctx, newer)
	if err != nil || !unchanged.Cursor.Equal(newer.Cursor) {
		t.Fatal("pre-bound evidence reused", unchanged, err)
	}
	gapEnd, err := s.UncoveredChunkEnd(ctx, next, next.From.Add(time.Hour))
	if err != nil || !gapEnd.Equal(next.From.Add(time.Hour)) {
		t.Fatal(gapEnd, err)
	}
}

func TestCompletedReuseCannotPretendToBeANewerFinalPass(t *testing.T) {
	s := testStore(t)
	ctx := context.Background()
	o := readyOrder(t, s, "proof-expiry")
	selected := time.Now().Add(-50 * time.Hour)
	expires := selected.Add(time.Hour)
	bound := expires.Add(LateVisibilityWindow)
	s.DB().Model(o).Updates(map[string]any{"selected_at": selected, "expires_at": expires, "next_expiry_check_at": bound})
	s.DB().Model(&model.PaymentAttempt{}).Where("order_id=?", o.ID).Updates(map[string]any{"selected_at": selected, "expires_at": expires, "settle_until": bound})
	var a model.PaymentAttempt
	s.DB().Where("order_id=?", o.ID).First(&a)
	s.DB().Create(&model.HistoricalScan{Automatic: true, OrderID: o.ID, AttemptID: a.ID, Provider: a.Provider, Receiver: a.Receiver, From: selected, To: expires, Cursor: expires, State: "completed", CoverageAfter: bound.Add(-time.Minute), CreatedAt: time.Now(), Operator: "system", Reason: "reused old proof"})
	s.ScanResult(ctx, a.Provider, time.Now(), nil)
	if err := s.Expire(ctx, 0); err != nil {
		t.Fatal(err)
	}
	current, _ := s.GetByTradeNo(ctx, o.TradeNo)
	if current.Status != model.Ready {
		t.Fatal("old proof refreshed by job completion", current.Status)
	}
	completedSettlementFixture(t, s, o.ID)
	if err := s.Expire(ctx, 0); err != nil {
		t.Fatal(err)
	}
	current, _ = s.GetByTradeNo(ctx, o.TradeNo)
	if current.Status != model.Expired {
		t.Fatal(current.Status)
	}
}
