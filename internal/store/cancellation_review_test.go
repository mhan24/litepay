package store

import (
	"context"
	"errors"
	"gorm.io/gorm"
	"litepay/internal/model"
	"testing"
)

func TestCanceledDepositRefundRecordPreservesEvidenceAndRequiresNewRecoveryReview(t *testing.T) {
	s := testStore(t)
	ctx := context.Background()
	o := readyOrder(t, s, "cancel-refund")
	if _, err := s.CancelReference(ctx, o.TradeNo, o.OutTradeNo); err != nil {
		t.Fatal(err)
	}
	event := evidence(o, "canceled-real-deposit")
	if result, err := s.Process(ctx, event, ""); err != nil || result != "order_closed" {
		t.Fatal(result, err)
	}
	var stored model.Transaction
	if err := s.DB().First(&stored, "provider=? AND event_id=?", event.Provider, event.EventID).Error; err != nil || stored.OrderID != nil || stored.AttemptID == nil {
		t.Fatal(stored, err)
	}
	stats, statsErr := s.QueueStats(ctx)
	if statsErr != nil || stats.CancellationReviewWaiting != 1 {
		t.Fatal("open review not shown in operations", stats, statsErr)
	}
	var n int64
	s.DB().Model(&model.CancellationReview{}).Count(&n)
	if n != 1 {
		t.Fatal(n)
	}
	if err := s.RecordCancellationRefund(ctx, o.TradeNo, event.Provider, event.EventID, "operator", "", "checked"); err == nil {
		t.Fatal("empty refund reference accepted")
	}
	if err := s.RecordCancellationRefund(ctx, o.TradeNo, event.Provider, "unknown", "operator", "refund-1", "checked"); err == nil {
		t.Fatal("typed evidence accepted")
	}
	if err := s.RecordCancellationRefund(ctx, o.TradeNo, event.Provider, event.EventID, "operator", "refund-1", "actual external refund verified"); err != nil {
		t.Fatal(err)
	}
	if ready, err := s.CancellationRefundReviewed(ctx, o.TradeNo, o.OutTradeNo); err != nil || !ready {
		t.Fatal(ready, err)
	}
	if ready, err := s.CancellationRefundReviewed(ctx, o.TradeNo, "another-merchant"); err != nil || ready {
		t.Fatal("wrong identity reviewed", ready, err)
	}
	if err := s.RecordCancellationRefund(ctx, o.TradeNo, event.Provider, event.EventID, "operator", "refund-2", "again"); err == nil {
		t.Fatal("duplicate refund record accepted")
	}
	// Replaying source evidence must neither lose the review nor create a second refund audit.
	if _, err := s.Process(ctx, event, ""); err != nil {
		t.Fatal(err)
	}
	s.DB().Model(&model.CancellationReviewAudit{}).Count(&n)
	if n != 1 {
		t.Fatal(n)
	}
	if err := s.BeginRecovery(ctx, "test restored snapshot"); err != nil {
		t.Fatal(err)
	}
	if ready, err := s.CancellationRefundReviewed(ctx, o.TradeNo, o.OutTradeNo); err != nil || ready {
		t.Fatal("old generation refund approval replayed", ready, err)
	}
	if err := s.RecordCancellationRefund(ctx, o.TradeNo, event.Provider, event.EventID, "operator", "refund-1", "rechecked existing refund after restore; no second transfer"); err != nil {
		t.Fatal(err)
	}
	s.DB().Model(&model.CancellationReviewAudit{}).Count(&n)
	if n != 2 {
		t.Fatal(n)
	}
	fresh, _ := s.GetByTradeNo(ctx, o.TradeNo)
	if fresh.Status != model.Canceled {
		t.Fatal(fresh)
	}
	var after model.Transaction
	s.DB().First(&after, "provider=? AND event_id=?", event.Provider, event.EventID)
	if after.OrderID != nil || after.Units != stored.Units || after.State != model.Confirmed {
		t.Fatal("refund record mutated incoming funds", after)
	}
	s.DB().Model(&model.Callback{}).Count(&n)
	if n != 0 {
		t.Fatal("canceled funds generated delivery")
	}
}

func TestMerchantCancelAfterPaidHoldsCallbackAndRefundClosesNotificationQueue(t *testing.T) {
	s := testStore(t)
	ctx := context.Background()
	o := readyOrder(t, s, "paid-refund")
	event := evidence(o, "paid-cancel-event")
	if _, err := s.Process(ctx, event, ""); err != nil {
		t.Fatal(err)
	}
	task, _, err := s.ReserveCallback(ctx, 5, 15)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = s.CancelReference(ctx, o.TradeNo, o.OutTradeNo); err == nil {
		t.Fatal("paid order falsely canceled")
	}
	fresh, _ := s.GetByTradeNo(ctx, o.TradeNo)
	if fresh.Status != model.Paid {
		t.Fatal(fresh)
	}
	if allowed, e := s.CallbackCanSend(ctx, *task); e != nil || allowed {
		t.Fatal("cancellation did not fence unsent delivery", allowed, e)
	}
	if err = s.RecordCancellationRefund(ctx, o.TradeNo, event.Provider, event.EventID, "operator", "refund-1", "checked"); err == nil {
		t.Fatal("refund recorded during in-flight delivery")
	}
	if err = s.ReleaseCallback(ctx, *task); err != nil {
		t.Fatal(err)
	}
	if _, _, e := s.ReserveCallback(ctx, 5, 15); !errors.Is(e, gorm.ErrRecordNotFound) {
		t.Fatal("held callback reserved", e)
	}
	if err = s.RetryCallback(ctx, o.TradeNo, "operator", "retry", 5); err == nil {
		t.Fatal("manual retry bypassed cancellation review")
	}
	if err = s.RecordCancellationRefund(ctx, o.TradeNo, event.Provider, event.EventID, "operator", "refund-proof", "actual refund completed"); err != nil {
		t.Fatal(err)
	}
	var cb model.Callback
	s.DB().First(&cb, "order_id=?", o.ID)
	if cb.State != "canceled_refunded" || cb.DeliveredAt != nil || cb.Attempts != 0 {
		t.Fatal(cb)
	}
	stats, e := s.QueueStats(ctx)
	if e != nil || stats.CallbackWaiting != 0 || stats.CancellationReviewWaiting != 0 {
		t.Fatal(stats, e)
	}
	var proof model.Transaction
	s.DB().First(&proof, "provider=? AND event_id=?", event.Provider, event.EventID)
	if proof.OrderID == nil || *proof.OrderID != o.ID || proof.State != model.Confirmed {
		t.Fatal("original payment ownership changed", proof)
	}
}

func TestAdminCannotTurnPaidOrderIntoMerchantCancellation(t *testing.T) {
	s := testStore(t)
	ctx := context.Background()
	o := readyOrder(t, s, "admin-cancel-paid")
	if _, err := s.Process(ctx, evidence(o, "admin-paid-event"), ""); err != nil {
		t.Fatal(err)
	}
	if err := s.Cancel(ctx, o.TradeNo); err == nil {
		t.Fatal("paid admin cancel accepted")
	}
	var n int64
	s.DB().Model(&model.CancellationReview{}).Count(&n)
	if n != 0 {
		t.Fatal("admin close manufactured merchant cancellation")
	}
	if _, _, err := s.ReserveCallback(ctx, 5, 15); err != nil {
		t.Fatal(err)
	}
}

func TestAdditionalDepositAfterCancellationRequiresItsOwnRefundAudit(t *testing.T) {
	s := testStore(t)
	ctx := context.Background()
	o := readyOrder(t, s, "cancel-extra-funds")
	first := evidence(o, "first-cancel-funds")
	if _, err := s.Process(ctx, first, ""); err != nil {
		t.Fatal(err)
	}
	if _, err := s.CancelReference(ctx, o.TradeNo, o.OutTradeNo); err == nil {
		t.Fatal("paid cancellation accepted")
	}
	if err := s.RecordCancellationRefund(ctx, o.TradeNo, first.Provider, first.EventID, "operator", "refund-one", "first actual refund"); err != nil {
		t.Fatal(err)
	}
	second := evidence(o, "second-cancel-funds")
	if result, err := s.Process(ctx, second, ""); err != nil || result != "additional_payment" {
		t.Fatal(result, err)
	}
	if reviewed, err := s.CancellationRefundReviewed(ctx, o.TradeNo, o.OutTradeNo); err != nil || reviewed {
		t.Fatal("first refund hid second deposit", reviewed, err)
	}
	stats, err := s.QueueStats(ctx)
	if err != nil || stats.CancellationReviewWaiting != 1 {
		t.Fatal(stats, err)
	}
	if err := s.RecordCancellationRefund(ctx, o.TradeNo, second.Provider, second.EventID, "operator", "refund-two", "second actual refund"); err != nil {
		t.Fatal(err)
	}
	if reviewed, err := s.CancellationRefundReviewed(ctx, o.TradeNo, o.OutTradeNo); err != nil || !reviewed {
		t.Fatal(reviewed, err)
	}
	var n int64
	s.DB().Model(&model.Transaction{}).Where("order_id=?", o.ID).Count(&n)
	if n != 1 {
		t.Fatal("second deposit changed unique claim", n)
	}
	s.DB().Model(&model.CancellationReviewAudit{}).Count(&n)
	if n != 2 {
		t.Fatal(n)
	}
}
