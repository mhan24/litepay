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

func TestMerchantCancellationCollectsAllEarlierDepositsBeforeRefundCompletes(t *testing.T) {
	s := testStore(t)
	ctx := context.Background()
	o := readyOrder(t, s, "cancel-two-earlier-deposits")
	first, second := evidence(o, "earlier-A"), evidence(o, "earlier-B")
	for i, e := range []model.Transaction{first, second} {
		want := []string{"paid", "additional_payment"}[i]
		if result, err := s.Process(ctx, e, ""); err != nil || result != want {
			t.Fatal(result, err)
		}
	}
	for i := 0; i < 2; i++ {
		if _, err := s.CancelReference(ctx, o.TradeNo, o.OutTradeNo); err == nil {
			t.Fatal("paid order was canceled")
		}
	}
	var reviews int64
	if err := s.DB().Model(&model.CancellationReview{}).Where("order_id=?", o.ID).Count(&reviews).Error; err != nil || reviews != 2 {
		t.Fatal("earlier additional receipt missing or duplicated", reviews, err)
	}
	if err := s.RecordCancellationRefund(ctx, o.TradeNo, first.Provider, first.EventID, "operator", "refund-A", "verified actual refund A"); err != nil {
		t.Fatal(err)
	}
	if summary, err := s.CancellationReviewSummary(ctx, o.ID); err != nil || summary.Current() || summary.Status() != "pending" || summary.Pending != 1 {
		t.Fatal("A refund prematurely completed B", summary, err)
	}
	if reviewed, err := s.CancellationRefundReviewed(ctx, o.TradeNo, o.OutTradeNo); err != nil || reviewed {
		t.Fatal(reviewed, err)
	}
	var cb model.Callback
	s.DB().First(&cb, "order_id=?", o.ID)
	if cb.State != "merchant_review" {
		t.Fatal("partial refund terminated notification hold", cb)
	}
	if _, _, err := s.ReserveCallback(ctx, 5, 15); !errors.Is(err, gorm.ErrRecordNotFound) {
		t.Fatal("partial refund allowed callback", err)
	}
	stats, err := s.QueueStats(ctx)
	if err != nil || stats.CancellationReviewWaiting != 1 {
		t.Fatal(stats, err)
	}
	if err = s.RecordCancellationRefund(ctx, o.TradeNo, second.Provider, second.EventID, "operator", "refund-B", "verified actual refund B"); err != nil {
		t.Fatal(err)
	}
	if summary, err := s.CancellationReviewSummary(ctx, o.ID); err != nil || !summary.Current() || summary.Status() != "refund_recorded" {
		t.Fatal(summary, err)
	}
	s.DB().First(&cb, "order_id=?", o.ID)
	if cb.State != "canceled_refunded" {
		t.Fatal(cb)
	}
	var stored model.Transaction
	s.DB().First(&stored, "provider=? AND event_id=?", second.Provider, second.EventID)
	if stored.OrderID != nil || stored.AttemptID == nil || stored.Reason != "additional_payment" || stored.State != model.Confirmed {
		t.Fatal("extra payment evidence mutated", stored)
	}
	fresh, _ := s.GetByTradeNo(ctx, o.TradeNo)
	if fresh.Status != model.Paid {
		t.Fatal("original paid status lost", fresh)
	}
}

func TestCancellationCannotTreatAmbiguousAdditionalReceiptAsRefunded(t *testing.T) {
	s := testStore(t)
	ctx := context.Background()
	o := readyOrder(t, s, "cancel-uncertain-extra")
	first, extra := evidence(o, "unique-A"), evidence(o, "uncertain-B")
	for _, e := range []model.Transaction{first, extra} {
		if _, err := s.Process(ctx, e, ""); err != nil {
			t.Fatal(err)
		}
	}
	// A separately imported instruction makes the extra receipt's ownership uncertain.
	other := readyOrder(t, s, "other-uncertain-extra")
	var a model.PaymentAttempt
	s.DB().Where("order_id=?", o.ID).First(&a)
	a.ID = 0
	a.OrderID = other.ID
	if err := s.DB().Create(&a).Error; err != nil {
		t.Fatal(err)
	}
	if _, err := s.CancelReference(ctx, o.TradeNo, o.OutTradeNo); err == nil {
		t.Fatal("paid order was canceled")
	}
	if err := s.RecordCancellationRefund(ctx, o.TradeNo, first.Provider, first.EventID, "operator", "refund-A", "claimed payment refunded"); err != nil {
		t.Fatal(err)
	}
	summary, err := s.CancellationReviewSummary(ctx, o.ID)
	if err != nil || summary.Current() || summary.Uncovered != 1 || summary.Status() != "pending" {
		t.Fatal("uncertain receipt silently considered refunded", summary, err)
	}
	stats, statsErr := s.QueueStats(ctx)
	queue, queueErr := s.CancellationReviewQueue(ctx)
	if statsErr != nil || queueErr != nil || stats.CancellationReviewWaiting != 1 || len(queue) != 1 || queue[0].EventID != extra.EventID {
		t.Fatal("uncertain receipt disappeared from operations", stats, queue, statsErr, queueErr)
	}
	if err := s.RecordCancellationRefund(ctx, o.TradeNo, extra.Provider, extra.EventID, "operator", "fake", "uncertain"); err == nil {
		t.Fatal("uncertain evidence approved")
	}
}

func TestEveryRefundReviewRequiresItsOwnAuditInCurrentGeneration(t *testing.T) {
	s := testStore(t)
	ctx := context.Background()
	o := readyOrder(t, s, "cancel-every-audit")
	first, second := evidence(o, "audit-A"), evidence(o, "audit-B")
	for _, e := range []model.Transaction{first, second} {
		if _, err := s.Process(ctx, e, ""); err != nil {
			t.Fatal(err)
		}
	}
	s.CancelReference(ctx, o.TradeNo, o.OutTradeNo)
	for _, e := range []model.Transaction{first, second} {
		if err := s.RecordCancellationRefund(ctx, o.TradeNo, e.Provider, e.EventID, "operator", "refund-"+e.EventID, "actual refund checked"); err != nil {
			t.Fatal(err)
		}
	}
	if err := s.DB().Where("review_id IN (SELECT id FROM cancellation_reviews WHERE event_id=?)", second.EventID).Delete(&model.CancellationReviewAudit{}).Error; err != nil {
		t.Fatal(err)
	}
	if reviewed, err := s.CancellationRefundReviewed(ctx, o.TradeNo, o.OutTradeNo); err != nil || reviewed {
		t.Fatal("one audit incorrectly validated both receipts", reviewed, err)
	}
	if summary, err := s.CancellationReviewSummary(ctx, o.ID); err != nil || summary.Status() != "recheck_required" || summary.Stale != 1 {
		t.Fatal(summary, err)
	}
	if err := s.RecordCancellationRefund(ctx, o.TradeNo, second.Provider, second.EventID, "operator", "refund-"+second.EventID, "existing actual refund rechecked"); err != nil {
		t.Fatal(err)
	}
	if reviewed, err := s.CancellationRefundReviewed(ctx, o.TradeNo, o.OutTradeNo); err != nil || !reviewed {
		t.Fatal(reviewed, err)
	}
}

func TestCancellationCollectsCrossPlatformHistoricalAttemptWithoutOtherOrderFunds(t *testing.T) {
	s := testStore(t)
	ctx := context.Background()
	old := readyOrder(t, s, "cancel-cross-platform")
	allowSelectionFixture(t, s, old.TradeNo)
	newChoice, err := s.Select(ctx, old.TradeNo, "okx", "USDC", "other-receiver", 2, 10000, 10000, 99, 120)
	if err != nil {
		t.Fatal(err)
	}
	other := readyOrder(t, s, "not-canceled-order")
	for _, e := range []model.Transaction{evidence(old, "binance-A"), evidence(newChoice, "okx-B"), evidence(other, "unrelated-C")} {
		if _, err = s.Process(ctx, e, ""); err != nil {
			t.Fatal(err)
		}
	}
	if _, err = s.CancelReference(ctx, old.TradeNo, old.OutTradeNo); err == nil {
		t.Fatal("paid cancellation accepted")
	}
	var rows []model.CancellationReview
	if err = s.DB().Order("id").Find(&rows).Error; err != nil || len(rows) != 2 {
		t.Fatal(rows, err)
	}
	for _, r := range rows {
		if r.OrderID != old.ID || r.EventID == "unrelated-C" {
			t.Fatal("other order included in refund review", r)
		}
	}
	if rows[0].Provider == rows[1].Provider {
		t.Fatal("historical platform omitted", rows)
	}
}
