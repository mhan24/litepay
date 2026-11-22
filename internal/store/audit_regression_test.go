package store

import (
	"bytes"
	"context"
	"math"
	"testing"
	"time"

	"litepay/internal/amount"
	"litepay/internal/model"
)

func TestLateCancelCollectsExistingUnclaimedReceiptAndSurfacesReview(t *testing.T) {
	ctx := context.Background()
	for _, initialStatus := range []string{model.Expired, model.Ready} {
		t.Run(initialStatus, func(t *testing.T) {
			s := testStore(t)
			o := readyOrder(t, s, "late-cancel-"+initialStatus)
			if initialStatus == model.Expired {
				if err := s.DB().Model(o).Update("status", model.Expired).Error; err != nil {
					t.Fatal(err)
				}
			} else {
				if err := s.DB().Model(&model.PaymentAttempt{}).Where("order_id=?", o.ID).Update("settle_until", time.Now().Add(-time.Second)).Error; err != nil {
					t.Fatal(err)
				}
			}
			event := evidence(o, "pre-cancel-deposit-"+initialStatus)
			res, err := s.Process(ctx, event, "")
			if err != nil || (initialStatus == model.Expired && res != "order_closed") || (initialStatus == model.Ready && res != "late_payment_review_required") {
				t.Fatalf("unexpected pre-cancel process result: %q err=%v", res, err)
			}
			canceled, err := s.CancelReference(ctx, o.TradeNo, o.OutTradeNo)
			if err != nil || canceled.Status != model.Canceled {
				t.Fatalf("expected order canceled, got %+v err=%v", canceled, err)
			}
			summary, err := s.CancellationReviewSummary(ctx, o.ID)
			if err != nil || !summary.Present() || summary.Current() || summary.Pending != 1 {
				t.Fatalf("expected open cancellation review after late cancel, got %+v err=%v", summary, err)
			}
			queue, err := s.CancellationReviewQueue(ctx)
			if err != nil || len(queue) != 1 || queue[0].EventID != event.EventID {
				t.Fatalf("expected review in queue, got %+v err=%v", queue, err)
			}
			if err := s.RecordCancellationRefund(ctx, o.TradeNo, event.Provider, event.EventID, "operator", "refund-late-1", "verified late cancel refund"); err != nil {
				t.Fatalf("expected refund record to succeed, got %v", err)
			}
			if reviewed, err := s.CancellationRefundReviewed(ctx, o.TradeNo, o.OutTradeNo); err != nil || !reviewed {
				t.Fatalf("expected reviewed=true, got %v err=%v", reviewed, err)
			}
		})
	}
}

func TestReplayCannotCorruptReviewedCancellationEvidenceOrDowngradeConfirmed(t *testing.T) {
	ctx := context.Background()
	s := testStore(t)
	o := readyOrder(t, s, "immutable-review")
	if err := s.Cancel(ctx, o.TradeNo); err != nil {
		t.Fatal(err)
	}
	event := evidence(o, "reviewed-event")
	if _, err := s.Process(ctx, event, ""); err != nil {
		t.Fatal(err)
	}
	// Replay with conflicting amount, state downgrade, or duplicate reference must not corrupt reviewed evidence.
	badUnits := event
	badUnits.Units++
	if _, err := s.Process(ctx, badUnits, ""); err != nil {
		t.Fatal(err)
	}
	downgrade := event
	downgrade.State = model.Unconfirmed
	if _, err := s.Process(ctx, downgrade, ""); err != nil {
		t.Fatal(err)
	}
	dupRef := evidence(o, "other-event-same-ref")
	dupRef.Reference = event.Reference
	if _, err := s.Process(ctx, dupRef, ""); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Process(ctx, event, ""); err != nil {
		t.Fatal(err)
	}
	var stored model.Transaction
	if err := s.DB().First(&stored, "provider=? AND event_id=?", event.Provider, event.EventID).Error; err != nil {
		t.Fatal(err)
	}
	if stored.State != model.Confirmed || stored.AttemptID == nil || stored.Units != event.Units {
		t.Fatalf("reviewed transaction mutated on replay: %+v", stored)
	}
	if err := s.RecordCancellationRefund(ctx, o.TradeNo, event.Provider, event.EventID, "operator", "refund-ref", "actual refund"); err != nil {
		t.Fatalf("refund recording failed after replay: %v", err)
	}

	// Unreviewed confirmed receipt cannot silently downgrade to pending/unconfirmed.
	o2 := readyOrder(t, s, "no-downgrade")
	if err := s.DB().Model(&model.PaymentAttempt{}).Where("order_id=?", o2.ID).Update("settle_until", time.Now().Add(-time.Second)).Error; err != nil {
		t.Fatal(err)
	}
	ev2 := evidence(o2, "confirmed-late")
	if res, err := s.Process(ctx, ev2, ""); err != nil || res != "late_payment_review_required" {
		t.Fatalf("got %q err=%v", res, err)
	}
	ev2Down := ev2
	ev2Down.State = model.Unconfirmed
	if res, err := s.Process(ctx, ev2Down, ""); err != nil || res != model.Ambiguous {
		t.Fatalf("expected conflicting downgrade to become ambiguous, got %q err=%v", res, err)
	}

	// Invalid state is rejected.
	invalid := evidence(o2, "bad-state")
	invalid.State = "bogus"
	if res, err := s.Process(ctx, invalid, ""); err == nil || res != "invalid_evidence" {
		t.Fatalf("expected invalid_evidence, got %q err=%v", res, err)
	}
}

func TestCancellationIntentConflictChecksBothIdentifiers(t *testing.T) {
	ctx := context.Background()
	s := testStore(t)
	if _, err := s.CancelReference(ctx, "T1", "M1"); err != nil {
		t.Fatal(err)
	}
	if _, err := s.CancelReference(ctx, "T1", "M2"); err == nil || err.Error() != "网关订单号与商户订单号不一致" {
		t.Fatalf("expected identifier mismatch error on same trade_no, got %v", err)
	}
	if _, err := s.CancelReference(ctx, "T2", "M1"); err == nil || err.Error() != "网关订单号与商户订单号不一致" {
		t.Fatalf("expected identifier mismatch error on same out_trade_no, got %v", err)
	}
}

func TestSameSecondGenerationRecoveryHandshakeAcceptedAndHealthValidated(t *testing.T) {
	ctx := context.Background()
	s := testStore(t)
	stamp := time.Now().Unix()

	r1, err := s.Recovery(ctx)
	if err != nil {
		t.Fatal(err)
	}
	h1 := model.IntegrationHealth{Name: "dujiao_cancel_sync", Generation: r1.Generation, Source: "sqlite", Healthy: true, ReportedAt: stamp}
	if err := s.AcknowledgeRecovery(ctx, r1.Generation, true, h1); err != nil {
		t.Fatal(err)
	}
	if err := s.BeginRecovery(ctx, "fast restore in same second"); err != nil {
		t.Fatal(err)
	}
	r2, err := s.Recovery(ctx)
	if err != nil {
		t.Fatal(err)
	}
	h2 := model.IntegrationHealth{Name: "dujiao_cancel_sync", Generation: r2.Generation, Source: "sqlite", Healthy: true, ReportedAt: stamp}
	if err := s.AcknowledgeRecovery(ctx, r2.Generation, true, h2); err != nil {
		t.Fatal(err)
	}
	if ready, err := s.RecoveryReady(ctx); err != nil || !ready {
		t.Fatalf("same-second new-generation recovery handshake was dropped: ready=%v err=%v", ready, err)
	}

	for _, bad := range []model.IntegrationHealth{
		{Name: "other", Generation: r2.Generation, ReportedAt: stamp + 1},
		{Name: "dujiao_cancel_sync", Generation: r2.Generation, Pending: -1, ReportedAt: stamp + 1},
		{Name: "dujiao_cancel_sync", Generation: r2.Generation, DiscoveryPending: 2, ReportedAt: stamp + 1},
	} {
		if err := s.AcknowledgeRecovery(ctx, r2.Generation, true, bad); err == nil {
			t.Fatalf("accepted invalid health payload: %+v", bad)
		}
	}
}

func TestLiteralsStartingWithEncPrefixAndKeyRotationReactivation(t *testing.T) {
	ctx := context.Background()
	s := testStore(t)
	keyA := bytes.Repeat([]byte{0x11}, 32)
	keyB := bytes.Repeat([]byte{0x22}, 32)
	if err := s.SetMasterKey(keyA); err != nil {
		t.Fatal(err)
	}
	literalSecret := "enc:v2:not-actually-ciphertext"
	literalToken := "enc:v1:literal-merchant-token"
	if err := s.SaveSettingPayload(ctx, `{"binance_api_secret":"`+literalSecret+`"}`, "admin"); err != nil {
		t.Fatal(err)
	}
	if _, err := s.InstallSigningKey(ctx, literalToken, "admin", false); err != nil {
		t.Fatal(err)
	}
	if err := s.RotateMasterKey(ctx, keyA, keyB); err != nil {
		t.Fatalf("rotation failed for enc:-prefixed literals: %v", err)
	}
	payload, err := s.LoadSettingPayload(ctx)
	if err != nil || payload != `{"binance_api_secret":"`+literalSecret+`"}` {
		t.Fatalf("unexpected decrypted setting payload: %q err=%v", payload, err)
	}
	cred, err := s.LoadCredential(ctx, "bepusdt_api_token")
	if err != nil || cred == nil || cred.Value != literalToken {
		t.Fatalf("unexpected decrypted credential: %+v err=%v", cred, err)
	}

	// Rotating A -> B -> A and A -> A must keep the active key unretired.
	idA, err := s.InstallSigningKey(ctx, "token-A", "admin", true)
	if err != nil {
		t.Fatal(err)
	}
	idB, err := s.InstallSigningKey(ctx, "token-B", "admin", true)
	if err != nil {
		t.Fatal(err)
	}
	idA2, err := s.InstallSigningKey(ctx, "token-A", "admin", true)
	if err != nil || idA2 != idA || idA == idB {
		t.Fatalf("expected reactivation of key A (%d), got %d (B=%d) err=%v", idA, idA2, idB, err)
	}
	if _, err := s.InstallSigningKey(ctx, "token-A", "admin", true); err != nil {
		t.Fatal(err)
	}
	o, err := s.Create(ctx, model.Order{TradeNo: "reactivated-key", OutTradeNo: "reactivated-key", OriginUnits: 10 * amount.Scale, ExpiresAt: time.Now().Add(time.Minute)})
	if err != nil || o.SigningKeyID != idA {
		t.Fatalf("expected active unretired key %d on new order, got %d err=%v", idA, o.SigningKeyID, err)
	}
	if _, err := s.InstallSigningKey(ctx, "", "admin", true); err == nil {
		t.Fatal("accepted empty signing token")
	}
}

func TestScanCursorUpdatesDoNotClobberSiblingFields(t *testing.T) {
	ctx := context.Background()
	s := testStore(t)
	now := time.Now()
	if err := s.DB().Create(&model.ScanCursor{Provider: "binance", HistoricalTurn: 5, ScannedUntil: now.Add(-time.Hour)}).Error; err != nil {
		t.Fatal(err)
	}
	if err := s.PendingResult(ctx, "binance", context.DeadlineExceeded); err != nil {
		t.Fatal(err)
	}
	if err := s.ScanResult(ctx, "binance", now, nil); err != nil {
		t.Fatal(err)
	}
	c, err := s.Cursor(ctx, "binance")
	if err != nil {
		t.Fatal(err)
	}
	if c.HistoricalTurn != 5 || c.PendingError == "" || c.LastError != "" || !c.ScannedUntil.Equal(now) {
		t.Fatalf("cursor update clobbered sibling columns: %+v", c)
	}
}

func TestCallbackExhaustionSkipsCancellationHoldAndReleasePreservesAttemptBase(t *testing.T) {
	ctx := context.Background()
	s := testStore(t)
	o := readyOrder(t, s, "cb-hold-exhaust")
	ev := evidence(o, "cb-hold-event")
	if _, err := s.Process(ctx, ev, ""); err != nil {
		t.Fatal(err)
	}
	task, _, err := s.ReserveCallback(ctx, 1, 10)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.CancelReference(ctx, o.TradeNo, o.OutTradeNo); err == nil {
		t.Fatal("expected paid conflict error")
	}
	// Simulate an expired in-flight lease while CancellationReview holds the order.
	past := time.Now().Add(-time.Minute)
	if err := s.DB().Model(&model.Callback{}).Where("order_id=?", o.ID).Updates(map[string]any{"next_at": past, "lease_until": past}).Error; err != nil {
		t.Fatal(err)
	}
	if _, _, err := s.ReserveCallback(ctx, 1, 10); err == nil {
		t.Fatal("expected no reservable callback")
	}
	var cb model.Callback
	if err := s.DB().First(&cb, "order_id=?", o.ID).Error; err != nil {
		t.Fatal(err)
	}
	if cb.State == "exhausted" {
		t.Fatalf("cancellation-held callback was marked exhausted: %+v", cb)
	}

	// ReleaseCallback cannot drop attempts below attempt_base.
	if err := s.DB().Model(&model.Callback{}).Where("order_id=?", o.ID).Updates(map[string]any{"attempt_base": 1, "lease_until": task.LeaseUntil}).Error; err != nil {
		t.Fatal(err)
	}
	if err := s.ReleaseCallback(ctx, *task); err != nil {
		t.Fatal(err)
	}
	if err := s.DB().First(&cb, "order_id=?", o.ID).Error; err != nil {
		t.Fatal(err)
	}
	if cb.Attempts < cb.AttemptBase {
		t.Fatalf("attempts dropped below attempt_base: %+v", cb)
	}
}

func TestStoreValidationBoundaryGuardsAndForeignTimezones(t *testing.T) {
	if _, err := Open("   "); err == nil {
		t.Fatal("accepted blank db path")
	}
	ctx := context.Background()
	s := testStore(t)
	if _, err := s.Create(ctx, model.Order{TradeNo: "", OutTradeNo: "x", OriginUnits: 1000, ExpiresAt: time.Now().Add(time.Minute)}); err == nil {
		t.Fatal("accepted empty trade_no")
	}
	if _, err := s.Create(ctx, model.Order{TradeNo: "x", OutTradeNo: "", OriginUnits: 1000, ExpiresAt: time.Now().Add(time.Minute)}); err == nil {
		t.Fatal("accepted empty out_trade_no")
	}
	if _, err := s.Create(ctx, model.Order{TradeNo: "x", OutTradeNo: "x", OriginUnits: 1000}); err == nil {
		t.Fatal("accepted zero expires_at")
	}
	if _, err := s.Create(ctx, model.Order{TradeNo: "huge", OutTradeNo: "huge", OriginUnits: math.MaxInt64 - 5, ExpiresAt: time.Now().Add(time.Minute)}); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Select(ctx, "huge", "binance", "USDT", "receiver", 2, 10000, 10000, 99, 120); err == nil {
		t.Fatal("accepted overflowing OriginUnits")
	}
	if _, err := s.Create(ctx, model.Order{TradeNo: "empty-rec", OutTradeNo: "empty-rec", OriginUnits: 10 * amount.Scale, ExpiresAt: time.Now().Add(time.Minute)}); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Select(ctx, "empty-rec", "binance", "USDT", "  ", 2, 10000, 10000, 99, 120); err == nil {
		t.Fatal("accepted empty receiver")
	}

	// Negative grace cannot prematurely expire Pending orders.
	if err := s.Expire(ctx, -3600); err != nil {
		t.Fatal(err)
	}
	pending, err := s.GetByTradeNo(ctx, "empty-rec")
	if err != nil || pending.Status != model.Pending {
		t.Fatalf("negative grace expired pending order: %+v err=%v", pending, err)
	}

	// Foreign timezone timestamps must normalize cleanly for SQLite comparisons.
	minus5 := time.FixedZone("EST", -5*3600)
	plus9 := time.FixedZone("JST", 9*3600)
	tzOrder, err := s.Create(ctx, model.Order{
		TradeNo:     "tz-order",
		OutTradeNo:  "tz-order",
		OriginUnits: 10 * amount.Scale,
		ExpiresAt:   time.Now().Add(2 * time.Minute).In(minus5),
	})
	if err != nil {
		t.Fatal(err)
	}
	tzOrder, err = s.Select(ctx, tzOrder.TradeNo, "binance", "USDT", "receiver", 2, 10000, 10000, 99, 120)
	if err != nil {
		t.Fatal(err)
	}
	if err := s.Expire(ctx, 0); err != nil {
		t.Fatal(err)
	}
	freshTZ, _ := s.GetByTradeNo(ctx, tzOrder.TradeNo)
	if freshTZ.Status != model.Ready {
		t.Fatalf("foreign timezone order prematurely expired: %+v", freshTZ)
	}
	txTZ := evidence(tzOrder, "tz-event")
	txTZ.OccurredAt = time.Now().In(plus9)
	if res, err := s.Process(ctx, txTZ, ""); err != nil || res != "paid" {
		t.Fatalf("foreign timezone deposit failed match: %q err=%v", res, err)
	}

	// HistoricalScan boundary checks.
	if err := s.HistoricalScanFailure(ctx, model.HistoricalScan{}, false); err == nil {
		t.Fatal("accepted zero-ID historical scan failure")
	}
}

func TestReconcilePaymentRejectsOutOfRangeAttemptAndCancellationReviewedTx(t *testing.T) {
	ctx := context.Background()
	s := testStore(t)

	// Case 1: attempt outside MallCompatible range must be rejected by ReconcilePayment.
	o1 := readyOrder(t, s, "reconcile-out-of-range")
	if err := s.DB().Model(o1).Updates(map[string]any{"status": model.Expired, "pay_units": amount.MaxMall}).Error; err != nil {
		t.Fatal(err)
	}
	if err := s.DB().Model(&model.PaymentAttempt{}).Where("order_id = ?", o1.ID).Update("units", amount.MaxMall).Error; err != nil {
		t.Fatal(err)
	}
	ev1 := model.Transaction{Provider: o1.Provider, EventID: "out-of-range-ev", Reference: "ref-oor", Receiver: o1.Receiver, Currency: o1.Currency, Units: amount.MaxMall, OccurredAt: time.Now(), State: model.Confirmed}
	if _, err := s.Process(ctx, ev1, ""); err != nil {
		t.Fatal(err)
	}
	if err := s.ReconcilePayment(ctx, o1.TradeNo, ev1.Provider, ev1.EventID, "operator", "manual check"); err == nil {
		t.Fatal("ReconcilePayment accepted out-of-range attempt units")
	}

	// Case 2: transaction already bound to a CancellationReview cannot be reconciled to an order.
	o2 := readyOrder(t, s, "reconcile-canceled-target")
	if err := s.Cancel(ctx, o2.TradeNo); err != nil {
		t.Fatal(err)
	}
	ev2 := evidence(o2, "cr-bound-ev")
	if _, err := s.Process(ctx, ev2, ""); err != nil {
		t.Fatal(err)
	}
	o3 := readyOrder(t, s, "reconcile-other-order")
	if err := s.DB().Model(o3).Update("status", model.Expired).Error; err != nil {
		t.Fatal(err)
	}
	if err := s.ReconcilePayment(ctx, o3.TradeNo, ev2.Provider, ev2.EventID, "operator", "cross-order check"); err == nil {
		t.Fatal("ReconcilePayment accepted transaction bound to a cancellation review")
	}
}

func TestLateConfirmationMovesTaskToManualReviewInsteadOfResolved(t *testing.T) {
	ctx := context.Background()
	s := testStore(t)
	o := readyOrder(t, s, "late-confirm-state")
	tx := evidence(o, "late-confirm-ev")
	tx.State = model.Unconfirmed
	if res, err := s.Process(ctx, tx, ""); err != nil || res != "pending" {
		t.Fatalf("unexpected pending result: %q err=%v", res, err)
	}
	if err := s.DB().Model(&model.ConfirmationTask{}).Where("provider = ? AND event_id = ?", tx.Provider, tx.EventID).Update("deadline", time.Now().Add(-time.Second)).Error; err != nil {
		t.Fatal(err)
	}
	tx.State = model.Confirmed
	res, err := s.Process(ctx, tx, "")
	if err != nil || res != "confirmation_review_required" {
		t.Fatalf("expected confirmation_review_required, got %q err=%v", res, err)
	}
	var task model.ConfirmationTask
	if err := s.DB().First(&task, "provider = ? AND event_id = ?", tx.Provider, tx.EventID).Error; err != nil {
		t.Fatal(err)
	}
	if task.State != "manual_review" {
		t.Fatalf("expired confirmation task marked %q instead of manual_review", task.State)
	}
	stats, err := s.ProviderSuccessStats(ctx)
	if err != nil || stats["binance"].Failed != 1 {
		t.Fatalf("expected Failed=1 in provider stats, got %+v err=%v", stats, err)
	}
}

func TestAmbiguousReplayOfAdditionalPaymentPreservesAttemptLinkForCancellationReview(t *testing.T) {
	ctx := context.Background()
	s := testStore(t)
	o := readyOrder(t, s, "ambig-additional")
	first := evidence(o, "first-claim")
	if res, err := s.Process(ctx, first, ""); err != nil || res != "paid" {
		t.Fatalf("first claim failed: %q err=%v", res, err)
	}
	extra := evidence(o, "extra-deposit")
	if res, err := s.Process(ctx, extra, ""); err != nil || res != "additional_payment" {
		t.Fatalf("expected additional_payment, got %q err=%v", res, err)
	}
	// Replay extra deposit with conflicting units -> becomes ambiguous, must preserve attempt_id.
	conflict := extra
	conflict.Units++
	if res, err := s.Process(ctx, conflict, ""); err != nil || res != model.Ambiguous {
		t.Fatalf("expected ambiguous, got %q err=%v", res, err)
	}
	var stored model.Transaction
	if err := s.DB().First(&stored, "provider = ? AND event_id = ?", extra.Provider, extra.EventID).Error; err != nil {
		t.Fatal(err)
	}
	if stored.AttemptID == nil || stored.State != model.Ambiguous {
		t.Fatalf("ambiguous additional payment lost attempt_id: %+v", stored)
	}
	if _, err := s.CancelReference(ctx, o.TradeNo, o.OutTradeNo); err == nil {
		t.Fatal("expected paid conflict error")
	}
	if err := s.RecordCancellationRefund(ctx, o.TradeNo, first.Provider, first.EventID, "operator", "refund-1", "refunded first deposit"); err != nil {
		t.Fatal(err)
	}
	summary, err := s.CancellationReviewSummary(ctx, o.ID)
	if err != nil || summary.Uncovered != 1 || summary.Current() {
		t.Fatalf("ambiguous extra receipt missing from uncovered cancellation review: %+v err=%v", summary, err)
	}
	queue, err := s.CancellationReviewQueue(ctx)
	if err != nil || len(queue) != 1 || queue[0].EventID != extra.EventID {
		t.Fatalf("expected ambiguous extra receipt in queue, got %+v err=%v", queue, err)
	}
}
