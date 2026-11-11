package store

import (
	"context"
	"litepay/internal/model"
	"path/filepath"
	"testing"
	"time"
)

func TestRecoveryGenerationStableOnRestartAndRotatesOnRecovery(t *testing.T) {
	path := filepath.Join(t.TempDir(), "recovery.db")
	ctx := context.Background()
	s, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	first, _ := s.Recovery(ctx)
	if first.Acknowledged {
		t.Fatal(first)
	}
	s.Close()
	s, err = Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	second, _ := s.Recovery(ctx)
	if first.Generation != second.Generation {
		t.Fatal("restart forced full replay")
	}
	if err = s.BeginRecovery(ctx, "restore test"); err != nil {
		t.Fatal(err)
	}
	restored, _ := s.Recovery(ctx)
	if restored.Generation == first.Generation || restored.Acknowledged {
		t.Fatal(restored)
	}
	old := model.IntegrationHealth{Name: "dujiao_cancel_sync", Generation: first.Generation, Healthy: true, ReportedAt: time.Now().Unix()}
	if err = s.AcknowledgeRecovery(ctx, first.Generation, true, old); err == nil {
		t.Fatal("old heartbeat acknowledged recovered DB")
	}
}
func TestCancellationBeforeCreationIsDurableAndChecksBothIdentities(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "cancel-intent.db")
	s, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	o, err := s.CancelReference(ctx, "future-gateway", "future-merchant")
	if err != nil || o.Status != model.Canceled {
		t.Fatal(o, err)
	}
	s.Close()
	s, err = Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	if _, err = s.CancelReference(ctx, "future-gateway", "future-merchant"); err != nil {
		t.Fatal(err)
	}
	if _, err = s.Create(ctx, model.Order{TradeNo: "new-generated", OutTradeNo: "future-merchant", OriginUnits: 1000000, ExpiresAt: time.Now().Add(time.Hour)}); err == nil {
		t.Fatal("late create bypassed cancellation")
	}
	if _, err = s.CancelReference(ctx, "different-gateway", "future-merchant"); err == nil {
		t.Fatal("mismatched IDs accepted")
	}
	real := readyOrder(t, s, "real-order")
	if _, err = s.CancelReference(ctx, "fake", real.OutTradeNo); err == nil {
		t.Fatal("real order overwritten by intent")
	}
	var count int64
	s.DB().Model(&model.CancellationIntent{}).Count(&count)
	if count != 1 {
		t.Fatal(count)
	}
}

func TestSignedCancellationMakesExpiredOrderIneligibleForManualRecovery(t *testing.T) {
	s := testStore(t)
	ctx := context.Background()
	o := readyOrder(t, s, "expired-cancel")
	s.DB().Model(o).Update("status", model.Expired)
	canceled, err := s.CancelReference(ctx, o.TradeNo, o.OutTradeNo)
	if err != nil || canceled.Status != model.Canceled {
		t.Fatal(canceled, err)
	}
	event := evidence(o, "canceled-expired-event")
	if result, err := s.Process(ctx, event, ""); err != nil || result != "order_closed" {
		t.Fatal(result, err)
	}
	if err = s.ReconcilePayment(ctx, o.TradeNo, event.Provider, event.EventID, "operator", "test merchant approval"); err == nil {
		t.Fatal("explicit merchant cancellation was reopened")
	}
}

func TestPaymentOperationsRequireFreshCompleteCurrentGenerationHealth(t *testing.T) {
	for _, failure := range []string{"source", "pending", "retry", "missing", "conflict", "discovery", "heartbeat", "last_success", "generation", "incomplete"} {
		t.Run(failure, func(t *testing.T) {
			s := testStore(t)
			ctx := context.Background()
			r, _ := s.Recovery(ctx)
			h := model.IntegrationHealth{Name: "dujiao_cancel_sync", Generation: r.Generation, Healthy: true, ReportedAt: time.Now().Unix()}
			if err := s.AcknowledgeRecovery(ctx, r.Generation, true, h); err != nil {
				t.Fatal(err)
			}
			if ready, e := s.PaymentOperationsReady(ctx); e != nil || !ready {
				t.Fatal(ready, e)
			}
			h.ReportedAt++
			complete := true
			switch failure {
			case "source":
				h.Healthy = false
			case "pending":
				h.Pending = 1
			case "retry":
				h.Failures = 1
			case "missing":
				h.Missing = 1
			case "conflict":
				h.Conflicts = 1
			case "discovery":
				h.DiscoveryPending = 1
			case "incomplete":
				complete = false
			}
			if err := s.AcknowledgeRecovery(ctx, r.Generation, complete, h); err != nil {
				t.Fatal(err)
			}
			var update map[string]any
			switch failure {
			case "heartbeat":
				update = map[string]any{"received_at": time.Now().Add(-46 * time.Second)}
			case "last_success":
				update = map[string]any{"last_success_at": time.Now().Add(-61 * time.Second)}
			case "generation":
				update = map[string]any{"generation": "different"}
			}
			if update != nil {
				if e := s.DB().Model(&model.IntegrationHealth{}).Where("name=?", h.Name).Updates(update).Error; e != nil {
					t.Fatal(e)
				}
			}
			if ready, e := s.PaymentOperationsReady(ctx); e != nil || ready {
				t.Fatal("unsafe payment operations", ready, e)
			}
			if ready, e := s.RecoveryReady(ctx); e != nil || !ready {
				t.Fatal("runtime health stopped evidence collection", ready, e)
			}
		})
	}
}

func TestReplayedReportCannotAcknowledgeRecovery(t *testing.T) {
	s := testStore(t)
	ctx := context.Background()
	r, _ := s.Recovery(ctx)
	h := model.IntegrationHealth{Name: "dujiao_cancel_sync", Generation: r.Generation, Healthy: true, ReportedAt: time.Now().Unix()}
	if err := s.AcknowledgeRecovery(ctx, r.Generation, false, h); err != nil {
		t.Fatal(err)
	}
	if err := s.AcknowledgeRecovery(ctx, r.Generation, true, h); err != nil {
		t.Fatal(err)
	}
	if ready, e := s.RecoveryReady(ctx); e != nil || ready {
		t.Fatal("replayed flag acknowledged recovery", ready, e)
	}
}

func TestUnsentCallbackReleasePreservesBudgetAndFencesOldLease(t *testing.T) {
	s := testStore(t)
	ctx := context.Background()
	o := readyOrder(t, s, "held-callback")
	if _, err := s.Process(ctx, evidence(o, "held-event"), ""); err != nil {
		t.Fatal(err)
	}
	first, _, err := s.ReserveCallback(ctx, 5, 15)
	if err != nil {
		t.Fatal(err)
	}
	if err = s.ReleaseCallback(ctx, *first); err != nil {
		t.Fatal(err)
	}
	second, _, err := s.ReserveCallback(ctx, 5, 15)
	if err != nil {
		t.Fatal(err)
	}
	if second.Attempts != 1 {
		t.Fatal("pause consumed retry budget", second)
	}
	if err = s.ReleaseCallback(ctx, *first); err != nil {
		t.Fatal(err)
	}
	var stored model.Callback
	if err = s.DB().First(&stored, "order_id=?", o.ID).Error; err != nil || stored.State != "in_flight" || !stored.LeaseUntil.Equal(second.LeaseUntil) {
		t.Fatal("old worker released new lease", stored, err)
	}
	if err = s.FinishCallback(ctx, *first, 200, true, "", 0); err != nil {
		t.Fatal(err)
	}
	if err = s.DB().First(&stored, "order_id=?", o.ID).Error; err != nil || stored.DeliveredAt != nil {
		t.Fatal("old worker completed new lease", stored, err)
	}
}
