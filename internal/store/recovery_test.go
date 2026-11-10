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
