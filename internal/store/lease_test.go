package store

import (
	"context"
	"litepay/internal/model"
	"testing"
	"time"
)

func TestExpiredLeaseHolderCannotOverwriteNewHolder(t *testing.T) {
	st := openTestStore(t)
	ctx := context.Background()
	o := createTestOrder(t, st, "LEASE-A", "LEASE-A", 0)
	if err := st.DB().Model(o).Updates(map[string]any{"status": model.StatusPaid, "notify_status": model.NotifyPending}).Error; err != nil {
		t.Fatal(err)
	}
	first, _ := st.GetByTradeNo(ctx, o.TradeNo)
	if ok, err := st.ClaimNotify(ctx, first, time.Minute); err != nil || !ok {
		t.Fatal("claim first", err)
	}
	if err := st.DB().Model(o).Update("next_notify_at", time.Now().Add(-time.Second)).Error; err != nil {
		t.Fatal(err)
	}
	second, _ := st.GetByTradeNo(ctx, o.TradeNo)
	if ok, err := st.ClaimNotify(ctx, second, time.Minute); err != nil || !ok {
		t.Fatal("claim second", err)
	}
	if err := st.FinishNotify(ctx, first, 1, true, "success", 200, "", "", nil); err != nil {
		t.Fatal(err)
	}
	current, _ := st.GetByTradeNo(ctx, o.TradeNo)
	if current.NotifyStatus != model.NotifySending {
		t.Fatalf("stale holder overwrote current lease: %s", current.NotifyStatus)
	}
	if err := st.FinishNotify(ctx, second, 1, true, "success", 200, "", "", nil); err != nil {
		t.Fatal(err)
	}
	current, _ = st.GetByTradeNo(ctx, o.TradeNo)
	if current.NotifyStatus != model.NotifySuccess {
		t.Fatal("current holder could not finish")
	}
}
