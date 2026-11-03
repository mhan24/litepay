package recovery_test

import (
	"context"
	"litepay/internal/model"
	"litepay/internal/recovery"
	"litepay/internal/store"
	"path/filepath"
	"testing"
	"time"
)

func TestSnapshotRestoresAllPaymentState(t *testing.T) {
	dir := t.TempDir()
	src := filepath.Join(dir, "live.db")
	dst := filepath.Join(dir, "snapshot.db")
	st, err := store.Open(src)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	o, err := st.CreateOrder(ctx, store.CreateOrderInput{Pid: "m", TradeNo: "backup", OutTradeNo: "backup", OriginMoney: "1.00", OriginCurrency: "USD", ExpiredAt: time.Now().Add(time.Hour)})
	if err != nil {
		t.Fatal(err)
	}
	o, err = st.SelectPayment(ctx, o.TradeNo, "okx", "USDT", "uid", 1, .01, 99, .01, 2, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if ok, err := st.ClaimAndPay(ctx, o, model.IncomingTx{Provider: "okx", TransactionID: "tx", Type: "3", Currency: "USDT", Amount: o.PayAmount, ReceiverAddr: "uid", TransactionTime: time.Now()}, 2); err != nil || !ok {
		t.Fatal("pay", err)
	}
	if err := st.SaveScanCursor(ctx, "okx", time.Now()); err != nil {
		t.Fatal(err)
	}
	if err := recovery.Snapshot(ctx, src, dst); err != nil {
		t.Fatal(err)
	}
	if err := recovery.Verify(ctx, dst); err != nil {
		t.Fatal(err)
	}
	restored, err := store.Open(dst)
	if err != nil {
		t.Fatal(err)
	}
	got, err := restored.GetByTradeNo(ctx, o.TradeNo)
	if err != nil || got.Status != model.StatusPaid || got.NotifyStatus != model.NotifyPending || got.PayUnits != o.PayUnits {
		t.Fatalf("order state lost: %+v %v", got, err)
	}
	var n int64
	for _, table := range []string{"incoming_txs", "amount_slots", "scan_cursors"} {
		if err := restored.DB().Table(table).Count(&n).Error; err != nil || n != 1 {
			t.Fatalf("%s: %d %v", table, n, err)
		}
	}
	if err := recovery.Snapshot(ctx, src, dst); err == nil {
		t.Fatal("overwrote unique backup")
	}
}
