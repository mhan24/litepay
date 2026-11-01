package store

import (
	"context"
	"testing"
	"time"

	"litepay/internal/model"
)

func TestCanceledOrderNeverQueuesExpiredNotification(t *testing.T) {
	ctx := context.Background()
	st := openTestStore(t)
	o := createTestOrder(t, st, "cancel-terminal", "cancel-terminal", 1)

	if err := st.CancelOrder(ctx, o.TradeNo); err != nil {
		t.Fatal(err)
	}
	canceled, err := st.GetByTradeNo(ctx, o.TradeNo)
	if err != nil {
		t.Fatal(err)
	}
	if canceled.Status != model.StatusCanceled || canceled.NotifyStatus != model.NotifyCanceled {
		t.Fatalf("canceled state = %s/%s", canceled.Status, canceled.NotifyStatus)
	}

	if err := st.CancelOrder(ctx, o.TradeNo); err != nil {
		t.Fatalf("repeated cancellation is not idempotent: %v", err)
	}
	if _, err := st.FinalizeExpired(ctx, 0); err != nil {
		t.Fatal(err)
	}
	again, _ := st.GetByTradeNo(ctx, o.TradeNo)
	if again.NotifyStatus != model.NotifyCanceled || again.NextNotifyAt != nil {
		t.Fatal("canceled order was queued for expiration notification")
	}
}

func TestPaidOrderCannotBeCanceled(t *testing.T) {
	ctx := context.Background()
	st := openTestStore(t)
	o := createTestOrder(t, st, "paid-terminal", "paid-terminal", 1)
	tx := model.IncomingTx{
		Provider: "binance", Type: "PAY", Amount: o.PayAmount, Currency: "USDT",
		ReceiverAddr: "binance-uid", TransactionID: "paid-terminal-tx",
		TransactionTime: time.Now(),
	}
	if ok, err := st.ClaimAndPay(ctx, o, tx, 2); err != nil || !ok {
		t.Fatalf("payment failed: ok=%v err=%v", ok, err)
	}
	if err := st.CancelOrder(ctx, o.TradeNo); err == nil {
		t.Fatal("paid order was canceled")
	}
	paid, _ := st.GetByTradeNo(ctx, o.TradeNo)
	if paid.Status != model.StatusPaid {
		t.Fatalf("paid order status changed to %s", paid.Status)
	}
}
