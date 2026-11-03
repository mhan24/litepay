package store

import (
	"context"
	"fmt"
	"litepay/internal/model"
	"sync"
	"testing"
	"time"
)

func TestPaidSlotSurvivesConcurrentAllocationsAndRepeatTransfer(t *testing.T) {
	st := openTestStore(t)
	ctx := context.Background()
	a := createTestOrder(t, st, "COOL-A", "COOL-A", 0)
	a, err := st.SelectPayment(ctx, a.TradeNo, "okx", "USDT", "uid", 10, .01, 99, .01, 2, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	tx := model.IncomingTx{TransactionID: "first", Provider: "okx", Type: "3", Amount: a.PayAmount, Currency: "USDT", ReceiverAddr: "uid", TransactionTime: time.Now()}
	if ok, err := st.ClaimAndPay(ctx, a, tx, 2); err != nil || !ok {
		t.Fatalf("pay: %v %v", ok, err)
	}
	var old model.AmountSlot
	if err := st.DB().Where("trade_no = ?", a.TradeNo).First(&old).Error; err != nil || old.OrderID != 0 || !old.ExpiresAt.After(time.Now()) {
		t.Fatalf("cooldown missing: %+v %v", old, err)
	}
	orders := make([]*model.Order, 6)
	for i := range orders {
		orders[i] = createTestOrder(t, st, fmt.Sprintf("COOL-%d", i), fmt.Sprintf("COOL-%d", i), 0)
	}
	var wg sync.WaitGroup
	for _, o := range orders {
		wg.Add(1)
		go func(o *model.Order) {
			defer wg.Done()
			b, err := st.SelectPayment(ctx, o.TradeNo, "okx", "USDT", "uid", 10, .01, 99, .01, 2, time.Minute)
			if err != nil {
				t.Errorf("allocate: %v", err)
				return
			}
			if b.PayAmount == a.PayAmount {
				t.Error("paid amount reused during cooldown")
			}
			repeat := tx
			repeat.TransactionID = "repeat-" + o.TradeNo
			repeat.TransactionTime = time.Now()
			if ok, _ := st.ClaimAndPay(ctx, b, repeat, 2); ok {
				t.Error("repeat payment credited a different order")
			}
		}(o)
	}
	wg.Wait()
}
