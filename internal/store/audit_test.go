package store

import (
	"context"
	"fmt"
	"testing"
	"time"

	"litepay/internal/model"
)

func TestAuditPrecisionAndRetiredAmountReservations(t *testing.T) {
	ctx := context.Background()
	st := openTestStore(t)
	a := createTestOrder(t, st, "audit-a", "a", 0)
	var err error
	a, err = st.SelectPayment(ctx, a.TradeNo, "binance", "USDT", "uid", 9.99, .01, 99, .01, 2, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	bad := model.IncomingTx{Provider: "binance", Currency: "USDT", ReceiverAddr: "uid", TransactionID: "underpaid", Type: "PAY", Amount: 9.6, TransactionTime: time.Now()}
	if ok, err := st.ClaimAndPay(ctx, a, bad, 6); err == nil || ok {
		t.Fatal("integer amount matched an underpayment")
	}
	bad.Amount = 10
	bad.TransactionTime = a.SelectedAt.Add(-time.Second)
	if ok, err := st.ClaimAndPay(ctx, a, bad, 2); err == nil || ok {
		t.Fatal("accepted a transfer before selection")
	}
	bad.TransactionTime = a.ExpiredAt.Add(time.Millisecond)
	if ok, err := st.ClaimAndPay(ctx, a, bad, 2); err == nil || ok {
		t.Fatal("accepted a transfer sent after expiry")
	}
	if err := st.CancelOrder(ctx, a.TradeNo); err != nil {
		t.Fatal(err)
	}
	b := createTestOrder(t, st, "audit-b", "b", 0)
	b, err = st.SelectPayment(ctx, b.TradeNo, "binance", "USDT", "uid", 9.99, .01, 99, .01, 2, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if b.PayAmount == a.PayAmount {
		t.Fatal("canceled amount reused inside original payment window")
	}
	c := createTestOrder(t, st, "audit-c", "c", 0)
	if _, err := st.SelectPayment(ctx, c.TradeNo, "binance", "USDT", "uid", 9.99, .0001, 99, .01, 4, time.Minute); err == nil {
		t.Fatal("allowed overlapping precision grids")
	}
}

func TestAuditLockedPrecisionSurvivesConfigChanges(t *testing.T) {
	ctx := context.Background()
	st := openTestStore(t)
	o := createTestOrder(t, st, "precision", "precision", 0)
	o, err := st.SelectPayment(ctx, o.TradeNo, "binance", "USDT", "uid", 2.5099, .0001, 99, .01, 4, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	tx := model.IncomingTx{Provider: "binance", Currency: "USDT", ReceiverAddr: "uid", TransactionID: "nearby", Type: "PAY", Amount: 2.5102, TransactionTime: time.Now()}
	if ok, err := st.ClaimAndPay(ctx, o, tx, 2); err == nil || ok {
		t.Fatal("current config widened locked precision")
	}
	tx.Amount = 2.51
	if ok, err := st.ClaimAndPay(ctx, o, tx, 2); err != nil || !ok {
		t.Fatalf("locked amount rejected: %v", err)
	}
}

func TestAuditStaleNotificationIsNotDelivered(t *testing.T) {
	ctx := context.Background()
	st := openTestStore(t)
	o := createTestOrder(t, st, "notify-old", "replace", 0)
	if err := st.ForceExpire(ctx, o.TradeNo); err != nil {
		t.Fatal(err)
	}
	if _, err := st.FinalizeExpired(ctx, 0); err != nil {
		t.Fatal(err)
	}
	old, _ := st.GetByTradeNo(ctx, o.TradeNo)
	_, err := st.CreateOrReuseCashierOrder(ctx, CreateOrderInput{Pid: "bepusdt", TradeNo: "notify-new", OutTradeNo: "replace", ExpiredAt: time.Now().Add(time.Hour)})
	if err != nil {
		t.Fatal(err)
	}
	called := false
	if err := st.WithCurrentNotification(ctx, old, func(*model.Order) { called = true }); err != nil {
		t.Fatal(err)
	}
	if called {
		t.Fatal("delivered expiration for a replaced merchant order")
	}
}

func TestAuditMatchableOrdersDoesNotStarveAfter500(t *testing.T) {
	st := openTestStore(t)
	ctx := context.Background()
	for i := 0; i < 501; i++ {
		o := createTestOrder(t, st, fmt.Sprintf("many-%d", i), fmt.Sprint(i), 0)
		if _, err := st.SelectPayment(ctx, o.TradeNo, "binance", "USDT", "uid", float64(i+1), .01, 1, .01, 2, time.Minute); err != nil {
			t.Fatal(err)
		}
	}
	orders, err := st.MatchableOrders(ctx, "binance", time.Minute)
	if err != nil || len(orders) != 501 {
		t.Fatalf("matchable=%d err=%v", len(orders), err)
	}
}

func TestAuditOrderReplacementWaitsForInFlightCallback(t *testing.T) {
	ctx := context.Background()
	st := openTestStore(t)
	o := createTestOrder(t, st, "inflight-old", "inflight", 0)
	if err := st.ForceExpire(ctx, o.TradeNo); err != nil {
		t.Fatal(err)
	}
	if _, err := st.FinalizeExpired(ctx, 0); err != nil {
		t.Fatal(err)
	}
	old, _ := st.GetByTradeNo(ctx, o.TradeNo)
	started, release := make(chan struct{}), make(chan struct{})
	deliveryDone := make(chan error, 1)
	go func() {
		deliveryDone <- st.WithCurrentNotification(ctx, old, func(*model.Order) { close(started); <-release })
	}()
	<-started
	replacementDone := make(chan error, 1)
	go func() {
		_, err := st.CreateOrReuseCashierOrder(ctx, CreateOrderInput{Pid: "bepusdt", TradeNo: "inflight-new", OutTradeNo: "inflight", ExpiredAt: time.Now().Add(time.Hour)})
		replacementDone <- err
	}()
	select {
	case err := <-replacementDone:
		close(release)
		t.Fatalf("replacement overtook callback: %v", err)
	case <-time.After(30 * time.Millisecond):
	}
	close(release)
	if err := <-deliveryDone; err != nil {
		t.Fatal(err)
	}
	if err := <-replacementDone; err != nil {
		t.Fatal(err)
	}
}

func TestAuditCallbackDoesNotBlockUnrelatedMerchantOrder(t *testing.T) {
	ctx := context.Background()
	st := openTestStore(t)
	o := createTestOrder(t, st, "lock-a", "merchant-a", 0)
	if err := st.ForceExpire(ctx, o.TradeNo); err != nil {
		t.Fatal(err)
	}
	if _, err := st.FinalizeExpired(ctx, 0); err != nil {
		t.Fatal(err)
	}
	old, _ := st.GetByTradeNo(ctx, o.TradeNo)
	started, release := make(chan struct{}), make(chan struct{})
	deliveryDone := make(chan error, 1)
	go func() {
		deliveryDone <- st.WithCurrentNotification(ctx, old, func(*model.Order) {
			close(started)
			<-release
		})
	}()
	<-started

	unrelatedDone := make(chan error, 1)
	go func() {
		_, err := st.CreateOrReuseCashierOrder(ctx, CreateOrderInput{
			Pid: "bepusdt", TradeNo: "lock-b", OutTradeNo: "merchant-b",
			ExpiredAt: time.Now().Add(time.Hour),
		})
		unrelatedDone <- err
	}()
	select {
	case err := <-unrelatedDone:
		if err != nil {
			close(release)
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		close(release)
		t.Fatal("callback for one merchant order blocked another merchant order")
	}
	close(release)
	if err := <-deliveryDone; err != nil {
		t.Fatal(err)
	}
}

func TestAuditClaimRejectsNegativeAmountsAndNormalizesProvider(t *testing.T) {
	st := openTestStore(t)
	o := createTestOrder(t, st, "direction", "direction", 1)
	tx := model.IncomingTx{Provider: "BINANCE", Currency: "usdt", ReceiverAddr: "binance-uid", TransactionID: "direction-tx", Type: "PAY", Amount: -o.PayAmount, TransactionTime: time.Now()}
	if ok, err := st.ClaimAndPay(context.Background(), o, tx, 2); err == nil || ok {
		t.Fatal("negative transfer accepted")
	}
	tx.Amount = o.PayAmount
	if ok, err := st.ClaimAndPay(context.Background(), o, tx, 2); err != nil || !ok {
		t.Fatalf("normalized transaction failed: %v", err)
	}
}

func TestAuditResetNotifyDoesNotReopenCompletedOrCanceledOrders(t *testing.T) {
	ctx := context.Background()
	st := openTestStore(t)
	paid := createTestOrder(t, st, "reset-paid", "reset-paid", 1)
	tx := model.IncomingTx{
		Provider: "binance", Currency: "USDT", ReceiverAddr: "binance-uid",
		TransactionID: "reset-paid-tx", Type: "PAY", Amount: paid.PayAmount,
		TransactionTime: time.Now(),
	}
	if ok, err := st.ClaimAndPay(ctx, paid, tx, 2); err != nil || !ok {
		t.Fatalf("payment failed: ok=%v err=%v", ok, err)
	}
	if err := st.DB().Model(&model.Order{}).Where("id = ?", paid.ID).
		Update("notify_status", model.NotifySuccess).Error; err != nil {
		t.Fatal(err)
	}
	if err := st.ResetNotify(ctx, paid.TradeNo); err == nil {
		t.Fatal("reset reopened a successfully notified payment")
	}

	canceled := createTestOrder(t, st, "reset-canceled", "reset-canceled", 1)
	if err := st.CancelOrder(ctx, canceled.TradeNo); err != nil {
		t.Fatal(err)
	}
	if err := st.ResetNotify(ctx, canceled.TradeNo); err == nil {
		t.Fatal("reset reopened a canceled order")
	}
}

func TestAuditResetNotifyIsRateLimited(t *testing.T) {
	ctx := context.Background()
	st := openTestStore(t)
	o := createTestOrder(t, st, "reset-limit", "reset-limit", 1)
	tx := model.IncomingTx{
		Provider: "binance", Currency: "USDT", ReceiverAddr: "binance-uid",
		TransactionID: "reset-limit-tx", Type: "PAY", Amount: o.PayAmount,
		TransactionTime: time.Now(),
	}
	if ok, err := st.ClaimAndPay(ctx, o, tx, 2); err != nil || !ok {
		t.Fatalf("payment failed: ok=%v err=%v", ok, err)
	}
	if err := st.DB().Model(&model.Order{}).Where("id = ?", o.ID).
		Update("notify_status", model.NotifyFailed).Error; err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 2; i++ {
		if err := st.ResetNotify(ctx, o.TradeNo); err != nil {
			t.Fatalf("reset %d failed: %v", i+1, err)
		}
		if err := st.DB().Model(&model.Order{}).Where("id = ?", o.ID).
			Update("notify_status", model.NotifyFailed).Error; err != nil {
			t.Fatal(err)
		}
	}
	if err := st.ResetNotify(ctx, o.TradeNo); err == nil {
		t.Fatal("unlimited notification retries accepted")
	}
}
