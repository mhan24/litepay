package store

import (
	"context"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"litepay/internal/model"
)

func openTestStore(t *testing.T) *Store {
	t.Helper()
	st, err := Open(filepath.Join(t.TempDir(), "test.db"))
	if err != nil {
		t.Fatal(err)
	}
	return st
}

func createTestOrder(t *testing.T, st *Store, tradeNo, outTradeNo string, amount float64) *model.Order {
	t.Helper()
	o, err := st.CreateOrder(context.Background(), CreateOrderInput{
		Protocol:       "bepusdt",
		Pid:            "bepusdt",
		TradeNo:        tradeNo,
		OutTradeNo:     outTradeNo,
		Name:           "test",
		OriginMoney:    "7.30",
		OriginCurrency: "USD",
		NotifyURL:      "https://merchant.example/notify",
		ReturnURL:      "https://merchant.example/return",
		ExpiredAt:      time.Now().Add(10 * time.Minute),
		Reselect:       true,
	})
	if err != nil {
		t.Fatal(err)
	}
	if amount == 0 {
		return o
	}
	o, err = st.SelectPayment(context.Background(), o.TradeNo, "binance", "USDT",
		"binance-uid", amount, 0.01, 99, 0.01, 2, 2*time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	return o
}

func slotCount(t *testing.T, st *Store) int64 {
	t.Helper()
	var n int64
	if err := st.DB().Model(&model.AmountSlot{}).Where("order_id <> 0").Count(&n).Error; err != nil {
		t.Fatal(err)
	}
	return n
}

func TestCreateOrderStaysPendingWithoutAmountSlot(t *testing.T) {
	st := openTestStore(t)
	o := createTestOrder(t, st, "LP-PENDING", "MERCHANT-PENDING", 0)
	if o.Status != model.StatusPending {
		t.Fatalf("status = %s, want PENDING", o.Status)
	}
	if got := slotCount(t, st); got != 0 {
		t.Fatalf("slots = %d, want 0", got)
	}
}

func TestCreateOrReuseCashierOrderIsAtomicAndPreservesExpiry(t *testing.T) {
	st := openTestStore(t)
	ctx := context.Background()
	expires := time.Now().Add(10 * time.Minute).Truncate(time.Second)
	input := func(tradeNo string) CreateOrderInput {
		return CreateOrderInput{
			Protocol: "bepusdt", Pid: "bepusdt", TradeNo: tradeNo,
			OutTradeNo: "MERCHANT-SAME", OriginMoney: "2.50", OriginCurrency: "USD",
			NotifyURL: "https://merchant.example/notify", ReturnURL: "https://merchant.example/return",
			ExpiredAt: expires, Reselect: true,
		}
	}

	const workers = 8
	results := make(chan *model.Order, workers)
	errs := make(chan error, workers)
	var wg sync.WaitGroup
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			o, err := st.CreateOrReuseCashierOrder(ctx, input("LP-CONCURRENT-"+string(rune('A'+i))))
			results <- o
			errs <- err
		}(i)
	}
	wg.Wait()
	close(results)
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatal(err)
		}
	}
	first := <-results
	if first == nil {
		t.Fatal("missing order")
	}
	for o := range results {
		if o.TradeNo != first.TradeNo || !o.ExpiredAt.Equal(expires) {
			t.Fatalf("concurrent create was not reused: first=%s got=%+v", first.TradeNo, o)
		}
	}
	var count int64
	if err := st.DB().Model(&model.Order{}).Where("out_trade_no = ?", "MERCHANT-SAME").Count(&count).Error; err != nil {
		t.Fatal(err)
	}
	if count != 1 {
		t.Fatalf("same merchant order created %d rows, want 1", count)
	}
}

func TestCreateOrReuseCashierOrderRetiresExpiredOrderSilently(t *testing.T) {
	st := openTestStore(t)
	ctx := context.Background()
	old := createTestOrder(t, st, "LP-OLD", "MERCHANT-REPLACE", 1)
	if err := st.DB().Model(&model.Order{}).Where("id = ?", old.ID).Updates(map[string]any{
		"status": model.StatusExpired, "expire_finalized": true,
		"notify_status": model.NotifyPending, "next_notify_at": time.Now(),
	}).Error; err != nil {
		t.Fatal(err)
	}

	replacement, err := st.CreateOrReuseCashierOrder(ctx, CreateOrderInput{
		Protocol: "bepusdt", Pid: "bepusdt", TradeNo: "LP-NEW",
		OutTradeNo: "MERCHANT-REPLACE", OriginMoney: "2.50", OriginCurrency: "USD",
		NotifyURL: "https://merchant.example/notify", ReturnURL: "https://merchant.example/return",
		ExpiredAt: time.Now().Add(10 * time.Minute), Reselect: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	if replacement.TradeNo != "LP-NEW" {
		t.Fatalf("replacement trade_no = %s", replacement.TradeNo)
	}
	retired, _ := st.GetByTradeNo(ctx, old.TradeNo)
	if retired.Status != model.StatusSuperseded || retired.NotifyStatus != "" ||
		retired.NextNotifyAt != nil || !retired.ExpireFinalized {
		t.Fatalf("old order was not silently retired: %+v", retired)
	}
	if got := slotCount(t, st); got != 0 {
		t.Fatalf("slots = %d after replacement, want 0", got)
	}
	due, err := st.DueNotifications(ctx, 5, 20)
	if err != nil {
		t.Fatal(err)
	}
	for _, o := range due {
		if o.ID == old.ID {
			t.Fatal("superseded order is still scheduled for an expired callback")
		}
	}
}

func TestCreateOrReuseCashierOrderRebuildsReadyOrderOnlyWhenRequested(t *testing.T) {
	st := openTestStore(t)
	ctx := context.Background()
	old, err := st.CreateOrReuseCashierOrder(ctx, CreateOrderInput{
		Protocol: "bepusdt", Pid: "bepusdt", TradeNo: "LP-READY-OLD",
		OutTradeNo: "MERCHANT-READY-REBUILD", OriginMoney: "2.50", OriginCurrency: "USD",
		NotifyURL: "https://merchant.example/notify", ReturnURL: "https://merchant.example/return",
		ExpiredAt: time.Now().Add(time.Hour), Reselect: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := st.SelectPayment(ctx, old.TradeNo, "binance", "USDT",
		"binance-uid", 2.5, .01, 99, .01, 2, time.Minute); err != nil {
		t.Fatal(err)
	}

	reused, err := st.CreateOrReuseCashierOrder(ctx, CreateOrderInput{
		Protocol: "bepusdt", Pid: "bepusdt", TradeNo: "LP-READY-REUSED",
		OutTradeNo: "MERCHANT-READY-REBUILD", OriginMoney: "3.50", OriginCurrency: "USD",
		ExpiredAt: time.Now().Add(time.Hour), Reselect: true,
	})
	if err != nil || reused.TradeNo != old.TradeNo {
		t.Fatalf("implicit retry replaced READY order: %+v err=%v", reused, err)
	}

	replacement, err := st.CreateOrReuseCashierOrder(ctx, CreateOrderInput{
		Protocol: "bepusdt", Pid: "bepusdt", TradeNo: "LP-READY-NEW",
		OutTradeNo: "MERCHANT-READY-REBUILD", OriginMoney: "3.50", OriginCurrency: "USD",
		NotifyURL: "https://merchant.example/notify", ReturnURL: "https://merchant.example/return",
		ExpiredAt: time.Now().Add(time.Hour), Reselect: true, ReplaceReady: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	if replacement.TradeNo != "LP-READY-NEW" || replacement.OriginMoney != "3.50" {
		t.Fatalf("ready order was not rebuilt: %+v", replacement)
	}
	retired, _ := st.GetByTradeNo(ctx, old.TradeNo)
	if retired.Status != model.StatusSuperseded || !retired.ExpireFinalized {
		t.Fatalf("old ready order was not retired: %+v", retired)
	}
}

func TestFinishNotifyCannotOverwriteSupersededOrder(t *testing.T) {
	st := openTestStore(t)
	ctx := context.Background()
	old := createTestOrder(t, st, "LP-NOTIFY-OLD", "MERCHANT-NOTIFY-REPLACE", 0)
	if err := st.DB().Model(&model.Order{}).Where("id = ?", old.ID).Updates(map[string]any{
		"status": model.StatusExpired, "expire_finalized": true,
		"notify_status": model.NotifyPending, "next_notify_at": time.Now(),
	}).Error; err != nil {
		t.Fatal(err)
	}
	stale, _ := st.GetByTradeNo(ctx, old.TradeNo)
	if _, err := st.CreateOrReuseCashierOrder(ctx, CreateOrderInput{
		Protocol: "bepusdt", Pid: "bepusdt", TradeNo: "LP-NOTIFY-NEW",
		OutTradeNo: "MERCHANT-NOTIFY-REPLACE", OriginMoney: "2.50", OriginCurrency: "USD",
		NotifyURL: "https://merchant.example/notify", ReturnURL: "https://merchant.example/return",
		ExpiredAt: time.Now().Add(10 * time.Minute), Reselect: true,
	}); err != nil {
		t.Fatal(err)
	}
	if err := st.FinishNotify(ctx, stale, 1, true, "success", 200, stale.NotifyURL, "", nil); err != nil {
		t.Fatal(err)
	}
	current, _ := st.GetByTradeNo(ctx, old.TradeNo)
	if current.Status != model.StatusSuperseded || current.NotifyStatus != "" ||
		current.NotifiedAt != nil {
		t.Fatalf("stale notification overwrote the retired order: %+v", current)
	}
}

func TestSelectPaymentReselectsAcrossProvidersAndCurrencies(t *testing.T) {
	st := openTestStore(t)
	ctx := context.Background()
	o := createTestOrder(t, st, "LP-RESELECT", "MERCHANT-RESELECT", 2)

	o, err := st.SelectPayment(ctx, o.TradeNo, "okx", "USDT",
		"okx-uid", 1, 0.01, 99, 0.01, 2, 2*time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if o.Status != model.StatusReady || o.Collection != "okx" {
		t.Fatalf("unexpected OKX USDT order: %+v", o)
	}
	if got := slotCount(t, st); got != 1 {
		t.Fatalf("slots after first selection = %d, want 1", got)
	}

	o, err = st.SelectPayment(ctx, o.TradeNo, "okx", "USDC",
		"okx-uid", 1, 0.01, 99, 0.01, 2, 2*time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if o.Currency != "USDC" {
		t.Fatalf("unexpected OKX USDC order: %+v", o)
	}
	if got := slotCount(t, st); got != 1 {
		t.Fatalf("slots after reselect = %d, want 1", got)
	}
	// 重选后旧档位保留为冷却占用（order_id=0，至原窗口结束），
	// 防止窗口内针对旧金额的迟到转账被归到复用该金额的其他订单。
	var reserved int64
	if err := st.DB().Model(&model.AmountSlot{}).
		Where("provider = ? AND currency = ? AND order_id = 0", "okx", "USDT").
		Count(&reserved).Error; err != nil {
		t.Fatal(err)
	}
	if reserved != 1 {
		t.Fatalf("reselect left %d cooldown slots for okx/USDT, want 1", reserved)
	}

	o, err = st.SelectPayment(ctx, o.TradeNo, "binance", "USDT",
		"binance-uid", 1, 0.01, 99, 0.01, 2, 2*time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if o.Collection != "binance" || o.Currency != "USDT" {
		t.Fatalf("unexpected Binance order: %+v", o)
	}
	if got := slotCount(t, st); got != 1 {
		t.Fatalf("slots after provider switch = %d, want 1", got)
	}
}

func TestSelectPaymentSameOKXCurrencyKeepsPreciseAmount(t *testing.T) {
	st := openTestStore(t)
	ctx := context.Background()
	o := createTestOrder(t, st, "LP-CHAIN", "MERCHANT-CHAIN", 0)
	o, err := st.SelectPayment(ctx, o.TradeNo, "okx", "USDT",
		"okx-uid", 1, 0.01, 99, 0.01, 2, 2*time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	originalAmount := o.PayAmount
	o, err = st.SelectPayment(ctx, o.TradeNo, "okx", "USDT",
		"okx-uid", 1, 0.01, 99, 0.01, 2, 2*time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if o.PayAmount != originalAmount {
		t.Fatalf("same-currency reselect changed amount: %+v", o)
	}
}

func TestClaimAndPayRejectsCrossProviderAndWrongOKXUID(t *testing.T) {
	st := openTestStore(t)
	ctx := context.Background()
	o := createTestOrder(t, st, "LP-CLAIM", "MERCHANT-CLAIM", 0)
	o, err := st.SelectPayment(ctx, o.TradeNo, "okx", "USDT",
		"okx-uid", 1, 0.01, 99, 0.01, 2, 2*time.Minute)
	if err != nil {
		t.Fatal(err)
	}

	baseTx := model.IncomingTx{
		TransactionID:   "tx-1",
		Provider:        "okx",
		Type:            "3",
		Amount:          o.PayAmount,
		Currency:        "USDT",
		ReceiverID:      "okx-uid",
		ReceiverAddr:    "okx-uid",
		TransactionTime: time.Now(),
	}
	if _, err := st.ClaimAndPay(ctx, o, func() model.IncomingTx {
		tx := baseTx
		tx.Provider = "binance"
		return tx
	}(), 2); err == nil {
		t.Fatal("accepted a Binance transaction for an OKX order")
	}
	if _, err := st.ClaimAndPay(ctx, o, func() model.IncomingTx {
		tx := baseTx
		tx.ReceiverAddr = "other-uid"
		return tx
	}(), 2); err == nil {
		t.Fatal("accepted a different OKX receiver UID")
	}
	if _, err := st.ClaimAndPay(ctx, o, func() model.IncomingTx {
		tx := baseTx
		tx.ReceiverAddr = ""
		return tx
	}(), 2); err == nil {
		t.Fatal("accepted an internal transfer without receiver UID")
	}
	ok, err := st.ClaimAndPay(ctx, o, baseTx, 2)
	if err != nil || !ok {
		t.Fatalf("valid transaction failed: ok=%v err=%v", ok, err)
	}
}

func TestClaimAndPayRequiresBinanceReceiverUID(t *testing.T) {
	st := openTestStore(t)
	ctx := context.Background()
	o := createTestOrder(t, st, "LP-BINANCE-RECEIVER", "MERCHANT-BINANCE-RECEIVER", 1)
	tx := model.IncomingTx{
		TransactionID:   "tx-binance-receiver",
		Provider:        "binance",
		Type:            "PAY",
		Amount:          o.PayAmount,
		Currency:        "USDT",
		TransactionTime: time.Now(),
	}
	if _, err := st.ClaimAndPay(ctx, o, tx, 2); err == nil {
		t.Fatal("accepted a Binance transfer without receiver UID")
	}
	ok, err := st.ClaimAndPay(ctx, o, func() model.IncomingTx {
		tx := tx
		tx.ReceiverID = o.PayAddress
		tx.ReceiverAddr = o.PayAddress
		return tx
	}(), 2)
	if err != nil || !ok {
		t.Fatalf("valid Binance transfer failed: ok=%v err=%v", ok, err)
	}
}

func TestClaimAndPayUsesRoundedMatching(t *testing.T) {
	st := openTestStore(t)
	ctx := context.Background()
	o := createTestOrder(t, st, "LP-PRECISION", "MERCHANT-PRECISION", 0)
	o, err := st.SelectPayment(ctx, o.TradeNo, "binance", "USDT",
		"binance-uid", 2.5099, 0.0001, 99, 0.01, 4, 2*time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if o.PayAmount != 2.51 {
		t.Fatalf("pay amount = %.6f, want 2.51", o.PayAmount)
	}

	base := model.IncomingTx{
		Provider: "binance", Type: "PAY", Currency: "USDT",
		ReceiverID: "binance-uid", ReceiverAddr: "binance-uid",
		TransactionTime: time.Now(),
	}
	withinPrecision := base
	withinPrecision.TransactionID = "tx-precision-ok"
	withinPrecision.Amount = 2.5100
	if ok, err := st.ClaimAndPay(ctx, o, withinPrecision, 4); err != nil || !ok {
		t.Fatalf("2.5100 should match 2.51 under rounded matching: ok=%v err=%v", ok, err)
	}

	o = createTestOrder(t, st, "LP-PRECISION-2", "MERCHANT-PRECISION-2", 0)
	o, err = st.SelectPayment(ctx, o.TradeNo, "binance", "USDT",
		"binance-uid", 2.5099, 0.0001, 99, 0.01, 4, 2*time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	roundedUp := base
	roundedUp.TransactionID = "tx-precision-bad"
	roundedUp.Amount = o.PayAmount + 0.005
	if ok, err := st.ClaimAndPay(ctx, o, roundedUp, 4); err == nil || ok {
		t.Fatalf("2.515 must round to 2.52 and not match 2.51: ok=%v err=%v", ok, err)
	}
}

func TestCanceledAndPendingOrdersDoNotAcceptPayment(t *testing.T) {
	st := openTestStore(t)
	ctx := context.Background()

	pending := createTestOrder(t, st, "LP-PENDING-TX", "MERCHANT-PENDING-TX", 0)
	if err := st.CancelOrder(ctx, pending.TradeNo); err != nil {
		t.Fatalf("cancel PENDING order: %v", err)
	}
	if err := st.CancelOrder(ctx, pending.TradeNo); err != nil {
		t.Fatalf("idempotent PENDING cancel failed: %v", err)
	}
	pending, _ = st.GetByTradeNo(ctx, pending.TradeNo)
	if pending.Status != model.StatusCanceled || !pending.ExpireFinalized {
		t.Fatalf("PENDING cancellation is incomplete: %+v", pending)
	}
	if _, err := st.ClaimAndPay(ctx, pending, model.IncomingTx{
		TransactionID: "tx-pending", Provider: "binance", Type: "PAY",
		Amount: 1, Currency: "USDT", TransactionTime: time.Now(),
	}, 2); err == nil {
		t.Fatal("accepted a transaction for a PENDING order")
	}

	canceled := createTestOrder(t, st, "LP-CANCEL", "MERCHANT-CANCEL", 0)
	canceled, err := st.SelectPayment(ctx, canceled.TradeNo, "binance", "USDT",
		"binance-uid", 1, 0.01, 99, 0.01, 2, 2*time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if err := st.CancelOrder(ctx, canceled.TradeNo); err != nil {
		t.Fatal(err)
	}
	if err := st.CancelOrder(ctx, canceled.TradeNo); err != nil {
		t.Fatalf("idempotent cancel failed: %v", err)
	}
	if _, err := st.ClaimAndPay(ctx, canceled, model.IncomingTx{
		TransactionID: "tx-canceled", Provider: "binance", Type: "PAY",
		Amount: canceled.PayAmount, Currency: "USDT", TransactionTime: time.Now(),
	}, 2); err == nil {
		t.Fatal("accepted a transaction for a CANCELED order")
	}
}

func TestCancelOrderDuringGraceStopsListeningWithoutExpiredCallback(t *testing.T) {
	st := openTestStore(t)
	ctx := context.Background()
	o := createTestOrder(t, st, "LP-CANCEL-GRACE", "MERCHANT-CANCEL-GRACE", 0)
	o, err := st.SelectPayment(ctx, o.TradeNo, "binance", "USDT",
		"binance-uid", 1, 0.01, 99, 0.01, 2, 2*time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if err := st.DB().Model(&model.Order{}).Where("id = ?", o.ID).
		Updates(map[string]any{"expired_at": time.Now().Add(-30 * time.Second), "selected_at": time.Now().Add(-time.Minute)}).Error; err != nil {
		t.Fatal(err)
	}
	if n, err := st.MarkExpired(ctx); err != nil || n != 1 {
		t.Fatalf("mark expired: n=%d err=%v", n, err)
	}

	if err := st.CancelOrder(ctx, o.TradeNo); err != nil {
		t.Fatal(err)
	}
	if err := st.CancelOrder(ctx, o.TradeNo); err != nil {
		t.Fatalf("idempotent cancel during grace failed: %v", err)
	}
	canceled, _ := st.GetByTradeNo(ctx, o.TradeNo)
	if canceled.Status != model.StatusCanceled || !canceled.ExpireFinalized ||
		canceled.NotifyStatus != model.NotifyCanceled || canceled.NextNotifyAt != nil {
		t.Fatalf("cancellation during grace is incomplete: %+v", canceled)
	}
	if got := slotCount(t, st); got != 0 {
		t.Fatalf("slots = %d after cancellation, want 0", got)
	}
	orders, err := st.MatchableOrders(ctx, "binance", time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	for _, candidate := range orders {
		if candidate.ID == o.ID {
			t.Fatal("canceled order is still matchable")
		}
	}
	if _, err := st.ClaimAndPay(ctx, canceled, model.IncomingTx{
		TransactionID: "tx-after-grace-cancel", Provider: "binance", Type: "PAY",
		Amount: canceled.PayAmount, Currency: "USDT", TransactionTime: time.Now(),
	}, 2); err == nil {
		t.Fatal("canceled grace-period order accepted a late payment")
	}
}

func TestCancelOrderCannotOverwriteScheduledExpiredCallback(t *testing.T) {
	st := openTestStore(t)
	ctx := context.Background()
	o := createTestOrder(t, st, "LP-CANCEL-FINALIZED", "MERCHANT-CANCEL-FINALIZED", 0)
	o, err := st.SelectPayment(ctx, o.TradeNo, "binance", "USDT",
		"binance-uid", 1, 0.01, 99, 0.01, 2, 0)
	if err != nil {
		t.Fatal(err)
	}
	if err := st.DB().Model(&model.Order{}).Where("id = ?", o.ID).
		Update("expired_at", time.Now().Add(-2*time.Minute)).Error; err != nil {
		t.Fatal(err)
	}
	if n, err := st.MarkExpired(ctx); err != nil || n != 1 {
		t.Fatalf("mark expired: n=%d err=%v", n, err)
	}
	if n, err := st.FinalizeExpired(ctx, time.Minute); err != nil || n != 1 {
		t.Fatalf("finalize expired: n=%d err=%v", n, err)
	}
	if err := st.CancelOrder(ctx, o.TradeNo); err == nil {
		t.Fatal("cancel overwrote an already scheduled expired callback")
	}
	current, _ := st.GetByTradeNo(ctx, o.TradeNo)
	if current.Status != model.StatusExpired || !current.ExpireFinalized ||
		current.NotifyStatus != model.NotifyPending || current.NextNotifyAt == nil {
		t.Fatalf("scheduled expiration changed by rejected cancel: %+v", current)
	}
}

func TestExpiredOrderCanPayWithinGraceAndSlotExpiresAfterGrace(t *testing.T) {
	st := openTestStore(t)
	ctx := context.Background()
	o := createTestOrder(t, st, "LP-GRACE", "MERCHANT-GRACE", 0)
	o, err := st.SelectPayment(ctx, o.TradeNo, "binance", "USDT",
		"binance-uid", 1, 0.01, 99, 0.01, 2, 2*time.Minute)
	if err != nil {
		t.Fatal(err)
	}

	if err := st.DB().Model(&model.Order{}).Where("id = ?", o.ID).
		Updates(map[string]any{"expired_at": time.Now().Add(-30 * time.Second), "selected_at": time.Now().Add(-time.Minute)}).Error; err != nil {
		t.Fatal(err)
	}
	if n, err := st.MarkExpired(ctx); err != nil || n != 1 {
		t.Fatalf("expire order: n=%d err=%v", n, err)
	}
	expired, _ := st.GetByTradeNo(ctx, o.TradeNo)
	if expired.NotifyStatus != "" || expired.NextNotifyAt != nil {
		t.Fatalf("grace-period expiration scheduled a premature callback: %+v", expired)
	}
	if n, err := st.FinalizeExpired(ctx, 2*time.Minute); err != nil || n != 0 {
		t.Fatalf("order finalized inside grace: n=%d err=%v", n, err)
	}

	o = expired
	ok, err := st.ClaimAndPay(ctx, o, model.IncomingTx{
		TransactionID: "tx-grace", Provider: "binance", Type: "PAY",
		Amount: o.PayAmount, Currency: "USDT", ReceiverID: "binance-uid",
		ReceiverAddr: "binance-uid", TransactionTime: time.Now().Add(-45 * time.Second),
	}, 2)
	if err != nil || !ok {
		t.Fatalf("expired order inside grace rejected: ok=%v err=%v", ok, err)
	}
	paid, _ := st.GetByTradeNo(ctx, o.TradeNo)
	if paid.Status != model.StatusPaid || paid.NotifyStatus != model.NotifyPending ||
		paid.ExpireFinalized {
		t.Fatalf("grace payment did not replace expiration with success: %+v", paid)
	}
}

func TestFinalizeExpiredStopsListeningAndSchedulesExpiredCallback(t *testing.T) {
	st := openTestStore(t)
	ctx := context.Background()
	o := createTestOrder(t, st, "LP-FINALIZE", "MERCHANT-FINALIZE", 0)
	o, err := st.SelectPayment(ctx, o.TradeNo, "binance", "USDT",
		"binance-uid", 1, 0.01, 99, 0.01, 2, 0)
	if err != nil {
		t.Fatal(err)
	}
	if err := st.DB().Model(&model.Order{}).Where("id = ?", o.ID).
		Update("expired_at", time.Now().Add(-2*time.Minute)).Error; err != nil {
		t.Fatal(err)
	}
	if n, err := st.MarkExpired(ctx); err != nil || n != 1 {
		t.Fatalf("mark expired: n=%d err=%v", n, err)
	}
	if n, err := st.FinalizeExpired(ctx, time.Minute); err != nil || n != 1 {
		t.Fatalf("finalize expired: n=%d err=%v", n, err)
	}
	o, _ = st.GetByTradeNo(ctx, o.TradeNo)
	t.Logf("DEBUG selected=%v expired=%v now=%v", o.SelectedAt, o.ExpiredAt, time.Now())
	if !o.ExpireFinalized || o.NotifyStatus != model.NotifyPending || o.NextNotifyAt == nil {
		t.Fatalf("expired order was not finalized with callback: %+v", o)
	}
	if got := slotCount(t, st); got != 0 {
		t.Fatalf("slots = %d after finalize, want 0", got)
	}
	orders, err := st.MatchableOrders(ctx, "binance", time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	for _, candidate := range orders {
		if candidate.ID == o.ID {
			t.Fatal("finalized expired order is still matchable")
		}
	}
	if _, err := st.ClaimAndPay(ctx, o, model.IncomingTx{
		TransactionID: "tx-after-finalize", Provider: "binance", Type: "PAY",
		Amount: o.PayAmount, Currency: "USDT", TransactionTime: time.Now(),
	}, 2); err == nil {
		t.Fatal("finalized expired order accepted a late payment")
	}
}

func TestForceExpireKeepsGraceAndFinalizesLate(t *testing.T) {
	st := openTestStore(t)
	ctx := context.Background()
	o := createTestOrder(t, st, "LP-FORCE", "MERCHANT-FORCE", 0)
	o, err := st.SelectPayment(ctx, o.TradeNo, "binance", "USDT",
		"binance-uid", 1, 0.01, 99, 0.01, 2, time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	selectedAt := time.Now().Add(-time.Minute)
	if err := st.DB().Model(&model.Order{}).Where("id = ?", o.ID).
		Updates(map[string]any{"selected_at": selectedAt, "expired_at": time.Now().Add(time.Hour)}).Error; err != nil {
		t.Fatal(err)
	}
	sentBeforeForce := time.Now()
	// 截止时间以数据库当前值为准（上面的 Updates 刚改过 expired_at）。
	o, _ = st.GetByTradeNo(ctx, o.TradeNo)
	deadlineBeforeForce := o.ExpiredAt
	if err := st.ForceExpire(ctx, o.TradeNo); err != nil {
		t.Fatal(err)
	}
	o, _ = st.GetByTradeNo(ctx, o.TradeNo)
	if o.Status != model.StatusExpired || o.ExpireFinalized ||
		o.NotifyStatus != "" || o.NextNotifyAt != nil {
		t.Fatalf("force expire should start grace, got %+v", o)
	}
	if !o.ExpiredAt.Equal(deadlineBeforeForce) {
		t.Fatalf("force expire changed the payment deadline: got=%v want=%v",
			o.ExpiredAt, deadlineBeforeForce)
	}
	if got := slotCount(t, st); got != 1 {
		t.Fatalf("slots = %d during force-expire grace, want 1", got)
	}
	// 转账时间以交易所流水为准，后台提前结束收银台不能改变已发生的付款。
	if ok, err := st.ClaimAndPay(ctx, o, model.IncomingTx{
		TransactionID: "tx-during-force-grace", Provider: "binance", Type: "PAY",
		Amount: o.PayAmount, Currency: "USDT", ReceiverAddr: "binance-uid",
		TransactionTime: sentBeforeForce,
	}, 2); err != nil || !ok {
		t.Fatalf("force-expire grace rejected a valid payment: ok=%v err=%v", ok, err)
	}
}

func TestForceExpireFinalizesOrderDuringGrace(t *testing.T) {
	st := openTestStore(t)
	ctx := context.Background()
	o := createTestOrder(t, st, "LP-FORCE-GRACE", "MERCHANT-FORCE-GRACE", 0)
	o, err := st.SelectPayment(ctx, o.TradeNo, "binance", "USDT",
		"binance-uid", 1, 0.01, 99, 0.01, 2, time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	if err := st.DB().Model(&model.Order{}).Where("id = ?", o.ID).
		Update("expired_at", time.Now().Add(-30*time.Second)).Error; err != nil {
		t.Fatal(err)
	}
	if err := st.ForceExpire(ctx, o.TradeNo); err != nil {
		t.Fatal(err)
	}
	o, _ = st.GetByTradeNo(ctx, o.TradeNo)
	if o.Status != model.StatusExpired || o.ExpireFinalized ||
		o.NotifyStatus != "" || o.NextNotifyAt != nil {
		t.Fatalf("force expire did not start a fresh grace: %+v", o)
	}
	if got := slotCount(t, st); got != 1 {
		t.Fatalf("slots = %d during force-expire grace, want 1", got)
	}
	if n, err := st.FinalizeExpired(ctx, time.Hour); err != nil || n != 0 {
		t.Fatalf("finalized before force-expire grace: n=%d err=%v", n, err)
	}
	if err := st.DB().Model(&model.Order{}).Where("id = ?", o.ID).
		Update("expired_at", time.Now().Add(-2*time.Hour)).Error; err != nil {
		t.Fatal(err)
	}
	if n, err := st.FinalizeExpired(ctx, time.Hour); err != nil || n != 1 {
		t.Fatalf("finalize force-expired order: n=%d err=%v", n, err)
	}
	o, _ = st.GetByTradeNo(ctx, o.TradeNo)
	if !o.ExpireFinalized || o.NotifyStatus != model.NotifyPending || o.NextNotifyAt == nil {
		t.Fatalf("force-expired order was not finalized: %+v", o)
	}
	if got := slotCount(t, st); got != 0 {
		t.Fatalf("slots = %d after force-expire finalize, want 0", got)
	}
}

func TestForceExpireCannotDuplicateScheduledCallback(t *testing.T) {
	st := openTestStore(t)
	ctx := context.Background()
	o := createTestOrder(t, st, "LP-FORCE-FINALIZED", "MERCHANT-FORCE-FINALIZED", 0)
	o, err := st.SelectPayment(ctx, o.TradeNo, "binance", "USDT",
		"binance-uid", 1, 0.01, 99, 0.01, 2, 0)
	if err != nil {
		t.Fatal(err)
	}
	if err := st.DB().Model(&model.Order{}).Where("id = ?", o.ID).
		Update("expired_at", time.Now().Add(-2*time.Minute)).Error; err != nil {
		t.Fatal(err)
	}
	if n, err := st.MarkExpired(ctx); err != nil || n != 1 {
		t.Fatalf("mark expired: n=%d err=%v", n, err)
	}
	if n, err := st.FinalizeExpired(ctx, time.Minute); err != nil || n != 1 {
		t.Fatalf("finalize expired: n=%d err=%v", n, err)
	}
	if err := st.ForceExpire(ctx, o.TradeNo); err == nil {
		t.Fatal("force expire accepted an already finalized order")
	}
	current, _ := st.GetByTradeNo(ctx, o.TradeNo)
	if current.NotifyStatus != model.NotifyPending || current.NextNotifyAt == nil {
		t.Fatalf("scheduled expiration changed by rejected force expire: %+v", current)
	}
}

func TestManualPayRequiresValidSelectionAndGrace(t *testing.T) {
	st := openTestStore(t)
	ctx := context.Background()
	pending := createTestOrder(t, st, "LP-MANUAL-PENDING", "MERCHANT-MANUAL-PENDING", 0)
	if _, err := st.ManualPay(ctx, pending.TradeNo, 2, 2*time.Minute); err == nil {
		t.Fatal("manual pay accepted a PENDING order")
	}

	ready := createTestOrder(t, st, "LP-MANUAL-READY", "MERCHANT-MANUAL-READY", 0)
	ready, err := st.SelectPayment(ctx, ready.TradeNo, "okx", "USDC",
		"okx-uid", 1, 0.0001, 99, 0.01, 4, 2*time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	paid, err := st.ManualPay(ctx, ready.TradeNo, 4, 2*time.Minute)
	if err != nil || paid.Status != model.StatusPaid {
		t.Fatalf("valid manual pay failed: status=%v err=%v", paid, err)
	}

	expired := createTestOrder(t, st, "LP-MANUAL-EXPIRED", "MERCHANT-MANUAL-EXPIRED", 0)
	expired, err = st.SelectPayment(ctx, expired.TradeNo, "binance", "USDT",
		"binance-uid", 1, 0.01, 99, 0.01, 2, 2*time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if err := st.DB().Model(&model.Order{}).Where("id = ?", expired.ID).
		Update("expired_at", time.Now().Add(-3*time.Minute)).Error; err != nil {
		t.Fatal(err)
	}
	if n, err := st.MarkExpired(ctx); err != nil || n != 1 {
		t.Fatalf("expire order: n=%d err=%v", n, err)
	}
	if _, err := st.ManualPay(ctx, expired.TradeNo, 2, 2*time.Minute); err == nil {
		t.Fatal("manual pay accepted an order past the grace period")
	}
}
