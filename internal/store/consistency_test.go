package store

import (
	"context"
	"litepay/internal/model"
	"testing"
	"time"
)

func TestScanCursorNeverRegresses(t *testing.T) {
	st := openTestStore(t)
	ctx := context.Background()
	now := time.Now()
	if err := st.SaveScanCursor(ctx, "binance", now); err != nil {
		t.Fatal(err)
	}
	if err := st.SaveScanCursor(ctx, "binance", now.Add(-time.Hour)); err != nil {
		t.Fatal(err)
	}
	cur, err := st.ScanCursor(ctx, "binance")
	if err != nil || !cur.ScannedUntil.Equal(now) {
		t.Fatalf("regressed cursor: %+v %v", cur, err)
	}
}

func TestFinalizationWaitsForSuccessfulScan(t *testing.T) {
	st := openTestStore(t)
	ctx := context.Background()
	o := createTestOrder(t, st, "WAIT-SCAN", "WAIT-SCAN", 0)
	o, err := st.SelectPayment(ctx, o.TradeNo, "okx", "USDT", "uid", 10, .01, 99, .01, 2, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	expired := time.Now().Add(-2 * time.Minute)
	if err := st.DB().Model(o).Update("expired_at", expired).Error; err != nil {
		t.Fatal(err)
	}
	if _, err := st.MarkExpired(ctx); err != nil {
		t.Fatal(err)
	}
	if n, err := st.FinalizeExpired(ctx, time.Minute); err != nil || n != 0 {
		t.Fatalf("finalized without scan: %d %v", n, err)
	}
	if err := st.SaveScanCursor(ctx, "okx", time.Now()); err != nil {
		t.Fatal(err)
	}
	if n, err := st.FinalizeExpired(ctx, time.Minute); err != nil || n != 1 {
		t.Fatalf("did not finalize covered order: %d %v", n, err)
	}
	var got model.Order
	st.DB().First(&got, o.ID)
	if !got.ExpireFinalized {
		t.Fatal("not finalized")
	}
}

func TestObservationUpdateFailureRollsBackPayment(t *testing.T) {
	st := openTestStore(t)
	ctx := context.Background()
	o := createTestOrder(t, st, "ATOMIC-OBS", "ATOMIC-OBS", 0)
	o, err := st.SelectPayment(ctx, o.TradeNo, "okx", "USDT", "uid", 10, .01, 99, .01, 2, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if err := st.RecordTxObservation(ctx, model.TxObservation{Provider: "okx", TransactionID: "atomic", Reason: model.ObservationReasonNoMatch}); err != nil {
		t.Fatal(err)
	}
	if err := st.DB().Exec(`CREATE TRIGGER fail_resolve BEFORE UPDATE ON tx_observations BEGIN SELECT RAISE(FAIL,'resolve failure'); END`).Error; err != nil {
		t.Fatal(err)
	}
	ok, err := st.ClaimAndPay(ctx, o, model.IncomingTx{Provider: "okx", TransactionID: "atomic", Type: "3", Amount: o.PayAmount, Currency: "USDT", ReceiverAddr: "uid", TransactionTime: time.Now()}, 2)
	if err == nil || ok {
		t.Fatal("expected failed atomic payment")
	}
	current, _ := st.GetByTradeNo(ctx, o.TradeNo)
	var n int64
	st.DB().Model(&model.IncomingTx{}).Count(&n)
	if current.Status == model.StatusPaid || n != 0 {
		t.Fatalf("partial commit: %s %d", current.Status, n)
	}
}

func TestHistoryGapSurvivesSuccessfulScan(t *testing.T) {
	st := openTestStore(t)
	ctx := context.Background()
	now := time.Now()
	start := now.Add(-30 * 24 * time.Hour)
	end := now.Add(-7 * 24 * time.Hour)
	if err := st.RecordScanGap(ctx, "okx", start, end); err != nil {
		t.Fatal(err)
	}
	if err := st.SaveScanCursor(ctx, "okx", now); err != nil {
		t.Fatal(err)
	}
	cur, err := st.ScanCursor(ctx, "okx")
	if err != nil || cur.GapStart == nil || !cur.GapStart.Equal(start) || cur.GapEnd == nil || !cur.GapEnd.Equal(end) {
		t.Fatalf("gap erased: %+v %v", cur, err)
	}
}
