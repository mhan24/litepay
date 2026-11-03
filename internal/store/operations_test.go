package store

import (
	"context"
	"litepay/internal/model"
	"testing"
	"time"
)

func TestReconciliationRequiresObservedUniqueTransaction(t *testing.T) {
	st := openTestStore(t)
	ctx := context.Background()
	o := createTestOrder(t, st, "RECON", "RECON", 0)
	o, err := st.SelectPayment(ctx, o.TradeNo, "okx", "USDT", "uid", 1, .01, 99, .01, 2, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := st.ReconcileObservation(ctx, o.TradeNo, "missing", 2); err == nil {
		t.Fatal("invented transaction accepted")
	}
	at := time.Now()
	if err := st.RecordTxObservation(ctx, model.TxObservation{Provider: "okx", TransactionID: "observed", Type: "3", Amount: o.PayAmount, AmountText: "1.01", Currency: "USDT", ReceiverID: "uid", TransactionTime: at, Reason: model.ObservationReasonNoMatch}); err != nil {
		t.Fatal(err)
	}
	if _, err := st.ReconcileObservation(ctx, o.TradeNo, "observed", 2); err != nil {
		t.Fatal(err)
	}
	var n int64
	st.DB().Model(&model.IncomingTx{}).Where("transaction_id = ?", "observed").Count(&n)
	if n != 1 {
		t.Fatal("real transaction not consumed")
	}
	if _, err := st.ReconcileObservation(ctx, o.TradeNo, "observed", 2); err == nil {
		t.Fatal("resolved evidence reused")
	}
}

func TestReadOnlyPreflightDoesNotCreateDatabaseOrCredentials(t *testing.T) {
	path := t.TempDir() + "/missing.db"
	if _, err := OpenReadOnly(path); err == nil {
		t.Fatal("created nonexistent database")
	}
	st := openTestStore(t)
	var file string
	st.DB().Raw("SELECT file FROM pragma_database_list WHERE name = 'main'").Scan(&file)
	ro, err := OpenReadOnly(file)
	if err != nil {
		t.Fatal(err)
	}
	if err := ro.DB().Create(&model.Credential{Name: "should-not-write", Value: "secret"}).Error; err == nil {
		t.Fatal("readonly diagnostic wrote credentials")
	}
}
