package store

import (
	"context"
	"database/sql"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"litepay/internal/model"
)

func TestUpgradePreIdentitySQLitePreservesPaidOrders(t *testing.T) {
	path := filepath.Join(t.TempDir(), "old-schema.db")
	db, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatal(err)
	}
	for _, stmt := range []string{
		`CREATE TABLE incoming_txs(provider text,transaction_id text,type text,amount real,currency text,order_trade_no text,raw text,PRIMARY KEY(provider,transaction_id))`,
		`INSERT INTO incoming_txs VALUES('binance','old-event:0','PAY',2.51,'USDT','PAID-OLD','{"transactionId":"old-event","orderId":"old-app"}')`,
		`CREATE TABLE orders(id integer PRIMARY KEY,trade_no text,status text,pay_amount real,origin_money text,origin_currency text,amount_precision integer)`,
		`INSERT INTO orders VALUES(1,'PAID-OLD','PAID',2.51,'2.50','USD',2)`,
	} {
		if _, err := db.Exec(stmt); err != nil {
			t.Fatal(err)
		}
	}
	db.Close()
	st, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	old, err := st.GetByTradeNo(context.Background(), "PAID-OLD")
	if err != nil || old.Status != model.StatusPaid || old.PayAmount != 2.51 {
		t.Fatalf("legacy paid order changed: %+v %v", old, err)
	}
	var row model.IncomingTx
	if err := st.DB().Where("transaction_id = ?", "old-event:0").First(&row).Error; err != nil || row.EventID != "old-event" || row.ReferenceID != "old-app" || row.TransactionID != "old-event:0" {
		t.Fatalf("legacy identity lost: %+v %v", row, err)
	}
	var guard model.EventClaim
	if err := st.DB().Where("provider = ? AND event_id = ?", "binance", "old-event").First(&guard).Error; err != nil || guard.TradeNo != "PAID-OLD" {
		t.Fatal("legacy event not reserved")
	}
}

func TestParentEventCanOnlyConsumeOneOrder(t *testing.T) {
	st := openTestStore(t)
	ctx := context.Background()
	a := createTestOrder(t, st, "EVENT-A", "A", 1)
	b := createTestOrder(t, st, "EVENT-B", "B", 2)
	var paid atomic.Int32
	var wg sync.WaitGroup
	for _, o := range []*model.Order{a, b} {
		wg.Add(1)
		go func(o *model.Order) {
			defer wg.Done()
			ok, err := st.ClaimAndPay(ctx, o, model.IncomingTx{Provider: "binance", EventID: "same-parent", TransactionID: "same-parent:" + o.TradeNo, CanonicalID: "same-parent:" + o.TradeNo, Type: "PAY", Currency: o.Currency, Amount: o.PayAmount, ReceiverAddr: o.PayAddress, TransactionTime: time.Now()}, 2)
			if err != nil {
				t.Error(err)
			}
			if ok {
				paid.Add(1)
			}
		}(o)
	}
	wg.Wait()
	if paid.Load() != 1 {
		t.Fatalf("parent credited %d orders", paid.Load())
	}
	var rows, slots int64
	st.DB().Model(&model.IncomingTx{}).Count(&rows)
	st.DB().Model(&model.AmountSlot{}).Where("order_id <> 0").Count(&slots)
	if rows != 1 || slots != 1 {
		t.Fatalf("rows=%d slots=%d", rows, slots)
	}
}

func TestIdentityMigrationPreservesLegacyClaimsAndBlocksAliases(t *testing.T) {
	path := filepath.Join(t.TempDir(), "legacy.db")
	st, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	for _, row := range []model.IncomingTx{
		{Provider: "binance", TransactionID: "parent:0", OrderTradeNo: "OLD-A", Raw: `{"transactionId":"parent","fundsDetail":[{"amount":"1","currency":"USDT"},{"amount":"2","currency":"USDC"}]}`},
		{Provider: "binance", TransactionID: "parent:1", OrderTradeNo: "OLD-B", Raw: `{"transactionId":"parent"}`},
		{Provider: "binance", TransactionID: "unknown:0", OrderTradeNo: "OLD-C"},
	} {
		if err := st.DB().Create(&row).Error; err != nil {
			t.Fatal(err)
		}
	}
	db, _ := st.DB().DB()
	db.Close()
	st, err = Open(path)
	if err != nil {
		t.Fatal(err)
	}
	var guard model.EventClaim
	if err := st.DB().Where("provider = ? AND event_id = ?", "binance", "parent").First(&guard).Error; err != nil || guard.State != "legacy_conflict" {
		t.Fatalf("legacy conflict missing %+v %v", guard, err)
	}
	var rows []model.IncomingTx
	st.DB().Order("transaction_id").Find(&rows)
	if len(rows) != 3 || rows[0].TransactionID != "parent:0" || rows[0].OrderTradeNo != "OLD-A" || rows[1].OrderTradeNo != "OLD-B" {
		t.Fatal("migration rewrote history")
	}
	o := createTestOrder(t, st, "NEW", "NEW", 1)
	for _, id := range []string{"parent", "unknown"} {
		ok, err := st.ClaimAndPay(context.Background(), o, model.IncomingTx{Provider: "binance", EventID: id, CanonicalID: id, TransactionID: id, Type: "PAY", Currency: o.Currency, Amount: o.PayAmount, ReceiverAddr: o.PayAddress, TransactionTime: time.Now(), Raw: `{"transactionId":"` + id + `"}`}, 2)
		if ok {
			t.Fatal("legacy alias paid again")
		}
		if id == "unknown" && err == nil {
			t.Fatal("unknown legacy identity not blocked")
		}
	}
	current, _ := st.GetByTradeNo(context.Background(), o.TradeNo)
	if current.Status != model.StatusReady {
		t.Fatal("blocked identity changed order")
	}
	db, _ = st.DB().DB()
	db.Close()
	if _, err := Open(path); err != nil {
		t.Fatalf("migration not repeatable: %v", err)
	}
}

func TestStoreRejectsAmbiguousOrMissingExchangeIdentity(t *testing.T) {
	st := openTestStore(t)
	o := createTestOrder(t, st, "IDENTITY", "IDENTITY", 1)
	for _, raw := range []string{`{"transactionId":"funds","fundsDetail":[{"amount":"1.01","currency":"USDT"},{"amount":"2.01","currency":"USDC"}]}`, `{"orderId":"only-reference"}`} {
		if ok, err := st.ClaimAndPay(context.Background(), o, model.IncomingTx{Provider: "binance", TransactionID: "funds", Type: "PAY", Currency: o.Currency, Amount: o.PayAmount, ReceiverAddr: o.PayAddress, TransactionTime: time.Now(), Raw: raw}, 2); ok || err == nil {
			t.Fatal("uncertain identity accepted")
		}
	}
}

func TestObservedAmbiguityCannotBeErasedByAnotherAPIShape(t *testing.T) {
	st := openTestStore(t)
	ctx := context.Background()
	o := createTestOrder(t, st, "SHAPE", "SHAPE", 1)
	if err := st.RecordTxObservation(ctx, model.TxObservation{Provider: "binance", TransactionID: "shape:0", EventID: "shape", CanonicalID: "shape", IdentityIssue: "ambiguous", Reason: model.ObservationReasonIdentity}); err != nil {
		t.Fatal(err)
	}
	if ok, err := st.ClaimAndPay(ctx, o, model.IncomingTx{Provider: "binance", TransactionID: "shape", EventID: "shape", CanonicalID: "shape", Type: "PAY", Currency: o.Currency, Amount: o.PayAmount, ReceiverAddr: o.PayAddress, TransactionTime: time.Now(), Raw: `{"transactionId":"shape"}`}, 2); ok || err == nil {
		t.Fatal("changed API shape erased ambiguity")
	}
	var count int64
	st.DB().Model(&model.IncomingTx{}).Count(&count)
	if count != 0 {
		t.Fatal("ambiguous shape created claim")
	}
}
