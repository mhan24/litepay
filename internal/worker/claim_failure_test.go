package worker

import (
	"context"
	"litepay/internal/model"
	"testing"
	"time"
)

func TestClaimDatabaseFailureDoesNotAdvanceCursor(t *testing.T) {
	f := newPollFixture(t, "DB-CLAIM")
	ctx := context.Background()
	until := time.Now().Add(-time.Minute)
	if err := f.st.SaveScanCursor(ctx, "binance", until); err != nil {
		t.Fatal(err)
	}
	if err := f.st.DB().Exec(`CREATE TRIGGER fail_claim BEFORE INSERT ON incoming_txs BEGIN SELECT RAISE(FAIL,'injected write failure'); END`).Error; err != nil {
		t.Fatal(err)
	}
	f.stub.set(stubBinanceTx{id: "db-claim", orderType: "PAY", amount: f.order.PayAmount, currency: "USDT", at: f.txTime(), receiver: "binance-uid"})
	f.tick(t)
	cur, err := f.st.ScanCursor(ctx, "binance")
	if err != nil || !cur.ScannedUntil.Equal(until) || cur.LastError == "" {
		t.Fatalf("technical failure treated as success: %+v %v", cur, err)
	}
	o, _ := f.st.GetByTradeNo(ctx, f.order.TradeNo)
	if o.Status == model.StatusPaid {
		t.Fatal("partial payment commit")
	}
	if err := f.st.DB().Exec(`DROP TRIGGER fail_claim`).Error; err != nil {
		t.Fatal(err)
	}
	// A new worker uses persisted progress, not the failed worker's memory.
	f.poller = NewPoller(f.poller.cfg, f.st)
	f.tick(t)
	o, _ = f.st.GetByTradeNo(ctx, f.order.TradeNo)
	if o.Status != model.StatusPaid {
		t.Fatal("payment lost after restart")
	}
}
