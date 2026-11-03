package store

import (
	"context"
	"database/sql"
	"path/filepath"
	"testing"
	"time"

	_ "github.com/glebarez/go-sqlite"

	"litepay/internal/model"
)

func openObservation(t *testing.T, provider, id string) model.TxObservation {
	t.Helper()
	return model.TxObservation{
		Provider: provider, TransactionID: id, Type: "PAY",
		Amount: 10.01, Currency: "USDT", ReceiverID: "binance-uid",
		TransactionTime: time.Now().Add(-time.Minute),
	}
}

// 核销失败的原因不能被同一轮/后续扫描的 no_match 覆盖。
func TestRecordTxObservationKeepsHighestPriorityReason(t *testing.T) {
	ctx := context.Background()
	st := openTestStore(t)

	obs := openObservation(t, "binance", "tx-priority")
	obs.Reason, obs.OrderTradeNo = model.ObservationReasonNoMatch, ""
	if err := st.RecordTxObservation(ctx, obs); err != nil {
		t.Fatal(err)
	}
	obs.Reason, obs.OrderTradeNo = model.ObservationReasonClaimFailed, "LP-A"
	if err := st.RecordTxObservation(ctx, obs); err != nil {
		t.Fatal(err)
	}

	// 订单终结后同一笔流水只剩 no_match，不能把 claim_failed 冲掉。
	obs.Reason, obs.OrderTradeNo = model.ObservationReasonNoMatch, ""
	if err := st.RecordTxObservation(ctx, obs); err != nil {
		t.Fatal(err)
	}

	got, err := st.RecentTxObservations(ctx, 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 1 {
		t.Fatalf("observations = %d, want 1: %+v", len(got), got)
	}
	if got[0].Reason != model.ObservationReasonClaimFailed || got[0].OrderTradeNo != "LP-A" {
		t.Fatalf("reason = %q order = %q, want claim_failed/LP-A", got[0].Reason, got[0].OrderTradeNo)
	}
}

// 已入账的流水不能再出现在未归属列表里，也不能被后续观察改回未归属。
func TestResolvedObservationStaysResolved(t *testing.T) {
	ctx := context.Background()
	st := openTestStore(t)

	obs := openObservation(t, "binance", "tx-resolved")
	obs.Reason, obs.OrderTradeNo = model.ObservationReasonClaimFailed, "LP-A"
	if err := st.RecordTxObservation(ctx, obs); err != nil {
		t.Fatal(err)
	}
	if err := st.ResolveTxObservation(ctx, "BINANCE ", " tx-resolved ", "LP-B"); err != nil {
		t.Fatal(err)
	}

	if got, err := st.RecentTxObservations(ctx, 10); err != nil {
		t.Fatal(err)
	} else if len(got) != 0 {
		t.Fatalf("resolved observation still listed: %+v", got)
	}
	if n, err := st.CountOpenTxObservations(ctx); err != nil || n != 0 {
		t.Fatalf("open count = %d err = %v, want 0", n, err)
	}

	var row model.TxObservation
	if err := st.DB().Where("provider = ? AND transaction_id = ?", "binance", "tx-resolved").Take(&row).Error; err != nil {
		t.Fatal(err)
	}
	if row.Status != model.ObservationResolved || row.ResolvedTradeNo != "LP-B" || row.ResolvedAt == nil {
		t.Fatalf("resolved fields not recorded: %+v", row)
	}
	if row.OrderTradeNo != "LP-A" {
		t.Fatalf("attempted order overwritten: %q", row.OrderTradeNo)
	}

	// 历史窗口里再次扫到同一笔流水时，观察不能把它改回未归属。
	again := openObservation(t, "binance", "tx-resolved")
	again.Reason = model.ObservationReasonNoMatch
	if err := st.RecordTxObservation(ctx, again); err != nil {
		t.Fatal(err)
	}
	if got, err := st.RecentTxObservations(ctx, 10); err != nil {
		t.Fatal(err)
	} else if len(got) != 0 {
		t.Fatalf("observation revived a resolved transaction: %+v", got)
	}
	if err := st.DB().Where("provider = ? AND transaction_id = ?", "binance", "tx-resolved").Take(&row).Error; err != nil {
		t.Fatal(err)
	}
	if row.Status != model.ObservationResolved || row.Reason != model.ObservationReasonClaimFailed {
		t.Fatalf("resolved row mutated: %+v", row)
	}
}

// 状态位上线前的旧行没有 status，靠 incoming_txs 交叉校验也不能误报。
func TestRecentTxObservationsSkipsAlreadyCreditedRows(t *testing.T) {
	ctx := context.Background()
	st := openTestStore(t)

	obs := openObservation(t, "binance", "tx-legacy")
	if err := st.RecordTxObservation(ctx, obs); err != nil {
		t.Fatal(err)
	}
	// 模拟旧版本写入的行：状态为空。
	if err := st.DB().Model(&model.TxObservation{}).
		Where("provider = ? AND transaction_id = ?", "binance", "tx-legacy").
		Update("status", "").Error; err != nil {
		t.Fatal(err)
	}
	if n, err := st.CountOpenTxObservations(ctx); err != nil || n != 1 {
		t.Fatalf("open count = %d err = %v, want 1", n, err)
	}

	if err := st.DB().Create(&model.IncomingTx{
		Provider: "binance", TransactionID: "tx-legacy", Type: "PAY",
		Amount: 10.01, Currency: "USDT", ReceiverAddr: "binance-uid",
		OrderTradeNo: "LP-X", TransactionTime: time.Now(),
	}).Error; err != nil {
		t.Fatal(err)
	}

	got, err := st.RecentTxObservations(ctx, 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 0 {
		t.Fatalf("credited transaction still reported as unassigned: %+v", got)
	}
	if n, err := st.CountOpenTxObservations(ctx); err != nil || n != 0 {
		t.Fatalf("open count = %d err = %v, want 0", n, err)
	}
}

func TestPruneTxObservationsDropsExpiredRows(t *testing.T) {
	ctx := context.Background()
	st := openTestStore(t)

	old := openObservation(t, "binance", "tx-old")
	if err := st.RecordTxObservation(ctx, old); err != nil {
		t.Fatal(err)
	}
	fresh := openObservation(t, "binance", "tx-fresh")
	if err := st.RecordTxObservation(ctx, fresh); err != nil {
		t.Fatal(err)
	}
	if err := st.DB().Model(&model.TxObservation{}).
		Where("transaction_id = ?", "tx-old").
		Update("updated_at", time.Now().Add(-40*24*time.Hour)).Error; err != nil {
		t.Fatal(err)
	}

	n, err := st.PruneTxObservations(ctx, time.Now().Add(-30*24*time.Hour))
	if err != nil {
		t.Fatal(err)
	}
	if n != 1 {
		t.Fatalf("pruned = %d, want 1", n)
	}
	got, err := st.RecentTxObservations(ctx, 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 1 || got[0].TransactionID != "tx-fresh" {
		t.Fatalf("remaining observations = %+v", got)
	}
}

// 旧库升级：AutoMigrate 新增 status/reason_rank 后必须保住历史行，
// 并按原因回填优先级，否则旧记录会被后续 no_match 覆盖。
func TestObservationLedgerMigratesLegacyRows(t *testing.T) {
	path := filepath.Join(t.TempDir(), "legacy.db")
	raw, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := raw.Exec(`CREATE TABLE tx_observations (
		provider TEXT, transaction_id TEXT, type TEXT, amount REAL, currency TEXT,
		receiver_id TEXT, transaction_time DATETIME, reason TEXT, order_trade_no TEXT,
		raw TEXT, created_at DATETIME, updated_at DATETIME,
		PRIMARY KEY (provider, transaction_id))`); err != nil {
		t.Fatal(err)
	}
	if _, err := raw.Exec(`INSERT INTO tx_observations
		(provider,transaction_id,type,amount,currency,receiver_id,transaction_time,reason,order_trade_no,raw,created_at,updated_at)
		VALUES ('binance','legacy-tx','PAY',10.01,'USDT','binance-uid',datetime('now'),'claim_failed','LP-OLD','',datetime('now'),datetime('now'))`); err != nil {
		t.Fatal(err)
	}
	if err := raw.Close(); err != nil {
		t.Fatal(err)
	}

	st, err := Open(path)
	if err != nil {
		t.Fatalf("旧库迁移失败: %v", err)
	}
	ctx := context.Background()

	var row model.TxObservation
	if err := st.DB().Where("transaction_id = ?", "legacy-tx").Take(&row).Error; err != nil {
		t.Fatalf("历史记录迁移后丢失: %v", err)
	}
	if row.Status != model.ObservationOpen || row.ReasonRank != model.TxObservationRank(model.ObservationReasonClaimFailed) {
		t.Fatalf("历史记录未回填: status=%q rank=%d", row.Status, row.ReasonRank)
	}
	if got, err := st.RecentTxObservations(ctx, 10); err != nil || len(got) != 1 {
		t.Fatalf("历史未归属记录应仍然可见: %+v err=%v", got, err)
	}

	// 迁移后的旧行仍受优先级保护。
	if err := st.RecordTxObservation(ctx, model.TxObservation{
		Provider: "binance", TransactionID: "legacy-tx", Type: "PAY",
		Amount: 10.01, Currency: "USDT", ReceiverID: "binance-uid",
		TransactionTime: time.Now(), Reason: model.ObservationReasonNoMatch,
	}); err != nil {
		t.Fatal(err)
	}
	if err := st.DB().Where("transaction_id = ?", "legacy-tx").Take(&row).Error; err != nil {
		t.Fatal(err)
	}
	if row.Reason != model.ObservationReasonClaimFailed {
		t.Fatalf("迁移后的历史原因被覆盖: %q", row.Reason)
	}
}
