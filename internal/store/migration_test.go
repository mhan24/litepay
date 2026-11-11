package store

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"litepay/internal/model"
)

func TestVersion8CallbacksAndEvidenceMigrateWithoutLosingRetryBudget(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "old.db")
	s, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	o := readyOrder(t, s, "old-paid")
	if _, err = s.Process(ctx, evidence(o, "old-evidence"), ""); err != nil {
		t.Fatal(err)
	}
	// Recreate exactly the previous callback table; new fields must migrate as non-NULL defaults.
	if err = s.DB().Exec("DROP TABLE callbacks").Error; err != nil {
		t.Fatal(err)
	}
	if err = s.DB().Exec("CREATE TABLE callbacks (order_id integer PRIMARY KEY, attempts integer, next_at datetime, delivered_at datetime, last_error text, updated_at datetime)").Error; err != nil {
		t.Fatal(err)
	}
	if err = s.DB().Exec("INSERT INTO callbacks (order_id, attempts, next_at, last_error) VALUES (?,5,?,?)", o.ID, time.Now().Add(-time.Hour), "old-failure").Error; err != nil {
		t.Fatal(err)
	}
	s.DB().Exec("PRAGMA user_version=8")
	s.Close()
	s, err = Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	var task model.Callback
	if err = s.DB().First(&task).Error; err != nil || task.AttemptBase != 0 || task.Attempts != 5 || task.State != "queued" {
		t.Fatal(task, err)
	}
	if _, _, err = s.ReserveCallback(ctx, 5, 10); err == nil {
		t.Fatal("exhausted legacy callback retried without authorization")
	}
	if err = s.DB().First(&task).Error; err != nil || task.State != "exhausted" {
		t.Fatal("legacy terminal state not persisted", task, err)
	}
	if err = s.RetryCallback(ctx, o.TradeNo, "operator", "legacy failure verified", 5); err != nil {
		t.Fatal(err)
	}
	next, _, err := s.ReserveCallback(ctx, 5, 10)
	if err != nil || next.Attempts != 6 || next.AttemptLimit != 10 {
		t.Fatal(next, err)
	}
}

func TestMigrationRecoversPreexistingTaskSelectionWithoutGuessing(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "reselect.db")
	s, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	old := readyOrder(t, s, "legacy-reselect")
	event := evidence(old, "old-linked")
	event.State = model.Unconfirmed
	if _, err = s.Process(ctx, event, ""); err != nil {
		t.Fatal(err)
	}
	allowSelectionFixture(t, s, old.TradeNo)
	if _, err = s.Select(ctx, old.TradeNo, "okx", "USDC", "new-receiver", 2, 10000, 10000, 99, 120); err != nil {
		t.Fatal(err)
	}
	// Simulate schema 9, which had the task association but no immutable instruction table.
	if err = s.DB().Exec("DELETE FROM payment_attempts").Error; err != nil {
		t.Fatal(err)
	}
	s.DB().Exec("PRAGMA user_version=9")
	s.Close()
	s, err = Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	var recovered model.PaymentAttempt
	if err = s.DB().Where("recovered_from_event = ?", event.EventID).First(&recovered).Error; err != nil {
		t.Fatal(err)
	}
	event.State = model.Confirmed
	if result, err := s.Process(ctx, event, ""); err != nil || result != "paid" {
		t.Fatal(result, err)
	}
	fresh, _ := s.GetByTradeNo(ctx, old.TradeNo)
	if fresh.Provider != old.Provider {
		t.Fatal("legacy task instruction lost")
	}
}

func TestVersion10ExpiryScheduleAndSelectionLimitsSurviveRestart(t *testing.T) {
	path := filepath.Join(t.TempDir(), "schema10.db")
	s, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	o := readyOrder(t, s, "v10-ready")
	// Remove new columns to exercise the actual additive migration.
	for _, column := range []string{"selection_changes", "last_selection_at", "next_expiry_check_at"} {
		if column == "next_expiry_check_at" {
			s.DB().Exec("DROP INDEX idx_order_expiry_due")
		}
		if err = s.DB().Migrator().DropColumn(&model.Order{}, column); err != nil {
			t.Fatal(err)
		}
	}
	s.DB().Exec("PRAGMA user_version=10")
	s.Close()
	s, err = Open(path)
	if err != nil {
		t.Fatal(err)
	}
	fresh, err := s.GetByTradeNo(context.Background(), o.TradeNo)
	if err != nil || fresh.SelectionChanges != 1 || fresh.LastSelectionAt == nil || fresh.NextExpiryCheckAt.Before(o.ExpiresAt.Add(LateVisibilityWindow-time.Second)) {
		t.Fatal(fresh, err)
	}
	if _, err = s.Select(context.Background(), o.TradeNo, "okx", "USDC", "other", 2, 10000, 10000, 99, 120); err != ErrSelectionThrottled {
		t.Fatal("migration lost last selection", err)
	}
	s.Close()
	s, err = Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	fresh, err = s.GetByTradeNo(context.Background(), o.TradeNo)
	if err != nil || fresh.SelectionChanges != 1 {
		t.Fatal(fresh, err)
	}
}

func TestVersion11HistoricalCheckpointMigration(t *testing.T) {
	path := filepath.Join(t.TempDir(), "schema11.db")
	s, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	o := readyOrder(t, s, "v11-history")
	var a model.PaymentAttempt
	s.DB().Where("order_id=?", o.ID).First(&a)
	job := model.HistoricalScan{Automatic: true, OrderID: o.ID, AttemptID: a.ID, Provider: a.Provider, Receiver: a.Receiver, From: a.SelectedAt, To: a.ExpiresAt, Cursor: a.SelectedAt, State: "pending", Operator: "system", Reason: "old range", Pages: 1, LT: "checkpoint"}
	s.DB().Create(&job)
	manual := job
	manual.ID = 0
	manual.Automatic = false
	s.DB().Create(&manual)
	for _, col := range []string{"priority", "chunk_millis"} {
		if err = s.DB().Migrator().DropColumn(&model.HistoricalScan{}, col); err != nil {
			t.Fatal(err)
		}
	}
	s.DB().Migrator().DropTable(&model.IntegrationHealth{})
	s.DB().Exec("PRAGMA user_version=11")
	s.Close()
	s, err = Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	var automatic, retained model.HistoricalScan
	s.DB().First(&automatic, job.ID)
	s.DB().First(&retained, manual.ID)
	if automatic.State != "superseded" || automatic.LT != "checkpoint" || automatic.Pages != 1 || retained.State != "pending" || retained.Priority != 1 || retained.LT != "checkpoint" || !retained.Cursor.Equal(job.Cursor) {
		t.Fatal(automatic, retained)
	}
}

func TestVersion12FairSchedulingIsAdditive(t *testing.T) {
	path := filepath.Join(t.TempDir(), "schema12.db")
	s, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now()
	s.DB().Create(&model.ScanCursor{Provider: "binance", ScannedUntil: now, LastSuccess: now, LastError: "retained"})
	if err = s.DB().Migrator().DropColumn(&model.ScanCursor{}, "historical_turn"); err != nil {
		t.Fatal(err)
	}
	s.DB().Exec("PRAGMA user_version=12")
	s.Close()
	s, err = Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	cursor, err := s.Cursor(context.Background(), "binance")
	if err != nil || cursor.HistoricalTurn != 0 || !cursor.ScannedUntil.Equal(now) || cursor.LastError != "retained" {
		t.Fatal(cursor, err)
	}
}

func TestVersion13RecoveryMigrationPreservesClaimsAndRequiresHandshake(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "schema13.db")
	s, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	o := readyOrder(t, s, "v13-evidence")
	if _, err = s.Process(ctx, evidence(o, "v13-confirmed"), ""); err != nil {
		t.Fatal(err)
	}
	if err = s.DB().Create(&model.IntegrationHealth{Name: "dujiao_cancel_sync", Healthy: true, Source: "sqlite", ReportedAt: time.Now().Unix(), ReceivedAt: time.Now(), LastSuccessAt: time.Now()}).Error; err != nil {
		t.Fatal(err)
	}
	for _, column := range []string{"generation", "missing"} {
		if err = s.DB().Migrator().DropColumn(&model.IntegrationHealth{}, column); err != nil {
			t.Fatal(err)
		}
	}
	s.DB().Migrator().DropTable(&model.GatewayRecovery{}, &model.CancellationIntent{})
	s.DB().Exec("PRAGMA user_version=13")
	s.Close()
	s, err = Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	fresh, err := s.GetByTradeNo(ctx, o.TradeNo)
	if err != nil || fresh.Status != model.Paid {
		t.Fatal(fresh, err)
	}
	var transactions, callbacks int64
	s.DB().Model(&model.Transaction{}).Where("order_id=?", o.ID).Count(&transactions)
	s.DB().Model(&model.Callback{}).Where("order_id=?", o.ID).Count(&callbacks)
	if transactions != 1 || callbacks != 1 {
		t.Fatal("migration lost payment ownership", transactions, callbacks)
	}
	r, err := s.Recovery(ctx)
	if err != nil || r.Acknowledged || len(r.Generation) != 32 {
		t.Fatal(r, err)
	}
	health, err := s.IntegrationHealth(ctx)
	if err != nil || health.Generation != "" || health.Missing != 0 {
		t.Fatal(health, err)
	}
}

func TestSchema14AddsDiscoveryHealthAndInvalidatesUnprovenHeartbeat(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "schema14.db")
	s, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	o := readyOrder(t, s, "v14-payment")
	if _, err = s.Process(ctx, evidence(o, "v14-event"), ""); err != nil {
		t.Fatal(err)
	}
	r, _ := s.Recovery(ctx)
	health := model.IntegrationHealth{Name: "dujiao_cancel_sync", Generation: r.Generation, Healthy: true, ReportedAt: time.Now().Unix()}
	if err = s.AcknowledgeRecovery(ctx, r.Generation, true, health); err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"discovery_pending", "discovery_lag_seconds"} {
		if err = s.DB().Migrator().DropColumn(&model.IntegrationHealth{}, name); err != nil {
			t.Fatal(err)
		}
	}
	s.DB().Exec("PRAGMA user_version=14")
	s.Close()
	s, err = Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	fresh, err := s.GetByTradeNo(ctx, o.TradeNo)
	if err != nil || fresh.Status != model.Paid {
		t.Fatal(fresh, err)
	}
	after, _ := s.Recovery(ctx)
	if after.Generation != r.Generation || !after.Acknowledged {
		t.Fatal("migration changed recovery identity", after)
	}
	current, err := s.IntegrationHealth(ctx)
	if err != nil || current.Healthy || current.DiscoveryPending != 0 || current.DiscoveryLagSeconds != 0 {
		t.Fatal(current, err)
	}
	if ready, e := s.PaymentOperationsReady(ctx); e != nil || ready {
		t.Fatal("pre-probe heartbeat released admission", ready, e)
	}
	health.ReportedAt++
	if err = s.AcknowledgeRecovery(ctx, r.Generation, true, health); err != nil {
		t.Fatal(err)
	}
	if ready, e := s.PaymentOperationsReady(ctx); e != nil || !ready {
		t.Fatal(ready, e)
	}
	var version int
	s.DB().Raw("PRAGMA user_version").Scan(&version)
	if version != 16 {
		t.Fatal(version)
	}
	var count int64
	s.DB().Model(&model.Callback{}).Where("order_id=?", o.ID).Count(&count)
	if count != 1 {
		t.Fatal(count)
	}
}

func TestSchema15BackfillsCanceledReceiptReviewWithoutClaimingFunds(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "schema15.db")
	s, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	o := readyOrder(t, s, "v15-canceled-money")
	if err = s.Cancel(ctx, o.TradeNo); err != nil {
		t.Fatal(err)
	}
	event := evidence(o, "v15-canceled-event")
	if _, err = s.Process(ctx, event, ""); err != nil {
		t.Fatal(err)
	}
	recovery, _ := s.Recovery(ctx)
	s.DB().Migrator().DropTable(&model.CancellationReviewAudit{}, &model.CancellationReview{})
	s.DB().Exec("PRAGMA user_version=15")
	s.Close()
	s, err = Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	var review model.CancellationReview
	if err = s.DB().First(&review).Error; err != nil || review.OrderID != o.ID || review.State != "open" {
		t.Fatal(review, err)
	}
	var tx model.Transaction
	s.DB().First(&tx, "event_id=?", event.EventID)
	if tx.OrderID != nil || tx.AttemptID == nil || tx.State != model.Confirmed {
		t.Fatal(tx)
	}
	after, _ := s.Recovery(ctx)
	if after.Generation != recovery.Generation {
		t.Fatal("migration rotated recovery generation")
	}
	var callbacks int64
	s.DB().Model(&model.Callback{}).Count(&callbacks)
	if callbacks != 0 {
		t.Fatal("migration manufactured delivery")
	}
}
