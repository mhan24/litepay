package store

import (
	"context"
	"errors"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"gorm.io/gorm"

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

func TestVersion8ConfirmationUsesOriginalOrderDeadline(t *testing.T) {
	path := filepath.Join(t.TempDir(), "old-confirmation.db")
	s, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	o := readyOrder(t, s, "old-confirmation")
	event := evidence(o, "old-unconfirmed")
	event.State = model.Unconfirmed
	if err := s.DB().Create(&event).Error; err != nil {
		t.Fatal(err)
	}
	for _, sql := range []string{"DROP TABLE payment_attempts", "DROP TABLE confirmation_tasks", "PRAGMA user_version=8"} {
		if err := s.DB().Exec(sql).Error; err != nil {
			t.Fatal(err)
		}
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	s, err = Open(path)
	if err != nil {
		t.Fatal(err)
	}
	checkTask := func() {
		t.Helper()
		var q model.ConfirmationTask
		if err := s.DB().Where("provider=? AND event_id=?", event.Provider, event.EventID).First(&q).Error; err != nil {
			t.Fatal(err)
		}
		if q.OrderID == nil || *q.OrderID != o.ID || q.State != "pending" || !q.Deadline.Equal(o.ExpiresAt.Add(LateVisibilityWindow)) {
			t.Fatalf("unconfirmed receipt lost its order or settlement deadline: %+v", q)
		}
	}
	checkTask()
	// A database upgraded by an earlier binary already contains the short, unlinked task.
	if err := s.DB().Model(&model.ConfirmationTask{}).Where("provider=? AND event_id=?", event.Provider, event.EventID).Updates(map[string]any{"order_id": nil, "deadline": event.OccurredAt.Add(ConfirmationWait)}).Error; err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	s, err = Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	checkTask()
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
	if version != 17 {
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

func TestSchema16BackfillsAdditionalCancellationReceiptAndPreservesRefundAudit(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "schema16.db")
	s, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	o := readyOrder(t, s, "v16-extra-cancel")
	first, second := evidence(o, "v16-A"), evidence(o, "v16-B")
	for _, e := range []model.Transaction{first, second} {
		if _, err = s.Process(ctx, e, ""); err != nil {
			t.Fatal(err)
		}
	}
	s.CancelReference(ctx, o.TradeNo, o.OutTradeNo)
	if err = s.RecordCancellationRefund(ctx, o.TradeNo, first.Provider, first.EventID, "operator", "existing-refund-A", "original actual refund"); err != nil {
		t.Fatal(err)
	}
	// Reproduce schema 16's omission and premature terminal callback.
	if err = s.DB().Where("event_id=?", second.EventID).Delete(&model.CancellationReview{}).Error; err != nil {
		t.Fatal(err)
	}
	s.DB().Model(&model.Callback{}).Where("order_id=?", o.ID).Update("state", "canceled_refunded")
	recovery, _ := s.Recovery(ctx)
	s.DB().Exec("PRAGMA user_version=16")
	s.Close()
	for i := 0; i < 2; i++ {
		s, err = Open(path)
		if err != nil {
			t.Fatal(err)
		}
		summary, e := s.CancellationReviewSummary(ctx, o.ID)
		if e != nil || summary.Total != 2 || summary.Pending != 1 || summary.Current() || summary.Generation != recovery.Generation {
			t.Fatal(summary, e)
		}
		var audits int64
		s.DB().Model(&model.CancellationReviewAudit{}).Count(&audits)
		if audits != 1 {
			t.Fatal("migration changed actual refund audit", audits)
		}
		var cb model.Callback
		s.DB().First(&cb, "order_id=?", o.ID)
		if cb.State != "merchant_review" {
			t.Fatal(cb)
		}
		var claim model.Transaction
		s.DB().First(&claim, "provider=? AND event_id=?", first.Provider, first.EventID)
		if claim.OrderID == nil || *claim.OrderID != o.ID {
			t.Fatal("original unique claim lost", claim)
		}
		var version int
		s.DB().Raw("PRAGMA user_version").Scan(&version)
		if version != 17 {
			t.Fatal(version)
		}
		s.Close()
	}
}

func TestFutureSchemaVersionRejectedAndTransactionalStepRollback(t *testing.T) {
	path := filepath.Join(t.TempDir(), "future.db")
	s, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := s.DB().Exec("PRAGMA user_version=99").Error; err != nil {
		t.Fatal(err)
	}
	s.Close()

	if _, err := Open(path); err == nil || !strings.Contains(err.Error(), "高于当前程序支持的最大版本") {
		t.Fatalf("expected future schema version rejection, got: %v", err)
	}

	// Verify table-driven migration executes each step in a transaction and rolls back on failure.
	txPath := filepath.Join(t.TempDir(), "tx-step.db")
	s, err = Open(txPath)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	if err := s.DB().Exec("PRAGMA user_version=15").Error; err != nil {
		t.Fatal(err)
	}
	steps := []schemaMigration{
		{
			version: 16,
			apply: func(db *gorm.DB) error {
				return db.Create(&model.Credential{Name: "step16_ok", Value: "1"}).Error
			},
		},
		{
			version: 17,
			apply: func(db *gorm.DB) error {
				if err := db.Create(&model.Credential{Name: "step17_rolled_back", Value: "1"}).Error; err != nil {
					return err
				}
				return errors.New("simulated step 17 failure")
			},
		},
	}
	if err := runSchemaMigrations(s.DB(), 15, 1, steps); err == nil {
		t.Fatal("expected failing step 17 to return error")
	}
	var version int
	s.DB().Raw("PRAGMA user_version").Scan(&version)
	if version != 16 {
		t.Fatalf("expected user_version to remain at committed step 16 after step 17 rollback, got %d", version)
	}
	var rolledBackCount int64
	s.DB().Model(&model.Credential{}).Where("name = ?", "step17_rolled_back").Count(&rolledBackCount)
	if rolledBackCount != 0 {
		t.Fatal("failed migration step was not rolled back")
	}
}

func TestCurrentSchemaRestartRepairsMissingConfirmationAndPaymentState(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "repair.db")
	s, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	o := readyOrder(t, s, "restart-unconfirmed")
	event := evidence(o, "missing-confirmation")
	event.State = model.Unconfirmed
	if _, err := s.Process(ctx, event, ""); err != nil {
		t.Fatal(err)
	}
	if err := s.DB().Where("provider = ? AND event_id = ?", event.Provider, event.EventID).Delete(&model.ConfirmationTask{}).Error; err != nil {
		t.Fatal(err)
	}
	// Simulate partially restored instructions and expiry metadata at the current schema version.
	if err := s.DB().Where("order_id = ?", o.ID).Delete(&model.PaymentAttempt{}).Error; err != nil {
		t.Fatal(err)
	}
	if err := s.DB().Model(&model.Order{}).Where("id = ?", o.ID).Updates(map[string]any{"selection_changes": 0, "last_selection_at": nil, "next_expiry_check_at": nil}).Error; err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	var deadline time.Time
	for i := 0; i < 2; i++ {
		s, err = Open(path)
		if err != nil {
			t.Fatal(err)
		}
		var version int
		if err := s.DB().Raw("PRAGMA user_version").Scan(&version).Error; err != nil || version != currentSchemaVersion {
			t.Fatal(version, err)
		}
		var task model.ConfirmationTask
		if err := s.DB().Where("provider = ? AND event_id = ?", event.Provider, event.EventID).First(&task).Error; err != nil {
			t.Fatal(err)
		}
		if task.OrderID == nil || *task.OrderID != o.ID || task.State != "pending" {
			t.Fatal("task association not repaired", task)
		}
		if i == 0 {
			deadline = task.Deadline
		} else if !deadline.Equal(task.Deadline) {
			t.Fatal("restart extended confirmation deadline")
		}
		fresh, err := s.GetByTradeNo(ctx, o.TradeNo)
		if err != nil || fresh.SelectionChanges != 1 || fresh.LastSelectionAt == nil || fresh.NextExpiryCheckAt.Before(o.ExpiresAt.Add(LateVisibilityWindow-time.Second)) {
			t.Fatal(fresh, err)
		}
		for _, table := range []any{&model.ConfirmationTask{}, &model.PaymentAttempt{}} {
			var count int64
			if err := s.DB().Model(table).Count(&count).Error; err != nil || count != 1 {
				t.Fatal("repair duplicated records", count, err)
			}
		}
		if err := s.Close(); err != nil {
			t.Fatal(err)
		}
	}
}

func TestRestartRestoresCurrentSelectionBesideOlderAttempts(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "partial-restore.db")
	s, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	o := readyOrder(t, s, "two-attempts")
	allowSelectionFixture(t, s, o.TradeNo)
	current, err := s.Select(ctx, o.TradeNo, "okx", "USDC", "second-account", 2, 10000, 10000, 99, 120)
	if err != nil {
		t.Fatal(err)
	}
	if err := s.DB().Where("order_id=? AND provider=?", o.ID, current.Provider).Delete(&model.PaymentAttempt{}).Error; err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	s, err = Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	if result, err := s.Process(ctx, evidence(current, "current-deposit"), ""); err != nil || result != "paid" {
		t.Fatal(result, err)
	}
	var count int64
	if err := s.DB().Model(&model.PaymentAttempt{}).Where("order_id=?", o.ID).Count(&count).Error; err != nil || count != 2 {
		t.Fatal(count, err)
	}
}
