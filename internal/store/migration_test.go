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
