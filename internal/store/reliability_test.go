package store

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"litepay/internal/model"
)

func TestDatabaseSingleInstanceLockAndReopen(t *testing.T) {
	path := filepath.Join(t.TempDir(), "payments.db")
	one, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	if two, err := Open(path); err == nil {
		two.Close()
		t.Fatal("second payment instance obtained same DB")
	}
	if err = one.Close(); err != nil {
		t.Fatal(err)
	}
	two, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	two.Close()
}

func TestConfirmationDeadlineDoesNotSlideOrPermitLateClaim(t *testing.T) {
	s := testStore(t)
	ctx := context.Background()
	o := readyOrder(t, s, "deadline")
	tx := evidence(o, "pending")
	tx.State = model.Unconfirmed
	if _, err := s.Process(ctx, tx, ""); err != nil {
		t.Fatal(err)
	}
	var before, after model.ConfirmationTask
	s.DB().First(&before)
	if _, err := s.Process(ctx, tx, ""); err != nil {
		t.Fatal(err)
	}
	s.DB().First(&after)
	if !after.Deadline.Equal(before.Deadline) {
		t.Fatal("deadline slides on duplicate scans")
	}
	s.DB().Model(&model.ConfirmationTask{}).Where("event_id = ?", tx.EventID).Update("deadline", time.Now().Add(-time.Second))
	tx.State = model.Confirmed
	result, err := s.Process(ctx, tx, "")
	if err != nil || result != "confirmation_review_required" {
		t.Fatal(result, err)
	}
	fresh, _ := s.GetByTradeNo(ctx, o.TradeNo)
	if fresh.Status != model.Ready {
		t.Fatal("late confirmation automatically claimed")
	}
}

func TestTONPageAndClaimsCommitAtomicallyAndSurviveRestart(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "payments.db")
	s, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now()
	before := model.TONScanCheckpoint{Receiver: "wallet", From: now.Add(-time.Hour), To: now, Active: true}
	if err = s.StartTONCheckpoint(ctx, before); err != nil {
		t.Fatal(err)
	}
	after := before
	after.Pages = 1
	after.LT = "200"
	after.Hash = "hash"
	after.OldestAt = now
	row := model.Transaction{Provider: "ton", EventID: "first", Receiver: "wallet", OccurredAt: now, State: model.Rejected}
	invalid := row
	invalid.EventID = "" // Fail after first row's evidence was inserted.
	if err = s.SaveTONPage(ctx, before, after, []model.Transaction{row, invalid}); err == nil {
		t.Fatal("invalid page committed")
	}
	var count int64
	s.DB().Model(&model.Transaction{}).Count(&count)
	if count != 0 {
		t.Fatal("partial page evidence committed")
	}
	cp, _ := s.TONCheckpoint(ctx, "wallet")
	if cp.LT != "" {
		t.Fatal("failed page checkpoint moved")
	}
	if err = s.SaveTONPage(ctx, before, after, []model.Transaction{row}); err != nil {
		t.Fatal(err)
	}
	c, _ := s.Cursor(ctx, "ton")
	if !c.ScannedUntil.IsZero() {
		t.Fatal("partial range claimed full coverage")
	}
	s.Close()
	s, err = Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	cp, err = s.TONCheckpoint(ctx, "wallet")
	if err != nil || cp.LT != "200" || cp.Pages != 1 {
		t.Fatal(cp, err)
	}
	complete := cp
	complete.Pages++
	complete.Active = false
	if err = s.SaveTONPage(ctx, cp, complete, nil); err != nil {
		t.Fatal(err)
	}
	c, _ = s.Cursor(ctx, "ton")
	if !c.ScannedUntil.Equal(now) {
		t.Fatal(c.ScannedUntil, now)
	}
	if err = s.SaveTONPage(ctx, cp, complete, nil); err == nil {
		t.Fatal("stale page overwrote checkpoint")
	}
}

func TestCallbackExhaustionAuditedRecoveryAndIdempotency(t *testing.T) {
	ctx := context.Background()
	s := testStore(t)
	o := readyOrder(t, s, "callback-recovery")
	if _, err := s.Process(ctx, evidence(o, "recovery-deposit"), ""); err != nil {
		t.Fatal(err)
	}
	task, _, err := s.ReserveCallback(ctx, 1, 10)
	if err != nil {
		t.Fatal(err)
	}
	if err = s.RetryCallback(ctx, o.TradeNo, "operator", "verified no delivery", 1); err == nil {
		t.Fatal("in-flight recovery allowed")
	}
	if err = s.FinishCallback(ctx, *task, 503, false, "temporary", 0); err != nil {
		t.Fatal(err)
	}
	var state model.Callback
	s.DB().First(&state)
	if state.State != "exhausted" {
		t.Fatal(state.State)
	}
	if err = s.RetryCallback(ctx, o.TradeNo, "operator", "", 1); err == nil {
		t.Fatal("unaudited retry allowed")
	}
	if err = s.RetryCallback(ctx, o.TradeNo, "operator", "verified no delivery", 1); err != nil {
		t.Fatal(err)
	}
	if err = s.RetryCallback(ctx, o.TradeNo, "operator", "duplicate click", 1); err == nil {
		t.Fatal("recovery click duplicated budget")
	}
	next, order, err := s.ReserveCallback(ctx, 1, 10)
	if err != nil || next.Attempts != 2 || order.TradeNo != o.TradeNo {
		t.Fatal(next, err)
	}
	// A delayed completion from the old attempt cannot acknowledge the new reservation.
	s.FinishCallback(ctx, *task, 200, true, "", 0)
	s.DB().First(&state)
	if state.DeliveredAt != nil {
		t.Fatal("stale callback acknowledged new lease")
	}
	if err = s.FinishCallback(ctx, *next, 200, true, "", 0); err != nil {
		t.Fatal(err)
	}
	if err = s.RetryCallback(ctx, o.TradeNo, "operator", "already delivered", 1); err == nil {
		t.Fatal("acknowledged task requeued")
	}
	var logs, audits, claims int64
	s.DB().Model(&model.NotifyLog{}).Count(&logs)
	s.DB().Model(&model.CallbackRecovery{}).Count(&audits)
	s.DB().Model(&model.Transaction{}).Where("order_id = ?", o.ID).Count(&claims)
	if logs != 2 || audits != 1 || claims != 1 {
		t.Fatal(logs, audits, claims)
	}
}

func TestFilePragmasSurviveConnectionReplacementAndEscapedPaths(t *testing.T) {
	for _, filename := range []string{"payments.db", "支付 ?#%&+; file.db"} {
		t.Run(filename, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), filename)
			st, err := Open(path)
			if err != nil {
				t.Fatal(err)
			}
			defer st.Close()
			db, err := st.DB().DB()
			if err != nil {
				t.Fatal(err)
			}
			if db.Stats().MaxOpenConnections != 1 {
				t.Fatal("connection limit changed")
			}
			if err := st.DB().Create(&model.Credential{Name: "connection-fixture", Value: "retained"}).Error; err != nil {
				t.Fatal(err)
			}
			for range 2 {
				conn, err := db.Conn(context.Background())
				if err != nil {
					t.Fatal(err)
				}
				for _, query := range []string{"PRAGMA foreign_keys=OFF", "PRAGMA secure_delete=OFF", "PRAGMA busy_timeout=37", "PRAGMA journal_mode=DELETE"} {
					if _, err := conn.ExecContext(context.Background(), query); err != nil {
						t.Fatal(err)
					}
				}
				if err := conn.Raw(func(any) error { return driver.ErrBadConn }); !errors.Is(err, driver.ErrBadConn) {
					t.Fatal("connection was not discarded", err)
				}
				if err := conn.Close(); err != nil && !errors.Is(err, sql.ErrConnDone) {
					t.Fatal(err)
				}
				for _, check := range []struct {
					query string
					want  int
				}{{"PRAGMA foreign_keys", 1}, {"PRAGMA secure_delete", 1}, {"PRAGMA busy_timeout", 5000}} {
					var got int
					if err := db.QueryRow(check.query).Scan(&got); err != nil || got != check.want {
						t.Fatalf("replacement connection %s=%d, want %d: %v", check.query, got, check.want, err)
					}
				}
				var mode string
				if err := db.QueryRow("PRAGMA journal_mode").Scan(&mode); err != nil || mode != "wal" {
					t.Fatal("WAL not restored", mode, err)
				}
				var value string
				if err := db.QueryRow("SELECT value FROM credentials WHERE name = ?", "connection-fixture").Scan(&value); err != nil || value != "retained" {
					t.Fatal("replacement opened a different database", value, err)
				}
			}
			if _, err := os.Stat(path); err != nil {
				t.Fatal("escaped database path was changed", err)
			}
			if _, err := db.Exec("CREATE TABLE pragma_parent (id INTEGER PRIMARY KEY)"); err != nil {
				t.Fatal(err)
			}
			if _, err := db.Exec("CREATE TABLE pragma_child (parent_id INTEGER REFERENCES pragma_parent(id))"); err != nil {
				t.Fatal(err)
			}
			if _, err := db.Exec("INSERT INTO pragma_child(parent_id) VALUES (42)"); err == nil {
				t.Fatal("replacement connection ignored foreign key constraint")
			}
		})
	}
}
