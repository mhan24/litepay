package store

import (
	"context"
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
