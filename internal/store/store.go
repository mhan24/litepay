// Package store owns payment invariants. Every claim, including reference inquiries,
// passes through Process in one SQLite transaction.
package store

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/glebarez/sqlite"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
	"gorm.io/gorm/logger"

	"litepay/internal/amount"
	"litepay/internal/model"
)

type Store struct{ db *gorm.DB }

func Open(path string) (*Store, error) {
	if path != ":memory:" {
		if err := os.MkdirAll(filepath.Dir(path), 0700); err != nil {
			return nil, err
		}
	}
	db, err := gorm.Open(sqlite.Open(path), &gorm.Config{Logger: logger.Default.LogMode(logger.Silent)})
	if err != nil {
		return nil, err
	}
	sqlDB, err := db.DB()
	if err != nil {
		return nil, err
	}
	sqlDB.SetMaxOpenConns(1)
	fail := func(err error) (*Store, error) { sqlDB.Close(); return nil, err }
	if err = db.Exec("PRAGMA foreign_keys=ON").Error; err != nil {
		return fail(err)
	}
	var version int
	if err = db.Raw("PRAGMA user_version").Scan(&version).Error; err != nil {
		return fail(err)
	}
	var tables int64
	if err = db.Raw("SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'").Scan(&tables).Error; err != nil {
		return fail(err)
	}
	if version != 8 && tables > 0 {
		return fail(errors.New("旧数据库不兼容，请配置一个新的空数据库"))
	}
	if err = db.AutoMigrate(&model.Order{}, &model.Slot{}, &model.Transaction{}, &model.Callback{}, &model.NotifyLog{}, &model.PaymentInquiry{}, &model.ScanCursor{}, &model.Setting{}, &model.Credential{}); err != nil {
		return fail(err)
	}
	if err = db.Exec("PRAGMA user_version=8").Error; err != nil {
		return fail(err)
	}
	if path != ":memory:" {
		if err = os.Chmod(path, 0600); err != nil {
			return fail(err)
		}
	}
	return &Store{db}, nil
}
func (s *Store) DB() *gorm.DB { return s.db }
func (s *Store) Close() error {
	d, e := s.db.DB()
	if e != nil {
		return e
	}
	return d.Close()
}
func (s *Store) Ping(ctx context.Context) error {
	d, e := s.db.DB()
	if e != nil {
		return e
	}
	return d.PingContext(ctx)
}
func (s *Store) GetByTradeNo(ctx context.Context, no string) (*model.Order, error) {
	var o model.Order
	e := s.db.WithContext(ctx).Where("trade_no = ?", no).First(&o).Error
	return &o, e
}
func (s *Store) HasActivePayments(ctx context.Context) (bool, error) {
	var n int64
	e := s.db.WithContext(ctx).Model(&model.Order{}).Where("status IN ?", []string{model.Pending, model.Ready}).Count(&n).Error
	return n > 0, e
}
func (s *Store) Create(ctx context.Context, o model.Order) (*model.Order, error) {
	err := s.db.WithContext(ctx).Transaction(func(db *gorm.DB) error {
		var old model.Order
		err := db.Where("out_trade_no = ?", o.OutTradeNo).First(&old).Error
		if err == nil {
			if old.OriginUnits != o.OriginUnits || old.NotifyURL != o.NotifyURL || old.ReturnURL != o.ReturnURL || old.CurrencyLimit != o.CurrencyLimit || old.Reselect != o.Reselect || old.Name != o.Name {
				return errors.New("商户订单号已用于不同订单")
			}
			o = old
			return nil
		}
		if !errors.Is(err, gorm.ErrRecordNotFound) {
			return err
		}
		o.Status = model.Pending
		return db.Create(&o).Error
	})
	return &o, err
}

// Select reserves integer slots. Detached slots stay occupied through their original cooldown.
func (s *Store) Select(ctx context.Context, no, provider, currency, receiver string, precision int, step, minUnits int64, maxOffset, grace int) (*model.Order, error) {
	var o model.Order
	if precision < 2 || precision > 6 || step <= 0 || step%amount.Quantum(precision) != 0 || maxOffset < 1 || maxOffset > 900 || minUnits <= 0 {
		return nil, errors.New("金额配置无效")
	}
	err := s.db.WithContext(ctx).Transaction(func(db *gorm.DB) error {
		if err := db.Where("trade_no = ?", no).First(&o).Error; err != nil {
			return err
		}
		now := time.Now()
		if (o.Status != model.Pending && o.Status != model.Ready) || !now.Before(o.ExpiresAt) {
			return errors.New("订单不能再选择付款方式")
		}
		if o.Provider == provider && o.Currency == currency && o.Receiver == receiver {
			return nil
		}
		if o.Status == model.Ready && !o.Reselect {
			return errors.New("订单不允许重选")
		}
		if err := db.Model(&model.Slot{}).Where("order_id = ?", o.ID).Update("order_id", nil).Error; err != nil {
			return err
		}
		if err := db.Where("expires_at < ?", now).Delete(&model.Slot{}).Error; err != nil {
			return err
		}
		q := amount.Quantum(precision)
		base := max(o.OriginUnits, minUnits)
		base = ((base + q - 1) / q) * q
		for i := 0; i <= maxOffset; i++ {
			units := base + int64(i)*step
			if units > amount.Max {
				return errors.New("金额超出范围")
			}
			slot := model.Slot{Provider: provider, Currency: currency, Units: units, OrderID: &o.ID, ExpiresAt: o.ExpiresAt.Add(time.Duration(grace) * time.Second)}
			r := db.Clauses(clause.OnConflict{DoNothing: true}).Create(&slot)
			if r.Error != nil {
				return r.Error
			}
			if r.RowsAffected == 0 {
				continue
			}
			o.Provider = provider
			o.Currency = currency
			o.Receiver = receiver
			o.Precision = precision
			o.PayUnits = units
			o.SelectedAt = &now
			o.Status = model.Ready
			return db.Save(&o).Error
		}
		return errors.New("可用金额档位已满，请稍后重试")
	})
	return &o, err
}
func (s *Store) Cancel(ctx context.Context, no string) error {
	return s.db.WithContext(ctx).Transaction(func(db *gorm.DB) error {
		var o model.Order
		if e := db.Where("trade_no = ?", no).First(&o).Error; e != nil {
			return e
		}
		if o.Status == model.Canceled {
			return nil
		}
		if o.Status != model.Pending && o.Status != model.Ready {
			return errors.New("订单已终结")
		}
		if e := db.Model(&o).Update("status", model.Canceled).Error; e != nil {
			return e
		}
		return db.Model(&model.Slot{}).Where("order_id = ?", o.ID).Update("order_id", nil).Error
	})
}

// Process records evidence and atomically consumes a unique provider event.
// expectedTrade is a restriction, never a hint that can override ownership.
func (s *Store) Process(ctx context.Context, t model.Transaction, expectedTrade string) (string, error) {
	if t.EventID == "" || (t.Provider != "binance" && t.Provider != "okx") || t.OccurredAt.IsZero() {
		return "invalid_evidence", errors.New("交易缺少可靠身份或时间")
	}
	result := "unmatched"
	err := s.db.WithContext(ctx).Transaction(func(db *gorm.DB) error {
		var old model.Transaction
		e := db.Where("provider = ? AND event_id = ?", t.Provider, t.EventID).First(&old).Error
		if e == nil {
			if old.OrderID != nil {
				var o model.Order
				if e = db.First(&o, *old.OrderID).Error; e != nil {
					return e
				}
				result = "already_claimed"
				if expectedTrade != "" && o.TradeNo != expectedTrade {
					result = "other_order"
				}
				return nil
			}
			if old.State == model.Ambiguous || old.State == model.Rejected {
				result = old.State
				return nil
			}
			if old.Receiver != t.Receiver || old.Currency != t.Currency || old.Units != t.Units || !old.OccurredAt.Equal(t.OccurredAt) || old.Assets != t.Assets {
				t.State = model.Ambiguous
				t.Reason = "conflicting_evidence"
			}
		} else if !errors.Is(e, gorm.ErrRecordNotFound) {
			return e
		}
		if t.Reference != "" {
			var duplicates int64
			if err := db.Model(&model.Transaction{}).Where("provider = ? AND reference = ? AND event_id <> ?", t.Provider, t.Reference, t.EventID).Count(&duplicates).Error; err != nil {
				return err
			}
			if duplicates > 0 {
				t.State = model.Ambiguous
				t.Reason = "duplicate_reference"
			}
		}
		t.OrderID = nil
		if e = db.Save(&t).Error; e != nil {
			return e
		}
		finish := func(reason string) error { result = reason; return db.Model(&t).Update("reason", reason).Error }
		if t.State != model.Confirmed {
			result = t.State
			return nil
		}
		if t.Units <= 0 || t.Receiver == "" || (t.Currency != "USDT" && t.Currency != "USDC") {
			return finish("invalid_payment")
		}
		var candidates []model.Order
		if e = db.Where("status = ? AND provider = ? AND receiver = ? AND currency = ? AND pay_units = ? AND selected_at <= ? AND expires_at >= ?", model.Ready, t.Provider, t.Receiver, t.Currency, t.Units, t.OccurredAt, t.OccurredAt).Find(&candidates).Error; e != nil {
			return e
		}
		if len(candidates) == 0 {
			return finish("no_match")
		}
		if len(candidates) != 1 {
			if err := db.Model(&t).Update("state", model.Ambiguous).Error; err != nil {
				return err
			}
			return finish("multiple_orders")
		}
		o := candidates[0]
		if expectedTrade != "" && o.TradeNo != expectedTrade {
			return finish("other_order")
		}
		// A matching amount must still hold this order's reservation: that row is the proof
		// the payment window was never released. The wall clock is deliberately not consulted,
		// so a scan that discovers an on-time payment late can still claim it. Closure is
		// enforced by Expire, which removes the order from the candidate query above.
		var slot model.Slot
		err := db.Where("order_id = ? AND provider = ? AND currency = ? AND units = ?", o.ID, o.Provider, o.Currency, o.PayUnits).First(&slot).Error
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return finish("reservation_released")
		}
		if err != nil {
			return err
		}
		now := time.Now()
		o.Status = model.Paid
		o.PaidAt = &now
		if e = db.Save(&o).Error; e != nil {
			return e
		}
		if e = db.Model(&t).Updates(map[string]any{"order_id": o.ID, "reason": "claimed"}).Error; e != nil {
			return e
		}
		if e = db.Model(&model.Slot{}).Where("order_id = ?", o.ID).Update("order_id", nil).Error; e != nil {
			return e
		}
		result = "paid"
		return db.Create(&model.Callback{OrderID: o.ID, NextAt: now}).Error
	})
	return result, err
}

// heldOpen keeps an order open while an exchange-unconfirmed deposit inside its payment window
// may still be confirmed into it. The retention argument bounds that wait, so an abandoned
// deposit cannot pin an order or the scanner forever.
const heldOpen = `NOT EXISTS (
	SELECT 1 FROM transactions t
	WHERE t.provider = orders.provider AND t.receiver = orders.receiver AND t.currency = orders.currency
		AND t.state = ? AND t.occurred_at >= orders.selected_at AND t.occurred_at <= orders.expires_at
		AND t.occurred_at >= ?
)`

// Expire closes orders only once a successful scan has covered their whole payment window,
// so a failing or lagging exchange query can never close an order the payer can still settle
// on time. It is the only place that releases a payment window: Process derives claimability
// from the reservation rows this method removes.
func (s *Store) Expire(ctx context.Context, grace int, retention time.Duration) error {
	window := -time.Duration(grace) * time.Second
	return s.db.WithContext(ctx).Transaction(func(db *gorm.DB) error {
		now := time.Now()
		// A pending order holds no amount tier and no deposit can be matched to it yet.
		if err := db.Model(&model.Order{}).Where("status = ? AND expires_at < ?", model.Pending, now.Add(window)).Update("status", model.Expired).Error; err != nil {
			return err
		}
		var cursors []model.ScanCursor
		if err := db.Find(&cursors).Error; err != nil {
			return err
		}
		for _, cursor := range cursors {
			covered := cursor.ScannedUntil
			if covered.After(now) {
				covered = now
			}
			// Coverage is only what a successful query actually scanned; a provider that never
			// succeeded keeps its orders open instead of closing them blind.
			if err := db.Model(&model.Order{}).
				Where("status = ? AND provider = ? AND expires_at < ?", model.Ready, cursor.Provider, covered.Add(window)).
				Where(heldOpen, model.Unconfirmed, now.Add(-retention)).
				Update("status", model.Expired).Error; err != nil {
				return err
			}
		}
		// Closed orders no longer need their amount tier. Tiers left behind by reselect, cancel
		// and payment stay reserved through their original cooldown.
		open := db.Model(&model.Order{}).Select("id").Where("status IN ?", []string{model.Pending, model.Ready})
		if err := db.Model(&model.Slot{}).Where("order_id IS NOT NULL AND order_id NOT IN (?)", open).Update("order_id", nil).Error; err != nil {
			return err
		}
		return db.Where("order_id IS NULL AND expires_at < ?", now).Delete(&model.Slot{}).Error
	})
}

// ScanDue reports whether an exchange history query can still change any order: an order is
// waiting for payment, or an unconfirmed deposit is recent enough to land in a payment window.
// While neither holds, the caller skips the query and leaves the cursor untouched, so the next
// real scan catches up from the last confirmed position instead of re-reading empty history.
func (s *Store) ScanDue(ctx context.Context, provider string, retention time.Duration) (bool, error) {
	cursor, err := s.Cursor(ctx, provider)
	if err != nil {
		return false, err
	}
	if cursor.LastSuccess.IsZero() {
		// The first successful query also proves the read-only credentials work.
		return true, nil
	}
	db := s.db.WithContext(ctx)
	var n int64
	if err := db.Model(&model.Order{}).Where("status = ? AND provider = ?", model.Ready, provider).Count(&n).Error; err != nil || n > 0 {
		return n > 0, err
	}
	if err := db.Model(&model.Transaction{}).Where("provider = ? AND state = ? AND occurred_at >= ?", provider, model.Unconfirmed, time.Now().Add(-retention)).Count(&n).Error; err != nil {
		return false, err
	}
	return n > 0, nil
}

// PendingTimes returns the original occurrence time of unconfirmed deposits that are still
// inside the retention window, oldest first, starting at offset and capped at limit. Exchange
// history pages by record creation time and a later status change does not move the record, so
// the scanner has to reopen exactly those moments; a regular incremental cursor that moved past
// them can no longer see them. Callers rotate the offset, so records beyond limit are rechecked
// on later rounds instead of starving. Deposits older than retention are excluded: they are
// never attributed automatically, their evidence stays available for manual review.
func (s *Store) PendingTimes(ctx context.Context, provider string, retention time.Duration, offset, limit int) ([]time.Time, error) {
	var rows []model.Transaction
	err := s.db.WithContext(ctx).
		Where("provider = ? AND state = ? AND occurred_at >= ?", provider, model.Unconfirmed, time.Now().Add(-retention)).
		Order("occurred_at").Offset(max(offset, 0)).Limit(limit).Find(&rows).Error
	if err != nil {
		return nil, err
	}
	times := make([]time.Time, 0, len(rows))
	for _, row := range rows {
		times = append(times, row.OccurredAt)
	}
	return times, nil
}

// PendingResult records the outcome of the unconfirmed-deposit recheck. It never touches
// ScannedUntil, so a failing recheck stays visible in the dashboard without making the regular
// scan or the expiry decision look broken.
func (s *Store) PendingResult(ctx context.Context, provider string, recheckErr error) error {
	cursor, err := s.Cursor(ctx, provider)
	if err != nil {
		return err
	}
	cursor.PendingAt = time.Now()
	cursor.PendingError = ""
	if recheckErr != nil {
		cursor.PendingError = fmt.Sprint(recheckErr)
	}
	return s.db.WithContext(ctx).Save(&cursor).Error
}

func (s *Store) ReserveInquiry(ctx context.Context, no, ref string) (*model.Order, error) {
	var o model.Order
	err := s.db.WithContext(ctx).Transaction(func(db *gorm.DB) error {
		if e := db.Where("trade_no = ?", no).First(&o).Error; e != nil {
			return e
		}
		if o.Status != model.Ready || o.SelectedAt == nil || time.Since(*o.SelectedAt) < time.Minute {
			return errors.New("当前订单不能查款，选择付款方式一分钟后可查款")
		}
		var row model.PaymentInquiry
		e := db.First(&row, "trade_no = ?", no).Error
		if e != nil && !errors.Is(e, gorm.ErrRecordNotFound) {
			return e
		}
		if row.Attempts >= 10 || (!row.LastAttemptAt.IsZero() && time.Since(row.LastAttemptAt) < time.Minute) {
			return errors.New("查款过于频繁")
		}
		row.TradeNo = no
		row.Reference = ref
		row.Attempts++
		row.LastAttemptAt = time.Now()
		row.Result = "查询中"
		return db.Save(&row).Error
	})
	return &o, err
}
func (s *Store) InquiryResult(ctx context.Context, no, result string) error {
	return s.db.WithContext(ctx).Model(&model.PaymentInquiry{}).Where("trade_no = ?", no).Update("result", result).Error
}
func (s *Store) ReserveCallback(ctx context.Context, maxAttempts, timeout int) (*model.Callback, *model.Order, error) {
	var task model.Callback
	var o model.Order
	err := s.db.WithContext(ctx).Transaction(func(db *gorm.DB) error {
		// Only paid orders are notified. Filtering here keeps a task whose order is not paid
		// from blocking the queue: it would never become deliverable and every reservation
		// would return the same row again.
		paid := db.Model(&model.Order{}).Select("id").Where("status = ?", model.Paid)
		if e := db.Where("delivered_at IS NULL AND attempts < ? AND next_at <= ? AND order_id IN (?)", maxAttempts, time.Now(), paid).Order("next_at").First(&task).Error; e != nil {
			return e
		}
		if e := db.First(&o, task.OrderID).Error; e != nil {
			return e
		}
		task.Attempts++
		task.NextAt = time.Now().Add(time.Duration(timeout+5) * time.Second)
		return db.Save(&task).Error
	})
	return &task, &o, err
}
func (s *Store) FinishCallback(ctx context.Context, task model.Callback, status int, ok bool, message string, delay time.Duration) error {
	return s.db.WithContext(ctx).Transaction(func(db *gorm.DB) error {
		updates := map[string]any{"last_error": message, "next_at": time.Now().Add(delay)}
		if ok {
			updates["delivered_at"] = time.Now()
		}
		r := db.Model(&model.Callback{}).Where("order_id = ? AND attempts = ? AND delivered_at IS NULL", task.OrderID, task.Attempts).Updates(updates)
		if r.Error != nil {
			return r.Error
		}
		if r.RowsAffected == 0 {
			return nil
		}
		return db.Create(&model.NotifyLog{OrderID: task.OrderID, Attempt: task.Attempts, StatusCode: status, Success: ok, Error: message}).Error
	})
}
func (s *Store) Cursor(ctx context.Context, provider string) (model.ScanCursor, error) {
	var c model.ScanCursor
	e := s.db.WithContext(ctx).First(&c, "provider = ?", provider).Error
	if errors.Is(e, gorm.ErrRecordNotFound) {
		return model.ScanCursor{Provider: provider}, nil
	}
	return c, e
}
func (s *Store) ScanResult(ctx context.Context, provider string, end time.Time, scanErr error) error {
	c, e := s.Cursor(ctx, provider)
	if e != nil {
		return e
	}
	if scanErr != nil {
		c.LastError = fmt.Sprint(scanErr)
	} else {
		if end.After(c.ScannedUntil) {
			c.ScannedUntil = end
		}
		c.LastSuccess = time.Now()
		c.LastError = ""
	}
	return s.db.WithContext(ctx).Save(&c).Error
}
