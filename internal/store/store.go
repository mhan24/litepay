// Package store owns payment invariants. Every claim, including reference inquiries,
// passes through Process in one SQLite transaction.
package store

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"syscall"
	"time"

	"github.com/glebarez/sqlite"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
	"gorm.io/gorm/logger"

	"litepay/internal/amount"
	"litepay/internal/gram"
	"litepay/internal/model"
)

type Store struct {
	db   *gorm.DB
	lock *os.File
}

func Open(path string) (*Store, error) {
	var lock *os.File
	if path != ":memory:" {
		absolute, err := filepath.Abs(path)
		if err != nil {
			return nil, err
		}
		if err = os.MkdirAll(filepath.Dir(absolute), 0700); err != nil {
			return nil, err
		}
		if canonical, e := filepath.EvalSymlinks(absolute); e == nil {
			absolute = canonical
		} else if canonical, e = filepath.EvalSymlinks(filepath.Dir(absolute)); e == nil {
			absolute = filepath.Join(canonical, filepath.Base(absolute))
		}
		path = absolute
		lock, err = os.OpenFile(path+".lock", os.O_CREATE|os.O_RDWR, 0600)
		if err != nil {
			return nil, err
		}
		if err = syscall.Flock(int(lock.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
			lock.Close()
			return nil, errors.New("数据库已有运行实例或恢复任务持有锁")
		}
	}
	success := false
	defer func() {
		if !success && lock != nil {
			lock.Close()
		}
	}()

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
	if version != 8 && version != 9 && version != 10 && version != 11 && version != 12 && version != 13 && version != 14 && version != 15 && version != 16 && tables > 0 {
		return fail(errors.New("旧数据库不兼容，请配置一个新的空数据库"))
	}
	if err = db.AutoMigrate(&model.Order{}, &model.Slot{}, &model.Transaction{}, &model.Callback{}, &model.NotifyLog{}, &model.ScanCursor{}, &model.Setting{}, &model.Credential{}, &model.ConfirmationTask{}, &model.TONScanCheckpoint{}, &model.CallbackRecovery{}, &model.PaymentAttempt{}, &model.SigningKey{}, &model.PaymentRecovery{}, &model.HistoricalScan{}, &model.IntegrationHealth{}, &model.HistoryCoverage{}, &model.GatewayRecovery{}, &model.CancellationIntent{}, &model.CancellationReview{}, &model.CancellationReviewAudit{}); err != nil {
		return fail(err)
	}
	if err = migratePaymentAttempts(db); err != nil {
		return fail(err)
	}
	if err = initializeExpiryAndSelection(db); err != nil {
		return fail(err)
	}
	if err = backfillConfirmationTasks(db); err != nil {
		return fail(err)
	}
	if version < 12 {
		if err = db.Model(&model.HistoricalScan{}).Where("automatic = ? AND state = ?", false, "pending").Update("priority", 1).Error; err != nil {
			return fail(err)
		}
		if err = db.Model(&model.HistoricalScan{}).Where("automatic = ? AND state = ?", true, "pending").Updates(map[string]any{"state": "superseded", "last_error": "已由付款窗口分组复核替代"}).Error; err != nil {
			return fail(err)
		}
	}
	if err = initializeRecovery(db); err != nil {
		return fail(err)
	}
	if version < 15 {
		// Earlier heartbeats did not prove the source discovery cursor was caught up.
		if err = db.Model(&model.IntegrationHealth{}).Where("name = ?", "dujiao_cancel_sync").Update("healthy", false).Error; err != nil {
			return fail(err)
		}
	}
	if version < 16 {
		if err = backfillCancellationReviews(db); err != nil {
			return fail(err)
		}
	}
	if err = db.Exec("PRAGMA user_version=16").Error; err != nil {
		return fail(err)
	}
	if path != ":memory:" {
		if err = os.Chmod(path, 0600); err != nil {
			return fail(err)
		}
	}
	success = true
	return &Store{db: db, lock: lock}, nil
}
func (s *Store) DB() *gorm.DB { return s.db }
func (s *Store) Close() error {
	if s.lock != nil {
		defer s.lock.Close()
	}
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
		if o.SigningKeyID == 0 {
			var key model.SigningKey
			e := db.Where("retired_at IS NULL").Order("id DESC").First(&key).Error
			if e == nil {
				o.SigningKeyID = key.ID
			} else if !errors.Is(e, gorm.ErrRecordNotFound) {
				return e
			}
		}
		var canceled int64
		if err := db.Model(&model.CancellationIntent{}).Where("out_trade_no=? OR trade_no=?", o.OutTradeNo, o.TradeNo).Count(&canceled).Error; err != nil {
			return err
		}
		if canceled > 0 {
			return errors.New("商户订单已取消，禁止重新创建")
		}
		o.Status = model.Pending
		return db.Create(&o).Error
	})
	return &o, err
}

// Select reserves integer slots and preserves every previously issued payment instruction.
func (s *Store) Select(ctx context.Context, no, provider, currency, receiver string, precision int, step, minUnits int64, maxOffset, grace int) (*model.Order, error) {
	return s.selectPayment(ctx, no, provider, currency, receiver, precision, step, minUnits, maxOffset, grace, 0, gram.Snapshot{})
}

func (s *Store) SelectGRAM(ctx context.Context, no, receiver string, quote gram.Snapshot, grace int) (*model.Order, error) {
	return s.selectPayment(ctx, no, "ton", "GRAM", receiver, 6, 1, 1, 99, grace, 1, quote)
}

func (s *Store) selectPayment(ctx context.Context, no, provider, currency, receiver string, precision int, step, minUnits int64, maxOffset, grace int, quoted int, quote gram.Snapshot) (*model.Order, error) {
	var o model.Order
	if provider == "ton" && (currency != "GRAM" || quoted == 0) {
		return nil, errors.New("GRAM 付款必须使用有效 USD 报价")
	}
	if provider != "ton" && ((provider != "binance" && provider != "okx") || (currency != "USDT" && currency != "USDC")) {
		return nil, errors.New("收款平台与币种不匹配")
	}
	if precision < 2 || precision > 6 || step <= 0 || step > amount.Scale || step%amount.Quantum(precision) != 0 || maxOffset < 1 || maxOffset > 900 || minUnits <= 0 {
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
		// 重选同一付款方式复用已占用档位，但选中金额也可能是修复前写入的：落在商城
		// 无法验签的区间时必须拒绝，不能把注定无法核销的金额继续交给用户付款。
		if o.Provider == provider && o.Currency == currency && o.Receiver == receiver {
			if amount.MallCompatible(o.PayUnits) {
				return nil
			}
			return errors.New("已选应付金额超出可核销范围，请取消后重新下单")
		}
		if o.Status == model.Ready && !o.Reselect {
			return errors.New("订单不允许重选")
		}
		if o.SelectionChanges >= MaxSelectionChanges {
			return ErrSelectionLimit
		}
		if o.LastSelectionAt != nil && now.Sub(*o.LastSelectionAt) < SelectionInterval {
			return ErrSelectionThrottled
		}
		// Returning to any saved instruction reuses its amount and quote; it never allocates again.
		var previous model.PaymentAttempt
		err := db.Where("order_id = ? AND provider = ? AND currency = ? AND receiver = ?", o.ID, provider, currency, receiver).Order("id").First(&previous).Error
		if err == nil {
			if !amount.MallCompatible(previous.Units) {
				return errors.New("历史付款金额超出可核销范围")
			}
			applyAttempt(&o, previous)
			o.SelectionChanges++
			o.LastSelectionAt = &now
			return db.Save(&o).Error
		} else if !errors.Is(err, gorm.ErrRecordNotFound) {
			return err
		}
		var attempts int64
		if err = db.Model(&model.PaymentAttempt{}).Where("order_id = ?", o.ID).Count(&attempts).Error; err != nil {
			return err
		}
		if attempts >= MaxPaymentAttempts {
			return errors.New("本订单已保存的付款方式数量达到上限，请使用已选方式")
		}

		q := amount.Quantum(precision)
		base := max(o.OriginUnits, minUnits)
		var quoteLimit int64
		if quoted != 0 {
			var err error
			base, err = gram.Quote(o.OriginUnits, quote)
			if err != nil {
				return err
			}
			quoteLimit, err = gram.MaxQuoteUnits(o.OriginUnits, quote)
			if err != nil {
				return err
			}
		}
		base = ((base + q - 1) / q) * q
		ceiling := base + step*int64(maxOffset)
		if quoted != 0 {
			ceiling = quoteLimit
		}
		units, actualPrecision, err := allocateSlot(db, o, provider, currency, base, ceiling, step, precision, grace, now)
		if err != nil {
			return err
		}
		o.RateUSD = ""
		o.RateAt = nil
		if quoted != 0 {
			o.RateUSD = quote.USD
			at := quote.PriceAt
			o.RateAt = &at
		}
		o.Provider = provider
		o.Currency = currency
		o.Receiver = receiver
		o.Precision = actualPrecision
		o.PayUnits = units
		o.SelectedAt = &now
		o.Status = model.Ready
		o.SelectionChanges++
		o.LastSelectionAt = &now
		o.NextExpiryCheckAt = o.ExpiresAt.Add(LateVisibilityWindow)
		if err = db.Save(&o).Error; err != nil {
			return err
		}
		return createAttempt(db, o)

	})
	return &o, err
}
func (s *Store) Cancel(ctx context.Context, no string) error {
	_, err := s.cancelReference(ctx, no, "", false)
	return err
}

// CancelReference accepts either signed identifier and checks agreement when both are supplied.
func (s *Store) CancelReference(ctx context.Context, tradeNo, outTradeNo string) (*model.Order, error) {
	return s.cancelReference(ctx, tradeNo, outTradeNo, true)
}
func (s *Store) cancelReference(ctx context.Context, tradeNo, outTradeNo string, merchant bool) (*model.Order, error) {
	var paidConflict bool
	var o model.Order
	err := s.db.WithContext(ctx).Transaction(func(db *gorm.DB) error {
		if tradeNo == "" && outTradeNo == "" {
			return errors.New("缺少订单号")
		}
		q := db
		if tradeNo != "" {
			q = q.Where("trade_no = ?", tradeNo)
		} else {
			q = q.Where("out_trade_no = ?", outTradeNo)
		}
		if err := q.First(&o).Error; err != nil {
			if !errors.Is(err, gorm.ErrRecordNotFound) || tradeNo == "" || outTradeNo == "" {
				return err
			}
			var occupied int64
			if e := db.Model(&model.Order{}).Where("trade_no=? OR out_trade_no=?", tradeNo, outTradeNo).Count(&occupied).Error; e != nil {
				return e
			}
			if occupied > 0 {
				return errors.New("网关订单号与商户订单号不一致")
			}
			intent := model.CancellationIntent{TradeNo: tradeNo, OutTradeNo: outTradeNo, CreatedAt: time.Now()}
			if e := db.Clauses(clause.OnConflict{DoNothing: true}).Create(&intent).Error; e != nil {
				return e
			}
			var saved model.CancellationIntent
			if e := db.First(&saved, "out_trade_no=?", outTradeNo).Error; e != nil {
				return e
			}
			if saved.TradeNo != tradeNo {
				return errors.New("网关订单号与商户订单号不一致")
			}
			o = model.Order{TradeNo: tradeNo, OutTradeNo: outTradeNo, Status: model.Canceled}
			return nil
		}
		if outTradeNo != "" && o.OutTradeNo != outTradeNo {
			return errors.New("网关订单号与商户订单号不一致")
		}
		if o.Status == model.Canceled {
			return nil
		}
		if o.Status == model.Paid {
			if !merchant {
				return errors.New("已支付订单不能关闭")
			}
			var rows []model.Transaction
			if err := db.Where("order_id=? AND state=?", o.ID, model.Confirmed).Limit(2).Find(&rows).Error; err != nil {
				return err
			}
			if len(rows) != 1 {
				return errors.New("已支付订单缺少唯一到账证据，需人工核对")
			}
			if err := ensureCancellationReview(db, o, rows[0]); err != nil {
				return err
			}
			paidConflict = true
			return nil
		}
		if o.Status != model.Pending && o.Status != model.Ready && o.Status != model.Expired {
			return errors.New("订单不能关闭")
		}
		if err := db.Model(&o).Update("status", model.Canceled).Error; err != nil {
			return err
		}
		o.Status = model.Canceled
		return db.Model(&model.Slot{}).Where("order_id = ?", o.ID).Update("order_id", nil).Error
	})
	if err == nil && paidConflict {
		err = errors.New("已支付订单不能关闭")
	}
	return &o, err
}

// Process records evidence and atomically consumes a unique provider event.
// expectedTrade is a restriction, never a hint that can override ownership.
func (s *Store) Process(ctx context.Context, t model.Transaction, expectedTrade string) (string, error) {
	if t.Provider == "ton" && t.State == model.Confirmed && t.MasterchainSeqno <= 0 {
		return "unfinalized", errors.New("TON 到账缺少主链最终性确认")
	}
	if t.EventID == "" || (t.Provider != "binance" && t.Provider != "okx" && t.Provider != "ton") || t.OccurredAt.IsZero() {
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
		if t.State == model.Unconfirmed {
			if err := ensureConfirmationTask(db, t); err != nil {
				return err
			}
		} else {
			if err := db.Model(&model.ConfirmationTask{}).Where("provider = ? AND event_id = ? AND state = ?", t.Provider, t.EventID, "pending").Update("state", "resolved").Error; err != nil {
				return err
			}
		}
		if t.State != model.Confirmed {
			result = t.State
			return nil
		}
		if t.Units <= 0 || t.Receiver == "" || ((t.Provider == "ton" && t.Currency != "GRAM") || (t.Provider != "ton" && t.Currency != "USDT" && t.Currency != "USDC")) {
			return finish("invalid_payment")
		}
		attempt, o, reason, err := matchAttempt(db, t, false)
		if err != nil {
			return err
		}
		if reason != "" {
			if reason == "additional_payment" || reason == "order_closed" && o.Status == model.Canceled {
				if err := db.Model(&t).Update("attempt_id", attempt.ID).Error; err != nil {
					return err
				}
			}
			var cancellationHeld int64
			if reason == "additional_payment" {
				if err := db.Model(&model.CancellationReview{}).Where("order_id=?", o.ID).Count(&cancellationHeld).Error; err != nil {
					return err
				}
			}
			if reason == "order_closed" && o.Status == model.Canceled || reason == "additional_payment" && cancellationHeld > 0 {
				if err := ensureCancellationReview(db, o, t); err != nil {
					return err
				}
			}
			if reason == "multiple_orders" {
				if err := db.Model(&t).Update("state", model.Ambiguous).Error; err != nil {
					return err
				}
			}
			return finish(reason)
		}
		if expectedTrade != "" && o.TradeNo != expectedTrade {
			return finish("other_order")
		}
		applyAttempt(&o, attempt)
		now := time.Now()
		o.Status = model.Paid
		o.PaidAt = &now
		if e = db.Save(&o).Error; e != nil {
			return e
		}
		if e = db.Model(&t).Updates(map[string]any{"order_id": o.ID, "attempt_id": attempt.ID, "reason": "claimed"}).Error; e != nil {
			return e
		}
		if e = db.Model(&model.Slot{}).Where("order_id = ?", o.ID).Update("order_id", nil).Error; e != nil {
			return e
		}
		result = "paid"
		state := "queued"
		if !now.Before(o.ExpiresAt) {
			state = "merchant_review"
		}
		return db.Create(&model.Callback{OrderID: o.ID, SigningKeyID: o.SigningKeyID, NextAt: now, State: state}).Error
	})
	return result, err
}

// IdleReconcileInterval keeps recording late/canceled-order deposits without creating claims.
const IdleReconcileInterval = time.Minute

func (s *Store) ScanDue(ctx context.Context, provider, receiver string) (bool, error) {
	cursor, err := s.Cursor(ctx, provider)
	if err != nil {
		return false, err
	}
	if cursor.LastSuccess.IsZero() || time.Since(cursor.LastSuccess) >= IdleReconcileInterval {
		return true, nil
	}
	var n int64
	db := s.db.WithContext(ctx)
	if err = db.Table("payment_attempts a").Joins("JOIN orders o ON o.id=a.order_id").Where("o.status = ? AND a.provider = ?", model.Ready, provider).Count(&n).Error; err != nil || n > 0 {
		return n > 0, err
	}
	if err = db.Model(&model.TONScanCheckpoint{}).Where("active = ? AND receiver = ?", true, receiver).Count(&n).Error; err != nil {
		return false, err
	}
	if provider == "ton" && n > 0 {
		return true, nil
	}
	err = db.Model(&model.ConfirmationTask{}).Where("provider = ? AND state = ? AND deadline > ?", provider, "pending", time.Now()).Count(&n).Error
	return n > 0, err
}

// ConfirmationWait is independent of the checkout's default validity. Linked tasks live until
// the actual merchant payment window ends plus this bound; unlinked evidence gets the same bound.
const ConfirmationWait = 48 * time.Hour

func ensureConfirmationTask(db *gorm.DB, t model.Transaction) error {
	var old model.ConfirmationTask
	if err := db.Where("provider = ? AND event_id = ?", t.Provider, t.EventID).First(&old).Error; err == nil {
		return nil
	} else if !errors.Is(err, gorm.ErrRecordNotFound) {
		return err
	}
	q := model.ConfirmationTask{Provider: t.Provider, EventID: t.EventID, OccurredAt: t.OccurredAt, Deadline: t.OccurredAt.Add(ConfirmationWait), State: "pending"}
	a, o, reason, err := matchAttempt(db, t, true)
	if err != nil {
		return err
	}
	if reason == "" {
		q.OrderID = &o.ID
		q.Deadline = a.SettleUntil
	}
	if !time.Now().Before(q.Deadline) {
		q.State = "manual_review"
	}
	return db.Create(&q).Error
}

func backfillConfirmationTasks(db *gorm.DB) error {
	var rows []model.Transaction
	if err := db.Where("state = ? AND NOT EXISTS (SELECT 1 FROM confirmation_tasks q WHERE q.provider = transactions.provider AND q.event_id = transactions.event_id)", model.Unconfirmed).Find(&rows).Error; err != nil {
		return err
	}
	for _, row := range rows {
		if err := ensureConfirmationTask(db, row); err != nil {
			return err
		}
	}
	return nil
}

func (s *Store) PendingTimes(ctx context.Context, provider string, offset, limit int) ([]time.Time, error) {
	db := s.db.WithContext(ctx)
	if err := db.Model(&model.ConfirmationTask{}).Where("state = ? AND deadline <= ?", "pending", time.Now()).Update("state", "manual_review").Error; err != nil {
		return nil, err
	}
	var rows []model.ConfirmationTask
	err := db.Where("provider = ? AND state = ? AND deadline > ?", provider, "pending", time.Now()).Where("EXISTS (SELECT 1 FROM transactions t WHERE t.provider = confirmation_tasks.provider AND t.event_id = confirmation_tasks.event_id AND t.state = ?)", model.Unconfirmed).Order("occurred_at, event_id").Offset(max(offset, 0)).Limit(limit).Find(&rows).Error
	times := make([]time.Time, 0, len(rows))
	for _, row := range rows {
		times = append(times, row.OccurredAt)
	}
	return times, err
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

func (s *Store) ReserveCallback(ctx context.Context, maxAttempts, timeout int) (*model.Callback, *model.Order, error) {
	var task model.Callback
	var o model.Order
	missing := false
	err := s.db.WithContext(ctx).Transaction(func(db *gorm.DB) error {
		if err := db.Model(&model.Callback{}).Where("delivered_at IS NULL AND attempts >= attempt_base + ? AND next_at <= ? AND state NOT IN ?", maxAttempts, time.Now(), []string{"exhausted", "merchant_review", "canceled_refunded"}).Updates(map[string]any{"state": "exhausted"}).Error; err != nil {
			return err
		}
		// Only paid orders are notified. Filtering here keeps a task whose order is not paid
		// from blocking the queue: it would never become deliverable and every reservation
		// would return the same row again.
		paid := db.Model(&model.Order{}).Select("id").Where("status = ?", model.Paid)
		if e := db.Where("delivered_at IS NULL AND attempts < attempt_base + ? AND next_at <= ? AND order_id IN (?) AND state NOT IN ?", maxAttempts, time.Now(), paid, []string{"exhausted", "merchant_review", "canceled_refunded"}).Where("NOT EXISTS(SELECT 1 FROM cancellation_reviews r WHERE r.order_id=callbacks.order_id)").Order("next_at").First(&task).Error; e != nil {
			if errors.Is(e, gorm.ErrRecordNotFound) {
				missing = true
				return nil
			}
			return e
		}
		if e := db.First(&o, task.OrderID).Error; e != nil {
			return e
		}
		task.Attempts++
		task.NextAt = time.Now().Add(time.Duration(timeout+5) * time.Second)
		task.LeaseUntil = task.NextAt
		task.State = "in_flight"
		task.AttemptLimit = task.AttemptBase + maxAttempts
		return db.Save(&task).Error
	})
	if err == nil && missing {
		err = gorm.ErrRecordNotFound
	}
	return &task, &o, err
}

// ReleaseCallback returns an unsent reservation without consuming the merchant retry
// budget. The lease fence prevents an old worker from releasing a newer attempt.
func (s *Store) ReleaseCallback(ctx context.Context, task model.Callback) error {
	return s.db.WithContext(ctx).Model(&model.Callback{}).Where("order_id = ? AND attempts = ? AND delivered_at IS NULL AND state = ? AND lease_until = ?", task.OrderID, task.Attempts, "in_flight", task.LeaseUntil).Updates(map[string]any{"attempts": task.Attempts - 1, "next_at": time.Now(), "lease_until": time.Time{}, "state": gorm.Expr("CASE WHEN EXISTS(SELECT 1 FROM cancellation_reviews r WHERE r.order_id=callbacks.order_id) THEN 'merchant_review' ELSE 'queued' END")}).Error
}

func (s *Store) FinishCallback(ctx context.Context, task model.Callback, status int, ok bool, message string, delay time.Duration) error {
	return s.db.WithContext(ctx).Transaction(func(db *gorm.DB) error {
		state := "queued"
		if !ok && task.Attempts >= task.AttemptLimit && task.AttemptLimit > 0 {
			state = "exhausted"
		}
		var holds int64
		if err := db.Model(&model.CancellationReview{}).Where("order_id=?", task.OrderID).Count(&holds).Error; err != nil {
			return err
		}
		if holds > 0 {
			state = "merchant_review"
		}
		updates := map[string]any{"last_error": message, "next_at": time.Now().Add(delay), "lease_until": time.Time{}, "state": state}
		if ok {
			updates["delivered_at"] = time.Now()
			updates["state"] = "delivered"
		}
		r := db.Model(&model.Callback{}).Where("order_id = ? AND attempts = ? AND delivered_at IS NULL AND state = ? AND lease_until = ?", task.OrderID, task.Attempts, "in_flight", task.LeaseUntil).Updates(updates)
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
