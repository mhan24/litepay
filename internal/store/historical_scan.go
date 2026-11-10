package store

import (
	"context"
	"errors"
	"strings"
	"time"
	"unicode/utf8"

	"gorm.io/gorm"
	"litepay/internal/model"
)

func (s *Store) RequestHistoricalScan(ctx context.Context, no, operator, reason string) error {
	reason = strings.TrimSpace(reason)
	if operator == "" || reason == "" || utf8.RuneCountInString(reason) > 300 {
		return errors.New("请填写核对原因（1~300 字）")
	}
	return s.db.WithContext(ctx).Transaction(func(db *gorm.DB) error {
		var o model.Order
		if err := db.Where("trade_no = ?", no).First(&o).Error; err != nil {
			return err
		}
		var attempts []model.PaymentAttempt
		if err := db.Where("order_id = ?", o.ID).Find(&attempts).Error; err != nil {
			return err
		}
		if len(attempts) == 0 {
			return errors.New("没有可补扫的付款尝试")
		}
		for _, a := range attempts {
			var n int64
			if err := db.Model(&model.HistoricalScan{}).Where("attempt_id = ? AND state = ?", a.ID, "pending").Count(&n).Error; err != nil {
				return err
			}
			if n > 0 {
				continue
			}
			end := a.ExpiresAt
			if end.After(time.Now()) {
				end = time.Now()
			}
			if err := db.Create(&model.HistoricalScan{OrderID: o.ID, AttemptID: a.ID, Provider: a.Provider, Receiver: a.Receiver, From: a.SelectedAt, To: end, Cursor: a.SelectedAt, State: "pending", Operator: operator, Reason: reason}).Error; err != nil {
				return err
			}
		}
		return nil
	})
}
func (s *Store) NextHistoricalScan(ctx context.Context, provider string) (model.HistoricalScan, error) {
	var q model.HistoricalScan
	err := s.db.WithContext(ctx).Where("provider = ? AND state = ?", provider, "pending").Order("updated_at,id").First(&q).Error
	return q, err
}
func (s *Store) HistoricalScanFailure(ctx context.Context, q model.HistoricalScan, blocked bool) error {
	state := "pending"
	message := "历史查询未完成，将继续重试"
	if blocked {
		state = "blocked"
		message = "历史收款账户与当前配置不一致，需人工核对账户配置后重新申请"
	}
	return s.db.WithContext(ctx).Model(&q).Updates(map[string]any{"state": state, "last_error": message, "updated_at": time.Now()}).Error
}
func (s *Store) SaveHistoricalScan(ctx context.Context, before, after model.HistoricalScan, rows []model.Transaction) error {
	return s.db.WithContext(ctx).Transaction(func(db *gorm.DB) error {
		var current model.HistoricalScan
		if err := db.First(&current, before.ID).Error; err != nil {
			return err
		}
		if current.State != "pending" || !current.Cursor.Equal(before.Cursor) || current.LT != before.LT || current.Pages != before.Pages {
			return errors.New("历史补扫检查点已变化")
		}
		bounded := &Store{db: db}
		for _, row := range rows {
			if row.Provider != before.Provider || row.Receiver != before.Receiver || row.OccurredAt.Before(before.From) || row.OccurredAt.After(before.To) {
				return errors.New("历史证据不在申请范围")
			}
			if _, err := bounded.Process(ctx, row, ""); err != nil {
				return err
			}
		}
		after.LastError = ""
		return db.Save(&after).Error
	})
}

// QueueSettlementScan continuously reopens historical creation-time windows. Successful
// incremental queries cannot establish that a previously invisible record does not exist.
// One account-wide job covers all its open instructions, instead of polling per invoice.
func (s *Store) QueueSettlementScan(ctx context.Context, provider, receiver string, lookback time.Duration) error {
	return s.db.WithContext(ctx).Transaction(func(db *gorm.DB) error {
		query := func() *gorm.DB {
			return db.Table("payment_attempts a").Select("a.*").Joins("JOIN orders o ON o.id=a.order_id").Where("o.status = ? AND a.provider = ? AND a.receiver = ? AND a.selected_at < ?", model.Ready, provider, receiver, time.Now().Add(-lookback))
		}
		var first, last model.PaymentAttempt
		if err := query().Order("a.selected_at").First(&first).Error; err != nil {
			if errors.Is(err, gorm.ErrRecordNotFound) {
				return nil
			}
			return err
		}
		var latest model.HistoricalScan
		err := db.Where("automatic = ? AND provider = ? AND receiver = ?", true, provider, receiver).Order("id DESC").First(&latest).Error
		if err != nil && !errors.Is(err, gorm.ErrRecordNotFound) {
			return err
		}
		if err == nil && (latest.State == "pending" || time.Since(latest.UpdatedAt) < time.Minute) {
			return nil
		}
		if err = query().Order("a.expires_at DESC").First(&last).Error; err != nil {
			return err
		}
		end := last.ExpiresAt
		if end.After(time.Now()) {
			end = time.Now()
		}
		return db.Create(&model.HistoricalScan{Automatic: true, OrderID: first.OrderID, AttemptID: first.ID, Provider: provider, Receiver: receiver, From: first.SelectedAt, To: end, Cursor: first.SelectedAt, State: "pending", Operator: "system", Reason: "付款窗口首次可见延迟对账"}).Error
	})
}
