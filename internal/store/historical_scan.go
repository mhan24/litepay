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
			if err := db.Create(&model.HistoricalScan{Priority: 1, OrderID: o.ID, AttemptID: a.ID, Provider: a.Provider, Receiver: a.Receiver, From: a.SelectedAt, To: end, Cursor: a.SelectedAt, State: "pending", Operator: operator, Reason: reason}).Error; err != nil {
				return err
			}
		}
		return nil
	})
}
func (s *Store) NextHistoricalScan(ctx context.Context, provider string) (model.HistoricalScan, error) {
	var q model.HistoricalScan
	err := s.db.WithContext(ctx).Where("provider = ? AND state = ?", provider, "pending").Order("priority DESC,updated_at,id").First(&q).Error
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

// Cohorts share one coverage result for instructions selected in the same hour.
// Final passes outrank routine rechecks; empty gaps between cohorts are never queried.
const SettlementRefresh = 15 * time.Minute

func (s *Store) QueueSettlementScan(ctx context.Context, provider, receiver string, lookback time.Duration) error {
	return s.db.WithContext(ctx).Transaction(func(db *gorm.DB) error {
		now := time.Now()
		for _, priority := range []int{2, 0} {
			var active int64
			if err := db.Model(&model.HistoricalScan{}).Where("automatic=1 AND provider=? AND receiver=? AND state='pending' AND priority=?", provider, receiver, priority).Count(&active).Error; err != nil {
				return err
			}
			if active > 0 {
				continue
			}
			base := func() *gorm.DB {
				q := db.Table("payment_attempts a").Select("a.*").Joins("JOIN orders o ON o.id=a.order_id").Where("o.status=? AND a.provider=? AND a.receiver=? AND a.selected_at<?", model.Ready, provider, receiver, now.Add(-lookback))
				if priority == 2 {
					return q.Where("a.settle_until<=?", now)
				}
				return q.Where("a.settle_until>?", now)
			}
			q := base().Where(`NOT EXISTS(SELECT 1 FROM historical_scans h WHERE h.automatic=1 AND h.provider=a.provider AND h.receiver=a.receiver AND h.state='completed' AND h."from"<=a.selected_at AND h."to">=CASE WHEN a.expires_at<? THEN a.expires_at ELSE ? END AND (h.created_at>=a.settle_until OR (?=0 AND h.updated_at>=?)))`, now, now, priority, now.Add(-SettlementRefresh))
			var first model.PaymentAttempt
			if err := q.Order("a.settle_until,a.selected_at,a.id").First(&first).Error; err != nil {
				if errors.Is(err, gorm.ErrRecordNotFound) {
					continue
				}
				return err
			}
			from := first.SelectedAt.Truncate(time.Hour)
			var last model.PaymentAttempt
			if err := base().Where("a.selected_at>=? AND a.selected_at<?", from, from.Add(time.Hour)).Order("a.expires_at DESC").First(&last).Error; err != nil {
				return err
			}
			end := last.ExpiresAt
			if end.After(now) {
				end = now
			}
			if err := db.Create(&model.HistoricalScan{Automatic: true, Priority: priority, OrderID: first.OrderID, AttemptID: first.ID, Provider: provider, Receiver: receiver, From: from, To: end, Cursor: from, State: "pending", Operator: "system", Reason: "付款窗口分组复核"}).Error; err != nil {
				return err
			}
		}
		return nil
	})
}

// A saturated request gets a smaller durable chunk, without advancing its coverage.
func (s *Store) ShrinkHistoricalChunk(ctx context.Context, job model.HistoricalScan, end time.Time) error {
	size := end.Sub(job.Cursor) / 2
	if size < time.Millisecond {
		return s.HistoricalScanFailure(ctx, job, false)
	}
	return s.db.WithContext(ctx).Model(&model.HistoricalScan{}).Where("id=? AND state='pending' AND cursor=? AND pages=?", job.ID, job.Cursor, job.Pages).Updates(map[string]any{"chunk_millis": max(1, size.Milliseconds()), "chunk_to": time.Time{}, "last_error": "历史窗口数据密集，已缩小时间片继续复核", "updated_at": time.Now()}).Error
}
