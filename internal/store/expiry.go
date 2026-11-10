package store

import (
	"context"
	"time"

	"gorm.io/gorm"
	"litepay/internal/model"
)

const ExpiryBatchSize = 100
const ExpiryRecheckInterval = 30 * time.Second

func initializeExpiryAndSelection(db *gorm.DB) error {
	if err := db.Exec(`UPDATE orders SET next_expiry_check_at=(SELECT MAX(settle_until) FROM payment_attempts a WHERE a.order_id=orders.id)
 WHERE status='READY' AND (next_expiry_check_at IS NULL OR next_expiry_check_at < '0002-01-01') AND EXISTS(SELECT 1 FROM payment_attempts a WHERE a.order_id=orders.id)`).Error; err != nil {
		return err
	}
	return db.Exec(`UPDATE orders SET selection_changes=(SELECT COUNT(*) FROM payment_attempts a WHERE a.order_id=orders.id), last_selection_at=(SELECT MAX(created_at) FROM payment_attempts a WHERE a.order_id=orders.id)
 WHERE selection_changes=0 AND EXISTS(SELECT 1 FROM payment_attempts a WHERE a.order_id=orders.id)`).Error
}

// Expire only visits due orders in bounded batches. All attempt/cursor/finality predicates
// are evaluated in SQL; future orders require no per-order reads and blocked ones back off.
func (s *Store) Expire(ctx context.Context, grace int) error {
	now := time.Now()
	return s.db.WithContext(ctx).Transaction(func(db *gorm.DB) error {
		if err := db.Model(&model.Order{}).Where("status = ? AND expires_at < ?", model.Pending, now.Add(-time.Duration(grace)*time.Second)).Update("status", model.Expired).Error; err != nil {
			return err
		}
		if err := db.Exec(`UPDATE confirmation_tasks SET state='manual_review',updated_at=? WHERE rowid IN
   (SELECT rowid FROM confirmation_tasks WHERE state='pending' AND deadline<=? ORDER BY deadline LIMIT 500)`, now, now).Error; err != nil {
			return err
		}
		var ids []uint
		if err := db.Model(&model.Order{}).Where("status = ? AND next_expiry_check_at <= ?", model.Ready, now).Order("next_expiry_check_at,id").Limit(ExpiryBatchSize).Pluck("id", &ids).Error; err != nil {
			return err
		}
		if len(ids) > 0 {
			eligible := `EXISTS(SELECT 1 FROM payment_attempts a WHERE a.order_id=orders.id)
    AND NOT EXISTS(SELECT 1 FROM payment_attempts a WHERE a.order_id=orders.id AND (
     a.settle_until > ? OR NOT EXISTS(SELECT 1 FROM scan_cursors c WHERE c.provider=a.provider AND c.scanned_until>=a.settle_until)
     OR NOT EXISTS(SELECT 1 FROM historical_scans h WHERE h.automatic=1 AND h.provider=a.provider AND h.receiver=a.receiver AND h.state='completed' AND h."from"<=a.selected_at AND h."to">=a.expires_at AND h.created_at>=a.settle_until)))
    AND NOT EXISTS(SELECT 1 FROM confirmation_tasks q WHERE q.order_id=orders.id AND q.state='pending' AND q.deadline>?)`
			if err := db.Model(&model.Order{}).Where("id IN ? AND status = ?", ids, model.Ready).Where(eligible, now, now).Update("status", model.Expired).Error; err != nil {
				return err
			}
			closed := db.Model(&model.Order{}).Select("id").Where("id IN ? AND status = ?", ids, model.Expired)
			if err := db.Model(&model.Slot{}).Where("order_id IN (?)", closed).Update("order_id", nil).Error; err != nil {
				return err
			}
			if err := db.Model(&model.Order{}).Where("id IN ? AND status = ?", ids, model.Ready).Update("next_expiry_check_at", now.Add(ExpiryRecheckInterval)).Error; err != nil {
				return err
			}
		}
		return db.Exec(`DELETE FROM slots WHERE rowid IN(SELECT rowid FROM slots WHERE order_id IS NULL AND expires_at<? ORDER BY expires_at LIMIT 1000)`, now).Error
	})
}
