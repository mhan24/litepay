package store

import (
	"context"
	"gorm.io/gorm"
	"litepay/internal/model"
	"time"
)

type QueueStats struct {
	FinalReviewWaiting         int64 `json:"final_review_waiting"`
	OldestFinalWindowSeconds   int64 `json:"oldest_final_window_seconds"`
	CallbackWaiting            int64 `json:"callback_waiting"`
	CallbackInFlight           int64 `json:"callback_in_flight"`
	CallbackReview             int64 `json:"callback_review"`
	OldestCallbackSeconds      int64 `json:"oldest_callback_seconds"`
	HistoricalPending          int64 `json:"historical_pending"`
	HistoricalRemainingSeconds int64 `json:"historical_remaining_seconds"`
	OldestHistoricalSeconds    int64 `json:"oldest_historical_seconds"`
}

func (s *Store) QueueStats(ctx context.Context) (QueueStats, error) {
	var out QueueStats
	db := s.db.WithContext(ctx)
	err := db.Raw(`SELECT COUNT(*) callback_waiting,CAST(COALESCE(MAX((julianday('now')-julianday(o.paid_at))*86400),0) AS INTEGER) oldest_callback_seconds FROM callbacks c JOIN orders o ON o.id=c.order_id WHERE o.status='PAID' AND c.delivered_at IS NULL AND c.state NOT IN ('merchant_review','exhausted')`).Scan(&out).Error
	if err != nil {
		return out, err
	}
	if err = db.Model(&model.Callback{}).Where("state='in_flight' AND delivered_at IS NULL").Count(&out.CallbackInFlight).Error; err != nil {
		return out, err
	}
	if err = db.Model(&model.Callback{}).Where("state='merchant_review' AND delivered_at IS NULL").Count(&out.CallbackReview).Error; err != nil {
		return out, err
	}
	var h struct {
		Pending   int64
		Remaining float64
		Oldest    float64
	}
	err = db.Raw(`SELECT count(*) pending,COALESCE(SUM(MAX(0,(julianday("to")-julianday(cursor))*86400)),0) remaining,COALESCE(MAX((julianday('now')-julianday("from"))*86400),0) oldest FROM historical_scans WHERE state='pending'`).Scan(&h).Error
	out.HistoricalPending = h.Pending
	out.HistoricalRemainingSeconds = int64(h.Remaining)
	out.OldestHistoricalSeconds = int64(h.Oldest)
	if err != nil {
		return out, err
	}
	var final struct {
		Waiting int64
		Oldest  int64
	}
	err = db.Raw(`SELECT count(*) waiting,CAST(COALESCE(MAX((julianday('now')-julianday(a.selected_at))*86400),0) AS INTEGER) oldest FROM payment_attempts a JOIN orders o ON o.id=a.order_id WHERE o.status='READY' AND a.settle_until<=? AND NOT EXISTS(SELECT 1 FROM historical_scans h WHERE h.automatic=1 AND h.provider=a.provider AND h.receiver=a.receiver AND h.state='completed' AND h."from"<=a.selected_at AND h."to">=a.expires_at AND (CASE WHEN h.coverage_after>'0002-01-01' THEN h.coverage_after ELSE h.created_at END)>=a.settle_until)`, time.Now()).Scan(&final).Error
	out.FinalReviewWaiting = final.Waiting
	out.OldestFinalWindowSeconds = final.Oldest
	return out, err
}
func (s *Store) SaveIntegrationHealth(ctx context.Context, h model.IntegrationHealth) error {
	return s.db.WithContext(ctx).Transaction(func(db *gorm.DB) error {
		var old model.IntegrationHealth
		err := db.First(&old, "name=?", h.Name).Error
		if err != nil && err != gorm.ErrRecordNotFound {
			return err
		}
		if err == nil && h.ReportedAt <= old.ReportedAt {
			return nil
		}
		if !h.Healthy {
			h.LastSuccessAt = old.LastSuccessAt
		} else {
			h.LastSuccessAt = time.Now()
		}
		h.ReceivedAt = time.Now()
		return db.Save(&h).Error
	})
}
func (s *Store) IntegrationHealth(ctx context.Context) (model.IntegrationHealth, error) {
	var h model.IntegrationHealth
	err := s.db.WithContext(ctx).First(&h, "name=?", "dujiao_cancel_sync").Error
	return h, err
}
