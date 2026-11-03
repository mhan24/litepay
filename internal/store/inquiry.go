package store

import (
	"context"
	"errors"
	"time"

	"gorm.io/gorm"
	"gorm.io/gorm/clause"
	"litepay/internal/model"
)

var ErrInquiryLimited = errors.New("补查次数受限，请稍后重试或联系商户")

// ReserveInquiry persists the reservation before any exchange request. Limits survive restarts.
func (s *Store) ReserveInquiry(ctx context.Context, tradeNo, reference string) error {
	now := time.Now()
	return s.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		if err := tx.Clauses(clause.OnConflict{DoNothing: true}).Create(&model.PaymentInquiry{TradeNo: tradeNo}).Error; err != nil {
			return err
		}
		res := tx.Model(&model.PaymentInquiry{}).Where("trade_no = ? AND attempts < ? AND last_attempt_at <= ?", tradeNo, 10, now.Add(-time.Minute)).Updates(map[string]any{
			"reference": reference, "result": "checking", "attempts": gorm.Expr("attempts + 1"), "last_attempt_at": now, "updated_at": now,
		})
		if res.Error != nil {
			return res.Error
		}
		if res.RowsAffected != 1 {
			return ErrInquiryLimited
		}
		return nil
	})
}

func (s *Store) FinishInquiry(ctx context.Context, tradeNo, result string) error {
	return s.db.WithContext(ctx).Model(&model.PaymentInquiry{}).Where("trade_no = ?", tradeNo).Updates(map[string]any{"result": result, "updated_at": time.Now()}).Error
}

func (s *Store) PaymentInquiry(ctx context.Context, tradeNo string) (*model.PaymentInquiry, error) {
	var row model.PaymentInquiry
	err := s.db.WithContext(ctx).Where("trade_no = ?", tradeNo).First(&row).Error
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return nil, nil
	}
	return &row, err
}
