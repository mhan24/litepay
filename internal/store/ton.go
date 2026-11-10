package store

import (
	"context"
	"errors"

	"gorm.io/gorm"
	"litepay/internal/model"
)

func (s *Store) TONCheckpoint(ctx context.Context, receiver string) (model.TONScanCheckpoint, error) {
	cp := model.TONScanCheckpoint{Receiver: receiver}
	err := s.db.WithContext(ctx).Where("receiver = ?", receiver).First(&cp).Error
	if errors.Is(err, gorm.ErrRecordNotFound) {
		err = nil
	}
	return cp, err
}

func (s *Store) StartTONCheckpoint(ctx context.Context, cp model.TONScanCheckpoint) error {
	return s.db.WithContext(ctx).Save(&cp).Error
}

func (s *Store) SaveTONPage(ctx context.Context, before, after model.TONScanCheckpoint, rows []model.Transaction) error {
	return s.db.WithContext(ctx).Transaction(func(db *gorm.DB) error {
		var current model.TONScanCheckpoint
		if err := db.Where("receiver = ?", before.Receiver).First(&current).Error; err != nil {
			return err
		}
		if current.LT != before.LT || current.Hash != before.Hash || !current.From.Equal(before.From) || !current.To.Equal(before.To) || !current.Active {
			return errors.New("TON 检查点已变化")
		}
		if after.Receiver != before.Receiver || !after.From.Equal(before.From) || !after.To.Equal(before.To) || after.Pages != before.Pages+1 {
			return errors.New("TON 检查点范围发生变化")
		}
		bounded := &Store{db: db}
		for _, row := range rows {
			if row.Provider != "ton" || row.Receiver != before.Receiver || row.OccurredAt.Before(before.From) || row.OccurredAt.After(before.To) {
				return errors.New("TON 分页证据不在扫描范围")
			}
			if _, err := bounded.Process(ctx, row, ""); err != nil {
				return err
			}
		}
		if err := db.Save(&after).Error; err != nil {
			return err
		}
		if !after.Active {
			return bounded.ScanResult(ctx, "ton", after.To, nil)
		}
		return nil
	})
}
