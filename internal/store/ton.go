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
	cp.From = normalizeTime(cp.From)
	cp.To = normalizeTime(cp.To)
	cp.OldestAt = normalizeTime(cp.OldestAt)
	if cp.Receiver == "" || cp.From.After(cp.To) || (cp.Active && cp.Pages > 0 && (cp.LT == "" || cp.Hash == "")) {
		return errors.New("TON 检查点参数无效")
	}
	return s.db.WithContext(ctx).Save(&cp).Error
}

func (s *Store) SaveTONPage(ctx context.Context, before, after model.TONScanCheckpoint, rows []model.Transaction) error {
	before.From = normalizeTime(before.From)
	before.To = normalizeTime(before.To)
	before.OldestAt = normalizeTime(before.OldestAt)
	after.From = normalizeTime(after.From)
	after.To = normalizeTime(after.To)
	after.OldestAt = normalizeTime(after.OldestAt)
	if after.Active && after.Pages > 0 && (after.LT == "" || after.Hash == "") {
		return errors.New("TON 活跃检查点缺少分页锚点")
	}
	return s.db.WithContext(ctx).Transaction(func(db *gorm.DB) error {
		var current model.TONScanCheckpoint
		if err := db.Where("receiver = ?", before.Receiver).First(&current).Error; err != nil {
			return err
		}
		if current.LT != before.LT || current.Hash != before.Hash || current.Pages != before.Pages || !current.From.Equal(before.From) || !current.To.Equal(before.To) || !current.Active {
			return errors.New("TON 检查点已变化")
		}
		if after.Receiver != before.Receiver || !after.From.Equal(before.From) || !after.To.Equal(before.To) || after.Pages != before.Pages+1 {
			return errors.New("TON 检查点范围发生变化")
		}
		bounded := &Store{db: db}
		for _, row := range rows {
			row.OccurredAt = normalizeTime(row.OccurredAt)
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
