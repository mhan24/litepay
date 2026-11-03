package store

import (
	"context"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
	"litepay/internal/model"
	"time"
)

func (s *Store) ReserveAuxiliary(ctx context.Context) error {
	return s.db.WithContext(ctx).Transaction(func(d *gorm.DB) error {
		if err := d.Clauses(clause.OnConflict{DoNothing: true}).Create(&model.AuxiliaryRun{ID: 1}).Error; err != nil {
			return err
		}
		res := d.Model(&model.AuxiliaryRun{}).Where("id = 1 AND checked_at <= ?", time.Now().Add(-5*time.Minute)).Updates(map[string]any{"checked_at": time.Now(), "result": "checking", "diagnostic": ""})
		if res.Error != nil {
			return res.Error
		}
		if res.RowsAffected != 1 {
			return ErrInquiryLimited
		}
		return nil
	})
}
func (s *Store) SaveAuxiliary(ctx context.Context, result, diagnostic string, rows []model.FundingBillEvidence) error {
	return s.db.WithContext(ctx).Transaction(func(d *gorm.DB) error {
		for _, row := range rows {
			row.UpdatedAt = time.Now()
			if err := d.Clauses(clause.OnConflict{UpdateAll: true}).Create(&row).Error; err != nil {
				return err
			}
		}
		return d.Model(&model.AuxiliaryRun{}).Where("id = 1").Updates(map[string]any{"result": result, "diagnostic": diagnostic}).Error
	})
}
func (s *Store) AuxiliaryEvidence(ctx context.Context) (*model.AuxiliaryRun, []model.FundingBillEvidence, error) {
	var runs []model.AuxiliaryRun
	var rows []model.FundingBillEvidence
	if err := s.db.WithContext(ctx).Limit(1).Find(&runs).Error; err != nil {
		return nil, nil, err
	}
	if err := s.db.WithContext(ctx).Order("updated_at DESC").Limit(20).Find(&rows).Error; err != nil {
		return nil, nil, err
	}
	if len(runs) == 0 {
		return nil, rows, nil
	}
	return &runs[0], rows, nil
}
