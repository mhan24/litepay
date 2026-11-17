package store

import (
	"context"
	"fmt"
	"strconv"

	"gorm.io/gorm"
	"gorm.io/gorm/clause"
	"litepay/internal/model"
)

// AdminSessionEpoch initializes or advances the durable revocation counter atomically.
func (s *Store) AdminSessionEpoch(ctx context.Context, bump bool) (uint64, error) {
	var epoch uint64
	err := s.db.WithContext(ctx).Transaction(func(db *gorm.DB) error {
		row := model.Credential{Name: "admin_session_epoch", Value: "1"}
		if err := db.Clauses(clause.OnConflict{DoNothing: true}).Create(&row).Error; err != nil {
			return err
		}
		if err := db.Where("name = ?", row.Name).First(&row).Error; err != nil {
			return err
		}
		var err error
		epoch, err = strconv.ParseUint(row.Value, 10, 64)
		if err != nil || epoch == 0 || epoch == ^uint64(0) {
			return fmt.Errorf("invalid admin session epoch: %q", row.Value)
		}
		if bump {
			epoch++
			return db.Model(&row).Update("value", strconv.FormatUint(epoch, 10)).Error
		}
		return nil
	})
	return epoch, err
}
