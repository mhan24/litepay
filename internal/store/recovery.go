package store

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
	"litepay/internal/model"
	"time"
)

func initializeRecovery(db *gorm.DB) error {
	var bytes [16]byte
	if _, err := rand.Read(bytes[:]); err != nil {
		return err
	}
	return db.Clauses(clause.OnConflict{DoNothing: true}).Create(&model.GatewayRecovery{ID: 1, Generation: hex.EncodeToString(bytes[:]), Reason: "首次启用恢复核对", CreatedAt: time.Now()}).Error
}
func (s *Store) Recovery(ctx context.Context) (model.GatewayRecovery, error) {
	var r model.GatewayRecovery
	err := s.db.WithContext(ctx).First(&r, 1).Error
	return r, err
}
func (s *Store) RecoveryReady(ctx context.Context) (bool, error) {
	r, err := s.Recovery(ctx)
	return r.Acknowledged, err
}
func (s *Store) AcknowledgeRecovery(ctx context.Context, generation string, complete bool, h model.IntegrationHealth) error {
	return s.db.WithContext(ctx).Transaction(func(db *gorm.DB) error {
		var r model.GatewayRecovery
		if err := db.First(&r, 1).Error; err != nil {
			return err
		}
		if generation != r.Generation {
			return gorm.ErrInvalidData
		}
		if err := (&Store{db: db}).SaveIntegrationHealth(ctx, h); err != nil {
			return err
		}
		if complete && h.Healthy && h.Failures == 0 && h.Pending == 0 && h.Missing == 0 && h.Conflicts == 0 {
			return db.Model(&r).Update("acknowledged", true).Error
		}
		return nil
	})
}

func (s *Store) BeginRecovery(ctx context.Context, reason string) error {
	var bytes [16]byte
	if _, err := rand.Read(bytes[:]); err != nil {
		return err
	}
	return s.db.WithContext(ctx).Model(&model.GatewayRecovery{}).Where("id=1").Updates(map[string]any{"generation": hex.EncodeToString(bytes[:]), "acknowledged": false, "reason": reason, "created_at": time.Now()}).Error
}
