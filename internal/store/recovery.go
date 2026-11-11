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

// PaymentOperationsReady gates new payment instructions and merchant delivery. Evidence
// collection uses RecoveryReady instead: losing a live bridge must not hide real deposits.
func (s *Store) PaymentOperationsReady(ctx context.Context) (bool, error) {
	var state struct {
		Acknowledged                                            bool
		Generation                                              string
		HealthGeneration                                        string
		Healthy                                                 bool
		Pending, Failures, Missing, Conflicts, DiscoveryPending int64
		ReceivedAt, LastSuccessAt                               time.Time
	}
	r := s.db.WithContext(ctx).Raw(`SELECT r.acknowledged,r.generation,h.generation health_generation,h.healthy,h.pending,h.failures,h.missing,h.conflicts,h.discovery_pending,h.received_at,h.last_success_at FROM gateway_recoveries r LEFT JOIN integration_healths h ON h.name='dujiao_cancel_sync' WHERE r.id=1`).Scan(&state)
	now := time.Now()
	return r.RowsAffected == 1 && state.Acknowledged && state.Generation != "" && state.Generation == state.HealthGeneration && state.Healthy && state.Pending == 0 && state.Failures == 0 && state.Missing == 0 && state.Conflicts == 0 && state.DiscoveryPending == 0 && !state.ReceivedAt.IsZero() && now.Sub(state.ReceivedAt) <= 45*time.Second && !state.LastSuccessAt.IsZero() && now.Sub(state.LastSuccessAt) <= time.Minute, r.Error
}

func (s *Store) AcknowledgeRecovery(ctx context.Context, generation string, complete bool, h model.IntegrationHealth) error {
	return s.db.WithContext(ctx).Transaction(func(db *gorm.DB) error {
		var r model.GatewayRecovery
		if err := db.First(&r, 1).Error; err != nil {
			return err
		}
		if generation != r.Generation || h.Generation != generation {
			return gorm.ErrInvalidData
		}
		var old model.IntegrationHealth
		err := db.First(&old, "name=?", h.Name).Error
		if err != nil && err != gorm.ErrRecordNotFound {
			return err
		}
		if err == nil && h.ReportedAt <= old.ReportedAt {
			return nil
		}
		// A fresh source heartbeat alone cannot release delivery while cancellation
		// replay is incomplete, including after the recovery generation was acknowledged.
		h.Healthy = h.Healthy && complete
		if err := (&Store{db: db}).SaveIntegrationHealth(ctx, h); err != nil {
			return err
		}
		if complete && h.Healthy && h.Failures == 0 && h.Pending == 0 && h.Missing == 0 && h.Conflicts == 0 && h.DiscoveryPending == 0 {
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
