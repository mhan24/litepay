package store

import (
	"context"
	"errors"
	"gorm.io/gorm"
	"litepay/internal/model"
	"time"
)

// Skip only verified ranges observed after this job's required settlement/freshness bound.
// The proof keeps its original timestamp; a reused range never becomes newer evidence.
func (s *Store) ReuseHistoricalCoverage(ctx context.Context, job model.HistoricalScan) (model.HistoricalScan, error) {
	if !job.Automatic || job.LT != "" {
		return job, nil
	}
	err := s.db.WithContext(ctx).Transaction(func(db *gorm.DB) error {
		original := job.Cursor
		required := job.RequiredAfter
		if required.IsZero() {
			required = job.CoverageAfter
		}
		if required.IsZero() {
			required = job.CreatedAt
		}
		for range 128 {
			var proof model.HistoryCoverage
			err := db.Where("provider=? AND receiver=? AND \"from\"<=? AND \"to\">? AND observed_at>=?", job.Provider, job.Receiver, job.Cursor, job.Cursor, required).Order("\"to\" DESC").First(&proof).Error
			if errors.Is(err, gorm.ErrRecordNotFound) {
				break
			}
			if err != nil {
				return err
			}
			if job.CoverageAfter.IsZero() || proof.ObservedAt.Before(job.CoverageAfter) {
				job.CoverageAfter = proof.ObservedAt
			}
			job.Cursor = proof.To
			if job.Cursor.After(job.To) {
				job.Cursor = job.To
			}
			if !job.Cursor.Before(job.To) {
				job.State = "completed"
				break
			}
		}
		if !original.Equal(job.Cursor) {
			job.ChunkTo = time.Time{}
			result := db.Model(&model.HistoricalScan{}).Where("id=? AND state='pending' AND cursor=? AND lt=? AND pages=?", job.ID, original, job.LT, job.Pages).Updates(map[string]any{"coverage_after": job.CoverageAfter, "cursor": job.Cursor, "chunk_to": job.ChunkTo, "state": job.State, "updated_at": time.Now()})
			if result.Error != nil {
				return result.Error
			}
			if result.RowsAffected != 1 {
				return errors.New("历史补扫检查点已变化")
			}
		}
		return nil
	})
	return job, err
}

func (s *Store) UncoveredChunkEnd(ctx context.Context, job model.HistoricalScan, end time.Time) (time.Time, error) {
	if !job.Automatic || job.LT != "" {
		return end, nil
	}
	required := job.RequiredAfter
	if required.IsZero() {
		required = job.CoverageAfter
	}
	if required.IsZero() {
		required = job.CreatedAt
	}
	var proof model.HistoryCoverage
	err := s.db.WithContext(ctx).Where("provider=? AND receiver=? AND \"from\">? AND \"from\"<? AND observed_at>=?", job.Provider, job.Receiver, job.Cursor, end, required).Order("\"from\"").First(&proof).Error
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return end, nil
	}
	if err != nil {
		return end, err
	}
	return proof.From, nil
}
