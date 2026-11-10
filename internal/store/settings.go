package store

import (
	"context"
	"errors"
	"time"

	"gorm.io/gorm"
	"gorm.io/gorm/clause"

	"litepay/internal/model"
)

// LoadSettingPayload 读取后台配置覆盖项（不存在时返回空串）。
func (s *Store) LoadSettingPayload(ctx context.Context) (string, error) {
	var row model.Setting
	err := s.db.WithContext(ctx).Where("id = 1").First(&row).Error
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return "", nil
	}
	if err != nil {
		return "", err
	}
	return row.Payload, nil
}

// SaveSettingPayload 保存后台配置覆盖项。
func (s *Store) SaveSettingPayload(ctx context.Context, payload, operator string) error {
	row := model.Setting{ID: 1, Payload: payload, UpdatedBy: operator, UpdatedAt: time.Now()}
	return s.db.WithContext(ctx).Clauses(clause.OnConflict{
		Columns:   []clause.Column{{Name: "id"}},
		DoUpdates: clause.AssignmentColumns([]string{"payload", "updated_by", "updated_at"}),
	}).Create(&row).Error
}

// SettingUpdatedAt 返回后台配置最近保存时间。
func (s *Store) SettingUpdatedAt(ctx context.Context) (time.Time, error) {
	var row model.Setting
	err := s.db.WithContext(ctx).Where("id = 1").First(&row).Error
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return time.Time{}, nil
	}
	if err != nil {
		return time.Time{}, err
	}
	return row.UpdatedAt, nil
}

// LoadCredential 读取系统生成的凭据。
func (s *Store) LoadCredential(ctx context.Context, name string) (*model.Credential, error) {
	var row model.Credential
	err := s.db.WithContext(ctx).Where("name = ?", name).First(&row).Error
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return &row, nil
}

// SaveCredential 保存或轮换系统凭据。
func (s *Store) SaveCredential(ctx context.Context, row model.Credential) error {
	row.UpdatedAt = time.Now()
	return s.db.WithContext(ctx).Clauses(clause.OnConflict{
		Columns:   []clause.Column{{Name: "name"}},
		DoUpdates: clause.AssignmentColumns([]string{"value", "updated_by", "updated_at"}),
	}).Create(&row).Error
}

func (s *Store) SaveSettingAndArchiveTON(ctx context.Context, payload, operator, receiver string) error {
	return s.db.WithContext(ctx).Transaction(func(db *gorm.DB) error {
		if err := (&Store{db: db}).SaveSettingPayload(ctx, payload, operator); err != nil {
			return err
		}
		return db.Model(&model.TONScanCheckpoint{}).Where("receiver <> ? AND active = ?", receiver, true).Update("active", false).Error
	})
}
