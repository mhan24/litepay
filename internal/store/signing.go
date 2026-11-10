package store

import (
	"context"
	"errors"
	"time"

	"gorm.io/gorm"
	"litepay/internal/model"
)

// InstallSigningKey pins existing rows once; future callbacks never read a mutable global key.
func (s *Store) InstallSigningKey(ctx context.Context, token, operator string, rotate bool) (uint, error) {
	var key model.SigningKey
	err := s.db.WithContext(ctx).Transaction(func(db *gorm.DB) error {
		var current model.Credential
		err := db.First(&current, "name = ?", "bepusdt_api_token").Error
		if err != nil && !errors.Is(err, gorm.ErrRecordNotFound) {
			return err
		}
		if current.Value != "" {
			var old model.SigningKey
			if e := db.Where("token = ?", current.Value).First(&old).Error; e != nil {
				if !errors.Is(e, gorm.ErrRecordNotFound) {
					return e
				}
				old.Token = current.Value
				if e = db.Create(&old).Error; e != nil {
					return e
				}
			}
			if e := db.Model(&model.Order{}).Where("signing_key_id = 0").Update("signing_key_id", old.ID).Error; e != nil {
				return e
			}
			if e := db.Model(&model.Callback{}).Where("signing_key_id = 0").Update("signing_key_id", old.ID).Error; e != nil {
				return e
			}
			if rotate {
				if e := db.Model(&old).Update("retired_at", time.Now()).Error; e != nil {
					return e
				}
			}
		}
		if e := db.Where("token = ?", token).First(&key).Error; e != nil {
			if !errors.Is(e, gorm.ErrRecordNotFound) {
				return e
			}
			key.Token = token
			if e = db.Create(&key).Error; e != nil {
				return e
			}
		}
		if e := db.Model(&model.Order{}).Where("signing_key_id = 0").Update("signing_key_id", key.ID).Error; e != nil {
			return e
		}
		if e := db.Model(&model.Callback{}).Where("signing_key_id = 0").Update("signing_key_id", key.ID).Error; e != nil {
			return e
		}
		if !rotate && current.Value == token {
			return nil
		}
		return (&Store{db: db}).SaveCredential(ctx, model.Credential{Name: "bepusdt_api_token", Value: token, UpdatedBy: operator})
	})
	return key.ID, err
}

func (s *Store) CallbackKey(ctx context.Context, task model.Callback) (string, error) {
	if task.SigningKeyID == 0 {
		return "", errors.New("回调缺少签名密钥版本")
	}
	var key model.SigningKey
	err := s.db.WithContext(ctx).First(&key, task.SigningKeyID).Error
	return key.Token, err
}
