package store

import (
	"context"
	"errors"
	"time"

	"gorm.io/gorm"
	"litepay/internal/model"
)

func findSigningKeyByPlaintext(db *gorm.DB, masterKey []byte, plainToken string) (model.SigningKey, bool, error) {
	var rows []model.SigningKey
	if err := db.Order("id").Find(&rows).Error; err != nil {
		return model.SigningKey{}, false, err
	}
	for _, row := range rows {
		candidate := row.Token
		if IsEncryptedSecret(candidate) {
			var err error
			candidate, err = DecryptSecret(masterKey, candidate)
			if err != nil {
				return model.SigningKey{}, false, err
			}
		}
		if candidate == plainToken {
			return row, true, nil
		}
	}
	return model.SigningKey{}, false, nil
}

// InstallSigningKey pins existing rows once; future callbacks never read a mutable global key.
func (s *Store) InstallSigningKey(ctx context.Context, token, operator string, rotate bool) (uint, error) {
	var key model.SigningKey
	err := s.db.WithContext(ctx).Transaction(func(db *gorm.DB) error {
		txStore := &Store{db: db, masterKey: s.masterKey}
		current, err := txStore.LoadCredential(ctx, "bepusdt_api_token")
		if err != nil {
			return err
		}
		currentValue := ""
		if current != nil {
			currentValue = current.Value
		}
		if currentValue != "" {
			old, found, e := findSigningKeyByPlaintext(db, s.masterKey, currentValue)
			if e != nil {
				return e
			}
			if !found {
				storedOld := currentValue
				if s.HasMasterKey() {
					storedOld, e = EncryptSecret(s.masterKey, currentValue)
					if e != nil {
						return e
					}
				}
				old.Token = storedOld
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
		foundKey, found, e := findSigningKeyByPlaintext(db, s.masterKey, token)
		if e != nil {
			return e
		}
		if found {
			key = foundKey
		} else {
			storedToken := token
			if s.HasMasterKey() {
				storedToken, e = EncryptSecret(s.masterKey, token)
				if e != nil {
					return e
				}
			}
			key.Token = storedToken
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
		if !rotate && currentValue == token {
			return nil
		}
		return txStore.SaveCredential(ctx, model.Credential{Name: "bepusdt_api_token", Value: token, UpdatedBy: operator})
	})
	return key.ID, err
}

func (s *Store) CallbackKey(ctx context.Context, task model.Callback) (string, error) {
	if task.SigningKeyID == 0 {
		return "", errors.New("回调缺少签名密钥版本")
	}
	var key model.SigningKey
	if err := s.db.WithContext(ctx).First(&key, task.SigningKeyID).Error; err != nil {
		return "", err
	}
	if IsEncryptedSecret(key.Token) {
		return DecryptSecret(s.masterKey, key.Token)
	}
	return key.Token, nil
}
