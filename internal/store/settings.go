package store

import (
	"context"
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"strings"
	"time"

	"gorm.io/gorm"
	"gorm.io/gorm/clause"

	"litepay/internal/model"
)

const (
	encryptedPrefix           = "enc:v1:"
	CredentialMigrationMarker = "credentials_encrypted_v1"
)

var sensitiveSettingKeys = []string{
	"binance_api_key",
	"binance_api_secret",
	"okx_api_key",
	"okx_api_secret",
	"okx_passphrase",
	"ton_api_key",
}

// IsEncryptedSecret 判断字段值是否为 AES-256-GCM 密文格式。
func IsEncryptedSecret(v string) bool {
	return strings.HasPrefix(v, encryptedPrefix)
}

// ParseMasterKey 解析 32 字节主密钥（支持 64 位 hex、base64 或 32 字节原始字符串）。
func ParseMasterKey(raw string) ([]byte, error) {
	trimmed := strings.TrimSpace(raw)
	if trimmed == "" {
		return nil, errors.New("主密钥不能为空")
	}
	if len(trimmed) == 64 {
		if decoded, err := hex.DecodeString(trimmed); err == nil && len(decoded) == 32 {
			return decoded, nil
		}
	}
	if len(trimmed) == 32 {
		return []byte(trimmed), nil
	}
	for _, enc := range []*base64.Encoding{base64.StdEncoding, base64.RawStdEncoding, base64.URLEncoding, base64.RawURLEncoding} {
		if decoded, err := enc.DecodeString(trimmed); err == nil && len(decoded) == 32 {
			return decoded, nil
		}
	}
	return nil, errors.New("主密钥长度无效，需为 32 字节（或 64 位 hex / Base64 编码）")
}

// LoadMasterKeyFromFile 从只读文件加载主密钥。
func LoadMasterKeyFromFile(path string) ([]byte, error) {
	data, err := os.ReadFile(strings.TrimSpace(path))
	if err != nil {
		return nil, fmt.Errorf("读取主密钥文件失败: %w", err)
	}
	return ParseMasterKey(string(data))
}

// ResolveMasterKey 从配置文件路径、LITEPAY_MASTER_KEY_FILE 或 LITEPAY_MASTER_KEY 解析主密钥。
func ResolveMasterKey(configFile string) ([]byte, error) {
	if p := strings.TrimSpace(configFile); p != "" {
		return LoadMasterKeyFromFile(p)
	}
	if p := strings.TrimSpace(os.Getenv("LITEPAY_MASTER_KEY_FILE")); p != "" {
		return LoadMasterKeyFromFile(p)
	}
	if raw := strings.TrimSpace(os.Getenv("LITEPAY_MASTER_KEY")); raw != "" {
		return ParseMasterKey(raw)
	}
	return nil, nil
}

// EncryptSecret 使用 AES-256-GCM 加密敏感字符串。
func EncryptSecret(key []byte, plaintext string) (string, error) {
	if len(key) != 32 {
		return "", errors.New("AES-256-GCM 主密钥必须为 32 字节")
	}
	block, err := aes.NewCipher(key)
	if err != nil {
		return "", err
	}
	gcm, err := cipher.NewGCM(block)
	if err != nil {
		return "", err
	}
	nonce := make([]byte, gcm.NonceSize())
	if _, err := io.ReadFull(rand.Reader, nonce); err != nil {
		return "", err
	}
	sealed := gcm.Seal(nonce, nonce, []byte(plaintext), nil)
	return encryptedPrefix + base64.RawStdEncoding.EncodeToString(sealed), nil
}

// DecryptSecret 解密 AES-256-GCM 密文。
func DecryptSecret(key []byte, value string) (string, error) {
	if !IsEncryptedSecret(value) {
		return "", errors.New("密文格式无效")
	}
	if len(key) != 32 {
		return "", errors.New("未配置有效主密钥，无法解密敏感凭据")
	}
	raw, err := base64.RawStdEncoding.DecodeString(strings.TrimPrefix(value, encryptedPrefix))
	if err != nil {
		return "", fmt.Errorf("解析密文失败: %w", err)
	}
	block, err := aes.NewCipher(key)
	if err != nil {
		return "", err
	}
	gcm, err := cipher.NewGCM(block)
	if err != nil {
		return "", err
	}
	if len(raw) < gcm.NonceSize() {
		return "", errors.New("密文长度不足")
	}
	nonce := raw[:gcm.NonceSize()]
	ciphertext := raw[gcm.NonceSize():]
	plain, err := gcm.Open(nil, nonce, ciphertext, nil)
	if err != nil {
		return "", fmt.Errorf("解密敏感凭据失败（主密钥不匹配或数据损坏）: %w", err)
	}
	return string(plain), nil
}

// SetMasterKey 设置用于凭据加解密的 32 字节主密钥。
func (s *Store) SetMasterKey(key []byte) error {
	if len(key) == 0 {
		s.masterKey = nil
		return nil
	}
	if len(key) != 32 {
		return errors.New("主密钥长度必须为 32 字节")
	}
	s.masterKey = append([]byte(nil), key...)
	return nil
}

// HasMasterKey 返回是否已配置有效主密钥。
func (s *Store) HasMasterKey() bool {
	return len(s.masterKey) == 32
}

func encryptSettingPayload(key []byte, payload string) (string, bool, error) {
	if strings.TrimSpace(payload) == "" || len(key) == 0 {
		return payload, false, nil
	}
	var m map[string]any
	if err := json.Unmarshal([]byte(payload), &m); err != nil {
		return "", false, err
	}
	changed := false
	for _, field := range sensitiveSettingKeys {
		raw, ok := m[field].(string)
		if !ok || strings.TrimSpace(raw) == "" {
			continue
		}
		if IsEncryptedSecret(raw) {
			if _, err := DecryptSecret(key, raw); err != nil {
				return "", false, err
			}
			continue
		}
		enc, err := EncryptSecret(key, raw)
		if err != nil {
			return "", false, err
		}
		m[field] = enc
		changed = true
	}
	if !changed {
		return payload, false, nil
	}
	out, err := json.Marshal(m)
	if err != nil {
		return "", false, err
	}
	return string(out), true, nil
}

func decryptSettingPayload(key []byte, payload string) (string, error) {
	if strings.TrimSpace(payload) == "" {
		return payload, nil
	}
	var m map[string]any
	if err := json.Unmarshal([]byte(payload), &m); err != nil {
		return "", err
	}
	changed := false
	for _, field := range sensitiveSettingKeys {
		raw, ok := m[field].(string)
		if !ok || !IsEncryptedSecret(raw) {
			continue
		}
		plain, err := DecryptSecret(key, raw)
		if err != nil {
			return "", err
		}
		m[field] = plain
		changed = true
	}
	if !changed {
		return payload, nil
	}
	out, err := json.Marshal(m)
	if err != nil {
		return "", err
	}
	return string(out), nil
}

// MigrateCredentials 执行一次性幂等迁移：将旧明文凭据用 AES-256-GCM 加密写回并标记已迁移。
func (s *Store) MigrateCredentials(ctx context.Context) error {
	if !s.HasMasterKey() {
		return errors.New("未配置主密钥，无法迁移加密凭据")
	}
	return s.db.WithContext(ctx).Transaction(func(db *gorm.DB) error {
		var setting model.Setting
		err := db.Where("id = 1").First(&setting).Error
		if err != nil && !errors.Is(err, gorm.ErrRecordNotFound) {
			return err
		}
		if err == nil && strings.TrimSpace(setting.Payload) != "" {
			encPayload, changed, e := encryptSettingPayload(s.masterKey, setting.Payload)
			if e != nil {
				return e
			}
			if changed {
				if e := db.Model(&model.Setting{}).Where("id = 1").Update("payload", encPayload).Error; e != nil {
					return e
				}
			}
		}

		var cred model.Credential
		err = db.Where("name = ?", "bepusdt_api_token").First(&cred).Error
		if err != nil && !errors.Is(err, gorm.ErrRecordNotFound) {
			return err
		}
		if err == nil && strings.TrimSpace(cred.Value) != "" {
			if IsEncryptedSecret(cred.Value) {
				if _, e := DecryptSecret(s.masterKey, cred.Value); e != nil {
					return e
				}
			} else {
				enc, e := EncryptSecret(s.masterKey, cred.Value)
				if e != nil {
					return e
				}
				if e := db.Model(&model.Credential{}).Where("name = ?", "bepusdt_api_token").Update("value", enc).Error; e != nil {
					return e
				}
			}
		}

		var keys []model.SigningKey
		if e := db.Find(&keys).Error; e != nil {
			return e
		}
		for _, k := range keys {
			if strings.TrimSpace(k.Token) == "" {
				continue
			}
			if IsEncryptedSecret(k.Token) {
				if _, e := DecryptSecret(s.masterKey, k.Token); e != nil {
					return e
				}
				continue
			}
			enc, e := EncryptSecret(s.masterKey, k.Token)
			if e != nil {
				return e
			}
			if e := db.Model(&model.SigningKey{}).Where("id = ?", k.ID).Update("token", enc).Error; e != nil {
				return e
			}
		}

		marker := model.Credential{
			Name:      CredentialMigrationMarker,
			Value:     "1",
			UpdatedBy: "migration",
			UpdatedAt: time.Now(),
		}
		return db.Clauses(clause.OnConflict{
			Columns:   []clause.Column{{Name: "name"}},
			DoUpdates: clause.AssignmentColumns([]string{"value", "updated_by", "updated_at"}),
		}).Create(&marker).Error
	})
}

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
	return decryptSettingPayload(s.masterKey, row.Payload)
}

// SaveSettingPayload 保存后台配置覆盖项。
func (s *Store) SaveSettingPayload(ctx context.Context, payload, operator string) error {
	stored := payload
	if s.HasMasterKey() {
		var err error
		stored, _, err = encryptSettingPayload(s.masterKey, payload)
		if err != nil {
			return err
		}
	}
	row := model.Setting{ID: 1, Payload: stored, UpdatedBy: operator, UpdatedAt: time.Now()}
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
	if IsEncryptedSecret(row.Value) {
		plain, err := DecryptSecret(s.masterKey, row.Value)
		if err != nil {
			return nil, err
		}
		row.Value = plain
	}
	return &row, nil
}

// SaveCredential 保存或轮换系统凭据。
func (s *Store) SaveCredential(ctx context.Context, row model.Credential) error {
	if s.HasMasterKey() && row.Name != CredentialMigrationMarker && row.Value != "" && !IsEncryptedSecret(row.Value) {
		enc, err := EncryptSecret(s.masterKey, row.Value)
		if err != nil {
			return err
		}
		row.Value = enc
	}
	row.UpdatedAt = time.Now()
	return s.db.WithContext(ctx).Clauses(clause.OnConflict{
		Columns:   []clause.Column{{Name: "name"}},
		DoUpdates: clause.AssignmentColumns([]string{"value", "updated_by", "updated_at"}),
	}).Create(&row).Error
}

func (s *Store) SaveSettingAndArchiveTON(ctx context.Context, payload, operator, receiver string) error {
	return s.db.WithContext(ctx).Transaction(func(db *gorm.DB) error {
		if err := (&Store{db: db, masterKey: s.masterKey}).SaveSettingPayload(ctx, payload, operator); err != nil {
			return err
		}
		return db.Model(&model.TONScanCheckpoint{}).Where("receiver <> ? AND active = ?", receiver, true).Update("active", false).Error
	})
}
