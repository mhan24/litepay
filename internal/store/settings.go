package store

import (
	"context"
	"crypto/aes"
	"crypto/cipher"
	"crypto/hmac"
	"crypto/rand"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"runtime"
	"strings"
	"time"

	"gorm.io/gorm"
	"gorm.io/gorm/clause"

	"litepay/internal/model"
)

const (
	encryptedPrefix           = "enc:v2:"
	legacyEncryptedPrefix     = "enc:v1:"
	credentialV2Marker        = "credentials_encrypted_v2"
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
	return strings.HasPrefix(v, encryptedPrefix) || strings.HasPrefix(v, legacyEncryptedPrefix)
}

// ParseMasterKey 解析 32 字节主密钥（仅支持 64 位 hex 或 Base64 编码）。
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
	for _, enc := range []*base64.Encoding{base64.StdEncoding, base64.RawStdEncoding, base64.URLEncoding, base64.RawURLEncoding} {
		if decoded, err := enc.DecodeString(trimmed); err == nil && len(decoded) == 32 {
			return decoded, nil
		}
	}
	return nil, errors.New("主密钥格式无效，需为 32 字节随机密钥的 64 位 hex 或 Base64 编码")
}

// LoadMasterKeyFromFile 从只读文件加载主密钥。
func LoadMasterKeyFromFile(path string, production bool) ([]byte, error) {
	f, err := os.Open(strings.TrimSpace(path)) // #nosec G703 -- This is an administrator-selected startup/CLI key file, never an HTTP-supplied path; permissions are checked on the opened file.
	if err != nil {
		return nil, fmt.Errorf("读取主密钥文件失败: %w", err)
	}
	defer f.Close()
	info, err := f.Stat()
	if err != nil {
		return nil, err
	}
	if runtime.GOOS != "windows" && info.Mode().Perm()&0o077 != 0 {
		if production {
			return nil, fmt.Errorf("主密钥文件 %s 权限过宽，请设置为 0400 或 0600", path)
		}
		slog.Warn("主密钥文件权限过宽，请设置为 0400 或 0600", "path", path)
	}
	data, err := io.ReadAll(io.LimitReader(f, 4097))
	if err != nil {
		return nil, err
	}
	return ParseMasterKey(string(data))
}

// ResolveMasterKey resolves a key file or an encoded environment value.
func ResolveMasterKey(configFile string, production bool) ([]byte, error) {
	if p := strings.TrimSpace(configFile); p != "" {
		return LoadMasterKeyFromFile(p, production)
	}
	if p := strings.TrimSpace(os.Getenv("LITEPAY_MASTER_KEY_FILE")); p != "" {
		return LoadMasterKeyFromFile(p, production)
	}
	if raw := strings.TrimSpace(os.Getenv("LITEPAY_MASTER_KEY")); raw != "" {
		return ParseMasterKey(raw)
	}
	return nil, nil
}

// EncryptSecret 使用 AES-256-GCM 加密敏感字符串。
func EncryptSecret(key []byte, plaintext, field string) (string, error) {
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
	sealed := gcm.Seal(nonce, nonce, []byte(plaintext), []byte(field))
	return encryptedPrefix + base64.RawStdEncoding.EncodeToString(sealed), nil
}

// DecryptSecret 解密 AES-256-GCM 密文。
func DecryptSecret(key []byte, value, field string) (string, error) {
	if !IsEncryptedSecret(value) {
		return "", errors.New("密文格式无效")
	}
	if len(key) != 32 {
		return "", errors.New("未配置有效主密钥，无法解密敏感凭据")
	}
	prefix, aad := encryptedPrefix, []byte(field)
	if strings.HasPrefix(value, legacyEncryptedPrefix) {
		prefix, aad = legacyEncryptedPrefix, nil
	}
	raw, err := base64.RawStdEncoding.DecodeString(strings.TrimPrefix(value, prefix))
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
	plain, err := gcm.Open(nil, nonce, ciphertext, aad)
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

func encryptSettingPayload(key []byte, payload string, allowEncrypted bool) (string, bool, error) {
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
		var (
			enc string
			err error
		)
		if allowEncrypted {
			enc, err = upgradeSecret(key, key, raw, field, false)
		} else {
			enc, err = EncryptSecret(key, raw, field)
		}
		if err != nil {
			return "", false, err
		}
		if enc == raw {
			continue
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
		plain, err := DecryptSecret(key, raw, field)
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

// upgradeSecret validates v2 values and upgrades legacy/plaintext values with field-bound AAD.
func upgradeSecret(oldKey, newKey []byte, value, field string, rotate bool) (string, error) {
	if value == "" {
		return value, nil
	}
	plain := value
	if IsEncryptedSecret(value) {
		var err error
		plain, err = DecryptSecret(oldKey, value, field)
		if err != nil {
			return "", err
		}
		if !rotate && strings.HasPrefix(value, encryptedPrefix) {
			return value, nil
		}
	}
	return EncryptSecret(newKey, plain, field)
}

func plainCredential(name string) bool {
	return name == CredentialMigrationMarker || name == credentialV2Marker || name == "admin_session_epoch"
}

func rewriteSecrets(db *gorm.DB, oldKey, newKey []byte, rotate bool) (bool, error) {
	changed := false
	var setting model.Setting
	err := db.Where("id = 1").First(&setting).Error
	if err != nil && !errors.Is(err, gorm.ErrRecordNotFound) {
		return false, err
	}
	if err == nil && strings.TrimSpace(setting.Payload) != "" {
		payload := setting.Payload
		if rotate {
			payload, err = decryptSettingPayload(oldKey, payload)
			if err != nil {
				return false, err
			}
		}
		encrypted, updated, err := encryptSettingPayload(newKey, payload, !rotate)
		if err != nil {
			return false, err
		}
		if updated {
			if err := db.Model(&setting).Update("payload", encrypted).Error; err != nil {
				return false, err
			}
			changed = true
		}
	}
	var credentials []model.Credential
	if err := db.Find(&credentials).Error; err != nil {
		return false, err
	}
	for _, row := range credentials {
		if plainCredential(row.Name) {
			continue
		}
		encrypted, err := upgradeSecret(oldKey, newKey, row.Value, row.Name, rotate)
		if err != nil {
			return false, err
		}
		if encrypted != row.Value {
			if err := db.Model(&row).Update("value", encrypted).Error; err != nil {
				return false, err
			}
			changed = true
		}
	}
	var keys []model.SigningKey
	if err := db.Find(&keys).Error; err != nil {
		return false, err
	}
	for _, row := range keys {
		encrypted, err := upgradeSecret(oldKey, newKey, row.Token, "signing_keys.token", rotate)
		if err != nil {
			return false, err
		}
		if encrypted != row.Token {
			if err := db.Model(&row).Update("token", encrypted).Error; err != nil {
				return false, err
			}
			changed = true
		}
	}
	return changed, nil
}

// scrubCredentials must run outside a transaction. Checkpoint again after VACUUM,
// which itself writes pages to WAL. A busy checkpoint must not mark cleanup complete.
func (s *Store) scrubCredentials(ctx context.Context) error {
	checkpoint := func() error {
		var result struct {
			Busy         int
			Log          int
			Checkpointed int
		}
		if err := s.db.WithContext(ctx).Raw("PRAGMA wal_checkpoint(TRUNCATE)").Scan(&result).Error; err != nil {
			return err
		}
		if result.Busy != 0 {
			return errors.New("凭据清理被数据库读者阻塞，请停止备份后重试")
		}
		return nil
	}
	if err := checkpoint(); err != nil {
		return err
	}
	if err := s.db.WithContext(ctx).Exec("VACUUM").Error; err != nil {
		return err
	}
	if err := checkpoint(); err != nil {
		return err
	}
	if err := s.SaveCredential(ctx, model.Credential{Name: CredentialMigrationMarker, Value: "1", UpdatedBy: "migration"}); err != nil {
		return err
	}
	if err := s.SaveCredential(ctx, model.Credential{Name: credentialV2Marker, Value: "1", UpdatedBy: "migration"}); err != nil {
		return err
	}
	slog.Warn("凭据加密迁移与数据库清理完成；请轮换交易所 API Key，并清理旧备份")
	return nil
}

func (s *Store) MigrateCredentials(ctx context.Context) error {
	if !s.HasMasterKey() {
		return errors.New("未配置主密钥，无法迁移加密凭据")
	}
	if err := s.db.WithContext(ctx).Exec("PRAGMA secure_delete=ON").Error; err != nil {
		return err
	}
	var cleaned int64
	if err := s.db.WithContext(ctx).Model(&model.Credential{}).Where("name = ? AND value = ?", credentialV2Marker, "1").Count(&cleaned).Error; err != nil {
		return err
	}
	changed := false
	if err := s.db.WithContext(ctx).Transaction(func(db *gorm.DB) error {
		var err error
		changed, err = rewriteSecrets(db, s.masterKey, s.masterKey, false)
		if err != nil {
			return err
		}
		if changed {
			return db.Where("name = ?", credentialV2Marker).Delete(&model.Credential{}).Error
		}
		return nil
	}); err != nil {
		return err
	}
	if changed || cleaned == 0 {
		return s.scrubCredentials(ctx)
	}
	return nil
}

// RotateMasterKey is an offline operation: all settings, credentials and historical
// signing keys commit together; a failed decryption leaves every row unchanged.
func (s *Store) RotateMasterKey(ctx context.Context, oldKey, newKey []byte) error {
	if len(oldKey) != 32 || len(newKey) != 32 {
		return errors.New("轮换需要有效的 32 字节新旧主密钥")
	}
	if hmac.Equal(oldKey, newKey) {
		return errors.New("新旧主密钥不能相同")
	}
	if err := s.db.WithContext(ctx).Exec("PRAGMA secure_delete=ON").Error; err != nil {
		return err
	}
	if err := s.db.WithContext(ctx).Transaction(func(db *gorm.DB) error {
		if _, err := rewriteSecrets(db, oldKey, newKey, true); err != nil {
			return err
		}
		return db.Where("name = ?", credentialV2Marker).Delete(&model.Credential{}).Error
	}); err != nil {
		return err
	}
	if err := s.SetMasterKey(newKey); err != nil {
		return err
	}
	if err := s.scrubCredentials(ctx); err != nil {
		return fmt.Errorf("密钥轮换已提交，请使用新密钥重启以重试数据库清理: %w", err)
	}
	return nil
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
		stored, _, err = encryptSettingPayload(s.masterKey, payload, false)
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
		plain, err := DecryptSecret(s.masterKey, row.Value, row.Name)
		if err != nil {
			return nil, err
		}
		row.Value = plain
	}
	return &row, nil
}

// SaveCredential 保存或轮换系统凭据。
func (s *Store) SaveCredential(ctx context.Context, row model.Credential) error {
	if strings.TrimSpace(row.Name) == "" {
		return errors.New("凭据名称不能为空")
	}
	if s.HasMasterKey() && !plainCredential(row.Name) && row.Value != "" {
		enc, err := EncryptSecret(s.masterKey, row.Value, row.Name)
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
