package store

import (
	"bytes"
	"context"
	"crypto/aes"
	"crypto/cipher"
	"encoding/base64"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"

	"litepay/internal/model"
)

func TestMasterKeyEncodingAndFilePermissions(t *testing.T) {
	key := bytes.Repeat([]byte{0x8e}, 32)
	for _, raw := range []string{strings.Repeat("ab", 32), base64.StdEncoding.EncodeToString(key), base64.RawURLEncoding.EncodeToString(key)} {
		if _, err := ParseMasterKey(raw); err != nil {
			t.Fatal(err)
		}
	}
	for _, raw := range []string{strings.Repeat("a", 32), "", "password", strings.Repeat("x", 64)} {
		if _, err := ParseMasterKey(raw); err == nil {
			t.Fatal("accepted invalid key encoding")
		}
	}
	if runtime.GOOS == "windows" {
		return
	}
	file := filepath.Join(t.TempDir(), "master.key")
	if err := os.WriteFile(file, []byte(strings.Repeat("ab", 32)), 0600); err != nil {
		t.Fatal(err)
	}
	for _, mode := range []os.FileMode{0644, 0640, 0604, 0610} {
		if err := os.Chmod(file, mode); err != nil {
			t.Fatal(err)
		}
		if _, err := LoadMasterKeyFromFile(file, true); err == nil {
			t.Fatalf("production accepted mode %o", mode)
		}
		if _, err := LoadMasterKeyFromFile(file, false); err != nil {
			t.Fatal(err)
		}
	}
	for _, mode := range []os.FileMode{0400, 0600} {
		if err := os.Chmod(file, mode); err != nil {
			t.Fatal(err)
		}
		if _, err := LoadMasterKeyFromFile(file, true); err != nil {
			t.Fatal(err)
		}
	}
}

func TestCredentialV2AADAndLegacyUpgrade(t *testing.T) {
	key := bytes.Repeat([]byte{0x83}, 32)
	encrypted, err := EncryptSecret(key, "secret", "binance_api_key")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := DecryptSecret(key, encrypted, "binance_api_secret"); err == nil {
		t.Fatal("wrong AAD accepted")
	}
	block, _ := aes.NewCipher(key)
	gcm, _ := cipher.NewGCM(block)
	nonce := make([]byte, gcm.NonceSize())
	legacy := legacyEncryptedPrefix + base64.RawStdEncoding.EncodeToString(gcm.Seal(nonce, nonce, []byte("legacy-secret"), nil))
	if plain, err := DecryptSecret(key, legacy, "binance_api_key"); err != nil || plain != "legacy-secret" {
		t.Fatal(plain, err)
	}
	st, err := Open(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	if err := st.DB().Create(&model.Setting{ID: 1, Payload: `{"binance_api_key":"` + legacy + `"}`}).Error; err != nil {
		t.Fatal(err)
	}
	if err := st.DB().Create(&model.Credential{Name: "bepusdt_api_token", Value: legacy}).Error; err != nil {
		t.Fatal(err)
	}
	if err := st.DB().Create(&model.SigningKey{Token: legacy}).Error; err != nil {
		t.Fatal(err)
	}
	if err := st.SetMasterKey(key); err != nil {
		t.Fatal(err)
	}
	if err := st.MigrateCredentials(context.Background()); err != nil {
		t.Fatal(err)
	}
	var setting model.Setting
	if err := st.DB().First(&setting).Error; err != nil {
		t.Fatal(err)
	}
	if strings.Contains(setting.Payload, legacyEncryptedPrefix) || !strings.Contains(setting.Payload, encryptedPrefix) {
		t.Fatal(setting.Payload)
	}
	var credential model.Credential
	st.DB().First(&credential, "name = ?", "bepusdt_api_token")
	if plain, err := DecryptSecret(key, credential.Value, "bepusdt_api_token"); err != nil || plain != "legacy-secret" {
		t.Fatal(plain, err)
	}
	var signing model.SigningKey
	st.DB().First(&signing)
	if plain, err := DecryptSecret(key, signing.Token, "signing_keys.token"); err != nil || plain != "legacy-secret" {
		t.Fatal(plain, err)
	}
}

func TestCredentialMigrationScrubsDiskAndRotationIsAtomic(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "secrets.db")
	st, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	keyA, keyB := bytes.Repeat([]byte{0x31}, 32), bytes.Repeat([]byte{0x42}, 32)
	secret, token := "old-plaintext-exchange-secret-unique", "old-plaintext-signing-token-unique"
	if err := st.SaveSettingPayload(ctx, `{"binance_api_secret":"`+secret+`"}`, "test"); err != nil {
		t.Fatal(err)
	}
	if _, err := st.InstallSigningKey(ctx, token, "test", false); err != nil {
		t.Fatal(err)
	}
	if err := st.DB().Exec("PRAGMA wal_checkpoint(TRUNCATE)").Error; err != nil {
		t.Fatal(err)
	}
	before, err := os.ReadFile(path)
	if err != nil || !bytes.Contains(before, []byte(secret)) {
		t.Fatal("fixture has no on-disk plaintext", err)
	}
	if err := st.SetMasterKey(keyA); err != nil {
		t.Fatal(err)
	}
	if err := st.MigrateCredentials(ctx); err != nil {
		t.Fatal(err)
	}
	for _, file := range []string{path, path + "-wal"} {
		data, err := os.ReadFile(file)
		if err != nil && !os.IsNotExist(err) {
			t.Fatal(err)
		}
		for _, value := range []string{secret, token} {
			if bytes.Contains(data, []byte(value)) {
				t.Fatalf("plaintext remains in %s", file)
			}
		}
	}
	var secure int
	if err := st.DB().Raw("PRAGMA secure_delete").Scan(&secure).Error; err != nil || secure != 1 {
		t.Fatal(secure, err)
	}
	var original model.Setting
	if err := st.DB().First(&original).Error; err != nil {
		t.Fatal(err)
	}
	// Settings are rewritten first; a bad signing key must roll them back too.
	var signing model.SigningKey
	st.DB().First(&signing)
	validToken := signing.Token
	if err := st.DB().Model(&signing).Update("token", "enc:v2:broken").Error; err != nil {
		t.Fatal(err)
	}
	if err := st.RotateMasterKey(ctx, keyA, keyB); err == nil {
		t.Fatal("rotation accepted corrupt signing key")
	}
	var unchanged model.Setting
	st.DB().First(&unchanged)
	if unchanged.Payload != original.Payload {
		t.Fatal("partial rotation committed")
	}
	if err := st.DB().Model(&signing).Update("token", validToken).Error; err != nil {
		t.Fatal(err)
	}
	if err := st.RotateMasterKey(ctx, keyA, keyB); err != nil {
		t.Fatal(err)
	}
	payload, err := st.LoadSettingPayload(ctx)
	if err != nil || !strings.Contains(payload, secret) {
		t.Fatal(payload, err)
	}
	cred, err := st.LoadCredential(ctx, "bepusdt_api_token")
	if err != nil || cred.Value != token {
		t.Fatal(cred, err)
	}
	st.DB().First(&signing)
	if _, err := DecryptSecret(keyA, signing.Token, "signing_keys.token"); err == nil {
		t.Fatal("old key still decrypts signing key")
	}
	if plain, err := st.CallbackKey(ctx, model.Callback{SigningKeyID: signing.ID}); err != nil || plain != token {
		t.Fatal(plain, err)
	}
	st.DB().First(&unchanged)
	if _, err := decryptSettingPayload(keyA, unchanged.Payload); err == nil {
		t.Fatal("old key still decrypts settings")
	}
	if err := st.MigrateCredentials(ctx); err != nil {
		t.Fatal(err)
	}
}

func TestBackupBeforeMasterKeyRotationRequiresItsOriginalKey(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	live, err := Open(filepath.Join(dir, "live.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer live.Close()
	oldKey, newKey := bytes.Repeat([]byte{0x31}, 32), bytes.Repeat([]byte{0x52}, 32)
	if err := live.SetMasterKey(oldKey); err != nil {
		t.Fatal(err)
	}
	const secret = "backup-original-exchange-secret"
	const token = "backup-original-signing-token"
	if err := live.SaveSettingPayload(ctx, `{"binance_api_secret":"`+secret+`"}`, "test"); err != nil {
		t.Fatal(err)
	}
	version, err := live.InstallSigningKey(ctx, token, "test", false)
	if err != nil {
		t.Fatal(err)
	}
	if err := live.MigrateCredentials(ctx); err != nil {
		t.Fatal(err)
	}
	// SQLite creates a consistent snapshot; the external master key is not part of it.
	backupPath := filepath.Join(dir, "before-rotation.db")
	if err := live.DB().Exec("VACUUM INTO ?", backupPath).Error; err != nil {
		t.Fatal(err)
	}
	if err := live.RotateMasterKey(ctx, oldKey, newKey); err != nil {
		t.Fatal(err)
	}
	if payload, err := live.LoadSettingPayload(ctx); err != nil || !strings.Contains(payload, secret) {
		t.Fatal("rotated live database cannot decrypt", err)
	}
	backup, err := Open(backupPath)
	if err != nil {
		t.Fatal(err)
	}
	defer backup.Close()
	if err := backup.SetMasterKey(newKey); err != nil {
		t.Fatal(err)
	}
	if _, err := backup.LoadSettingPayload(ctx); err == nil {
		t.Fatal("new key unexpectedly decrypts old backup")
	}
	if _, err := backup.CallbackKey(ctx, model.Callback{SigningKeyID: version}); err == nil {
		t.Fatal("new key unexpectedly decrypts backup signing keys")
	}
	if err := backup.SetMasterKey(oldKey); err != nil {
		t.Fatal(err)
	}
	if payload, err := backup.LoadSettingPayload(ctx); err != nil || !strings.Contains(payload, secret) {
		t.Fatal("backup did not recover with original key", err)
	}
	if got, err := backup.CallbackKey(ctx, model.Callback{SigningKeyID: version}); err != nil || got != token {
		t.Fatal("backup callback key did not recover", err)
	}
}
