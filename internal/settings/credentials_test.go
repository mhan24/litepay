package settings

import (
	"bytes"
	"context"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"litepay/internal/config"
	"litepay/internal/model"
	"litepay/internal/store"
)

func TestMasterKeyEncryptDecryptRoundTripAndWrongKeyFails(t *testing.T) {
	keyA, err := store.ParseMasterKey("0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef")
	if err != nil {
		t.Fatal(err)
	}
	keyB, err := store.ParseMasterKey("fedcba9876543210fedcba9876543210fedcba9876543210fedcba9876543210")
	if err != nil {
		t.Fatal(err)
	}

	cipherText, err := store.EncryptSecret(keyA, "super-secret-exchange-token", "test-token")
	if err != nil {
		t.Fatal(err)
	}
	if !store.IsEncryptedSecret(cipherText) || strings.Contains(cipherText, "super-secret-exchange-token") {
		t.Fatalf("expected encrypted secret without plaintext, got: %s", cipherText)
	}
	plainText, err := store.DecryptSecret(keyA, cipherText, "test-token")
	if err != nil || plainText != "super-secret-exchange-token" {
		t.Fatalf("roundtrip failed: plain=%q err=%v", plainText, err)
	}
	if _, err := store.DecryptSecret(keyB, cipherText, "test-token"); err == nil {
		t.Fatal("expected decryption with wrong master key to fail")
	}
}

func TestMasterKeyStartupPolicyAndIdempotentMigration(t *testing.T) {
	ctx := context.Background()
	st, err := store.Open(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()

	// 1. Production without master key refuses to start.
	t.Setenv("LITEPAY_MASTER_KEY", "")
	t.Setenv("LITEPAY_MASTER_KEY_FILE", "")
	prodCfg := config.Default()
	prodCfg.Server.Production = true
	prodMgr := New(prodCfg, st)
	if err := prodMgr.Load(ctx); err == nil || !strings.Contains(err.Error(), "主密钥") {
		t.Fatalf("expected production without master key to fail, got: %v", err)
	}

	// 2. Development mode without master key logs a warning and starts cleanly; writes legacy plaintext.
	var logBuf bytes.Buffer
	prevLogger := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(&logBuf, nil)))
	t.Cleanup(func() { slog.SetDefault(prevLogger) })

	devCfg := config.Default()
	devCfg.Server.Production = false
	devMgr := New(devCfg, st)
	devMgr.verifyAccount = func(context.Context, *config.Config, string) error { return nil }
	if err := devMgr.Load(ctx); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(logBuf.String(), "未配置主密钥") {
		t.Fatalf("expected warning in dev mode without master key, got: %s", logBuf.String())
	}

	bKey := "plain-binance-key"
	bSecret := "plain-binance-secret"
	bUID := "123456"
	okxKey := "plain-okx-key"
	okxSecret := "plain-okx-secret"
	okxPass := "plain-okx-pass"
	okxUID := "654321"
	bybitKey := "plain-bybit-key"
	bybitSecret := "plain-bybit-secret"
	if err := devMgr.Update(ctx, Patch{
		BinanceAPIKey:    &bKey,
		BinanceAPISecret: &bSecret,
		BinanceUID:       &bUID,
		OKXAPIKey:        &okxKey,
		OKXAPISecret:     &okxSecret,
		OKXPassphrase:    &okxPass,
		OKXUID:           &okxUID,
		BybitAPIKey:      &bybitKey,
		BybitAPISecret:   &bybitSecret,
	}, "admin"); err != nil {
		t.Fatal(err)
	}
	origToken := devMgr.Get().BEpusdt.APIToken

	// Confirm legacy DB currently has plaintext before migration.
	var rawSetting model.Setting
	if err := st.DB().First(&rawSetting, "id = 1").Error; err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(rawSetting.Payload, "plain-binance-secret") {
		t.Fatalf("expected legacy plaintext in DB before migration, got: %s", rawSetting.Payload)
	}

	// 3. Configure master key via LITEPAY_MASTER_KEY_FILE and run Load (triggers one-time migration).
	keyFile := filepath.Join(t.TempDir(), "master.key")
	const hexKey = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
	if err := os.WriteFile(keyFile, []byte(hexKey+"\n"), 0400); err != nil {
		t.Fatal(err)
	}
	t.Setenv("LITEPAY_MASTER_KEY_FILE", keyFile)

	migratedMgr := New(prodCfg, st)
	if err := migratedMgr.Load(ctx); err != nil {
		t.Fatalf("expected Load with master key file to succeed and migrate, got: %v", err)
	}

	// Verify DB rows are now encrypted at rest and marker is written.
	if err := st.DB().First(&rawSetting, "id = 1").Error; err != nil {
		t.Fatal(err)
	}
	for _, secret := range []string{bKey, bSecret, okxKey, okxSecret, okxPass, bybitKey, bybitSecret} {
		if strings.Contains(rawSetting.Payload, secret) {
			t.Fatalf("plaintext secret %q still present in settings table after migration", secret)
		}
	}
	var rawCred model.Credential
	if err := st.DB().First(&rawCred, "name = ?", "bepusdt_api_token").Error; err != nil {
		t.Fatal(err)
	}
	if !store.IsEncryptedSecret(rawCred.Value) || strings.Contains(rawCred.Value, origToken) {
		t.Fatalf("expected bepusdt_api_token encrypted at rest, got: %s", rawCred.Value)
	}
	var marker model.Credential
	if err := st.DB().First(&marker, "name = ?", store.CredentialMigrationMarker).Error; err != nil || marker.Value != "1" {
		t.Fatalf("expected migration marker = 1, got: %+v err=%v", marker, err)
	}
	var rawSignKey model.SigningKey
	if err := st.DB().First(&rawSignKey).Error; err != nil {
		t.Fatal(err)
	}
	if !store.IsEncryptedSecret(rawSignKey.Token) {
		t.Fatalf("expected signing_keys.token encrypted at rest, got: %s", rawSignKey.Token)
	}

	// Verify in-memory decrypted snapshot matches original plaintext credentials.
	got := migratedMgr.Get()
	if got.Binance.APIKey != bKey || got.Binance.APISecret != bSecret ||
		got.OKX.APIKey != okxKey || got.OKX.APISecret != okxSecret || got.OKX.Passphrase != okxPass ||
		got.Bybit.APIKey != bybitKey || got.Bybit.APISecret != bybitSecret || got.BEpusdt.APIToken != origToken {
		t.Fatalf("decrypted config mismatch: %+v", got)
	}

	// 4. Idempotency: running MigrateCredentials / Load again keeps ciphertext intact and succeeds.
	beforePayload := rawSetting.Payload
	beforeCred := rawCred.Value
	if err := st.MigrateCredentials(ctx); err != nil {
		t.Fatalf("second MigrateCredentials failed: %v", err)
	}
	st.DB().First(&rawSetting, "id = 1")
	st.DB().First(&rawCred, "name = ?", "bepusdt_api_token")
	if rawSetting.Payload != beforePayload || rawCred.Value != beforeCred {
		t.Fatal("idempotent migration re-encrypted already migrated ciphertexts")
	}

	// 5. Wrong master key fails to load migrated database.
	wrongKey, _ := store.ParseMasterKey("fedcba9876543210fedcba9876543210fedcba9876543210fedcba9876543210")
	if err := st.SetMasterKey(wrongKey); err != nil {
		t.Fatal(err)
	}
	wrongMgr := New(prodCfg, st)
	if err := wrongMgr.Load(ctx); err == nil {
		t.Fatal("expected Load with wrong master key to fail")
	}
}
