package settings

import (
	"context"
	"errors"
	"reflect"
	"testing"

	"litepay/internal/config"
	"litepay/internal/gram"
	"litepay/internal/store"
)

func verificationManager(t *testing.T, cfg *config.Config) *Manager {
	t.Helper()
	st, err := store.Open(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { st.Close() })
	m := New(cfg, st)
	if err = m.Load(context.Background()); err != nil {
		t.Fatal(err)
	}
	return m
}

func TestVerificationReportsOnlyAccountsActuallyChecked(t *testing.T) {
	cfg := config.Default()
	cfg.Binance.APIKey = "key"
	cfg.Binance.APISecret = "secret"
	cfg.Checkout.Binance.UID = "receiver"
	cfg.Checkout.OKX.UID = "okx-receiver"
	m := verificationManager(t, cfg)
	var called []string
	m.verifyAccount = func(_ context.Context, _ *config.Config, provider string) error {
		called = append(called, provider)
		return nil
	}
	checks, err := m.UpdateWithVerification(context.Background(), Patch{VerifyAccounts: true}, "test")
	if err != nil || !reflect.DeepEqual(called, []string{config.ProviderBinance}) {
		t.Fatal(called, err)
	}
	if len(checks) != 3 || !checks[0].Verified || checks[1].Verified || checks[2].Verified {
		t.Fatal(checks)
	}
	if !reflect.DeepEqual(checks[1].Missing, []string{"API Key", "API Secret", "Passphrase"}) {
		t.Fatal(checks[1])
	}
	if m.Get().OKXEnabled() {
		t.Fatal("incomplete OKX configuration enabled")
	}
}

func TestFailedVerificationDoesNotPublishOrSaveConfig(t *testing.T) {
	cfg := config.Default()
	cfg.Binance.APIKey = "key"
	cfg.Binance.APISecret = "secret"
	cfg.Checkout.Binance.UID = "receiver"
	cfg.OKX.APIKey = "key"
	cfg.OKX.APISecret = "secret"
	cfg.OKX.Passphrase = "phrase"
	cfg.Checkout.OKX.UID = "okx-receiver"
	m := verificationManager(t, cfg)
	before, _ := m.st.LoadSettingPayload(context.Background())
	m.verifyAccount = func(_ context.Context, _ *config.Config, provider string) error {
		if provider == config.ProviderOKX {
			return errors.New("OKX account rejected")
		}
		return nil
	}
	note := "must not be saved"
	checks, err := m.UpdateWithVerification(context.Background(), Patch{VerifyAccounts: true, CheckoutNote: &note}, "test")
	if err == nil || len(checks) != 0 {
		t.Fatal("failed verification returned success", checks, err)
	}
	after, _ := m.st.LoadSettingPayload(context.Background())
	if m.Get().Checkout.Note != "" || after != before {
		t.Fatal("failed configuration persisted")
	}
}

func TestCompleteVerificationChecksEveryEnabledProvider(t *testing.T) {
	cfg := config.Default()
	cfg.Binance.APIKey = "key"
	cfg.Binance.APISecret = "secret"
	cfg.Checkout.Binance.UID = "receiver"
	cfg.OKX.APIKey = "key"
	cfg.OKX.APISecret = "secret"
	cfg.OKX.Passphrase = "phrase"
	cfg.Checkout.OKX.UID = "okx-receiver"
	cfg.TON.Address = gram.DefaultAddress
	cfg.Checkout.Currencies = append(cfg.Checkout.Currencies, "GRAM")
	m := verificationManager(t, cfg)
	var called []string
	m.verifyAccount = func(_ context.Context, _ *config.Config, provider string) error {
		called = append(called, provider)
		return nil
	}
	checks, err := m.UpdateWithVerification(context.Background(), Patch{VerifyAccounts: true}, "test")
	if err != nil || !reflect.DeepEqual(called, []string{"binance", "okx", "ton"}) {
		t.Fatal(called, err)
	}
	for _, check := range checks {
		if !check.Verified || len(check.Missing) != 0 {
			t.Fatal(check)
		}
	}
}

func TestOrdinarySaveStillVerifiesChangedCredentials(t *testing.T) {
	cfg := config.Default()
	cfg.OKX.APIKey = "old"
	cfg.OKX.APISecret = "secret"
	cfg.OKX.Passphrase = "phrase"
	cfg.Checkout.OKX.UID = "receiver"
	m := verificationManager(t, cfg)
	called := false
	m.verifyAccount = func(_ context.Context, c *config.Config, provider string) error {
		called = true
		if provider != "okx" || c.OKX.APIKey != "new" {
			t.Fatal(provider)
		}
		return errors.New("invalid credentials")
	}
	key := "new"
	if err := m.Update(context.Background(), Patch{OKXAPIKey: &key}, "test"); err == nil || !called || m.Get().OKX.APIKey != "old" {
		t.Fatal(err, called)
	}
}

func TestProviderCurrenciesLegacyPersistenceAndScopedVerification(t *testing.T) {
	ctx := context.Background()
	cfg := config.Default()
	cfg.Checkout.Currencies = []string{"USDT", "USDC", "GRAM"}
	cfg.Binance.APIKey = "key"
	cfg.Binance.APISecret = "secret"
	cfg.Checkout.Binance.UID = "binance"
	cfg.OKX.APIKey = "key"
	cfg.OKX.APISecret = "secret"
	cfg.OKX.Passphrase = "phrase"
	cfg.Checkout.OKX.UID = "okx"
	m := verificationManager(t, cfg)
	var calls []string
	m.verifyAccount = func(_ context.Context, _ *config.Config, provider string) error {
		calls = append(calls, provider)
		return nil
	}
	usdc := []string{"USDC"}
	checks, err := m.UpdateWithVerification(ctx, Patch{OKXCurrencies: &usdc, VerifyAccounts: true, VerifyProvider: "okx"}, "test")
	if err != nil || !reflect.DeepEqual(calls, []string{"okx"}) || len(checks) != 1 || !checks[0].Verified {
		t.Fatal(calls, checks, err)
	}
	loaded := New(config.Default(), m.st)
	if err = loaded.Load(ctx); err != nil {
		t.Fatal(err)
	}
	c := loaded.Get()
	if !reflect.DeepEqual(c.ProviderCurrencies("binance"), []string{"USDT", "USDC"}) || !reflect.DeepEqual(c.ProviderCurrencies("okx"), usdc) || !reflect.DeepEqual(c.ProviderCurrencies("ton"), []string{"GRAM"}) {
		t.Fatal(c.Checkout)
	}
	empty := []string{}
	if err = loaded.Update(ctx, Patch{BinanceCurrencies: &empty, OKXCurrencies: &empty, TONCurrencies: &empty}, "test"); err != nil {
		t.Fatal(err)
	}
	reloaded := New(config.Default(), m.st)
	if err = reloaded.Load(ctx); err != nil {
		t.Fatal(err)
	}
	if reloaded.Get().AnyProviderEnabled() || len(reloaded.Get().Checkout.Currencies) != 0 {
		t.Fatal("cleared providers resurrected on reload")
	}
}
