package settings

import (
	"context"
	"errors"
	"reflect"
	"testing"

	"litepay/internal/config"
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
	cfg.Server.Production = false
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
	if len(checks) != 4 || !checks[0].Verified || checks[1].Verified || checks[2].Verified || checks[3].Verified {
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
	cfg.Server.Production = false
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
	cfg.Server.Production = false
	cfg.Binance.APIKey = "key"
	cfg.Binance.APISecret = "secret"
	cfg.Checkout.Binance.UID = "receiver"
	cfg.OKX.APIKey = "key"
	cfg.OKX.APISecret = "secret"
	cfg.OKX.Passphrase = "phrase"
	cfg.Checkout.OKX.UID = "okx-receiver"
	cfg.Bybit.APIKey = "key"
	cfg.Bybit.APISecret = "secret"
	cfg.Checkout.Bybit.UID = "bybit-receiver"
	cfg.Bitget.APIKey = "key"
	cfg.Bitget.APISecret = "secret"
	cfg.Bitget.Passphrase = "phrase"
	cfg.Checkout.Bitget.UID = "bitget-receiver"
	m := verificationManager(t, cfg)
	var called []string
	m.verifyAccount = func(_ context.Context, _ *config.Config, provider string) error {
		called = append(called, provider)
		return nil
	}
	checks, err := m.UpdateWithVerification(context.Background(), Patch{VerifyAccounts: true}, "test")
	if err != nil || !reflect.DeepEqual(called, []string{"binance", "okx", "bybit", "bitget"}) {
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
	cfg.Server.Production = false
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
	cfg.Server.Production = false
	cfg.Checkout.Currencies = []string{"USDT", "USDC"}
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
	baseCfg := config.Default()
	baseCfg.Server.Production = false
	loaded := New(baseCfg, m.st)
	if err = loaded.Load(ctx); err != nil {
		t.Fatal(err)
	}
	c := loaded.Get()
	if !reflect.DeepEqual(c.ProviderCurrencies("binance"), []string{"USDT", "USDC"}) || !reflect.DeepEqual(c.ProviderCurrencies("okx"), usdc) {
		t.Fatal(c.Checkout)
	}
	empty := []string{}
	if err = loaded.Update(ctx, Patch{BinanceCurrencies: &empty, OKXCurrencies: &empty, BybitCurrencies: &empty, BitgetCurrencies: &empty}, "test"); err != nil {
		t.Fatal(err)
	}
	reloaded := New(baseCfg, m.st)
	if err = reloaded.Load(ctx); err != nil {
		t.Fatal(err)
	}
	if reloaded.Get().AnyProviderEnabled() || len(reloaded.Get().Checkout.Currencies) != 0 {
		t.Fatal("cleared providers resurrected on reload")
	}
}

func TestProductionTokenRotationRequiresMerchantCompatibility(t *testing.T) {
	t.Setenv("LITEPAY_MASTER_KEY", "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef")
	ctx := context.Background()
	cfg := config.Default()
	cfg.Server.Production = true
	m := verificationManager(t, cfg)
	before := m.Get().BEpusdt
	if _, err := m.RotateAPIToken(ctx, "operator"); err == nil {
		t.Fatal("production single-token merchant rotated")
	}
	after := m.Get().BEpusdt
	if before != after {
		t.Fatal("rejected rotation published key")
	}
	stored, err := m.st.LoadCredential(ctx, "bepusdt_api_token")
	if err != nil || stored.Value != before.APIToken {
		t.Fatal("rejected rotation changed stored token", err)
	}
}

func TestEnablingProviderViaCurrencyVerifiesAccountAndValidatesTONLookback(t *testing.T) {
	ctx := context.Background()
	cfg := config.Default()
	cfg.Server.Production = false
	cfg.Binance.APIKey = "key"
	cfg.Binance.APISecret = "secret"
	cfg.Checkout.Binance.UID = "receiver"
	cfg.Checkout.BinanceCurrencies = []string{}
	m := verificationManager(t, cfg)
	if m.Get().BinanceEnabled() {
		t.Fatal("expected binance disabled without currencies")
	}
	var calls []string
	m.verifyAccount = func(_ context.Context, _ *config.Config, provider string) error {
		calls = append(calls, provider)
		return errors.New("account check failed")
	}
	usdt := []string{"USDT"}
	if err := m.Update(ctx, Patch{BinanceCurrencies: &usdt}, "test"); err == nil || len(calls) != 1 || calls[0] != "binance" {
		t.Fatalf("expected account verification when enabling provider via currencies, calls=%v err=%v", calls, err)
	}

	invalidHash := "plaintext-admin-password"
	p := Patch{AdminPasswordHash: &invalidHash}
	target := clone(m.Get())
	p.Apply(target)
	if target.Admin.Password == invalidHash {
		t.Fatal("expected Patch.Apply to ignore non-bcrypt AdminPasswordHash")
	}

	nilStoreMgr := New(config.Default(), nil)
	if err := nilStoreMgr.Load(ctx); err != nil {
		t.Fatalf("expected Load with nil store to be a no-op without panic, got: %v", err)
	}
}

func TestSnapshotCopiesSlicesAndPreservesCurrenciesAndRetryDelays(t *testing.T) {
	cfg := config.Default()
	cfg.Server.Production = false
	cfg.BEpusdt.APIToken = "0123456789abcdef0123456789abcdef"
	cfg.Checkout.Currencies = []string{"USDT", "USDC"}
	cfg.Checkout.BinanceCurrencies = []string{"USDT"}
	cfg.Checkout.OKXCurrencies = []string{"USDC"}
	cfg.Checkout.ProviderOrder = []string{config.ProviderOKX, config.ProviderBinance, config.ProviderBybit, config.ProviderBitget}
	cfg.Notify.RetryDelays = []int{5, 15, 60}
	normalize(cfg)

	snap := Snapshot(cfg)
	if snap.CheckoutCurrencies == nil || !reflect.DeepEqual(*snap.CheckoutCurrencies, cfg.Checkout.Currencies) {
		t.Fatalf("Snapshot dropped CheckoutCurrencies: %+v", snap.CheckoutCurrencies)
	}
	if snap.NotifyRetryDelays == nil || !reflect.DeepEqual(*snap.NotifyRetryDelays, []int{5, 15, 60}) {
		t.Fatalf("Snapshot dropped NotifyRetryDelays: %+v", snap.NotifyRetryDelays)
	}

	// Mutating slices returned by Snapshot must not alias or corrupt cfg.
	(*snap.CheckoutCurrencies)[0] = "MUTATED"
	(*snap.BinanceCurrencies)[0] = "MUTATED"
	(*snap.ProviderOrder)[0] = "MUTATED"
	(*snap.NotifyRetryDelays)[0] = 999
	if cfg.Checkout.Currencies[0] != "USDT" || cfg.Checkout.BinanceCurrencies[0] != "USDT" ||
		cfg.Checkout.ProviderOrder[0] != config.ProviderOKX || cfg.Notify.RetryDelays[0] != 5 {
		t.Fatalf("Snapshot aliased backing config slices: %+v %+v", cfg.Checkout, cfg.Notify)
	}

	// Round-tripping a fresh Snapshot through Patch.Apply must remain valid.
	roundTrip := config.Default()
	roundTrip.Server.Production = false
	roundTrip.BEpusdt.APIToken = cfg.BEpusdt.APIToken
	Snapshot(cfg).Apply(roundTrip)
	normalize(roundTrip)
	if err := Validate(roundTrip); err != nil {
		t.Fatalf("Snapshot round-trip failed Validate: %v", err)
	}

	step := " 0.01 "
	minAmt := " 0.05 "
	Patch{OrderAmountStep: &step, OrderMinAmount: &minAmt}.Apply(roundTrip)
	if roundTrip.Order.AmountStep != "0.01" || roundTrip.Order.MinAmount != "0.05" {
		t.Fatalf("expected trimmed order amounts, got step=%q min=%q", roundTrip.Order.AmountStep, roundTrip.Order.MinAmount)
	}

	// Unnormalized config.Default() keeps nil per-provider slices so Snapshot round-trip preserves inherited currencies.
	defaultCfg := config.Default()
	defaultCfg.Server.Production = false
	defaultCfg.BEpusdt.APIToken = cfg.BEpusdt.APIToken
	defaultSnap := Snapshot(defaultCfg)
	if defaultSnap.BinanceCurrencies != nil || defaultSnap.OKXCurrencies != nil || defaultSnap.BybitCurrencies != nil || defaultSnap.BitgetCurrencies != nil {
		t.Fatalf("expected nil per-provider slices for unnormalized config, got %+v", defaultSnap)
	}
	dst := config.Default()
	dst.Server.Production = false
	dst.BEpusdt.APIToken = cfg.BEpusdt.APIToken
	defaultSnap.Apply(dst)
	normalize(dst)
	if !reflect.DeepEqual(dst.Checkout.Currencies, []string{"USDT", "USDC"}) || !reflect.DeepEqual(dst.ProviderCurrencies(config.ProviderBinance), []string{"USDT", "USDC"}) {
		t.Fatalf("Snapshot(config.Default()) round-trip lost inherited currencies: %+v", dst.Checkout)
	}
}

func TestValidateRegisterURLRejectsTabsAndBackslashHost(t *testing.T) {
	for _, bad := range []string{
		"https://example.com/\tpath",
		"https://example.com\r\n/path",
		"https://evil.com\\@example.com/register",
		"https://user:pass@example.com/register",
		"javascript:alert(1)",
	} {
		cfg := config.Default()
		cfg.Server.Production = false
		cfg.BEpusdt.APIToken = "0123456789abcdef0123456789abcdef"
		cfg.Checkout.Binance.RegisterURL = bad
		if err := Validate(cfg); err == nil {
			t.Fatalf("expected invalid register URL %q to be rejected", bad)
		}
	}
	cfg := config.Default()
	cfg.Server.Production = false
	cfg.BEpusdt.APIToken = "0123456789abcdef0123456789abcdef"
	cfg.Checkout.Binance.RegisterURL = "https://accounts.binance.com/register?ref=12345"
	if err := Validate(cfg); err != nil {
		t.Fatalf("valid register URL rejected: %v", err)
	}
}

func TestMinimumAmountFitsCheckoutPrecision(t *testing.T) {
	cfg := config.Default()
	cfg.BEpusdt.APIToken = "0123456789abcdef0123456789abcdef"
	for _, tc := range []struct {
		precision int
		minimum   string
		valid     bool
	}{
		{2, "999999.99", true},
		{2, "999999.990001", false},
		{6, "999999.999999", true},
		{6, "1000000", false},
	} {
		cfg.Order.AmountPrecision = tc.precision
		cfg.Order.MinAmount = tc.minimum
		if err := Validate(cfg); (err == nil) != tc.valid {
			t.Errorf("precision=%d min=%s: %v", tc.precision, tc.minimum, err)
		}
	}
}

func TestLoadValidatesEffectiveOrderSettings(t *testing.T) {
	ctx := context.Background()
	st, err := store.Open(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	cfg := config.Default()
	cfg.Server.Production = false
	cfg.Order.ExpireMinutes = 1441
	cfg.Order.AmountStep = "0.00001"
	manager := New(cfg, st)
	if err := manager.Load(ctx); err == nil {
		t.Fatal("invalid order configuration accepted on fresh database")
	}
	if err := st.SaveSettingPayload(ctx, `{"order_expire_minutes":10}`, "test"); err != nil {
		t.Fatal(err)
	}
	if err := manager.Load(ctx); err == nil {
		t.Fatal("step finer than the order precision was accepted")
	}
	if err := st.SaveSettingPayload(ctx, `{"order_expire_minutes":10,"order_amount_step":"0.01"}`, "test"); err != nil {
		t.Fatal(err)
	}
	if err := manager.Load(ctx); err != nil {
		t.Fatalf("valid stored overrides rejected: %v", err)
	}
}
