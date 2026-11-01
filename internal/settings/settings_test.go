package settings

import (
	"context"
	"encoding/json"
	"path/filepath"
	"strings"
	"testing"

	"litepay/internal/config"
	"litepay/internal/store"
)

func TestLoadIgnoresUnknownStoredFields(t *testing.T) {
	cfg := config.Default()
	cfg.Database.Path = filepath.Join(t.TempDir(), "settings.db")
	cfg.BEpusdt.APIToken = "0123456789abcdef0123456789abcdef"
	st, err := store.Open(cfg.Database.Path)
	if err != nil {
		t.Fatal(err)
	}

	unknown := map[string]any{
		"old_currency": "BTC",
		"old_uid":      "old-receiver",
		"old_note":     "old note",
	}
	payload, err := json.Marshal(unknown)
	if err != nil {
		t.Fatal(err)
	}
	if err := st.SaveSettingPayload(context.Background(), string(payload), "test"); err != nil {
		t.Fatal(err)
	}

	mgr := New(cfg, st)
	if err := mgr.Load(context.Background()); err != nil {
		t.Fatal(err)
	}
	got := mgr.Get()
	if got.Checkout.Binance.UID != "" ||
		got.Checkout.Note != "" {
		t.Fatalf("unknown settings changed current checkout fields: %+v", got.Checkout)
	}
}

func TestLoadGeneratesTokenOnlyWhenCredentialIsMissing(t *testing.T) {
	cfg := config.Default()
	cfg.Database.Path = filepath.Join(t.TempDir(), "settings.db")
	cfg.BEpusdt.APIToken = "bootstrap-token-0123456789"
	st, err := store.Open(cfg.Database.Path)
	if err != nil {
		t.Fatal(err)
	}
	saved := map[string]any{
		"checkout_currencies": []string{"USDT", "USDC"},
	}
	payload, err := json.Marshal(saved)
	if err != nil {
		t.Fatal(err)
	}
	if err := st.SaveSettingPayload(context.Background(), string(payload), "test"); err != nil {
		t.Fatal(err)
	}

	mgr := New(cfg, st)
	if err := mgr.Load(context.Background()); err != nil {
		t.Fatal(err)
	}
	first := mgr.Get().BEpusdt.APIToken
	if first == cfg.BEpusdt.APIToken || len(first) != 48 {
		t.Fatalf("missing token was not replaced by a generated token")
	}

	reloaded := New(cfg, st)
	if err := reloaded.Load(context.Background()); err != nil {
		t.Fatal(err)
	}
	if got := reloaded.Get().BEpusdt.APIToken; got != first {
		t.Fatal("generated token rotated again after restart")
	}
}

func TestUpdatePersistsCurrentCheckoutFields(t *testing.T) {
	cfg := config.Default()
	cfg.Database.Path = filepath.Join(t.TempDir(), "settings.db")
	cfg.BEpusdt.APIToken = "0123456789abcdef0123456789abcdef"
	st, err := store.Open(cfg.Database.Path)
	if err != nil {
		t.Fatal(err)
	}
	mgr := New(cfg, st)
	if err := mgr.Update(context.Background(), Patch{}, "test"); err != nil {
		t.Fatal(err)
	}
	payload, err := st.LoadSettingPayload(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	var doc map[string]any
	if err := json.Unmarshal([]byte(payload), &doc); err != nil {
		t.Fatal(err)
	}
	if _, exists := doc["binance_uid"]; !exists {
		t.Fatal("new settings payload is missing binance_uid")
	}
	if _, exists := doc["okx_uid"]; !exists {
		t.Fatal("new settings payload is missing okx_uid")
	}
	if _, exists := doc["checkout_currencies"]; !exists {
		t.Fatal("new settings payload is missing checkout_currencies")
	}
	if _, exists := doc["checkout_note"]; !exists {
		t.Fatal("new settings payload is missing checkout_note")
	}
}

func TestValidateRejectsEmptyCurrencyList(t *testing.T) {
	cfg := config.Default()
	cfg.BEpusdt.APIToken = "0123456789abcdef0123456789abcdef"
	cfg.Checkout.Currencies = nil
	if err := Validate(cfg); err == nil {
		t.Fatal("accepted a configuration with no enabled currency")
	}
}

func TestValidateRejectsAmountStepSmallerThanPrecision(t *testing.T) {
	cfg := config.Default()
	cfg.BEpusdt.APIToken = "0123456789abcdef0123456789abcdef"
	cfg.Order.AmountPrecision = 2
	cfg.Order.AmountStep = 0.001
	if err := Validate(cfg); err == nil {
		t.Fatal("accepted an amount step that rounds to zero")
	}
	cfg.Order.AmountPrecision = 3
	if err := Validate(cfg); err != nil {
		t.Fatalf("rejected compatible step and precision: %v", err)
	}
}

func TestUpdateRejectsUnverifiedCollectionAccounts(t *testing.T) {
	for _, provider := range []string{"binance", "okx"} {
		t.Run(provider, func(t *testing.T) {
			cfg := config.Default()
			cfg.BEpusdt.APIToken = "0123456789abcdef0123456789abcdef"
			// The private address must be rejected by the production HTTP transport.
			cfg.Binance.BaseURL = "http://127.0.0.1:1"
			cfg.OKX.BaseURL = "http://127.0.0.1:1"
			st, err := store.Open(filepath.Join(t.TempDir(), "settings.db"))
			if err != nil {
				t.Fatal(err)
			}
			mgr := New(cfg, st)
			key, secret, uid, pass := "key", "secret", "123456", "pass"
			p := Patch{}
			if provider == "binance" {
				p.BinanceAPIKey = &key
				p.BinanceAPISecret = &secret
				p.BinanceUID = &uid
			} else {
				p.OKXAPIKey = &key
				p.OKXAPISecret = &secret
				p.OKXPassphrase = &pass
				p.OKXUID = &uid
			}
			if err := mgr.Update(context.Background(), p, "test"); err == nil {
				t.Fatal("saved an unverified account")
			}
			if mgr.Get().BinanceEnabled() || mgr.Get().OKXEnabled() {
				t.Fatal("published unverified credentials")
			}
			payload, err := st.LoadSettingPayload(context.Background())
			if err != nil {
				t.Fatal(err)
			}
			if strings.TrimSpace(payload) != "" {
				t.Fatal("persisted rejected settings")
			}
		})
	}
}
