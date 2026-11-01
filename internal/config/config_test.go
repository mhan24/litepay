package config

import (
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
)

func TestBinanceRequiresReadCredentialsAndReceiverUID(t *testing.T) {
	c := Default()
	c.Binance.APIKey, c.Binance.APISecret = "key", "secret"
	if c.BinanceEnabled() {
		t.Fatal("Binance must not enable checkout without receiver UID")
	}
	c.Checkout.Binance.UID = "receiver"
	if !c.BinanceEnabled() {
		t.Fatal("credentials and receiver UID must enable checkout without extra receiver data")
	}
	c.Binance.APISecret = ""
	if c.BinanceEnabled() {
		t.Fatal("receiver UID without credentials must not enable checkout")
	}
}

func TestOKXRequiresReadCredentialsAndReceiverUID(t *testing.T) {
	c := Default()
	c.OKX.APIKey, c.OKX.APISecret, c.OKX.Passphrase = "key", "secret", "pass"
	if c.OKXEnabled() {
		t.Fatal("OKX must not enable checkout without receiver UID")
	}
	c.Checkout.OKX.UID = "123456789"
	if !c.OKXEnabled() {
		t.Fatal("OKX with credentials and receiver UID must enable checkout")
	}
}

func TestNormalizeCurrenciesOnlyKeepsUSDTAndUSDC(t *testing.T) {
	got := NormalizeCurrencies([]string{" usdc ", "BTC", "USDT", "USDT", "ETH", "USDE"})
	want := []string{"USDC", "USDT"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("currencies = %#v, want %#v", got, want)
	}
	if got := NormalizeCurrencies(nil); len(got) != 0 {
		t.Fatalf("empty currencies = %#v, want an empty list", got)
	}
}

func TestYAMLIgnoresBusinessConfiguration(t *testing.T) {
	path := filepath.Join(t.TempDir(), "config.yaml")
	body := []byte(`
server:
  addr: "127.0.0.1:9090"
  base_url: "https://pay.example.test"
database:
  path: "/tmp/litepay-test.db"
admin:
  username: "operator"
  password: "test-password"
bepusdt:
  api_token: "yaml-token-must-be-ignored"
payee:
  currencies: ["BTC"]
binance:
  api_key: "yaml-key-must-be-ignored"
rate:
  fixed: 99
`)
	if err := os.WriteFile(path, body, 0o600); err != nil {
		t.Fatal(err)
	}
	c, err := Load(path)
	if err != nil {
		t.Fatal(err)
	}
	if c.Server.Addr != "127.0.0.1:9090" || c.Admin.Username != "operator" {
		t.Fatal("startup settings must still load from YAML")
	}
	if c.BEpusdt.APIToken != "" || c.Binance.APIKey != "" {
		t.Fatal("business settings must not load from YAML")
	}
	if !reflect.DeepEqual(c.Checkout.Currencies, SupportedCurrencies) {
		t.Fatalf("currencies = %#v, want built-in defaults", c.Checkout.Currencies)
	}
}

func TestAdminStartupBounds(t *testing.T) {
	load := func(t *testing.T, admin string) (*Config, error) {
		t.Helper()
		path := filepath.Join(t.TempDir(), "config.yaml")
		if err := os.WriteFile(path, []byte("admin:\n"+admin), 0o600); err != nil {
			t.Fatal(err)
		}
		return Load(path)
	}

	if _, err := load(t, "  password: \"short\"\n"); err == nil {
		t.Fatal("accepted an admin password shorter than 6 characters")
	}
	if _, err := load(t, "  password: \"long enough password\"\n  session_hours: 721\n"); err == nil {
		t.Fatal("accepted an unbounded admin session")
	}
	if _, err := load(t, "  username: \""+strings.Repeat("a", 65)+"\"\n"); err == nil {
		t.Fatal("accepted an oversized admin username")
	}
	c, err := load(t, "  password: \"long enough password\"\n  session_hours: 720\n")
	if err != nil {
		t.Fatalf("rejected valid admin settings: %v", err)
	}
	if c.Admin.SessionHours != 720 {
		t.Fatalf("session hours = %d", c.Admin.SessionHours)
	}
}
