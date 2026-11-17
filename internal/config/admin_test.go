package config

import (
	"bytes"
	"io"
	"os"
	"strings"
	"testing"
)

func TestAdminPasswordPolicyAndSessionHours(t *testing.T) {
	// 1. Production rejects plaintext password when password is set.
	c := Default()
	c.Server.Production = true
	c.Server.BaseURL = "https://pay.example.com"
	c.Server.NotifyHosts = []string{"shop.example.com"}
	c.Server.TrustedProxies = []string{"127.0.0.1"}
	c.Admin.Password = "plaintext-long-password-123"
	if err := c.normalize(); err == nil || !strings.Contains(err.Error(), "bcrypt") {
		t.Fatalf("expected production plaintext password rejection, got: %v", err)
	}

	// 2. Production accepts valid $2a$, $2b$, and $2y$ bcrypt hashes and rejects malformed $2 hashes.
	for _, prefix := range []string{"$2a$", "$2b$", "$2y$"} {
		c = Default()
		c.Server.Production = true
		c.Server.BaseURL = "https://pay.example.com"
		c.Server.NotifyHosts = []string{"shop.example.com"}
		c.Server.TrustedProxies = []string{"127.0.0.1"}
		c.Admin.Password = prefix + "10$N9qo8uLOickgx2ZMRZoMyeIjZAgcfl7p92ldGxad68LJZdL17lhWy"
		if !IsBcryptHash(c.Admin.Password) {
			t.Fatalf("expected %s to be recognized as bcrypt hash", prefix)
		}
		if err := c.normalize(); err != nil {
			t.Fatalf("expected %s hash accepted in production, got: %v", prefix, err)
		}
	}
	for _, badHash := range []string{"$2a$short", "$2y$not-valid-bcrypt-hash", "$2x$10$N9qo8uLOickgx2ZMRZoMyeIjZAgcfl7p92ldGxad68LJZdL17lhWy"} {
		c = Default()
		c.Server.Production = true
		c.Server.BaseURL = "https://pay.example.com"
		c.Server.NotifyHosts = []string{"shop.example.com"}
		c.Server.TrustedProxies = []string{"127.0.0.1"}
		c.Admin.Password = badHash
		if IsBcryptHash(c.Admin.Password) {
			t.Fatalf("expected malformed hash %q to be rejected", badHash)
		}
		if err := c.normalize(); err == nil {
			t.Fatalf("expected malformed hash %q to fail normalize", badHash)
		}
	}

	// 3. Development mode requires at least 12 characters for plaintext password.
	c = Default()
	c.Server.Production = false
	c.Admin.Password = "short-pass1" // 11 chars
	if err := c.normalize(); err == nil || !strings.Contains(err.Error(), "12") {
		t.Fatalf("expected <12 char plaintext password rejection, got: %v", err)
	}
	c.Admin.Password = "twelve-chars" // 12 chars
	if err := c.normalize(); err != nil {
		t.Fatalf("expected 12 char plaintext password accepted in dev, got: %v", err)
	}

	// 4. Production caps session_hours at 72; dev caps at 720.
	c = Default()
	c.Server.Production = true
	c.Server.BaseURL = "https://pay.example.com"
	c.Server.NotifyHosts = []string{"shop.example.com"}
	c.Server.TrustedProxies = []string{"127.0.0.1"}
	c.Admin.SessionHours = 73
	if err := c.normalize(); err == nil || !strings.Contains(err.Error(), "72") {
		t.Fatalf("expected production session_hours > 72 rejection, got: %v", err)
	}
	c.Admin.SessionHours = 72
	if err := c.normalize(); err != nil {
		t.Fatalf("expected production session_hours = 72 accepted, got: %v", err)
	}
	c.Server.Production = false
	c.Admin.SessionHours = 120
	if err := c.normalize(); err != nil {
		t.Fatalf("expected dev session_hours = 120 accepted, got: %v", err)
	}
}

func TestProductionEmptyTrustedProxiesWarnsOnStartup(t *testing.T) {
	origStderr := os.Stderr
	r, w, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	os.Stderr = w
	t.Cleanup(func() { os.Stderr = origStderr })

	c := Default()
	c.Server.Production = true
	c.Server.BaseURL = "https://pay.example.com"
	c.Server.NotifyHosts = []string{"shop.example.com"}
	c.Server.TrustedProxies = nil
	if err := c.normalize(); err != nil {
		t.Fatal(err)
	}
	w.Close()
	var buf bytes.Buffer
	io.Copy(&buf, r)
	if !strings.Contains(buf.String(), "trusted_proxies") {
		t.Fatalf("expected trusted_proxies startup warning on stderr, got: %q", buf.String())
	}
}

func TestReadinessTokenMinimumLengthAtStartup(t *testing.T) {
	for _, token := range []string{"", "1234567890123456", "short", "123456789012345"} {
		for _, alias := range []bool{false, true} {
			c := Default()
			c.Server.Production = false
			if alias {
				c.Server.ProbeToken = token
			} else {
				c.Server.ReadinessToken = token
			}
			err := c.normalize()
			if (err != nil) != (token != "" && len(token) < 16) {
				t.Fatalf("token=%q alias=%v err=%v", token, alias, err)
			}
		}
	}
}

func TestNormalizeWebRoutesOrderAndPortChecks(t *testing.T) {
	c := Default()
	c.Server.Production = false
	c.Server.Addr = "[::]:9090"
	c.Server.StoreURL = "http://127.0.0.1:9090/shop"
	if err := c.normalize(); err == nil {
		t.Fatal("expected StoreURL pointing to inferred IPv6 gateway port to be rejected")
	}

	c = Default()
	c.Server.Production = false
	c.Server.BaseURL = "https://pay.example.com"
	c.Server.StoreURL = "https://pay.example.com:8443/shop"
	if err := c.normalize(); err != nil {
		t.Fatalf("expected different port on same host to be allowed as separate store site, got: %v", err)
	}

	c = Default()
	c.Server.Production = false
	c.Server.BaseURL = "https://pay.example.com"
	c.Server.StoreURL = "https://pay.example.com:443/shop"
	if err := c.normalize(); err == nil {
		t.Fatal("expected default HTTPS port 443 matching gateway base_url to be rejected")
	}
}
