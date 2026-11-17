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

	// 2. Production accepts $2a$, $2b$, and $2y$ bcrypt hashes.
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
