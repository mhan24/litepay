package config

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestContainerRequiresExplicitProductionConfiguration(t *testing.T) {
	raw, err := os.ReadFile("../../config.production.yaml")
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(t.TempDir(), "container.yaml")
	os.WriteFile(path, raw, 0600)
	if _, err = Load(path); err == nil || !strings.Contains(err.Error(), "production") {
		t.Fatal("unconfigured image booted", err)
	}
	text := strings.ReplaceAll(string(raw), `base_url: ""`, `base_url: "https://pay.example.com"`)
	text = strings.ReplaceAll(text, `notify_hosts: []`, `notify_hosts: ["shop.example.com"]`)
	os.WriteFile(path, []byte(text), 0600)
	cfg, err := Load(path)
	if err != nil || !cfg.Server.Production || cfg.Database.Path != "/app/data/litepay.db" {
		t.Fatal(cfg, err)
	}
	if err = cfg.ValidateNotifyURL("http://shop.example.com/notify"); err == nil {
		t.Fatal("container allowed plaintext callback")
	}
	if err = cfg.ValidateNotifyURL("https://other.example.com/notify"); err == nil {
		t.Fatal("container allowed unlisted merchant")
	}
	dev, err := Load("../../config.example.yaml")
	if err != nil || dev.Server.Production {
		t.Fatal("explicit local development config invalid", err)
	}
}
