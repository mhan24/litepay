package config

import (
	"encoding/json"
	"os"
	"path/filepath"
	"regexp"
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

func TestCISecurityToolsUseFixedVersions(t *testing.T) {
	raw, err := os.ReadFile("../../.github/workflows/test.yml")
	if err != nil {
		t.Fatal(err)
	}
	installs := 0
	for _, line := range strings.Split(string(raw), "\n") {
		line = strings.TrimSpace(line)
		if !strings.HasPrefix(line, "go install ") {
			continue
		}
		installs++
		_, version, ok := strings.Cut(line, "@")
		if !ok || !regexp.MustCompile(`^v[0-9]+\.[0-9]+\.[0-9]+$`).MatchString(version) {
			t.Fatalf("security tool version floats: %s", line)
		}
	}
	if installs != 2 {
		t.Fatalf("expected vulnerability and security scanners, got %d", installs)
	}
}

func TestProductionImageHealthcheckUsesLocalProbe(t *testing.T) {
	raw, err := os.ReadFile("../../Dockerfile")
	if err != nil {
		t.Fatal(err)
	}
	probes := 0
	for _, line := range strings.Split(string(raw), "\n") {
		if !strings.HasPrefix(line, "HEALTHCHECK ") {
			continue
		}
		probes++
		for _, option := range []string{"--interval=30s", "--timeout=3s", "--retries=3"} {
			if !strings.Contains(line, option) {
				t.Fatal("missing image probe setting", option)
			}
		}
		_, command, ok := strings.Cut(line, " CMD ")
		var args []string
		if !ok || json.Unmarshal([]byte(command), &args) != nil || len(args) == 0 || args[0] != "wget" || args[len(args)-1] != "http://127.0.0.1:8080/healthz" {
			t.Fatal("invalid image health command", line)
		}
	}
	if probes != 1 {
		t.Fatal("expected one runtime healthcheck", probes)
	}
}
