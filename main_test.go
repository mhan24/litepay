package main

import (
	"context"
	"flag"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"litepay/internal/store"
)

func TestRotateMasterKeyCommandExitsAfterAtomicRotation(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "litepay.db")
	oldFile, newFile := filepath.Join(dir, "old.key"), filepath.Join(dir, "new.key")
	oldHex, newHex := strings.Repeat("83", 32), strings.Repeat("91", 32)
	for file, key := range map[string]string{oldFile: oldHex, newFile: newHex} {
		if err := os.WriteFile(file, []byte(key), 0400); err != nil {
			t.Fatal(err)
		}
	}
	st, err := store.Open(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	oldKey, _ := store.ParseMasterKey(oldHex)
	if err := st.SetMasterKey(oldKey); err != nil {
		t.Fatal(err)
	}
	if err := st.SaveSettingPayload(context.Background(), `{"binance_api_secret":"cli-rotation-secret"}`, "test"); err != nil {
		t.Fatal(err)
	}
	if err := st.Close(); err != nil {
		t.Fatal(err)
	}
	// Use the production image's configuration, with its explicit startup prerequisites.
	raw, err := os.ReadFile("config.production.yaml")
	if err != nil {
		t.Fatal(err)
	}
	configText := strings.ReplaceAll(string(raw), `base_url: ""`, `base_url: "https://pay.example.com"`)
	configText = strings.ReplaceAll(configText, `notify_hosts: []`, `notify_hosts: ["shop.example.com"]`)
	configText = strings.ReplaceAll(configText, `"/app/data/litepay.db"`, `"`+dbPath+`"`)
	configFile := filepath.Join(dir, "config.yaml")
	if err := os.WriteFile(configFile, []byte(configText), 0600); err != nil {
		t.Fatal(err)
	}
	t.Setenv("LITEPAY_MASTER_KEY_FILE", newFile)
	previousFlags, previousArgs := flag.CommandLine, os.Args
	t.Cleanup(func() { flag.CommandLine, os.Args = previousFlags, previousArgs })
	flag.CommandLine = flag.NewFlagSet("litepay", flag.ContinueOnError)
	os.Args = []string{"litepay", "-config", configFile, "-rotate-master-key-from", oldFile}
	if err := run(); err != nil {
		t.Fatal(err)
	}
	reopened, err := store.Open(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	newKey, _ := store.ParseMasterKey(newHex)
	if err := reopened.SetMasterKey(newKey); err != nil {
		t.Fatal(err)
	}
	payload, err := reopened.LoadSettingPayload(context.Background())
	if err != nil || !strings.Contains(payload, "cli-rotation-secret") {
		t.Fatal(payload, err)
	}
	if err := reopened.SetMasterKey(oldKey); err != nil {
		t.Fatal(err)
	}
	if _, err := reopened.LoadSettingPayload(context.Background()); err == nil {
		t.Fatal("old key still works after CLI rotation")
	}
}
