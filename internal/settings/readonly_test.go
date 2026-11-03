package settings

import (
	"context"
	"litepay/internal/config"
	"litepay/internal/model"
	"litepay/internal/store"
	"path/filepath"
	"testing"
)

func TestReadOnlyLoadUsesStoredTokenWithoutWriting(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "settings.db")
	st, err := store.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	const token = "readonly-existing-token-123456789"
	if err := st.SaveCredential(ctx, model.Credential{Name: "bepusdt_api_token", Value: token, UpdatedBy: "existing"}); err != nil {
		t.Fatal(err)
	}
	if err := st.SaveSettingPayload(ctx, `{"checkout_currencies":["USDT","USDC"]}`, "existing"); err != nil {
		t.Fatal(err)
	}
	before, err := st.LoadCredential(ctx, "bepusdt_api_token")
	if err != nil {
		t.Fatal(err)
	}
	ro, err := store.OpenReadOnly(path)
	if err != nil {
		t.Fatal(err)
	}
	mgr := New(config.Default(), ro)
	if err := mgr.LoadReadOnly(ctx); err != nil {
		t.Fatalf("readonly preflight cannot load valid credentials: %v", err)
	}
	if mgr.Get().BEpusdt.APIToken != token {
		t.Fatal("stored token not loaded")
	}
	after, err := st.LoadCredential(ctx, "bepusdt_api_token")
	if err != nil {
		t.Fatal(err)
	}
	if after.Value != before.Value || !after.UpdatedAt.Equal(before.UpdatedAt) || after.UpdatedBy != before.UpdatedBy {
		t.Fatal("readonly load rotated or modified token")
	}
}
