package maintenance

import (
	"context"
	"os"
	"path/filepath"
	"testing"
)

func TestGateBlocksUntilExplicitRelease(t *testing.T) {
	path := filepath.Join(t.TempDir(), "gate")
	t.Setenv("LITEPAY_DEPLOY_GATE", path)
	if Blocked() {
		t.Fatal("missing gate blocks")
	}
	if err := os.WriteFile(path, []byte("maintenance"), 0600); err != nil {
		t.Fatal(err)
	}
	if !Blocked() {
		t.Fatal("gate ignored")
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if Wait(ctx) {
		t.Fatal("workers released on cancellation")
	}
	if err := os.Remove(path); err != nil {
		t.Fatal(err)
	}
	if !Wait(context.Background()) {
		t.Fatal("release failed")
	}
}
