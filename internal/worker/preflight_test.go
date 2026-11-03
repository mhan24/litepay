package worker

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
)

func TestPreflightUsesGETAndRedactsCredentials(t *testing.T) {
	f := newPollFixture(t, "PREFLIGHT")
	f.cfg.Binance.APISecret = "do-not-print-this-secret"
	results := Preflight(context.Background(), f.cfg)
	raw, _ := json.Marshal(results)
	if !results[0].AccountAndHistoryOK {
		t.Fatalf("mock read access failed: %s", raw)
	}
	if strings.Contains(string(raw), "do-not-print") || strings.Contains(string(raw), "binance-uid") {
		t.Fatal("preflight exposed credentials or UID")
	}
}
