package worker

import (
	"context"
	"errors"
	"litepay/internal/config"
	"litepay/internal/gram"
	"litepay/internal/model"
	"litepay/internal/settings"
	"litepay/internal/store"
	"testing"
	"time"
)

type checkpointSource struct{ fail bool }

func (*checkpointSource) AccountUID(context.Context) (string, error) { return gram.DefaultAddress, nil }
func (*checkpointSource) History(context.Context, time.Time, time.Time) ([]model.Transaction, error) {
	return nil, errors.New("TON must use pages")
}
func (f *checkpointSource) HistoryPage(_ context.Context, cp model.TONScanCheckpoint) ([]model.Transaction, model.TONScanCheckpoint, error) {
	if f.fail {
		return nil, cp, errors.New("provider unavailable")
	}
	cp.Pages++
	cp.LT = "10"
	cp.Hash = "checkpoint"
	cp.Active = cp.Pages < 2
	return nil, cp, nil
}
func TestTONAdmissionWaitsForCompleteCoverageAndKeepsCheckpointOnFailure(t *testing.T) {
	ctx := context.Background()
	st, err := store.Open(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	cfg := config.Default()
	cfg.Server.Production = false
	cfg.TON.Address = gram.DefaultAddress
	cfg.Checkout.Currencies = []string{"GRAM"}
	mgr := settings.New(cfg, st)
	if err = mgr.Load(ctx); err != nil {
		t.Fatal(err)
	}
	p := NewPoller(mgr, st)
	source := &checkpointSource{}
	p.UseTestSource("ton", source)
	// A previous process/account may have a recent durable cursor. A partial page
	// from this process must not borrow that cursor's successful admission proof.
	if err = st.ScanResult(ctx, "ton", time.Now(), nil); err != nil {
		t.Fatal(err)
	}
	if err = p.Scan(ctx, "ton"); err != nil {
		t.Fatal(err)
	}
	if ready, e := p.ProviderReady(ctx, mgr.Get(), "ton"); e != nil || ready {
		t.Fatal("partial history admitted payments", ready, e)
	}
	if err = p.Scan(ctx, "ton"); err != nil {
		t.Fatal(err)
	}
	if ready, e := p.ProviderReady(ctx, mgr.Get(), "ton"); e != nil || !ready {
		t.Fatal(ready, e)
	}
	before, _ := st.Cursor(ctx, "ton")
	source.fail = true
	st.DB().Model(&model.ScanCursor{}).Where("provider=?", "ton").Update("last_success", time.Now().Add(-4*time.Minute))
	if err = p.Scan(ctx, "ton"); err == nil {
		t.Fatal("failed page accepted")
	}
	after, _ := st.Cursor(ctx, "ton")
	if !after.ScannedUntil.Equal(before.ScannedUntil) {
		t.Fatal("failed scan advanced coverage")
	}
	cp, e := st.TONCheckpoint(ctx, gram.DefaultAddress)
	if e != nil || !cp.Active {
		t.Fatal("failure lost resumable scan", cp, e)
	}
	if ready, e := p.ProviderReady(ctx, mgr.Get(), "ton"); e != nil || ready {
		t.Fatal("failed scan admitted payments", ready, e)
	}
	source.fail = false
	for i := 0; i < 2; i++ {
		if err = p.Scan(ctx, "ton"); err != nil {
			t.Fatal(err)
		}
	}
	if ready, e := p.ProviderReady(ctx, mgr.Get(), "ton"); e != nil || !ready {
		t.Fatal("recovered coverage did not release admission", ready, e)
	}
}
