package server

import (
	"context"
	"litepay/internal/config"
	"litepay/internal/model"
	"litepay/internal/settings"
	"litepay/internal/store"
	"litepay/internal/worker"
	"testing"
	"time"
)

type emptyScanner struct{ uid string }

func (f emptyScanner) AccountUID(context.Context) (string, error) { return f.uid, nil }
func (f emptyScanner) History(context.Context, time.Time, time.Time) ([]model.Transaction, error) {
	return []model.Transaction{}, nil
}

// Existing checkout fixtures explicitly complete the same account verification and
// scan admission path as production. New outage tests construct an unprimed poller.
func verifiedPoller(t *testing.T, mgr *settings.Manager, st *store.Store) *worker.Poller {
	t.Helper()
	p := worker.NewPoller(mgr, st)
	cfg := mgr.Get()
	for _, provider := range []string{"binance", "okx", "ton"} {
		if !cfg.ProviderEnabled(provider) {
			continue
		}
		uid := cfg.Checkout.Binance.UID
		if provider == "okx" {
			uid = cfg.Checkout.OKX.UID
		}
		if provider == "ton" {
			uid = cfg.TON.Address
		}
		p.UseTestSource(provider, emptyScanner{uid})
	}
	if !cfg.Server.CancelSyncRequired {
		primeScanners(t, p, mgr)
	}
	return p
}
func primeScanners(t *testing.T, p *worker.Poller, mgr *settings.Manager) {
	t.Helper()
	ctx := context.Background()
	cfg := mgr.Get()
	for _, provider := range []string{config.ProviderBinance, config.ProviderOKX, config.ProviderTON} {
		if !cfg.ProviderEnabled(provider) {
			continue
		}
		for i := 0; i < 100; i++ {
			if err := p.Scan(ctx, provider); err != nil {
				t.Fatal(err)
			}
			ready, err := p.ProviderReady(ctx, cfg, provider)
			if err != nil {
				t.Fatal(err)
			}
			if ready {
				break
			}
			if i == 99 {
				t.Fatal("fixture failed to catch up", provider)
			}
		}
	}
}
