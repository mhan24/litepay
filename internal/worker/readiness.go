package worker

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"time"

	"litepay/internal/config"
)

const ScanFreshness = 2 * time.Minute
const ScanCoverageFreshness = 3 * time.Minute

// The scope is private and never returned by status APIs. A new account, key or
// endpoint must pass verification and scanning again before receiving instructions.
func scanScope(cfg *config.Config, provider string) string {
	var account any
	switch provider {
	case config.ProviderBinance:
		account = []any{provider, cfg.Binance, cfg.Checkout.Binance.UID}
	case config.ProviderOKX:
		account = []any{provider, cfg.OKX, cfg.Checkout.OKX.UID}
	case config.ProviderTON:
		account = []any{provider, cfg.TON}
	}
	raw, _ := json.Marshal(account)
	hash := sha256.Sum256(raw)
	return hex.EncodeToString(hash[:])
}

func (p *Poller) scopeScanned(cfg *config.Config, provider string) bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.scannedScope[provider] == scanScope(cfg, provider)
}

func (p *Poller) recordScanScope(cfg *config.Config, provider string) {
	p.mu.Lock()
	p.scannedScope[provider] = scanScope(cfg, provider)
	p.mu.Unlock()
}

// ProviderReady is an admission check only. It never stops scans or historical
// recovery, and a persisted cursor alone cannot bypass this process's first scan.
func (p *Poller) ProviderReady(ctx context.Context, cfg *config.Config, provider string) (bool, error) {
	if !cfg.ProviderEnabled(provider) || !p.scopeScanned(cfg, provider) {
		return false, nil
	}
	cursor, err := p.st.Cursor(ctx, provider)
	now := time.Now()
	return err == nil && cursor.LastError == "" && !cursor.LastSuccess.IsZero() && now.Sub(cursor.LastSuccess) <= ScanFreshness && !cursor.ScannedUntil.IsZero() && now.Sub(cursor.ScannedUntil) <= ScanCoverageFreshness, err
}

// UseTestSource substitutes a read-only exchange fixture for end-to-end tests.
// It does not skip AccountUID verification or the first successful scan.
func (p *Poller) UseTestSource(provider string, fixture source) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.clients[provider] = &connection{config: *p.cfg.Get(), source: fixture}
	delete(p.scannedScope, provider)
}
