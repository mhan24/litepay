package worker

import (
	"context"
	"litepay/internal/config"
	"net/http"
)

// Test-only bridge: acceptance tests use production paths without weakening callback SSRF policy.
func AcceptanceScan(p *Poller, ctx context.Context, cfg *config.Config, provider string) {
	p.tick(ctx, cfg, provider)
}
func AcceptanceNotify(n *Notifier, ctx context.Context, transport http.RoundTripper) {
	n.transport = transport
	n.tick(ctx)
}
func AcceptanceReap(r *Reaper, ctx context.Context) { r.once(ctx) }
