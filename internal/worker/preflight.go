package worker

import (
	"context"
	"litepay/internal/config"
	"litepay/internal/settings"
	"time"
)

// PreflightResult intentionally contains no keys, UID, raw response or transaction details.
type PreflightResult struct {
	Provider            string `json:"provider"`
	Enabled             bool   `json:"enabled"`
	AccountAndHistoryOK bool   `json:"account_and_history_ok"`
	Records             int    `json:"records"`
	Diagnosis           string `json:"diagnosis"`
}

// Preflight only performs authenticated GETs; it cannot create orders or transfer funds.
func Preflight(ctx context.Context, cfg *config.Config) []PreflightResult {
	p := NewPoller(settings.New(cfg, nil), nil)
	out := []PreflightResult{}
	for _, provider := range []string{config.ProviderBinance, config.ProviderOKX} {
		result := PreflightResult{Provider: provider, Enabled: cfg.ProviderEnabled(provider), Diagnosis: "not_configured"}
		if result.Enabled {
			end := time.Now()
			start := end.Add(-2 * time.Minute)
			var txs []incomingTransaction
			var err error
			if provider == config.ProviderBinance {
				txs, err = p.binanceTransactions(ctx, cfg, start, end)
			} else {
				txs, err = p.okxTransactions(ctx, cfg, start, end)
			}
			if err != nil {
				result.Diagnosis = "account_uid_or_history_check_failed; check read permission, UID, clock, network and limits locally"
			} else {
				result.AccountAndHistoryOK = true
				result.Records = len(txs)
				result.Diagnosis = "read_only_access_verified; receipt_visibility_and_delivery_not_verified"
			}
		}
		out = append(out, result)
	}
	return out
}
