package server

import (
	"github.com/gin-gonic/gin"
	"litepay/internal/config"
	"litepay/internal/model"
)

func (h *Handlers) providerAccepting(c *gin.Context, cfg *config.Config, provider string) bool {
	return h.providerState(c, cfg, provider).Ready
}

func (h *Handlers) anyProviderAccepting(c *gin.Context, cfg *config.Config, o *model.Order) bool {
	for _, option := range providerOptions(cfg, o) {
		if option.Enabled && h.providerAccepting(c, cfg, option.Key) {
			return true
		}
	}
	return false
}

func (h *Handlers) orderProviderAccepting(c *gin.Context, cfg *config.Config, o *model.Order) bool {
	if o.Provider == "" {
		return h.anyProviderAccepting(c, cfg, o)
	}
	receiver := cfg.Checkout.Binance.UID
	if o.Provider == config.ProviderOKX {
		receiver = cfg.Checkout.OKX.UID
	}
	if o.Provider == config.ProviderTON {
		receiver = cfg.TON.Address
	}
	return receiver == o.Receiver && h.providerAccepting(c, cfg, o.Provider)
}

type providerHealth struct {
	Provider   string `json:"provider"`
	Configured bool   `json:"configured"`
	Ready      bool   `json:"ready"`
	Reason     string `json:"reason,omitempty"`
}

func (h *Handlers) providerState(c *gin.Context, cfg *config.Config, provider string) providerHealth {
	state := providerHealth{Provider: provider, Configured: cfg.ProviderEnabled(provider)}
	ready, err := h.poller.ProviderReady(c.Request.Context(), cfg, provider)
	switch {
	case !state.Configured:
		state.Reason = "平台未启用"
	case err != nil || !ready:
		state.Reason = "账户核验或收款扫描尚未就绪"
	case provider == config.ProviderTON && !h.poller.GRAM.Snapshot().Fresh:
		state.Reason = "GRAM/USD 行情检测尚未就绪"
	default:
		state.Ready = true
	}
	return state
}

func (h *Handlers) providerHealths(c *gin.Context) []providerHealth {
	cfg := h.C()
	out := make([]providerHealth, 0, 3)
	for _, provider := range []string{config.ProviderBinance, config.ProviderOKX, config.ProviderTON} {
		out = append(out, h.providerState(c, cfg, provider))
	}
	return out
}
func (h *Handlers) ProviderReady(c *gin.Context) {
	for _, state := range h.providerHealths(c) {
		if state.Provider == c.Param("provider") {
			status := 200
			if !state.Ready {
				status = 503
			}
			c.JSON(status, state)
			return
		}
	}
	c.JSON(404, gin.H{"error": "unknown provider"})
}
