package server

import (
	"github.com/gin-gonic/gin"
	"litepay/internal/config"
	"litepay/internal/model"
)

func (h *Handlers) providerAccepting(c *gin.Context, cfg *config.Config, provider string) bool {
	ready, err := h.poller.ProviderReady(c.Request.Context(), cfg, provider)
	return err == nil && ready
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

func (h *Handlers) providerHealths(c *gin.Context) []providerHealth {
	cfg := h.C()
	out := make([]providerHealth, 0, 3)
	for _, provider := range []string{config.ProviderBinance, config.ProviderOKX, config.ProviderTON} {
		state := providerHealth{Provider: provider, Configured: cfg.ProviderEnabled(provider)}
		if !state.Configured {
			state.Reason = "平台未启用"
		} else if !h.providerAccepting(c, cfg, provider) {
			state.Reason = "账户核验或收款扫描尚未就绪"
		} else if provider == config.ProviderTON && !h.poller.GRAM.Snapshot().Fresh {
			state.Reason = "GRAM/USD 行情检测尚未就绪"
		} else {
			state.Ready = true
		}
		out = append(out, state)
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
