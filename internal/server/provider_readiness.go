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
