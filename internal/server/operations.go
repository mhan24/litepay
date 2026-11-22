package server

import (
	"errors"
	"github.com/gin-gonic/gin"
	"gorm.io/gorm"
	"litepay/internal/model"
	"litepay/internal/sign"
	"strconv"
	"strings"
	"time"
)

func (h *Handlers) CancelSyncHeartbeat(c *gin.Context) {
	raw, err := readBoundedBody(c, 4096)
	if err != nil {
		h.bepusdtFail(c, "invalid health report")
		return
	}
	body, err := decodeBEpusdtJSON(raw)
	if err != nil || !sign.BEpusdtVerifyGeneric(body, h.C().BEpusdt.APIToken) {
		h.bepusdtSigFail(c, "invalid_signature", "signature verification failed")
		return
	}
	text := func(key string) string { v, _ := body[key].(string); return strings.TrimSpace(v) }
	now := time.Now()
	stamp, err := strconv.ParseInt(text("timestamp"), 10, 64)
	if err != nil || stamp < now.Unix()-60 || stamp > now.Unix()+10 {
		h.bepusdtFail(c, "stale health report")
		return
	}
	source := text("source")
	if source != "sqlite" && source != "postgresql" {
		h.bepusdtFail(c, "invalid source")
		return
	}
	counts := make([]int64, 6)
	for i, key := range []string{"pending", "failures", "conflicts", "missing", "discovery_pending", "discovery_lag_seconds"} {
		value := text(key)
		counts[i], err = strconv.ParseInt(value, 10, 64)
		limit := int64(100000000)
		if key == "discovery_lag_seconds" {
			limit = 1000000000000
		}
		if err != nil || counts[i] < 0 || counts[i] > limit || key == "discovery_pending" && counts[i] > 1 {
			h.bepusdtFail(c, "invalid count")
			return
		}
	}
	generation := text("generation")
	if generation == "" {
		h.bepusdtFail(c, "invalid generation")
		return
	}
	if h.replays != nil && !h.replays.accept("cancel-sync-status", body, now) {
		h.bepusdtFail(c, "replayed health report")
		return
	}
	report := model.IntegrationHealth{DiscoveryPending: counts[4], DiscoveryLagSeconds: counts[5], Generation: generation, Missing: counts[3], Name: "dujiao_cancel_sync", Source: source, ReportedAt: stamp, Healthy: text("healthy") == "true", Pending: counts[0], Failures: counts[1], Conflicts: counts[2]}
	if err = h.st.AcknowledgeRecovery(c.Request.Context(), generation, text("replay_complete") == "true", report); err != nil {
		h.bepusdtFail(c, "health write failed")
		return
	}
	c.JSON(200, bepusdtResponse{StatusCode: 200, Message: "success"})
}
func (h *Handlers) AdminOperations(c *gin.Context) {
	ctx := c.Request.Context()
	stats, err := h.st.QueueStats(ctx)
	if err != nil {
		c.JSON(503, gin.H{"error": "queue metrics unavailable"})
		return
	}
	recovery, recErr := h.st.Recovery(ctx)
	if recErr != nil {
		c.JSON(503, gin.H{"error": "recovery state unavailable"})
		return
	}
	health, healthErr := h.st.IntegrationHealth(ctx)
	if healthErr != nil && !errors.Is(healthErr, gorm.ErrRecordNotFound) {
		c.JSON(503, gin.H{"error": "integration health unavailable"})
		return
	}
	var binanceUsage any
	if h.poller != nil {
		binanceUsage = h.poller.BinanceUsage()
	}
	c.JSON(200, gin.H{"recovery": recovery, "payment_operations_ready": !h.paymentOperationsBlocked(c), "queues": stats, "cancel_sync": health, "binance": binanceUsage, "cancel_sync_required": h.C().Server.CancelSyncRequired, "production": h.C().Server.Production, "providers": h.providerHealths(c)})
}

func (h *Handlers) CancelSyncGeneration(c *gin.Context) {
	raw, err := readBoundedBody(c, 4096)
	if err != nil {
		h.bepusdtFail(c, "invalid generation request")
		return
	}
	body, err := decodeBEpusdtJSON(raw)
	if err != nil || !sign.BEpusdtVerifyGeneric(body, h.C().BEpusdt.APIToken) {
		h.bepusdtSigFail(c, "invalid_signature", "signature verification failed")
		return
	}
	now := time.Now()
	stamp, _ := body["timestamp"].(string)
	at, err := strconv.ParseInt(strings.TrimSpace(stamp), 10, 64)
	if err != nil || at < now.Unix()-60 || at > now.Unix()+10 {
		h.bepusdtFail(c, "stale generation request")
		return
	}
	if h.replays != nil && !h.replays.accept("cancel-sync-generation", body, now) {
		h.bepusdtFail(c, "replayed generation request")
		return
	}
	r, err := h.st.Recovery(c.Request.Context())
	if err != nil {
		h.bepusdtFail(c, "recovery state unavailable")
		return
	}
	c.JSON(200, bepusdtResponse{StatusCode: 200, Message: "success", Data: gin.H{"generation": r.Generation, "replay_required": !r.Acknowledged}})
}
func (h *Handlers) paymentOperationsBlocked(c *gin.Context) bool {
	if !h.C().Server.CancelSyncRequired {
		return false
	}
	ready, err := h.st.PaymentOperationsReady(c.Request.Context())
	return err != nil || !ready
}

func (h *Handlers) CancellationReviewStatus(c *gin.Context) {
	raw, err := readBoundedBody(c, 4096)
	if err != nil {
		h.bepusdtFail(c, "invalid review request")
		return
	}
	body, err := decodeBEpusdtJSON(raw)
	if err != nil || !sign.BEpusdtVerifyGeneric(body, h.C().BEpusdt.APIToken) {
		h.bepusdtSigFail(c, "invalid_signature", "signature verification failed")
		return
	}
	now := time.Now()
	text := func(k string) string { v, _ := body[k].(string); return strings.TrimSpace(v) }
	at, err := strconv.ParseInt(text("timestamp"), 10, 64)
	tradeID, orderID := text("trade_id"), text("order_id")
	if err != nil || at < now.Unix()-60 || at > now.Unix()+10 || tradeID == "" || orderID == "" || len(tradeID) > 128 || len(orderID) > 128 {
		h.bepusdtFail(c, "invalid review request")
		return
	}
	if h.replays != nil && !h.replays.accept("cancellation-review", body, now) {
		h.bepusdtFail(c, "replayed review request")
		return
	}
	review, err := h.st.CancellationReviewByReference(c.Request.Context(), tradeID, orderID)
	if err != nil {
		h.bepusdtFail(c, "review unavailable")
		return
	}
	c.JSON(200, bepusdtResponse{StatusCode: 200, Message: "success", Data: gin.H{"trade_id": tradeID, "order_id": orderID, "generation": review.Generation, "refund_recorded": review.Current(), "cancellation_review_status": review.Status()}})
}
