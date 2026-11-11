package server

import (
	"github.com/gin-gonic/gin"
	"io"
	"litepay/internal/model"
	"litepay/internal/sign"
	"net/http"
	"strconv"
	"time"
)

func (h *Handlers) CancelSyncHeartbeat(c *gin.Context) {
	raw, err := io.ReadAll(http.MaxBytesReader(c.Writer, c.Request.Body, 4096))
	if err != nil {
		h.bepusdtFail(c, "invalid health report")
		return
	}
	body, err := decodeBEpusdtJSON(raw)
	if err != nil || !sign.BEpusdtVerifyGeneric(body, h.C().BEpusdt.APIToken) {
		h.bepusdtFail(c, "signature verification failed")
		return
	}
	text := func(key string) string { v, _ := body[key].(string); return v }
	stamp, err := strconv.ParseInt(text("timestamp"), 10, 64)
	if err != nil || stamp < time.Now().Unix()-60 || stamp > time.Now().Unix()+10 {
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
	report := model.IntegrationHealth{DiscoveryPending: counts[4], DiscoveryLagSeconds: counts[5], Generation: generation, Missing: counts[3], Name: "dujiao_cancel_sync", Source: source, ReportedAt: stamp, Healthy: text("healthy") == "true", Pending: counts[0], Failures: counts[1], Conflicts: counts[2]}
	if err = h.st.AcknowledgeRecovery(c.Request.Context(), generation, text("replay_complete") == "true", report); err != nil {
		h.bepusdtFail(c, "health write failed")
		return
	}
	c.JSON(200, bepusdtResponse{StatusCode: 200, Message: "success"})
}
func (h *Handlers) AdminOperations(c *gin.Context) {
	stats, err := h.st.QueueStats(c.Request.Context())
	if err != nil {
		c.JSON(503, gin.H{"error": "queue metrics unavailable"})
		return
	}
	health, _ := h.st.IntegrationHealth(c.Request.Context())
	c.JSON(200, gin.H{"recovery": func() any { r, _ := h.st.Recovery(c.Request.Context()); return r }(), "payment_operations_ready": !h.paymentOperationsBlocked(c), "queues": stats, "cancel_sync": health, "binance": h.poller.BinanceUsage(), "cancel_sync_required": h.C().Server.CancelSyncRequired, "production": h.C().Server.Production, "providers": h.providerHealths(c)})
}

func (h *Handlers) CancelSyncGeneration(c *gin.Context) {
	raw, err := io.ReadAll(http.MaxBytesReader(c.Writer, c.Request.Body, 4096))
	if err != nil {
		h.bepusdtFail(c, "invalid generation request")
		return
	}
	body, err := decodeBEpusdtJSON(raw)
	if err != nil || !sign.BEpusdtVerifyGeneric(body, h.C().BEpusdt.APIToken) {
		h.bepusdtFail(c, "signature verification failed")
		return
	}
	stamp, _ := body["timestamp"].(string)
	at, err := strconv.ParseInt(stamp, 10, 64)
	if err != nil || at < time.Now().Unix()-60 || at > time.Now().Unix()+10 {
		h.bepusdtFail(c, "stale generation request")
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
	raw, err := io.ReadAll(http.MaxBytesReader(c.Writer, c.Request.Body, 4096))
	if err != nil {
		h.bepusdtFail(c, "invalid review request")
		return
	}
	body, err := decodeBEpusdtJSON(raw)
	if err != nil || !sign.BEpusdtVerifyGeneric(body, h.C().BEpusdt.APIToken) {
		h.bepusdtFail(c, "signature verification failed")
		return
	}
	text := func(k string) string { v, _ := body[k].(string); return v }
	at, err := strconv.ParseInt(text("timestamp"), 10, 64)
	if err != nil || at < time.Now().Unix()-60 || at > time.Now().Unix()+10 || text("trade_id") == "" || text("order_id") == "" || len(text("trade_id")) > 128 || len(text("order_id")) > 128 {
		h.bepusdtFail(c, "invalid review request")
		return
	}
	reviewed, generation, err := h.st.CancellationRefundReviewStatus(c.Request.Context(), text("trade_id"), text("order_id"))
	if err != nil {
		h.bepusdtFail(c, "review unavailable")
		return
	}
	c.JSON(200, bepusdtResponse{StatusCode: 200, Message: "success", Data: gin.H{"trade_id": text("trade_id"), "order_id": text("order_id"), "generation": generation, "refund_recorded": reviewed}})
}
