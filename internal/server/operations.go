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
	counts := make([]int64, 3)
	for i, key := range []string{"pending", "failures", "conflicts"} {
		counts[i], err = strconv.ParseInt(text(key), 10, 64)
		if err != nil || counts[i] < 0 || counts[i] > 100000000 {
			h.bepusdtFail(c, "invalid count")
			return
		}
	}
	report := model.IntegrationHealth{Name: "dujiao_cancel_sync", Source: source, ReportedAt: stamp, Healthy: text("healthy") == "true", Pending: counts[0], Failures: counts[1], Conflicts: counts[2]}
	if err = h.st.SaveIntegrationHealth(c.Request.Context(), report); err != nil {
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
	c.JSON(200, gin.H{"queues": stats, "cancel_sync": health, "binance": h.poller.BinanceUsage(), "cancel_sync_required": h.C().Server.CancelSyncRequired})
}
