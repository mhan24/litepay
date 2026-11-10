package server

import (
	"context"
	"github.com/gin-gonic/gin"
	"litepay/internal/config"
	"litepay/internal/model"
	"litepay/internal/settings"
	"litepay/internal/store"
	"litepay/internal/worker"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func TestPublicReadLimitCombinesCashierAndStatus(t *testing.T) {
	h := &Handlers{readLimit: newSelectionLimiter()}
	r := gin.New()
	r.SetTrustedProxies(nil)
	calls := 0
	handler := func(c *gin.Context) { calls++; c.Status(200) }
	r.GET("/pay/:trade_no", h.LimitPublicRead, handler)
	r.GET("/api/order/status", h.LimitPublicRead, handler)
	for i := range 100 {
		path := "/pay/one"
		if i%2 == 0 {
			path = "/api/order/status?order_id=one"
		}
		w := getRequest(r, path)
		if i < 60 && w.Code != 200 || i >= 60 && (w.Code != 429 || w.Header().Get("Retry-After") == "") {
			t.Fatal(i, w.Code)
		}
	}
	if calls != 60 {
		t.Fatal(calls)
	}
	if w := getRequest(r, "/pay/two"); w.Code != 200 {
		t.Fatal(w.Code)
	}
}

func TestRequiredCancellationHeartbeatControlsReadiness(t *testing.T) {
	ctx := context.Background()
	st, err := store.Open(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	cfg := config.Default()
	cfg.Server.Production = false
	cfg.Server.CancelSyncRequired = true
	cfg.Binance.APIKey = "key"
	cfg.Binance.APISecret = "secret"
	cfg.Checkout.Binance.UID = "receiver"
	mgr := settings.New(cfg, st)
	if err = mgr.Load(ctx); err != nil {
		t.Fatal(err)
	}
	engine, err := New(mgr, st, worker.NewPoller(mgr, st))
	if err != nil {
		t.Fatal(err)
	}
	st.ScanResult(ctx, "binance", time.Now(), nil)
	if w := getRequest(engine, "/readyz"); w.Code != 503 || !strings.Contains(w.Body.String(), "取消同步") {
		t.Fatal(w.Code, w.Body.String())
	}
	recovery, _ := st.Recovery(ctx)
	health := model.IntegrationHealth{Generation: recovery.Generation, Name: "dujiao_cancel_sync", Source: "sqlite", ReportedAt: time.Now().Unix(), Healthy: true}
	if err = st.AcknowledgeRecovery(ctx, recovery.Generation, true, health); err != nil {
		t.Fatal(err)
	}
	if w := getRequest(engine, "/readyz"); w.Code != 200 {
		t.Fatal(w.Code, w.Body.String())
	}
	st.DB().Model(&model.IntegrationHealth{}).Where("name=?", health.Name).Update("received_at", time.Now().Add(-time.Minute))
	if w := getRequest(engine, "/readyz"); w.Code != 503 {
		t.Fatal(w.Code)
	}
	// An old, replayed success report cannot refresh a dead bridge.
	if err = st.AcknowledgeRecovery(ctx, recovery.Generation, true, health); err != nil {
		t.Fatal(err)
	}
	if w := getRequest(engine, "/readyz"); w.Code != 503 {
		t.Fatal("replay made readiness healthy", w.Code)
	}
	w := httptest.NewRecorder()
	engine.ServeHTTP(w, httptest.NewRequest("POST", "/api/v1/order/cancel-sync-status", strings.NewReader(`{"signature":"fake"}`)))
	if strings.Contains(w.Body.String(), `"status_code":200`) {
		t.Fatal("unsigned health accepted")
	}
}

func TestMissingGatewayPreventsHealthyReadinessEvenWhenSignedHealthy(t *testing.T) {
	ctx := context.Background()
	st, err := store.Open(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	cfg := config.Default()
	cfg.Server.Production = false
	cfg.Server.CancelSyncRequired = true
	mgr := settings.New(cfg, st)
	if err = mgr.Load(ctx); err != nil {
		t.Fatal(err)
	}
	engine, err := New(mgr, st, worker.NewPoller(mgr, st))
	if err != nil {
		t.Fatal(err)
	}
	recovery, _ := st.Recovery(ctx)
	health := model.IntegrationHealth{Name: "dujiao_cancel_sync", Source: "sqlite", Generation: recovery.Generation, ReportedAt: time.Now().Unix(), Healthy: true, Missing: 1}
	if err = st.AcknowledgeRecovery(ctx, recovery.Generation, true, health); err != nil {
		t.Fatal(err)
	}
	current, _ := st.Recovery(ctx)
	if current.Acknowledged {
		t.Fatal("missing exception acknowledged recovery")
	}
	if w := getRequest(engine, "/readyz"); w.Code != 503 || !strings.Contains(w.Body.String(), "取消同步") {
		t.Fatal(w.Code, w.Body.String())
	}
}
