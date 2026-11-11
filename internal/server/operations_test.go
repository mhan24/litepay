package server

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"github.com/gin-gonic/gin"
	"litepay/internal/config"
	"litepay/internal/model"
	"litepay/internal/settings"
	"litepay/internal/sign"
	"litepay/internal/store"
	"net/http/httptest"
	"net/url"
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
	p := verifiedPoller(t, mgr, st)
	engine, err := New(mgr, st, p)
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
	primeScanners(t, p, mgr)
	if w := getRequest(engine, "/readyz"); w.Code != 200 {
		t.Fatal(w.Code, w.Body.String())
	}
	_, err = st.Create(ctx, model.Order{TradeNo: "runtime-pause", OutTradeNo: "runtime-pause", OriginUnits: 1000000, ExpiresAt: time.Now().Add(time.Hour)})
	if err != nil {
		t.Fatal(err)
	}
	if w := getRequest(engine, "/pay/runtime-pause"); w.Code != 200 {
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

	if r, _ := st.Recovery(ctx); !r.Acknowledged {
		t.Fatal("runtime failure changed recovery generation acknowledgement")
	}
	if w := postForm(engine, "/api/v1/order/create-order", nil); !strings.Contains(w.Body.String(), "暂停接单") {
		t.Fatal(w.Code, w.Body.String())
	}
	if w := getRequest(engine, "/pay/runtime-pause"); w.Code != 503 {
		t.Fatal(w.Code, w.Body.String())
	}
	if w := postForm(engine, "/api/order/payment", url.Values{"trade_no": {"runtime-pause"}, "provider": {"binance"}, "currency": {"USDT"}}); w.Code != 503 {
		t.Fatal(w.Code, w.Body.String())
	}
	if w := getRequest(engine, "/api/order/status?order_id=runtime-pause"); !strings.Contains(w.Body.String(), `"accepting_payment":false`) {
		t.Fatal(w.Body.String())
	}
	health.ReportedAt++
	if err = st.AcknowledgeRecovery(ctx, recovery.Generation, false, health); err != nil {
		t.Fatal(err)
	}
	if w := getRequest(engine, "/pay/runtime-pause"); w.Code != 503 {
		t.Fatal("fresh incomplete reconciliation released intake", w.Code)
	}
	// A replayed success must not acknowledge a report rejected as stale.
	if err = st.AcknowledgeRecovery(ctx, recovery.Generation, true, health); err != nil {
		t.Fatal(err)
	}
	if w := getRequest(engine, "/readyz"); w.Code != 503 {
		t.Fatal("replayed complete flag released intake", w.Code)
	}
	health.ReportedAt++
	if err = st.AcknowledgeRecovery(ctx, recovery.Generation, true, health); err != nil {
		t.Fatal(err)
	}
	primeScanners(t, p, mgr)
	if w := getRequest(engine, "/readyz"); w.Code != 200 {
		t.Fatal(w.Code, w.Body.String())
	}
	if w := getRequest(engine, "/pay/runtime-pause"); w.Code != 200 {
		t.Fatal(w.Code, w.Body.String())
	}
	if w := postForm(engine, "/api/order/payment", url.Values{"trade_no": {"runtime-pause"}, "provider": {"binance"}, "currency": {"USDT"}}); w.Code != 200 {
		t.Fatal(w.Code, w.Body.String())
	}
	if w := getRequest(engine, "/api/order/status?order_id=runtime-pause"); !strings.Contains(w.Body.String(), `"accepting_payment":true`) {
		t.Fatal(w.Body.String())
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
	p := verifiedPoller(t, mgr, st)
	engine, err := New(mgr, st, p)
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

func TestPaidConflictRemainsUnreadyAfterAcknowledgement(t *testing.T) {
	for _, required := range []bool{true, false} {
		t.Run(map[bool]string{true: "required", false: "optional"}[required], func(t *testing.T) {
			ctx := context.Background()
			st, err := store.Open(":memory:")
			if err != nil {
				t.Fatal(err)
			}
			defer st.Close()
			cfg := config.Default()
			cfg.Server.Production = false
			cfg.Server.CancelSyncRequired = required
			cfg.Binance.APIKey = "key"
			cfg.Binance.APISecret = "secret"
			cfg.Checkout.Binance.UID = "receiver"
			mgr := settings.New(cfg, st)
			if err = mgr.Load(ctx); err != nil {
				t.Fatal(err)
			}
			p := verifiedPoller(t, mgr, st)
			engine, err := New(mgr, st, p)
			if err != nil {
				t.Fatal(err)
			}
			if err = st.ScanResult(ctx, "binance", time.Now(), nil); err != nil {
				t.Fatal(err)
			}
			r, _ := st.Recovery(ctx)
			health := model.IntegrationHealth{Name: "dujiao_cancel_sync", Generation: r.Generation, Healthy: true, ReportedAt: time.Now().Unix()}
			if err = st.AcknowledgeRecovery(ctx, r.Generation, true, health); err != nil {
				t.Fatal(err)
			}
			primeScanners(t, p, mgr)
			if w := getRequest(engine, "/readyz"); w.Code != 200 {
				t.Fatal(w.Code, w.Body.String())
			}
			health.ReportedAt++
			health.Conflicts = 1
			// Defense against a signed sender reporting healthy=true despite its conflict count.
			if err = st.AcknowledgeRecovery(ctx, r.Generation, true, health); err != nil {
				t.Fatal(err)
			}
			if w := getRequest(engine, "/readyz"); w.Code != 503 {
				t.Fatal(w.Code, w.Body.String())
			}
			h, _ := st.IntegrationHealth(ctx)
			if h.Healthy || h.Conflicts != 1 {
				t.Fatal(h)
			}
			r, _ = st.Recovery(ctx)
			if !r.Acknowledged {
				t.Fatal("lost acknowledged recovery")
			}
		})
	}
}

func TestSignedHeartbeatCannotHideUndiscoveredSourceWork(t *testing.T) {
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
	engine, err := New(mgr, st, verifiedPoller(t, mgr, st))
	if err != nil {
		t.Fatal(err)
	}
	r, _ := st.Recovery(ctx)
	body := map[string]any{"generation": r.Generation, "timestamp": fmt.Sprint(time.Now().Unix()), "source": "sqlite", "healthy": "true", "pending": "0", "failures": "0", "missing": "0", "conflicts": "0", "replay_complete": "true"}
	send := func() string {
		t.Helper()
		body["signature"] = sign.BEpusdtSignGeneric(body, mgr.Get().BEpusdt.APIToken)
		raw, _ := json.Marshal(body)
		w := httptest.NewRecorder()
		engine.ServeHTTP(w, httptest.NewRequest("POST", "/api/v1/order/cancel-sync-status", bytes.NewReader(raw)))
		return w.Body.String()
	}
	if result := send(); strings.Contains(result, `"status_code":200`) {
		t.Fatal("legacy report without discovery evidence accepted", result)
	}
	delete(body, "signature")
	body["discovery_pending"] = "1"
	body["discovery_lag_seconds"] = "120"
	if result := send(); !strings.Contains(result, `"status_code":200`) {
		t.Fatal(result)
	}
	health, err := st.IntegrationHealth(ctx)
	if err != nil || health.Healthy || health.DiscoveryPending != 1 || health.DiscoveryLagSeconds != 120 {
		t.Fatal(health, err)
	}
	r, _ = st.Recovery(ctx)
	if r.Acknowledged {
		t.Fatal("empty local queue hid unread source backlog")
	}
}
