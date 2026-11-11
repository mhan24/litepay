package worker

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"gorm.io/gorm"

	"litepay/internal/config"
	"litepay/internal/model"
	"litepay/internal/settings"
	"litepay/internal/store"
)

func TestRuntimeCancellationOutageCollectsDepositsWithoutDelivery(t *testing.T) {
	ctx := context.Background()
	st, err := store.Open(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	var delivered atomic.Int64
	merchant := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { delivered.Add(1); fmt.Fprint(w, "success") }))
	defer merchant.Close()
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
	r, _ := st.Recovery(ctx)
	health := model.IntegrationHealth{Name: "dujiao_cancel_sync", Generation: r.Generation, Source: "sqlite", Healthy: true, ReportedAt: time.Now().Unix()}
	if err = st.AcknowledgeRecovery(ctx, r.Generation, true, health); err != nil {
		t.Fatal(err)
	}
	_, err = st.Create(ctx, model.Order{TradeNo: "runtime-deposit", OutTradeNo: "runtime-deposit", OriginUnits: 1000000, ExpiresAt: time.Now().Add(time.Hour), NotifyURL: merchant.URL})
	if err != nil {
		t.Fatal(err)
	}
	o, err := st.Select(ctx, "runtime-deposit", "binance", "USDT", "receiver", 2, 10000, 10000, 99, 120)
	if err != nil {
		t.Fatal(err)
	}
	// The generation is already acknowledged when the bridge disappears.
	if err = st.DB().Model(&model.IntegrationHealth{}).Where("name=?", health.Name).Update("received_at", time.Now().Add(-time.Minute)).Error; err != nil {
		t.Fatal(err)
	}
	event := model.Transaction{Provider: "binance", EventID: "outage-deposit", Reference: "outage-deposit", Receiver: "receiver", Currency: "USDT", Units: o.PayUnits, OccurredAt: time.Now(), State: model.Confirmed}
	p := NewPoller(mgr, st)
	p.clients["binance"] = &connection{config: *mgr.Get(), source: &fixture{rows: []model.Transaction{event}}}
	if err = p.Scan(ctx, "binance"); err != nil {
		t.Fatal(err)
	}
	var evidence model.Transaction
	if err = st.DB().First(&evidence, "event_id=?", event.EventID).Error; err != nil || evidence.State != model.Confirmed || evidence.OrderID == nil || *evidence.OrderID != o.ID {
		t.Fatal(evidence, err)
	}
	cursor, err := st.Cursor(ctx, "binance")
	if err != nil || cursor.ScannedUntil.Before(event.OccurredAt) {
		t.Fatal(cursor, err)
	}
	n := NewNotifier(mgr, st)
	n.UseTestTransport()
	assertHeld := func() {
		t.Helper()
		if did, e := n.Deliver(ctx); did || e != nil {
			t.Fatal("unsafe delivery", did, e)
		}
		var cb model.Callback
		if e := st.DB().First(&cb, "order_id=?", o.ID).Error; e != nil || cb.Attempts != 0 || cb.DeliveredAt != nil {
			t.Fatal(cb, e)
		}
		if delivered.Load() != 0 {
			t.Fatal("merchant received paused callback")
		}
	}
	assertHeld()
	// A live source cannot release delivery until its reconciliation is complete.
	health.ReportedAt++
	if err = st.AcknowledgeRecovery(ctx, r.Generation, false, health); err != nil {
		t.Fatal(err)
	}
	assertHeld()
	// Conflicts also block delivery, even if a buggy signed sender claims to be healthy.
	health.ReportedAt++
	health.Conflicts = 1
	if err = st.AcknowledgeRecovery(ctx, r.Generation, true, health); err != nil {
		t.Fatal(err)
	}
	assertHeld()
	health.ReportedAt++
	health.Conflicts = 0
	if err = st.AcknowledgeRecovery(ctx, r.Generation, true, health); err != nil {
		t.Fatal(err)
	}
	// Lose health after the callback lease was taken but before its HTTP request.
	hook := "test:bridge_fails_during_signing"
	if err = st.DB().Callback().Query().After("gorm:query").Register(hook, func(db *gorm.DB) {
		if db.Statement.Table == "signing_keys" {
			if e := st.DB().Model(&model.IntegrationHealth{}).Where("name=?", health.Name).Update("healthy", false).Error; e != nil {
				t.Error(e)
			}
		}
	}); err != nil {
		t.Fatal(err)
	}
	assertHeld()
	if err = st.DB().Callback().Query().Remove(hook); err != nil {
		t.Fatal(err)
	}
	health.ReportedAt++
	if err = st.AcknowledgeRecovery(ctx, r.Generation, true, health); err != nil {
		t.Fatal(err)
	}
	if did, e := n.Deliver(ctx); !did || e != nil {
		t.Fatal(did, e)
	}
	if delivered.Load() != 1 {
		t.Fatal(delivered.Load())
	}
	if did, e := n.Deliver(ctx); did || e != nil {
		t.Fatal("duplicate delivery", did, e)
	}
}
