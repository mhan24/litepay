package worker

import (
	"context"
	"encoding/json"
	"fmt"
	"litepay/internal/config"
	"litepay/internal/model"
	"litepay/internal/settings"
	"litepay/internal/store"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestCallbackPoolDrainsThousandWithoutDuplicateReservations(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	st, err := store.Open(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	var inFlight, peak, delivered, firstWave atomic.Int64
	gate := make(chan struct{})
	var mu sync.Mutex
	seen := map[string]bool{}
	merchant := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		current := inFlight.Add(1)
		defer inFlight.Add(-1)
		for old := peak.Load(); current > old; old = peak.Load() {
			if peak.CompareAndSwap(old, current) {
				break
			}
		}
		var body map[string]any
		if e := json.NewDecoder(r.Body).Decode(&body); e != nil {
			t.Error(e)
			w.WriteHeader(400)
			return
		}
		no, _ := body["trade_id"].(string)
		mu.Lock()
		if seen[no] {
			t.Errorf("duplicate reservation sent for %s", no)
		}
		seen[no] = true
		mu.Unlock()
		if wave := firstWave.Add(1); wave <= 8 {
			if wave == 8 {
				close(gate)
			}
			select {
			case <-gate:
			case <-r.Context().Done():
				return
			}
		}
		time.Sleep(2 * time.Millisecond)
		delivered.Add(1)
		fmt.Fprint(w, "success")
	}))
	defer merchant.Close()
	cfg := config.Default()
	cfg.Server.Production = false
	cfg.Server.CallbackWorkers = 8
	mgr := settings.New(cfg, st)
	if err = mgr.Load(ctx); err != nil {
		t.Fatal(err)
	}
	var key model.SigningKey
	st.DB().First(&key)
	now := time.Now()
	orders := make([]model.Order, 1000)
	for i := range orders {
		no := fmt.Sprint("notify-load-", i)
		orders[i] = model.Order{TradeNo: no, OutTradeNo: no, SigningKeyID: key.ID, OriginUnits: 1000000, PayUnits: 1000000, Precision: 6, Provider: "binance", Receiver: "receiver", Currency: "USDT", Status: model.Paid, PaidAt: &now, ExpiresAt: now.Add(time.Hour), NotifyURL: merchant.URL}
	}
	if err = st.DB().CreateInBatches(&orders, 100).Error; err != nil {
		t.Fatal(err)
	}
	callbacks := make([]model.Callback, len(orders))
	events := make([]model.Transaction, len(orders))
	for i, o := range orders {
		callbacks[i] = model.Callback{OrderID: o.ID, SigningKeyID: key.ID, NextAt: now, State: "queued"}
		id := o.ID
		events[i] = model.Transaction{Provider: "binance", EventID: o.TradeNo, OrderID: &id, State: model.Confirmed, Receiver: "receiver", Currency: "USDT", Units: o.PayUnits, OccurredAt: now}
	}
	if err = st.DB().CreateInBatches(&callbacks, 100).Error; err != nil {
		t.Fatal(err)
	}
	if err = st.DB().CreateInBatches(&events, 100).Error; err != nil {
		t.Fatal(err)
	}
	notifier := NewNotifier(mgr, st)
	notifier.UseTestTransport()
	runCtx, stop := context.WithCancel(ctx)
	done := make(chan struct{})
	start := time.Now()
	go func() { defer close(done); notifier.Run(runCtx) }()
	defer func() { stop(); <-done }()
	var acknowledged int64
	for ctx.Err() == nil {
		if err = st.DB().Model(&model.Callback{}).Where("delivered_at IS NOT NULL").Count(&acknowledged).Error; err != nil {
			t.Fatal(err)
		}
		if acknowledged == 1000 {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if acknowledged != 1000 || delivered.Load() != 1000 || peak.Load() < 2 || peak.Load() > 8 {
		t.Fatal(acknowledged, delivered.Load(), peak.Load(), ctx.Err())
	}
	t.Logf("1000 callbacks acknowledged in %s; peak concurrency=%d; mock merchant, not production throughput", time.Since(start), peak.Load())
	stats, err := st.QueueStats(ctx)
	if err != nil || stats.CallbackWaiting != 0 {
		t.Fatal(stats, err)
	}
}
