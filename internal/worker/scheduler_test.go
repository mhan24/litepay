package worker

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"gorm.io/gorm"
	"litepay/internal/config"
	"litepay/internal/model"
	"litepay/internal/settings"
	"litepay/internal/store"
)

type isolatedSource struct {
	uid     string
	entered chan struct{}
	block   bool
	once    sync.Once
}

func (s *isolatedSource) AccountUID(context.Context) (string, error) { return s.uid, nil }
func (s *isolatedSource) History(ctx context.Context, from, to time.Time) ([]model.Transaction, error) {
	s.once.Do(func() { close(s.entered) })
	if s.block {
		<-ctx.Done()
		return nil, ctx.Err()
	}
	return nil, nil
}

func TestIndependentProviderSchedulingAndExpiryDuringTimeout(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	st, err := store.Open(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	cfg := config.Default()
	cfg.Server.Production = false
	cfg.Binance.APIKey = "key"
	cfg.Binance.APISecret = "secret"
	cfg.Checkout.Binance.UID = "receiver"
	cfg.OKX.APIKey = "key"
	cfg.OKX.APISecret = "secret"
	cfg.OKX.Passphrase = "pass"
	cfg.Checkout.OKX.UID = "receiver"
	mgr := settings.New(cfg, st)
	if err = mgr.Load(ctx); err != nil {
		t.Fatal(err)
	}
	p := NewPoller(mgr, st)
	slow := &isolatedSource{uid: "receiver", entered: make(chan struct{}), block: true}
	fast := &isolatedSource{uid: "receiver", entered: make(chan struct{})}
	p.clients["binance"] = &connection{config: *mgr.Get(), source: slow}
	p.clients["okx"] = &connection{config: *mgr.Get(), source: fast}
	_, err = st.Create(ctx, model.Order{TradeNo: "expiry-worker", OutTradeNo: "expiry-worker", OriginUnits: 1000000, ExpiresAt: time.Now().Add(-time.Hour)})
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan struct{})
	go func() { p.Run(ctx); close(done) }()
	for name, ch := range map[string]chan struct{}{"slow": slow.entered, "fast": fast.entered} {
		select {
		case <-ch:
		case <-time.After(2 * time.Second):
			t.Fatal("provider blocked", name)
		}
	}
	deadline := time.Now().Add(3 * time.Second)
	for {
		o, _ := st.GetByTradeNo(ctx, "expiry-worker")
		if o.Status == model.Expired {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("expiry blocked by provider")
		}
		time.Sleep(20 * time.Millisecond)
	}
	cancel()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("scheduler did not shut down")
	}
}

type panickingSource struct {
	entered chan struct{}
	once    sync.Once
}

func (s *panickingSource) AccountUID(context.Context) (string, error) { return "receiver", nil }
func (s *panickingSource) History(context.Context, time.Time, time.Time) ([]model.Transaction, error) {
	s.once.Do(func() { close(s.entered) })
	panic("simulated provider panic")
}

func TestPollerRunRecoversFromProviderPanicAndKeepsOtherProvidersAndExpiryRunning(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	st, err := store.Open(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	cfg := config.Default()
	cfg.Server.Production = false
	cfg.Binance.APIKey = "key"
	cfg.Binance.APISecret = "secret"
	cfg.Checkout.Binance.UID = "receiver"
	cfg.OKX.APIKey = "key"
	cfg.OKX.APISecret = "secret"
	cfg.OKX.Passphrase = "pass"
	cfg.Checkout.OKX.UID = "receiver"
	mgr := settings.New(cfg, st)
	if err = mgr.Load(ctx); err != nil {
		t.Fatal(err)
	}
	p := NewPoller(mgr, st)
	panicSrc := &panickingSource{entered: make(chan struct{})}
	healthySrc := &isolatedSource{uid: "receiver", entered: make(chan struct{})}
	p.clients["binance"] = &connection{config: *mgr.Get(), source: panicSrc}
	p.clients["okx"] = &connection{config: *mgr.Get(), source: healthySrc}
	_, err = st.Create(ctx, model.Order{TradeNo: "expiry-during-panic", OutTradeNo: "expiry-during-panic", OriginUnits: 1000000, ExpiresAt: time.Now().Add(-time.Hour)})
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan struct{})
	go func() { p.Run(ctx); close(done) }()
	for name, ch := range map[string]chan struct{}{"panic": panicSrc.entered, "healthy": healthySrc.entered} {
		select {
		case <-ch:
		case <-time.After(2 * time.Second):
			t.Fatal("provider did not run", name)
		}
	}
	deadline := time.Now().Add(3 * time.Second)
	for {
		o, _ := st.GetByTradeNo(ctx, "expiry-during-panic")
		if o.Status == model.Expired {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("expiry did not run after provider panic")
		}
		time.Sleep(20 * time.Millisecond)
	}
	cancel()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("scheduler did not shut down cleanly after panic recovery")
	}
}

func TestNotifierRunRecoversFromPanicAndReleasesLeaseOnShutdownCancel(t *testing.T) {
	ctx := context.Background()
	st, err := store.Open(filepath.Join(t.TempDir(), "notifier.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	var delivered atomic.Int64
	merchant := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		delivered.Add(1)
		fmt.Fprint(w, "success")
	}))
	defer merchant.Close()
	cfg := config.Default()
	cfg.Server.Production = false
	cfg.Server.CallbackWorkers = 1
	cfg.Notify.MaxAttempts = 1
	mgr := settings.New(cfg, st)
	if err = mgr.Load(ctx); err != nil {
		t.Fatal(err)
	}
	_, err = st.Create(ctx, model.Order{TradeNo: "panic-recover-cb", OutTradeNo: "panic-recover-cb", OriginUnits: 1000000, NotifyURL: merchant.URL, ExpiresAt: time.Now().Add(time.Hour)})
	if err != nil {
		t.Fatal(err)
	}
	o, err := st.Select(ctx, "panic-recover-cb", "binance", "USDT", "receiver", 2, 10000, 10000, 99, 120)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = st.Process(ctx, model.Transaction{Provider: "binance", EventID: "panic-ev", Receiver: "receiver", Currency: "USDT", Units: o.PayUnits, OccurredAt: time.Now(), State: model.Confirmed}, ""); err != nil {
		t.Fatal(err)
	}

	var panicked atomic.Bool
	hook := "test:notifier_panic_once"
	if err = st.DB().Callback().Query().After("gorm:query").Register(hook, func(db *gorm.DB) {
		if db.Statement.Table == "signing_keys" && panicked.CompareAndSwap(false, true) {
			panic("simulated notifier panic")
		}
	}); err != nil {
		t.Fatal(err)
	}
	// Make lease expire quickly so the recovered worker can re-reserve or reset lease.
	n := NewNotifier(mgr, st)
	n.UseTestTransport()
	runCtx, cancel := context.WithCancel(ctx)
	done := make(chan struct{})
	go func() { n.Run(runCtx); close(done) }()
	deadline := time.Now().Add(3 * time.Second)
	for !panicked.Load() {
		if time.Now().After(deadline) {
			cancel()
			<-done
			t.Fatal("panic hook never triggered")
		}
		time.Sleep(10 * time.Millisecond)
	}
	_ = st.DB().Callback().Query().Remove(hook)
	// Clear lease so the recovered worker can deliver immediately without waiting 15s.
	if err = st.DB().Model(&model.Callback{}).Where("order_id = ?", o.ID).Updates(map[string]any{"state": "queued", "attempts": 0, "next_at": time.Now(), "lease_until": time.Time{}}).Error; err != nil {
		cancel()
		<-done
		t.Fatal(err)
	}
	deadline = time.Now().Add(3 * time.Second)
	for delivered.Load() < 1 {
		if time.Now().After(deadline) {
			cancel()
			<-done
			t.Fatal("recovered notifier worker did not deliver callback")
		}
		time.Sleep(10 * time.Millisecond)
	}
	cancel()
	<-done

	// Part 2: shutdown cancellation during in-flight HTTP request releases lease without exhausting attempts.
	inFlight := make(chan struct{})
	stopSlow := make(chan struct{})
	slowMerchant := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		close(inFlight)
		select {
		case <-r.Context().Done():
		case <-stopSlow:
		}
	}))
	defer slowMerchant.Close()
	defer close(stopSlow)
	_, err = st.Create(ctx, model.Order{TradeNo: "shutdown-release-cb", OutTradeNo: "shutdown-release-cb", OriginUnits: 2000000, NotifyURL: slowMerchant.URL, ExpiresAt: time.Now().Add(time.Hour)})
	if err != nil {
		t.Fatal(err)
	}
	o2, err := st.Select(ctx, "shutdown-release-cb", "binance", "USDT", "receiver", 2, 10000, 10000, 99, 120)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = st.Process(ctx, model.Transaction{Provider: "binance", EventID: "shutdown-ev", Receiver: "receiver", Currency: "USDT", Units: o2.PayUnits, OccurredAt: time.Now(), State: model.Confirmed}, ""); err != nil {
		t.Fatal(err)
	}
	runCtx2, cancel2 := context.WithCancel(ctx)
	done2 := make(chan struct{})
	go func() { n.Run(runCtx2); close(done2) }()
	select {
	case <-inFlight:
	case <-time.After(2 * time.Second):
		cancel2()
		<-done2
		t.Fatal("slow merchant never received request")
	}
	cancel2()
	select {
	case <-done2:
	case <-time.After(2 * time.Second):
		t.Fatal("notifier did not shut down cleanly")
	}
	var cb model.Callback
	if err = st.DB().First(&cb, "order_id = ?", o2.ID).Error; err != nil {
		t.Fatal(err)
	}
	if cb.State != "queued" || cb.Attempts != 0 {
		t.Fatalf("expected shutdown cancellation to release callback lease (queued, attempts=0), got state=%s attempts=%d", cb.State, cb.Attempts)
	}
}
