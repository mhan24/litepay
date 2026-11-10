package worker

import (
	"context"
	"sync"
	"testing"
	"time"

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
