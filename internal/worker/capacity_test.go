package worker

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"litepay/internal/config"
	"litepay/internal/model"
	"litepay/internal/settings"
	"litepay/internal/store"
)

func TestScanProgressWithExpiryBacklog(t *testing.T) {
	ctx := context.Background()
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
	mgr := settings.New(cfg, st)
	if err = mgr.Load(ctx); err != nil {
		t.Fatal(err)
	}
	now := time.Now()
	selected, expires, settle := now.Add(-50*time.Hour), now.Add(-49*time.Hour), now.Add(-time.Hour)
	old := make([]model.Order, 10000)
	for i := range old {
		old[i] = model.Order{TradeNo: fmt.Sprint("backlog-", i), OutTradeNo: fmt.Sprint("backlog-", i), OriginUnits: 30000000, PayUnits: 30000000 + int64(i), Precision: 6, Provider: "binance", Currency: "USDT", Receiver: "receiver", SelectedAt: &selected, ExpiresAt: expires, NextExpiryCheckAt: settle, Status: model.Ready}
	}
	if err = st.DB().CreateInBatches(&old, 200).Error; err != nil {
		t.Fatal(err)
	}
	attempts := make([]model.PaymentAttempt, len(old))
	for i, o := range old {
		attempts[i] = model.PaymentAttempt{OrderID: o.ID, Provider: o.Provider, Receiver: o.Receiver, Currency: o.Currency, Units: o.PayUnits, Precision: 6, SelectedAt: selected, ExpiresAt: expires, SettleUntil: settle}
	}
	if err = st.DB().CreateInBatches(&attempts, 200).Error; err != nil {
		t.Fatal(err)
	}
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	stop := make(chan struct{})
	var wg sync.WaitGroup
	wg.Go(func() {
		for {
			select {
			case <-stop:
				return
			case <-ticker.C:
				if e := st.Expire(ctx, 120); e != nil {
					t.Error(e)
					return
				}
			}
		}
	})
	defer func() { close(stop); wg.Wait() }()
	db, _ := st.DB().DB()
	before := db.Stats()
	start := time.Now()
	var events []model.Transaction
	for i := range 50 {
		no := fmt.Sprint("load-pay-", i)
		_, err = st.Create(ctx, model.Order{TradeNo: no, OutTradeNo: no, OriginUnits: 10000000, ExpiresAt: now.Add(time.Hour)})
		if err != nil {
			t.Fatal(err)
		}
		o, e := st.Select(ctx, no, "binance", "USDT", "receiver", 2, 10000, 10000, 99, 120)
		if e != nil {
			t.Fatal(e)
		}
		events = append(events, model.Transaction{Provider: "binance", EventID: no, Reference: no, Currency: "USDT", Receiver: "receiver", Units: o.PayUnits, OccurredAt: time.Now(), State: model.Confirmed})
	}
	p := NewPoller(mgr, st)
	source := &fixture{rows: events}
	p.clients["binance"] = &connection{config: *mgr.Get(), source: source}
	paid := int64(0)
	scans := 0
	for ; scans < 60; scans++ {
		if err = p.Scan(ctx, "binance"); err != nil {
			t.Fatal(err)
		}
		st.DB().Model(&model.Order{}).Where("trade_no LIKE ? AND status = ?", "load-pay-%", model.Paid).Count(&paid)
		if paid == 50 {
			break
		}
	}
	after := db.Stats()
	if paid != 50 {
		t.Fatal("payment scan backlog did not drain", paid, scans)
	}
	t.Logf("10k settling orders; 100Hz expiry stress; 50 new payments all settled in %s, scan rounds=%d, DB waits=%d total=%s (mock provider, not production throughput)", time.Since(start), scans+1, after.WaitCount-before.WaitCount, after.WaitDuration-before.WaitDuration)
}
