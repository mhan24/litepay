package worker

import (
	"context"
	"fmt"
	"litepay/internal/binance"
	"litepay/internal/config"
	"litepay/internal/model"
	"litepay/internal/settings"
	"litepay/internal/store"
	"testing"
	"time"
)

type denseHistory struct{ fixture }

func (d *denseHistory) History(ctx context.Context, from, to time.Time) ([]model.Transaction, error) {
	if to.Sub(from) > 10*time.Minute {
		return nil, &binance.APIError{Kind: binance.ErrorBudget}
	}
	return d.fixture.History(ctx, from, to)
}
func TestHistoricalBudgetShrinksAndResumesWithoutLosingEvidence(t *testing.T) {
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
	_, err = st.Create(ctx, model.Order{TradeNo: "dense", OutTradeNo: "dense", OriginUnits: 1000000, ExpiresAt: time.Now().Add(time.Minute)})
	if err != nil {
		t.Fatal(err)
	}
	o, err := st.Select(ctx, "dense", "binance", "USDT", "receiver", 2, 10000, 10000, 99, 120)
	if err != nil {
		t.Fatal(err)
	}
	selected := time.Now().Truncate(time.Hour).Add(-2*time.Hour + 5*time.Minute)
	expires := selected.Add(30 * time.Minute)
	st.DB().Model(o).Updates(map[string]any{"selected_at": selected, "expires_at": expires})
	st.DB().Model(&model.PaymentAttempt{}).Where("order_id=?", o.ID).Updates(map[string]any{"selected_at": selected, "expires_at": expires, "settle_until": expires.Add(store.LateVisibilityWindow)})
	source := &denseHistory{fixture: fixture{rows: []model.Transaction{{Provider: "binance", EventID: "dense-event", Receiver: "receiver", Currency: "USDT", Units: o.PayUnits, OccurredAt: selected.Add(time.Minute), State: model.Confirmed}}}}
	p := NewPoller(mgr, st)
	p.clients["binance"] = &connection{config: *mgr.Get(), source: source, uid: "receiver"}
	for range 2 {
		if err = p.scanHistorical(ctx, mgr.Get(), "binance"); err != nil {
			t.Fatal(err)
		}
	}
	job, err := st.NextHistoricalScan(ctx, "binance")
	if err != nil || !job.Cursor.Equal(selected.Truncate(time.Hour)) || job.Pages != 0 {
		t.Fatal(job, err)
	}
	for range 6 {
		if err = p.scanHistorical(ctx, mgr.Get(), "binance"); err != nil {
			t.Fatal(err)
		}
	}
	fresh, _ := st.GetByTradeNo(ctx, o.TradeNo)
	if fresh.Status != model.Paid {
		t.Fatal("dense evidence lost", fresh.Status)
	}
}

func TestOverlappingDayWindowsReuseRealCoverageAcrossCohorts(t *testing.T) {
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
	base := time.Now().Truncate(time.Hour).Add(-150 * time.Hour)
	for i := range 48 {
		selected := base.Add(time.Duration(i)*time.Hour + 5*time.Minute)
		expires := selected.Add(24 * time.Hour)
		order := model.Order{TradeNo: fmt.Sprint("overlap-", i), OutTradeNo: fmt.Sprint("overlap-", i), OriginUnits: 1000000, Status: model.Ready, Provider: "binance", Receiver: "receiver", Currency: "USDT", PayUnits: 1000000 + int64(i), SelectedAt: &selected, ExpiresAt: expires, NextExpiryCheckAt: expires.Add(store.LateVisibilityWindow)}
		if err = st.DB().Create(&order).Error; err != nil {
			t.Fatal(err)
		}
		if err = st.DB().Create(&model.PaymentAttempt{OrderID: order.ID, Provider: order.Provider, Receiver: order.Receiver, Currency: order.Currency, Units: order.PayUnits, SelectedAt: selected, ExpiresAt: expires, SettleUntil: expires.Add(store.LateVisibilityWindow)}).Error; err != nil {
			t.Fatal(err)
		}
	}
	source := &fixture{}
	p := NewPoller(mgr, st)
	p.clients["binance"] = &connection{config: *mgr.Get(), source: source, uid: "receiver"}
	for range 72 {
		if err = p.scanHistorical(ctx, mgr.Get(), "binance"); err != nil {
			t.Fatal(err)
		}
	}
	var complete int64
	st.DB().Model(&model.HistoricalScan{}).Where("state='completed'").Count(&complete)
	if complete != 48 || source.calls != 72 {
		t.Fatal("overlap caused repeated API queries", complete, source.calls)
	}
	st.ScanResult(ctx, "binance", time.Now(), nil)
	if err = st.Expire(ctx, 0); err != nil {
		t.Fatal(err)
	}
	var ready int64
	st.DB().Model(&model.Order{}).Where("status=?", model.Ready).Count(&ready)
	if ready != 0 {
		t.Fatal("shared coverage failed final settlement", ready)
	}
	t.Log("48 overlapping 24-hour windows: 72 mock API calls; coverage timestamps preserved; all final reviews complete")
}
