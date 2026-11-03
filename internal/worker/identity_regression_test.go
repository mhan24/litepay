package worker

import (
	"context"
	"fmt"
	"litepay/internal/model"
	"litepay/internal/settings"
	"litepay/internal/store"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"
)

func TestConcurrentInquiryCannotStealAnotherOrdersReference(t *testing.T) {
	f := newPollFixture(t, "OWNER")
	ctx := context.Background()
	other, err := f.st.CreateOrder(ctx, store.CreateOrderInput{Pid: "bepusdt", TradeNo: "OTHER", OutTradeNo: "M-OTHER", OriginMoney: "5.00", OriginCurrency: "USD", ExpiredAt: time.Now().Add(10 * time.Minute)})
	if err != nil {
		t.Fatal(err)
	}
	other, err = f.st.SelectPayment(ctx, other.TradeNo, "binance", "USDT", "binance-uid", 5, .01, 99, .01, 2, 2*time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	f.st.DB().Model(other).Updates(map[string]any{"created_at": time.Now().Add(-2 * time.Minute), "selected_at": time.Now().Add(-time.Minute - time.Second)})
	f.stub.set(stubBinanceTx{id: "shared-reference", orderType: "PAY", amount: f.order.PayAmount, currency: "USDT", at: time.Now(), receiver: "binance-uid"})
	var wg sync.WaitGroup
	for _, trade := range []string{f.order.TradeNo, other.TradeNo} {
		wg.Add(1)
		go func(trade string) {
			defer wg.Done()
			p := NewPoller(f.poller.cfg, f.st)
			p.CheckPayment(ctx, trade, "shared-reference")
		}(trade)
	}
	wg.Wait()
	owner, _ := f.st.GetByTradeNo(ctx, f.order.TradeNo)
	wrong, _ := f.st.GetByTradeNo(ctx, other.TradeNo)
	if owner.Status != model.StatusPaid || wrong.Status != model.StatusReady || wrong.NextNotifyAt != nil {
		t.Fatalf("ownership violated: %+v %+v", owner, wrong)
	}
	f.tick(t)
	var count, slots int64
	f.st.DB().Model(&model.IncomingTx{}).Count(&count)
	f.st.DB().Model(&model.AmountSlot{}).Where("order_id = ?", other.ID).Count(&slots)
	if count != 1 || slots != 1 {
		t.Fatalf("claims=%d other slots=%d", count, slots)
	}
}

func TestAmbiguousBinanceParentPreservesAllAssetsWithoutPaying(t *testing.T) {
	f := newPollFixture(t, "MULTI-OWNER")
	ctx := context.Background()
	other, err := f.st.CreateOrder(ctx, store.CreateOrderInput{Pid: "bepusdt", TradeNo: "MULTI-OTHER", OutTradeNo: "MULTI-OTHER", OriginMoney: "5.01", OriginCurrency: "USD", ExpiredAt: time.Now().Add(time.Hour)})
	if err != nil {
		t.Fatal(err)
	}
	other, err = f.st.SelectPayment(ctx, other.TradeNo, "binance", "USDC", "binance-uid", 5.01, .01, 99, .01, 2, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	f.st.DB().Model(other).Updates(map[string]any{"created_at": time.Now().Add(-2 * time.Minute), "selected_at": time.Now().Add(-61 * time.Second)})
	api := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/api/v3/account" {
			fmt.Fprint(w, `{"uid":"binance-uid"}`)
			return
		}
		fmt.Fprintf(w, `{"code":"000000","success":true,"data":[{"transactionId":"multi-parent","orderId":"app-multi","orderType":"PAY","transactionTime":%d,"amount":"100.00","currency":"USDT","receiverInfo":{"binanceId":"binance-uid"},"fundsDetail":[{"currency":"USDT","amount":"5.01"},{"currency":"USDC","amount":"5.02"}]}]}`, time.Now().Add(-time.Second).UnixMilli())
	}))
	defer api.Close()
	f.cfg.Binance.BaseURL = api.URL
	f.poller = NewPoller(settings.New(f.cfg, f.st), f.st)
	f.tick(t)
	for _, o := range []*model.Order{f.order, other} {
		current, _ := f.st.GetByTradeNo(ctx, o.TradeNo)
		if current.Status != model.StatusReady || current.NextNotifyAt != nil {
			t.Fatal("ambiguous event paid or notified")
		}
	}
	obs := f.observations(t)
	if len(obs) != 2 || obs[0].EventID != "multi-parent" || obs[1].EventID != "multi-parent" || obs[0].IdentityIssue != "ambiguous" || obs[0].Currency == obs[1].Currency {
		t.Fatalf("asset evidence lost: %+v", obs)
	}
	var claims, slots int64
	f.st.DB().Model(&model.IncomingTx{}).Count(&claims)
	f.st.DB().Model(&model.AmountSlot{}).Where("order_id <> 0").Count(&slots)
	if claims != 0 || slots != 2 {
		t.Fatalf("claims=%d slots=%d", claims, slots)
	}
	result, err := f.poller.CheckPayment(ctx, f.order.TradeNo, "app-multi")
	if err != nil || result != "needs_review" {
		t.Fatalf("inquiry bypassed ambiguity: %s %v", result, err)
	}
}

func TestScanAndInquiryConcurrencySharesOneClaim(t *testing.T) {
	f := newPollFixture(t, "RACE-SCAN")
	f.stub.set(stubBinanceTx{id: "race-event", orderType: "C2C", amount: f.order.PayAmount, currency: "USDT", at: f.txTime(), receiver: "binance-uid"})
	var wg sync.WaitGroup
	wg.Add(2)
	go func() { defer wg.Done(); f.tick(t) }()
	go func() { defer wg.Done(); f.poller.CheckPayment(context.Background(), f.order.TradeNo, "race-event") }()
	wg.Wait()
	got, _ := f.st.GetByTradeNo(context.Background(), f.order.TradeNo)
	var claims, events int64
	f.st.DB().Model(&model.IncomingTx{}).Count(&claims)
	f.st.DB().Model(&model.EventClaim{}).Count(&events)
	if got.Status != model.StatusPaid || claims != 1 || events != 1 {
		t.Fatalf("%s claims=%d events=%d", got.Status, claims, events)
	}
}
