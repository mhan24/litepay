package worker

import (
	"context"
	"testing"
	"time"

	"litepay/internal/config"
	"litepay/internal/model"
	"litepay/internal/settings"
	"litepay/internal/store"
)

func TestLateSettlementCannotAutomaticallyNotifyCanceledMerchant(t *testing.T) {
	ctx := context.Background()
	st, err := store.Open(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	cfg := config.Default()
	cfg.Server.Production = false
	mgr := settings.New(cfg, st)
	if err = mgr.Load(ctx); err != nil {
		t.Fatal(err)
	}
	_, err = st.Create(ctx, model.Order{TradeNo: "late-notify", OutTradeNo: "late-notify", OriginUnits: 1000000, ExpiresAt: time.Now().Add(time.Minute), NotifyURL: "https://shop.example/notify"})
	if err != nil {
		t.Fatal(err)
	}
	o, err := st.Select(ctx, "late-notify", "binance", "USDT", "receiver", 2, 10000, 10000, 99, 120)
	if err != nil {
		t.Fatal(err)
	}
	selected, expires := time.Now().Add(-20*time.Minute), time.Now().Add(-10*time.Minute)
	st.DB().Model(o).Updates(map[string]any{"selected_at": selected, "expires_at": expires})
	st.DB().Model(&model.PaymentAttempt{}).Where("order_id = ?", o.ID).Updates(map[string]any{"selected_at": selected, "expires_at": expires})
	event := model.Transaction{Provider: "binance", EventID: "late-valid", Reference: "late-ref", Currency: "USDT", Receiver: "receiver", Units: o.PayUnits, OccurredAt: selected.Add(time.Minute), State: model.Confirmed}
	if result, err := st.Process(ctx, event, ""); err != nil || result != "paid" {
		t.Fatal(result, err)
	}
	var task model.Callback
	st.DB().First(&task)
	if task.State != "merchant_review" || task.Attempts != 0 {
		t.Fatal(task)
	}
	n := NewNotifier(mgr, st)
	if did, err := n.Deliver(ctx); err != nil || did {
		t.Fatal("late callback escaped review hold", did, err)
	}
	if err := st.RetryCallback(ctx, o.TradeNo, "operator", "", 5); err == nil {
		t.Fatal("unreviewed release allowed")
	}
	if err := st.RetryCallback(ctx, o.TradeNo, "operator", "商城确认未取消且同意交付", 5); err != nil {
		t.Fatal(err)
	}
	st.DB().First(&task)
	if task.State != "queued" || task.Recoveries != 1 {
		t.Fatal(task)
	}
}
