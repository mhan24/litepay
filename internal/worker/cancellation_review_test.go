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

func TestMerchantCancellationDuringPayloadBuildBlocksOnlyThatCallback(t *testing.T) {
	ctx := context.Background()
	st, err := store.Open(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	var calls atomic.Int64
	merchant := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { calls.Add(1); fmt.Fprint(w, "success") }))
	defer merchant.Close()
	cfg := config.Default()
	cfg.Server.Production = false
	mgr := settings.New(cfg, st)
	if err = mgr.Load(ctx); err != nil {
		t.Fatal(err)
	}
	makePaid := func(no string) {
		t.Helper()
		_, err := st.Create(ctx, model.Order{TradeNo: no, OutTradeNo: no, OriginUnits: 1000000, NotifyURL: merchant.URL, ExpiresAt: time.Now().Add(time.Hour)})
		if err != nil {
			t.Fatal(err)
		}
		o, err := st.Select(ctx, no, "binance", "USDT", "receiver", 2, 10000, 10000, 99, 120)
		if err != nil {
			t.Fatal(err)
		}
		if _, err = st.Process(ctx, model.Transaction{Provider: "binance", EventID: no, Receiver: "receiver", Currency: "USDT", Units: o.PayUnits, OccurredAt: time.Now(), State: model.Confirmed}, ""); err != nil {
			t.Fatal(err)
		}
	}
	makePaid("canceled-during-signing")
	hook := "test:merchant_cancel_during_signing"
	if err = st.DB().Callback().Query().After("gorm:query").Register(hook, func(db *gorm.DB) {
		if db.Statement.Table == "signing_keys" {
			if _, e := st.CancelReference(ctx, "canceled-during-signing", "canceled-during-signing"); e == nil {
				t.Error("paid merchant cancellation falsely succeeded")
			}
		}
	}); err != nil {
		t.Fatal(err)
	}
	n := NewNotifier(mgr, st)
	n.UseTestTransport()
	if did, e := n.Deliver(ctx); did || e != nil {
		t.Fatal("callback escaped cancellation fence", did, e)
	}
	if err = st.DB().Callback().Query().Remove(hook); err != nil {
		t.Fatal(err)
	}
	if calls.Load() != 0 {
		t.Fatal("merchant received canceled callback")
	}
	var cb model.Callback
	st.DB().First(&cb)
	if cb.State != "merchant_review" || cb.Attempts != 0 {
		t.Fatal(cb)
	}
	makePaid("other-paid-order")
	if did, e := n.Deliver(ctx); !did || e != nil {
		t.Fatal("one cancellation blocked unrelated delivery", did, e)
	}
	if calls.Load() != 1 {
		t.Fatal(calls.Load())
	}
}
