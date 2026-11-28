package worker

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"litepay/internal/config"
	"litepay/internal/model"
	"litepay/internal/settings"
	"litepay/internal/sign"
	"litepay/internal/store"
)

func TestTokenRotationPinsHistoricalCallbacks(t *testing.T) {
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
	old := mgr.Get().BEpusdt
	var got int
	merchant := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var body map[string]any
		decoder := json.NewDecoder(r.Body)
		if err := decoder.Decode(&body); err != nil {
			t.Error(err)
			return
		}
		// The merchant decodes JSON numbers as float64 and includes their %v text in the signature.
		for _, field := range []string{"amount", "actual_amount"} {
			body[field] = fmt.Sprint(body[field])
		}
		if body["amount"] != "5e-05" || body["actual_amount"] != "0.0001" {
			t.Errorf("unexpected legacy callback amounts: %v, %v", body["amount"], body["actual_amount"])
		}
		if !sign.BEpusdtVerifyGeneric(body, old.APIToken) || sign.BEpusdtVerifyGeneric(body, mgr.Get().BEpusdt.APIToken) {
			t.Error("historical signing key changed")
		}
		got++
		fmt.Fprint(w, "success")
	}))
	defer merchant.Close()
	_, err = st.Create(ctx, model.Order{TradeNo: "key-old", OutTradeNo: "key-old", OriginUnits: 50, ExpiresAt: time.Now().Add(time.Hour), NotifyURL: merchant.URL})
	if err != nil {
		t.Fatal(err)
	}
	o, err := st.Select(ctx, "key-old", "binance", "USDT", "receiver", 6, 10, 1, 5, 120)
	if err != nil {
		t.Fatal(err)
	}
	_, err = st.Process(ctx, model.Transaction{Provider: "binance", EventID: "key-event", Reference: "key-ref", Currency: "USDT", Receiver: "receiver", Units: o.PayUnits, OccurredAt: time.Now(), State: model.Confirmed}, "")
	if err != nil {
		t.Fatal(err)
	}
	if _, err = mgr.RotateAPIToken(ctx, "operator"); err != nil {
		t.Fatal(err)
	}
	if mgr.Get().BEpusdt.TokenVersion == old.TokenVersion {
		t.Fatal("key version did not change")
	}
	n := NewNotifier(mgr, st)
	n.UseTestTransport()
	if did, err := n.Deliver(ctx); err != nil || !did || got != 1 {
		t.Fatal(did, got, err)
	}
	var task model.Callback
	st.DB().First(&task)
	if task.SigningKeyID != old.TokenVersion {
		t.Fatal(task.SigningKeyID)
	}
	newer, err := st.Create(ctx, model.Order{TradeNo: "key-new", OutTradeNo: "key-new", OriginUnits: 1, ExpiresAt: time.Now().Add(time.Minute)})
	if err != nil || newer.SigningKeyID != mgr.Get().BEpusdt.TokenVersion {
		t.Fatal(newer, err)
	}
	reload := settings.New(cfg, st)
	if err = reload.Load(ctx); err != nil {
		t.Fatal(err)
	}
	if reload.Get().BEpusdt.TokenVersion != mgr.Get().BEpusdt.TokenVersion {
		t.Fatal("key version lost across load")
	}
}
