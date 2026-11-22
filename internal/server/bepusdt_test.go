package server

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/gin-gonic/gin"

	"litepay/internal/config"
	"litepay/internal/model"
	"litepay/internal/settings"
	"litepay/internal/sign"
	"litepay/internal/store"
)

func TestBEpusdtCaseVariantKeysCannotTamperSignedFields(t *testing.T) {
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
	cfg.Order.ExpireMinutes = 10
	mgr := settings.New(cfg, st)
	if err := mgr.Load(ctx); err != nil {
		t.Fatal(err)
	}
	engine, err := New(mgr, st, verifiedPoller(t, mgr, st))
	if err != nil {
		t.Fatal(err)
	}
	token := mgr.Get().BEpusdt.APIToken

	signedCreate := map[string]any{
		"order_id":     "case-tamper-order",
		"amount":       json.Number("12.34"),
		"notify_url":   "https://shop.example/notify",
		"redirect_url": "https://shop.example/return",
		"currencies":   "USDT",
		"reselect":     false,
		"timeout":      300,
		"name":         "VIP Plan",
	}
	signedCreate["signature"] = sign.BEpusdtSignGeneric(signedCreate, token)

	// Append empty/null case-variant keys that BEpusdtVerifyGeneric skips.
	signedCreate["CURRENCIES"] = ""
	signedCreate["RESELECT"] = nil
	signedCreate["TIMEOUT"] = nil
	signedCreate["NAME"] = ""
	signedCreate["ORDER_ID"] = ""

	raw, err := json.Marshal(signedCreate)
	if err != nil {
		t.Fatal(err)
	}
	req := httptest.NewRequest(http.MethodPost, "/api/v1/order/create-order", bytes.NewReader(raw))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	engine.ServeHTTP(w, req)

	var created struct {
		StatusCode int `json:"status_code"`
		Data       struct {
			TradeID string `json:"trade_id"`
		} `json:"data"`
	}
	if err := json.Unmarshal(w.Body.Bytes(), &created); err != nil || created.StatusCode != 200 || created.Data.TradeID == "" {
		t.Fatalf("unexpected create response: %s (%v)", w.Body.String(), err)
	}

	o, err := st.GetByTradeNo(ctx, created.Data.TradeID)
	if err != nil {
		t.Fatal(err)
	}
	if o.CurrencyLimit != "USDT" {
		t.Fatalf("currencies overridden by case-variant key: %q", o.CurrencyLimit)
	}
	if o.Reselect {
		t.Fatalf("reselect overridden by case-variant key")
	}
	if o.Name != "VIP Plan" {
		t.Fatalf("name overridden by case-variant key: %q", o.Name)
	}
	if ttl := o.ExpiresAt.Sub(o.CreatedAt); ttl < 290*time.Second || ttl > 310*time.Second {
		t.Fatalf("timeout overridden by case-variant key: ttl=%v", ttl)
	}

	// Cancel: signed mismatch with TRADE_ID="" must still be rejected as mismatched.
	other, err := st.Create(ctx, model.Order{
		TradeNo:     "case-tamper-other",
		OutTradeNo:  "case-tamper-other-out",
		OriginUnits: 1_000_000,
		ExpiresAt:   time.Now().Add(time.Minute),
	})
	if err != nil {
		t.Fatal(err)
	}
	cancelMismatch := map[string]any{
		"trade_id": o.TradeNo,
		"order_id": other.OutTradeNo,
	}
	cancelMismatch["signature"] = sign.BEpusdtSignGeneric(cancelMismatch, token)
	cancelMismatch["TRADE_ID"] = ""
	rawCancel, _ := json.Marshal(cancelMismatch)
	req = httptest.NewRequest(http.MethodPost, "/api/v1/order/cancel-transaction", bytes.NewReader(rawCancel))
	req.Header.Set("Content-Type", "application/json")
	w = httptest.NewRecorder()
	engine.ServeHTTP(w, req)
	var cancelResp bepusdtResponse
	if err := json.Unmarshal(w.Body.Bytes(), &cancelResp); err != nil || cancelResp.StatusCode == 200 {
		t.Fatalf("expected mismatched cancel to fail even with TRADE_ID=\"\": %s", w.Body.String())
	}
	fresh, err := st.GetByTradeNo(ctx, o.TradeNo)
	if err != nil || fresh.Status != model.Pending {
		t.Fatalf("order status mutated on mismatched cancel: %+v (%v)", fresh, err)
	}
}

func TestBEpusdtRejectsInvalidFieldTypesAndNilBodySafely(t *testing.T) {
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
	if err := mgr.Load(ctx); err != nil {
		t.Fatal(err)
	}
	engine, err := New(mgr, st, verifiedPoller(t, mgr, st))
	if err != nil {
		t.Fatal(err)
	}
	token := mgr.Get().BEpusdt.APIToken

	for name, payload := range map[string]map[string]any{
		"float_timeout": {
			"order_id": "t1", "amount": json.Number("10"), "notify_url": "https://shop.example/n",
			"redirect_url": "https://shop.example/r", "timeout": json.Number("120.5"),
		},
		"string_reselect": {
			"order_id": "t2", "amount": json.Number("10"), "notify_url": "https://shop.example/n",
			"redirect_url": "https://shop.example/r", "reselect": "false",
		},
		"long_fiat": {
			"order_id": "t3", "amount": json.Number("10"), "notify_url": "https://shop.example/n",
			"redirect_url": "https://shop.example/r", "fiat": strings.Repeat("X", 64),
		},
	} {
		t.Run(name, func(t *testing.T) {
			payload["signature"] = sign.BEpusdtSignGeneric(payload, token)
			raw, _ := json.Marshal(payload)
			req := httptest.NewRequest(http.MethodPost, "/api/v1/order/create-order", bytes.NewReader(raw))
			w := httptest.NewRecorder()
			engine.ServeHTTP(w, req)
			var resp bepusdtResponse
			if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil || resp.StatusCode == 200 {
				t.Fatalf("expected rejection for %s, got %s", name, w.Body.String())
			}
		})
	}

	// Direct call with nil Request.Body must not panic.
	h := &Handlers{cfg: mgr, st: st}
	for _, call := range []func(*gin.Context){h.BEpusdtCreateOrder, h.BEpusdtCancelOrder} {
		w := httptest.NewRecorder()
		c, _ := gin.CreateTestContext(w)
		c.Request = &http.Request{Method: http.MethodPost, Body: nil}
		call(c)
		if w.Code != http.StatusOK || !strings.Contains(w.Body.String(), `"status_code":400`) {
			t.Fatalf("unexpected response for nil body: %d %s", w.Code, w.Body.String())
		}
	}
}
