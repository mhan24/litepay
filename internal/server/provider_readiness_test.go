package server

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"litepay/internal/sign"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"

	"litepay/internal/config"
	"litepay/internal/model"
	"litepay/internal/settings"
	"litepay/internal/store"
	"litepay/internal/worker"
)

type outageSource struct {
	uid                     string
	identityErr, historyErr error
	rows                    []model.Transaction
}

func (f *outageSource) AccountUID(context.Context) (string, error) { return f.uid, f.identityErr }
func (f *outageSource) History(_ context.Context, from, to time.Time) ([]model.Transaction, error) {
	if f.historyErr != nil {
		return nil, f.historyErr
	}
	var rows []model.Transaction
	for _, r := range f.rows {
		if !r.OccurredAt.Before(from) && !r.OccurredAt.After(to) {
			rows = append(rows, r)
		}
	}
	return rows, nil
}

func TestProviderAdmissionRequiresCurrentAccountAndScanAndRecovers(t *testing.T) {
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
	cfg.OKX.APIKey = "okx-key"
	cfg.OKX.APISecret = "okx-secret"
	cfg.OKX.Passphrase = "phrase"
	cfg.Checkout.OKX.UID = "receiver"
	mgr := settings.New(cfg, st)
	if err = mgr.Load(ctx); err != nil {
		t.Fatal(err)
	}
	p := worker.NewPoller(mgr, st)
	binance := &outageSource{uid: "receiver"}
	okx := &outageSource{uid: "receiver"}
	p.UseTestSource("binance", binance)
	p.UseTestSource("okx", okx)
	engine, err := New(mgr, st, p)
	if err != nil {
		t.Fatal(err)
	}
	// A durable success from a previous process cannot substitute for this boot's verification.
	for _, provider := range []string{"binance", "okx"} {
		if err = st.ScanResult(ctx, provider, time.Now(), nil); err != nil {
			t.Fatal(err)
		}
	}
	_, err = st.Create(ctx, model.Order{TradeNo: "old-instruction", OutTradeNo: "old-instruction", OriginUnits: 10000000, ExpiresAt: time.Now().Add(time.Hour)})
	if err != nil {
		t.Fatal(err)
	}
	old, err := st.Select(ctx, "old-instruction", "binance", "USDT", "receiver", 2, 10000, 10000, 99, 120)
	if err != nil {
		t.Fatal(err)
	}
	if w := getRequest(engine, "/pay/old-instruction"); w.Code != 503 || strings.Contains(w.Body.String(), "复制金额") {
		t.Fatal(w.Code, w.Body.String())
	}
	submit := func(no, provider string) int {
		t.Helper()
		w := postForm(engine, "/api/order/payment", url.Values{"trade_no": {no}, "provider": {provider}, "currency": {"USDT"}})
		return w.Code
	}
	if got := submit(old.TradeNo, "binance"); got != 503 {
		t.Fatal(got)
	}
	okx.identityErr = errors.New("HTTP 401")
	if err = p.Scan(ctx, "okx"); err == nil {
		t.Fatal("revoked account verified")
	}
	if ready, _ := p.ProviderReady(ctx, mgr.Get(), "okx"); ready {
		t.Fatal("failed initial verification admitted payments")
	}
	// Healthy Binance remains usable while OKX is unavailable.
	if err = p.Scan(ctx, "binance"); err != nil {
		t.Fatal(err)
	}
	no := createOrderViaBEpusdt(t, engine, mgr.Get().BEpusdt.APIToken, "https://shop.example")
	if got := submit(no, "binance"); got != 200 {
		t.Fatal(got)
	}
	if got := submit(no, "okx"); got != 503 {
		t.Fatal(got)
	}
	binance.historyErr = errors.New("HTTP 401 revoked API key")
	if err = p.Scan(ctx, "binance"); err == nil {
		t.Fatal("scan did not fail")
	}
	if got := submit(no, "binance"); got != 503 {
		t.Fatal("failed scan admitted instructions", got)
	}
	if w := getRequest(engine, "/api/order/status?order_id="+no); !strings.Contains(w.Body.String(), `"accepting_payment":false`) {
		t.Fatal(w.Body.String())
	}
	// A valid signed create request must fail when every eligible platform is down.
	body := map[string]any{"order_id": "all-down", "amount": 10, "notify_url": "https://shop.example/notify", "redirect_url": "https://shop.example/return"}
	w := signedCreateRequest(t, engine, mgr.Get(), body)
	if !strings.Contains(w.Body.String(), "收款扫描尚未就绪") {
		t.Fatal(w.Body.String())
	}
	// Past money is still collected and attributed once its source recovers; admission
	// does not gate the scanner or its evidence path.
	binance.historyErr = nil
	binance.rows = []model.Transaction{{Provider: "binance", EventID: "old-money", Reference: "old-money", Receiver: "receiver", Currency: "USDT", Units: old.PayUnits, OccurredAt: time.Now(), State: model.Confirmed}}
	if err = p.Scan(ctx, "binance"); err != nil {
		t.Fatal(err)
	}
	paid, _ := st.GetByTradeNo(ctx, old.TradeNo)
	if paid.Status != model.Paid {
		t.Fatal("past funds lost", paid)
	}
	if got := submit(no, "binance"); got != 200 {
		t.Fatal("recovered platform stayed blocked", got)
	}
	// Current credentials, verification and scan coverage are bound together.
	changed := *mgr.Get()
	changed.Binance.APISecret = "rotated"
	if ready, _ := p.ProviderReady(ctx, &changed, "binance"); ready {
		t.Fatal("old scan authorized changed credentials")
	}
	for _, column := range []string{"last_success", "scanned_until"} {
		if err = st.DB().Model(&model.ScanCursor{}).Where("provider=?", "binance").Update(column, time.Now().Add(-4*time.Minute)).Error; err != nil {
			t.Fatal(err)
		}
		if got := submit(no, "binance"); got != 503 {
			t.Fatal("stale scan admitted instructions", column, got)
		}
		if err = p.Scan(ctx, "binance"); err != nil {
			t.Fatal(err)
		}
	}
	okx.identityErr = nil
	if err = p.Scan(ctx, "okx"); err != nil {
		t.Fatal(err)
	}
	if w := getRequest(engine, "/readyz"); w.Code != 200 {
		t.Fatal(w.Code, w.Body.String())
	}
}

func signedCreateRequest(t *testing.T, engine http.Handler, cfg *config.Config, payload map[string]any) *httptest.ResponseRecorder {
	t.Helper()
	payload["signature"] = sign.BEpusdtSignGeneric(payload, cfg.BEpusdt.APIToken)
	body, err := json.Marshal(payload)
	if err != nil {
		t.Fatal(err)
	}
	req := httptest.NewRequest("POST", "/api/v1/order/create-order", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	engine.ServeHTTP(w, req)
	return w
}
