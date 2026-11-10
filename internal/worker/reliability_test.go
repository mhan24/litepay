package worker

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"testing"
	"time"

	"litepay/internal/config"
	"litepay/internal/model"
	"litepay/internal/settings"
	"litepay/internal/store"
)

func TestMerchantTimeoutConfirmationSurvivesDefaultRetentionAndRestart(t *testing.T) {
	for _, hours := range []int{1, 6, 24} {
		t.Run(fmt.Sprint(hours), func(t *testing.T) {
			ctx := context.Background()
			path := filepath.Join(t.TempDir(), "payments.db")
			st, err := store.Open(path)
			if err != nil {
				t.Fatal(err)
			}
			cfg := config.Default()
			cfg.Server.Production = false
			cfg.OKX.APIKey = "key"
			cfg.OKX.APISecret = "secret"
			cfg.OKX.Passphrase = "phrase"
			cfg.Checkout.OKX.UID = "receiver"
			mgr := settings.New(cfg, st)
			if err = mgr.Load(ctx); err != nil {
				t.Fatal(err)
			}
			selected := time.Now().Add(-25 * time.Minute)
			expires := selected.Add(time.Duration(hours) * time.Hour)
			o, err := st.Create(ctx, model.Order{TradeNo: "long", OutTradeNo: "long", OriginUnits: 10_000_000, ExpiresAt: expires})
			if err != nil {
				t.Fatal(err)
			}
			o, err = st.Select(ctx, o.TradeNo, "okx", "USDT", "receiver", 2, 10000, 10000, 99, 120)
			if err != nil {
				t.Fatal(err)
			}
			if err = st.DB().Model(o).Update("selected_at", selected).Error; err != nil {
				t.Fatal(err)
			}
			syncAttemptFixture(t, st)
			deposit := model.Transaction{Provider: "okx", EventID: "long-pending", Reference: "withdraw", Receiver: "receiver", Currency: "USDT", Units: o.PayUnits, OccurredAt: selected.Add(5 * time.Minute), State: model.Unconfirmed}
			if _, err = st.Process(ctx, deposit, ""); err != nil {
				t.Fatal(err)
			}
			if err = st.ScanResult(ctx, "okx", time.Now(), nil); err != nil {
				t.Fatal(err)
			}
			if err = st.Close(); err != nil {
				t.Fatal(err)
			}
			st, err = store.Open(path)
			if err != nil {
				t.Fatal(err)
			}
			defer st.Close()
			mgr = settings.New(cfg, st)
			if err = mgr.Load(ctx); err != nil {
				t.Fatal(err)
			}
			deposit.State = model.Confirmed
			f := &fixture{rows: []model.Transaction{deposit}}
			poller := NewPoller(mgr, st)
			poller.clients["okx"] = &connection{config: *mgr.Get(), source: f}
			if err = poller.Scan(ctx, "okx"); err != nil {
				t.Fatal(err)
			}
			fresh, err := st.GetByTradeNo(ctx, o.TradeNo)
			if err != nil || fresh.Status != model.Paid {
				t.Fatal("merchant timeout deposit lost", fresh, err)
			}
			var task model.ConfirmationTask
			if err = st.DB().First(&task).Error; err != nil || task.OrderID == nil || *task.OrderID != o.ID || task.State != "resolved" || !task.Deadline.Equal(expires.Add(store.ConfirmationWait)) {
				t.Fatal(task, err)
			}
			var count int64
			st.DB().Model(&model.Callback{}).Count(&count)
			if count != 1 {
				t.Fatal(count)
			}
		})
	}
}

func TestCanceledPaymentStillRecordsIdleAccountEvidence(t *testing.T) {
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
	_, err = st.Create(ctx, model.Order{TradeNo: "cancel", OutTradeNo: "cancel", OriginUnits: 10_000_000, ExpiresAt: time.Now().Add(time.Hour)})
	if err != nil {
		t.Fatal(err)
	}
	o, err := st.Select(ctx, "cancel", "binance", "USDT", "receiver", 2, 10000, 10000, 99, 120)
	if err != nil {
		t.Fatal(err)
	}
	if err = st.Cancel(ctx, o.TradeNo); err != nil {
		t.Fatal(err)
	}
	at := time.Now().Add(-time.Second)
	if err = st.ScanResult(ctx, "binance", at.Add(-time.Minute), nil); err != nil {
		t.Fatal(err)
	}
	st.DB().Model(&model.ScanCursor{}).Where("provider = ?", "binance").Update("last_success", time.Now().Add(-2*store.IdleReconcileInterval))
	f := &fixture{rows: []model.Transaction{{Provider: "binance", EventID: "late-canceled", Reference: "late", Currency: "USDT", Receiver: "receiver", Units: o.PayUnits, OccurredAt: at, State: model.Confirmed}}}
	p := NewPoller(mgr, st)
	p.clients["binance"] = &connection{config: *mgr.Get(), source: f}
	if err = p.Scan(ctx, "binance"); err != nil {
		t.Fatal(err)
	}
	var evidence model.Transaction
	if err = st.DB().First(&evidence, "event_id = ?", "late-canceled").Error; err != nil || evidence.OrderID != nil {
		t.Fatal(evidence, err)
	}
	fresh, _ := st.GetByTradeNo(ctx, o.TradeNo)
	if fresh.Status != model.Canceled {
		t.Fatal(fresh.Status)
	}
	var count int64
	st.DB().Model(&model.Callback{}).Count(&count)
	if count != 0 {
		t.Fatal("canceled payment delivered")
	}
}

func TestCallbackRecoveryReusesMerchantIdempotencyKey(t *testing.T) {
	ctx := context.Background()
	st, err := store.Open(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	received, delivered := 0, 0
	seen := map[string]bool{}
	var payloads []string
	merchant := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		data, _ := io.ReadAll(r.Body)
		payloads = append(payloads, string(data))
		var body map[string]any
		json.Unmarshal(data, &body)
		key := body["order_id"].(string)
		received++
		if !seen[key] {
			seen[key] = true
			delivered++
		}
		if received == 1 {
			http.Error(w, "ACK lost after fulfillment", 503)
			return
		}
		fmt.Fprint(w, "success")
	}))
	defer merchant.Close()
	cfg := config.Default()
	cfg.Server.Production = false
	cfg.Notify.MaxAttempts = 1
	mgr := settings.New(cfg, st)
	if err = mgr.Load(ctx); err != nil {
		t.Fatal(err)
	}
	_, err = st.Create(ctx, model.Order{TradeNo: "recovery", OutTradeNo: "merchant-recovery", OriginUnits: 1_000_000, NotifyURL: merchant.URL, ExpiresAt: time.Now().Add(time.Hour)})
	if err != nil {
		t.Fatal(err)
	}
	o, err := st.Select(ctx, "recovery", "binance", "USDT", "receiver", 2, 10000, 10000, 99, 120)
	if err != nil {
		t.Fatal(err)
	}
	_, err = st.Process(ctx, model.Transaction{Provider: "binance", EventID: "one-deposit", Reference: "one", Currency: "USDT", Receiver: "receiver", Units: o.PayUnits, OccurredAt: time.Now(), State: model.Confirmed}, "")
	if err != nil {
		t.Fatal(err)
	}
	n := NewNotifier(mgr, st)
	n.UseTestTransport()
	if did, err := n.Deliver(ctx); err != nil || !did {
		t.Fatal(did, err)
	}
	if did, err := n.Deliver(ctx); err != nil || did {
		t.Fatal("exhausted task retried itself", did, err)
	}
	if err = st.RetryCallback(ctx, o.TradeNo, "operator", "checked merchant and ACK recovery", 1); err != nil {
		t.Fatal(err)
	}
	if did, err := n.Deliver(ctx); err != nil || !did {
		t.Fatal(did, err)
	}
	if received != 2 || delivered != 1 || payloads[0] != payloads[1] {
		t.Fatal("recovery changed idempotency key", received, delivered)
	}
}

func TestAuditedHistoricalScanFindsEventBeyondAutomaticWindow(t *testing.T) {
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
	_, err = st.Create(ctx, model.Order{TradeNo: "history", OutTradeNo: "history", OriginUnits: 1000000, ExpiresAt: time.Now().Add(time.Minute)})
	if err != nil {
		t.Fatal(err)
	}
	o, err := st.Select(ctx, "history", "binance", "USDT", "receiver", 2, 10000, 10000, 99, 120)
	if err != nil {
		t.Fatal(err)
	}
	from := time.Now().Add(-72 * time.Hour)
	to := from.Add(3 * time.Minute)
	st.DB().Model(o).Updates(map[string]any{"selected_at": from, "expires_at": to, "status": model.Expired})
	st.DB().Model(&model.PaymentAttempt{}).Where("order_id = ?", o.ID).Updates(map[string]any{"selected_at": from, "expires_at": to, "settle_until": to.Add(store.LateVisibilityWindow)})
	event := model.Transaction{Provider: "binance", EventID: "indexed-3-days-late", Reference: "3-days", Currency: "USDT", Receiver: "receiver", Units: o.PayUnits, OccurredAt: from.Add(time.Minute), State: model.Confirmed}
	if err = st.RequestHistoricalScan(ctx, o.TradeNo, "operator", "merchant waiting; verify old transfer"); err != nil {
		t.Fatal(err)
	}
	// Duplicate clicks do not enqueue an overlapping active request.
	st.RequestHistoricalScan(ctx, o.TradeNo, "operator", "second click")
	p := NewPoller(mgr, st)
	p.clients["binance"] = &connection{config: *mgr.Get(), source: &fixture{rows: []model.Transaction{event}}}
	if err = p.Scan(ctx, "binance"); err != nil {
		t.Fatal(err)
	}
	var stored model.Transaction
	if err = st.DB().First(&stored, "event_id = ?", event.EventID).Error; err != nil || stored.OrderID != nil {
		t.Fatal(stored, err)
	}
	fresh, _ := st.GetByTradeNo(ctx, o.TradeNo)
	if fresh.Status != model.Expired {
		t.Fatal("admin rescan automatically delivered closed order")
	}
	var jobs []model.HistoricalScan
	st.DB().Find(&jobs)
	if len(jobs) != 1 || jobs[0].State != "completed" || jobs[0].Operator != "operator" {
		t.Fatal(jobs)
	}
	if err = st.ReconcilePayment(ctx, o.TradeNo, event.Provider, event.EventID, "operator", "platform proof and merchant state verified"); err != nil {
		t.Fatal(err)
	}
	fresh, _ = st.GetByTradeNo(ctx, o.TradeNo)
	if fresh.Status != model.Paid {
		t.Fatal(fresh.Status)
	}
}

func TestFirstVisibleDepositBehindIncrementalCursorIsFoundByWindowRescan(t *testing.T) {
	ctx := context.Background()
	st, err := store.Open(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	cfg := config.Default()
	cfg.Server.Production = false
	cfg.OKX.APIKey = "key"
	cfg.OKX.APISecret = "secret"
	cfg.OKX.Passphrase = "pass"
	cfg.Checkout.OKX.UID = "receiver"
	mgr := settings.New(cfg, st)
	if err = mgr.Load(ctx); err != nil {
		t.Fatal(err)
	}
	_, err = st.Create(ctx, model.Order{TradeNo: "unseen", OutTradeNo: "unseen", OriginUnits: 1000000, ExpiresAt: time.Now().Add(time.Minute)})
	if err != nil {
		t.Fatal(err)
	}
	o, err := st.Select(ctx, "unseen", "okx", "USDT", "receiver", 2, 10000, 10000, 99, 120)
	if err != nil {
		t.Fatal(err)
	}
	selected, expires := time.Now().Add(-20*time.Minute), time.Now().Add(-10*time.Minute)
	st.DB().Model(o).Updates(map[string]any{"selected_at": selected, "expires_at": expires})
	st.DB().Model(&model.PaymentAttempt{}).Where("order_id = ?", o.ID).Updates(map[string]any{"selected_at": selected, "expires_at": expires, "settle_until": expires.Add(store.LateVisibilityWindow)})
	st.ScanResult(ctx, "okx", time.Now(), nil)
	// No previous evidence and no ConfirmationTask: the event first appears 19 minutes late.
	event := model.Transaction{Provider: "okx", EventID: "unseen-behind-cursor", Reference: "late-ref", Receiver: "receiver", Currency: "USDT", Units: o.PayUnits, OccurredAt: selected.Add(time.Minute), State: model.Confirmed}
	source := &fixture{rows: []model.Transaction{event}}
	p := NewPoller(mgr, st)
	p.clients["okx"] = &connection{config: *mgr.Get(), source: source}
	if err = p.Scan(ctx, "okx"); err != nil {
		t.Fatal(err)
	}
	if len(source.windows) < 2 || !source.windows[0][0].After(event.OccurredAt) || source.windows[1][0].After(event.OccurredAt) {
		t.Fatal("history window was not reopened", source.windows)
	}
	fresh, _ := st.GetByTradeNo(ctx, o.TradeNo)
	if fresh.Status != model.Paid {
		t.Fatal("previously invisible deposit lost", fresh.Status)
	}
}
