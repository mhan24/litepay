package worker

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"litepay/internal/binance"
	"litepay/internal/config"
	"litepay/internal/model"
	"litepay/internal/settings"
	"litepay/internal/store"
)

type cancelOpaqueSource struct {
	uid     string
	cancel  context.CancelFunc
	onCall  int
	calls   int
	rows    []model.Transaction
	failMsg string
}

func (s *cancelOpaqueSource) AccountUID(context.Context) (string, error) { return s.uid, nil }
func (s *cancelOpaqueSource) History(_ context.Context, start, end time.Time) ([]model.Transaction, error) {
	s.calls++
	if s.calls == s.onCall {
		s.cancel()
		return nil, errors.New(s.failMsg)
	}
	var out []model.Transaction
	for _, r := range s.rows {
		if !r.OccurredAt.Before(start) && !r.OccurredAt.After(end) {
			out = append(out, r)
		}
	}
	return out, nil
}

func TestScanShutdownCancellationDoesNotPersistFalseError(t *testing.T) {
	baseCtx := context.Background()
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
	if err = mgr.Load(baseCtx); err != nil {
		t.Fatal(err)
	}
	if err = st.ScanResult(baseCtx, "okx", time.Now().Add(-time.Minute), nil); err != nil {
		t.Fatal(err)
	}

	scanCtx, cancel := context.WithCancel(baseCtx)
	defer cancel()
	src := &cancelOpaqueSource{uid: "receiver", cancel: cancel, onCall: 1, failMsg: "OKX network request failed"}
	p := NewPoller(mgr, st)
	p.clients["okx"] = &connection{config: *mgr.Get(), source: src, uid: "receiver"}

	if err = p.Scan(scanCtx, "okx"); !errors.Is(err, context.Canceled) {
		t.Fatalf("expected context.Canceled, got %v", err)
	}
	cursor, err := st.Cursor(baseCtx, "okx")
	if err != nil {
		t.Fatal(err)
	}
	if cursor.LastError != "" {
		t.Fatalf("shutdown cancellation overwrote cursor last_error with %q", cursor.LastError)
	}
}

func TestScanPendingRecheckCancelPreservesIncrementalScanResult(t *testing.T) {
	baseCtx := context.Background()
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
	if err = mgr.Load(baseCtx); err != nil {
		t.Fatal(err)
	}
	prev := time.Now().Add(-20 * time.Minute)
	if err = st.ScanResult(baseCtx, "okx", prev, nil); err != nil {
		t.Fatal(err)
	}
	oldPending := model.Transaction{
		Provider:   "okx",
		EventID:    "old-pending-cancel",
		Reference:  "ref-old-cancel",
		Receiver:   "receiver",
		Currency:   "USDT",
		Units:      1_000_000,
		OccurredAt: time.Now().Add(-90 * time.Minute),
		State:      model.Unconfirmed,
	}
	if _, err = st.Process(baseCtx, oldPending, ""); err != nil {
		t.Fatal(err)
	}

	scanCtx, cancel := context.WithCancel(baseCtx)
	defer cancel()
	// Call 1 = incremental scan (succeeds); Call 2 = pending recheck (shutdown cancels).
	src := &cancelOpaqueSource{uid: "receiver", cancel: cancel, onCall: 2, failMsg: "OKX network request failed"}
	p := NewPoller(mgr, st)
	p.clients["okx"] = &connection{config: *mgr.Get(), source: src, uid: "receiver"}

	if err = p.Scan(scanCtx, "okx"); !errors.Is(err, context.Canceled) {
		t.Fatalf("expected context.Canceled, got %v", err)
	}
	cursor, err := st.Cursor(baseCtx, "okx")
	if err != nil {
		t.Fatal(err)
	}
	if !cursor.ScannedUntil.After(prev) {
		t.Fatalf("completed incremental scan cursor was discarded on pending recheck cancel: prev=%v got=%v", prev, cursor.ScannedUntil)
	}
	if cursor.PendingError != "" {
		t.Fatalf("shutdown cancellation overwrote pending_error with %q", cursor.PendingError)
	}
}

func TestPollerClientCacheIsolatedPerProvider(t *testing.T) {
	ctx := context.Background()
	st, err := store.Open(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	cfg := config.Default()
	cfg.Server.Production = false
	cfg.Binance.APIKey = "b-key"
	cfg.Binance.APISecret = "b-secret"
	cfg.Checkout.Binance.UID = "b-uid"
	cfg.OKX.APIKey = "o-key"
	cfg.OKX.APISecret = "o-secret"
	cfg.OKX.Passphrase = "o-pass"
	cfg.Checkout.OKX.UID = "o-uid"
	mgr := settings.New(cfg, st)
	if err = mgr.Load(ctx); err != nil {
		t.Fatal(err)
	}

	p := NewPoller(mgr, st)
	bSrc := &fixture{}
	p.clients[config.ProviderBinance] = &connection{config: *mgr.Get(), source: bSrc, uid: "b-uid"}

	// Changing OKX settings or Binance RegisterURL / PollInterval must not evict Binance client cache.
	otherChanged := *mgr.Get()
	otherChanged.OKX.APISecret = "o-secret-2"
	otherChanged.Checkout.Binance.RegisterURL = "https://example.com/register"
	otherChanged.Binance.PollInterval = 15
	got, err := p.client(ctx, &otherChanged, config.ProviderBinance)
	if err != nil {
		t.Fatal(err)
	}
	if got != bSrc {
		t.Fatal("unrelated provider/display change evicted cached Binance client")
	}

	// Changing Binance API secret must invalidate the cached client.
	secretChanged := otherChanged
	secretChanged.Binance.APISecret = "b-secret-rotated"
	p.UseTestTransports()
	p.clients[config.ProviderBinance] = &connection{config: *mgr.Get(), source: bSrc, uid: "b-uid"}
	got2, err := p.client(ctx, &secretChanged, config.ProviderBinance)
	if err == nil && got2 == bSrc {
		t.Fatal("rotated Binance secret reused stale cached client")
	}
}

type failOnceSource struct {
	uid        string
	failFirst  bool
	calls      int
	seenRanges [][2]time.Time
}

func (f *failOnceSource) AccountUID(context.Context) (string, error) { return f.uid, nil }
func (f *failOnceSource) History(_ context.Context, start, end time.Time) ([]model.Transaction, error) {
	f.calls++
	if f.failFirst && f.calls == 1 {
		return nil, errors.New("transient error")
	}
	f.seenRanges = append(f.seenRanges, [2]time.Time{start, end})
	return nil, nil
}

func TestPendingRecheckFailureDoesNotSkipFailedRecords(t *testing.T) {
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
	cfg.Order.ExpireMinutes = 120
	mgr := settings.New(cfg, st)
	if err = mgr.Load(ctx); err != nil {
		t.Fatal(err)
	}
	var times []time.Time
	for i := 0; i < 3; i++ {
		at := time.Now().Add(-time.Duration(90-10*i) * time.Minute)
		times = append(times, at)
		if _, err = st.Process(ctx, model.Transaction{
			Provider:   "binance",
			EventID:    fmt.Sprintf("p-fail-%d", i),
			Reference:  fmt.Sprintf("r-fail-%d", i),
			Receiver:   "receiver",
			Currency:   "USDT",
			Units:      1_000_000,
			OccurredAt: at,
			State:      model.Unconfirmed,
		}, ""); err != nil {
			t.Fatal(err)
		}
	}
	p := NewPoller(mgr, st)
	src := &failOnceSource{uid: "receiver", failFirst: true}
	now := time.Now()
	if err = p.recheckPending(ctx, mgr.Get(), "binance", src, now.Add(-time.Minute), now); err == nil {
		t.Fatal("expected first recheckPending call to fail on item 0")
	}
	// Offset must remain at 0 so the next call retries item 0 instead of skipping all 3 items.
	if err = p.recheckPending(ctx, mgr.Get(), "binance", src, now.Add(-time.Minute), now); err != nil {
		t.Fatal(err)
	}
	if len(src.seenRanges) != 3 {
		t.Fatalf("expected all 3 records retried after failure on item 0, got %d calls", len(src.seenRanges))
	}
	if src.seenRanges[0][0].After(times[0]) || src.seenRanges[0][1].Before(times[0]) {
		t.Fatalf("first retried window did not cover record 0: window=%v at=%v", src.seenRanges[0], times[0])
	}
}

type denseRecheckSource struct {
	uid   string
	maxW  time.Duration
	rows  []model.Transaction
	calls int
}

func (d *denseRecheckSource) AccountUID(context.Context) (string, error) { return d.uid, nil }
func (d *denseRecheckSource) History(_ context.Context, start, end time.Time) ([]model.Transaction, error) {
	d.calls++
	if end.Sub(start) > d.maxW {
		return nil, &binance.APIError{Kind: binance.ErrorIncomplete}
	}
	var out []model.Transaction
	for _, r := range d.rows {
		if !r.OccurredAt.Before(start) && !r.OccurredAt.After(end) {
			out = append(out, r)
		}
	}
	return out, nil
}

func TestPendingRecheckShrinksWindowOnOverflow(t *testing.T) {
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
	cfg.Binance.LookbackBuffer = 60
	cfg.Checkout.Binance.UID = "receiver"
	mgr := settings.New(cfg, st)
	if err = mgr.Load(ctx); err != nil {
		t.Fatal(err)
	}
	at := time.Now().Add(-90 * time.Minute)
	pending := model.Transaction{
		Provider:   "binance",
		EventID:    "dense-recheck-1",
		Reference:  "dense-ref-1",
		Receiver:   "receiver",
		Currency:   "USDT",
		Units:      1_000_000,
		OccurredAt: at,
		State:      model.Unconfirmed,
	}
	if _, err = st.Process(ctx, pending, ""); err != nil {
		t.Fatal(err)
	}
	confirmed := pending
	confirmed.State = model.Confirmed
	src := &denseRecheckSource{uid: "receiver", maxW: 2 * time.Minute, rows: []model.Transaction{confirmed}}
	p := NewPoller(mgr, st)
	now := time.Now()
	if err = p.recheckPending(ctx, mgr.Get(), "binance", src, now.Add(-time.Minute), now); err != nil {
		t.Fatalf("recheckPending failed on dense overflow instead of shrinking radius: %v", err)
	}
	if src.calls < 2 {
		t.Fatalf("expected window halving retries, got %d calls", src.calls)
	}
	var stored model.Transaction
	if err = st.DB().First(&stored, "event_id = ?", pending.EventID).Error; err != nil {
		t.Fatal(err)
	}
	if stored.State != model.Confirmed {
		t.Fatalf("expected confirmed state after shrunk recheck, got %s", stored.State)
	}
}

func TestHistoricalScanShutdownCancelDoesNotRecordFailure(t *testing.T) {
	baseCtx := context.Background()
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
	if err = mgr.Load(baseCtx); err != nil {
		t.Fatal(err)
	}
	_, err = st.Create(baseCtx, model.Order{TradeNo: "hist-cancel", OutTradeNo: "hist-cancel", OriginUnits: 1_000_000, ExpiresAt: time.Now().Add(time.Minute)})
	if err != nil {
		t.Fatal(err)
	}
	o, err := st.Select(baseCtx, "hist-cancel", "okx", "USDT", "receiver", 2, 10000, 10000, 99, 120)
	if err != nil {
		t.Fatal(err)
	}
	if err = st.RequestHistoricalScan(baseCtx, o.TradeNo, "operator", "verify shutdown cancel"); err != nil {
		t.Fatal(err)
	}
	scanCtx, cancel := context.WithCancel(baseCtx)
	defer cancel()
	src := &cancelOpaqueSource{uid: "receiver", cancel: cancel, onCall: 1, failMsg: "OKX network request failed"}
	p := NewPoller(mgr, st)
	p.clients["okx"] = &connection{config: *mgr.Get(), source: src, uid: "receiver"}

	if err = p.scanHistorical(scanCtx, mgr.Get(), "okx"); !errors.Is(err, context.Canceled) {
		t.Fatalf("expected context.Canceled, got %v", err)
	}
	var job model.HistoricalScan
	if err = st.DB().First(&job, "order_id = ?", o.ID).Error; err != nil {
		t.Fatal(err)
	}
	if job.LastError != "" || job.State != "pending" {
		t.Fatalf("shutdown cancellation polluted historical scan job: state=%q last_error=%q", job.State, job.LastError)
	}
}

func TestNotifierRecoveryBatchResetsRetryDelaySchedule(t *testing.T) {
	ctx := context.Background()
	st, err := store.Open(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	merchant := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "temporary failure", http.StatusServiceUnavailable)
	}))
	defer merchant.Close()
	cfg := config.Default()
	cfg.Server.Production = false
	cfg.Notify.RetryDelays = []int{5, 15, 30, 60, 180}
	cfg.Notify.MaxAttempts = 2
	mgr := settings.New(cfg, st)
	if err = mgr.Load(ctx); err != nil {
		t.Fatal(err)
	}
	_, err = st.Create(ctx, model.Order{TradeNo: "backoff-recovery", OutTradeNo: "backoff-recovery", OriginUnits: 1_000_000, NotifyURL: merchant.URL, ExpiresAt: time.Now().Add(time.Hour)})
	if err != nil {
		t.Fatal(err)
	}
	o, err := st.Select(ctx, "backoff-recovery", "binance", "USDT", "receiver", 2, 10000, 10000, 99, 120)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = st.Process(ctx, model.Transaction{Provider: "binance", EventID: "backoff-ev", Receiver: "receiver", Currency: "USDT", Units: o.PayUnits, OccurredAt: time.Now(), State: model.Confirmed}, ""); err != nil {
		t.Fatal(err)
	}
	// Simulate 5 prior attempts already exhausted, then operator triggers a fresh recovery batch.
	if err = st.DB().Model(&model.Callback{}).Where("order_id = ?", o.ID).Updates(map[string]any{"attempts": 5, "state": "exhausted", "next_at": time.Now().Add(-time.Second)}).Error; err != nil {
		t.Fatal(err)
	}
	if err = st.RetryCallback(ctx, o.TradeNo, "operator", "verified merchant ready", 2); err != nil {
		t.Fatal(err)
	}

	n := NewNotifier(mgr, st)
	n.UseTestTransport()
	before := time.Now()
	if did, err := n.Deliver(ctx); err != nil || !did {
		t.Fatalf("expected recovery delivery attempt, got did=%v err=%v", did, err)
	}
	var cb model.Callback
	if err = st.DB().First(&cb, "order_id = ?", o.ID).Error; err != nil {
		t.Fatal(err)
	}
	delay := cb.NextAt.Sub(before)
	if delay > 10*time.Second {
		t.Fatalf("recovery batch first retry used lifetime attempts backoff (%v) instead of 5s", delay)
	}
}

func TestNotifierParentDeadlineExceededReleasesLease(t *testing.T) {
	baseCtx := context.Background()
	st, err := store.Open(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	stopSlow := make(chan struct{})
	merchant := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		select {
		case <-r.Context().Done():
		case <-stopSlow:
		}
	}))
	defer merchant.Close()
	defer close(stopSlow)
	cfg := config.Default()
	cfg.Server.Production = false
	cfg.Notify.MaxAttempts = 1
	cfg.Notify.TimeoutSeconds = 10
	mgr := settings.New(cfg, st)
	if err = mgr.Load(baseCtx); err != nil {
		t.Fatal(err)
	}
	_, err = st.Create(baseCtx, model.Order{TradeNo: "deadline-cb", OutTradeNo: "deadline-cb", OriginUnits: 1_000_000, NotifyURL: merchant.URL, ExpiresAt: time.Now().Add(time.Hour)})
	if err != nil {
		t.Fatal(err)
	}
	o, err := st.Select(baseCtx, "deadline-cb", "binance", "USDT", "receiver", 2, 10000, 10000, 99, 120)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = st.Process(baseCtx, model.Transaction{Provider: "binance", EventID: "deadline-ev", Receiver: "receiver", Currency: "USDT", Units: o.PayUnits, OccurredAt: time.Now(), State: model.Confirmed}, ""); err != nil {
		t.Fatal(err)
	}
	n := NewNotifier(mgr, st)
	n.UseTestTransport()
	parentCtx, cancel := context.WithTimeout(baseCtx, 80*time.Millisecond)
	defer cancel()
	if _, err = n.Deliver(parentCtx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("expected context.DeadlineExceeded, got %v", err)
	}
	var cb model.Callback
	if err = st.DB().First(&cb, "order_id = ?", o.ID).Error; err != nil {
		t.Fatal(err)
	}
	if cb.State != "queued" || cb.Attempts != 0 {
		t.Fatalf("parent deadline consumed attempt instead of releasing lease: state=%s attempts=%d", cb.State, cb.Attempts)
	}
}

func TestNotifierRequiresConfirmedTransactionForCallback(t *testing.T) {
	ctx := context.Background()
	st, err := store.Open(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	var calls atomic.Int64
	merchant := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		fmt.Fprint(w, "success")
	}))
	defer merchant.Close()
	cfg := config.Default()
	cfg.Server.Production = false
	mgr := settings.New(cfg, st)
	if err = mgr.Load(ctx); err != nil {
		t.Fatal(err)
	}
	now := time.Now()
	order := model.Order{
		SigningKeyID: mgr.Get().BEpusdt.TokenVersion,
		TradeNo:      "unconfirmed-tx-cb",
		OutTradeNo:   "unconfirmed-tx-cb",
		OriginUnits:  1_000_000,
		PayUnits:     1_000_000,
		Status:       model.Paid,
		PaidAt:       &now,
		ExpiresAt:    now.Add(time.Hour),
		NotifyURL:    merchant.URL,
	}
	if err = st.DB().Create(&order).Error; err != nil {
		t.Fatal(err)
	}
	id := order.ID
	if err = st.DB().Create(&model.Transaction{
		Provider:   "binance",
		EventID:    "not-confirmed-ev",
		OrderID:    &id,
		State:      model.Unconfirmed,
		Receiver:   "receiver",
		Currency:   "USDT",
		Units:      1_000_000,
		OccurredAt: now,
	}).Error; err != nil {
		t.Fatal(err)
	}
	if err = st.DB().Create(&model.Callback{
		OrderID:      order.ID,
		SigningKeyID: order.SigningKeyID,
		State:        "queued",
		NextAt:       now.Add(-time.Second),
	}).Error; err != nil {
		t.Fatal(err)
	}

	n := NewNotifier(mgr, st)
	n.UseTestTransport()
	if did, err := n.Deliver(ctx); err == nil || did {
		t.Fatalf("expected error for non-confirmed transaction, got did=%v err=%v", did, err)
	}
	if calls.Load() != 0 {
		t.Fatal("callback was sent for a non-confirmed transaction")
	}
}
