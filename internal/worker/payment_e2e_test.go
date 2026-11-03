package worker_test

import (
	"bytes"
	"context"
	"crypto/md5"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"litepay/internal/maintenance"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"litepay/internal/config"
	"litepay/internal/model"
	"litepay/internal/server"
	"litepay/internal/settings"
	"litepay/internal/store"
	"litepay/internal/worker"
)

// Independent merchant signing oracle: natural JSON values, sorted non-empty keys.
// Contract pinned to dujiao-next 5c2f06ad56b82d6a6d68886704bca0b7d04ceb06.
func merchantSign(body map[string]any, token string) string {
	keys := []string{}
	for k, v := range body {
		if k != "signature" && v != nil && fmt.Sprint(v) != "" {
			keys = append(keys, k)
		}
	}
	sort.Strings(keys)
	pairs := []string{}
	for _, k := range keys {
		var value string
		switch v := body[k].(type) {
		case float64:
			value = strconv.FormatFloat(v, 'f', -1, 64)
		default:
			value = fmt.Sprint(v)
		}
		pairs = append(pairs, k+"="+value)
	}
	sum := md5.Sum([]byte(strings.Join(pairs, "&") + token))
	return hex.EncodeToString(sum[:])
}

type merchant struct {
	mu               sync.Mutex
	token, trade     string
	paid             bool
	deliveries, hits int
	dropACK          bool
}

func (m *merchant) serve(w http.ResponseWriter, r *http.Request) {
	m.mu.Lock()
	defer m.mu.Unlock()
	var body map[string]any
	if json.NewDecoder(r.Body).Decode(&body) != nil || body["signature"] != merchantSign(body, m.token) || body["trade_id"] != m.trade || body["order_id"] != "SHOP-1" || body["amount"] != 2.5 || body["actual_amount"] != 2.51 || body["token"] != "receiver" || body["block_transaction_id"] == "" || body["status"] != float64(2) {
		fmt.Fprint(w, "fail")
		return
	}
	m.hits++
	if !m.paid {
		m.paid = true
		m.deliveries++
	}
	if m.dropACK {
		m.dropACK = false
		conn, _, err := w.(http.Hijacker).Hijack()
		if err == nil {
			conn.Close()
		}
		return
	}
	fmt.Fprint(w, "success")
}

type exchange struct {
	mu                                             sync.Mutex
	provider, currency, amount, kind, receiver, id string
	at                                             time.Time
	fail                                           bool
}

func (e *exchange) serve(w http.ResponseWriter, r *http.Request) {
	e.mu.Lock()
	defer e.mu.Unlock()
	switch r.URL.Path {
	case "/api/v3/account":
		fmt.Fprint(w, `{"uid":"receiver"}`)
	case "/api/v5/public/time":
		fmt.Fprintf(w, `{"code":"0","data":[{"ts":"%d"}]}`, time.Now().UnixMilli())
	case "/api/v5/account/config":
		fmt.Fprint(w, `{"code":"0","data":[{"uid":"receiver"}]}`)
	default:
		if e.fail {
			w.WriteHeader(503)
			fmt.Fprint(w, `{"error":"temporarily unavailable"}`)
			return
		}
		rows := []any{}
		if e.id != "" {
			if e.provider == "binance" {
				row := map[string]any{"orderType": e.kind, "transactionId": e.id, "amount": e.amount, "currency": e.currency, "transactionTime": e.at.UnixMilli(), "receiverInfo": map[string]string{"binanceId": e.receiver}}
				if e.currency == "USDC" {
					row["fundsDetail"] = []any{map[string]any{"currency": e.currency, "amount": e.amount}}
				}
				rows = append(rows, row)
			} else if r.URL.Query().Get("ccy") == e.currency {
				rows = append(rows, map[string]any{"depId": e.id, "fromWdId": "from-" + e.id, "amt": e.amount, "ccy": e.currency, "ts": fmt.Sprint(e.at.UnixMilli()), "state": "2", "type": e.kind})
			}
		}
		code := "0"
		if e.provider == "binance" {
			code = "000000"
		}
		json.NewEncoder(w).Encode(map[string]any{"code": code, "success": true, "data": rows})
	}
}

func TestPaymentAcceptanceHTTPToMerchantDelivery(t *testing.T) {
	runPaymentAcceptance(t, false)
}

func TestPaymentInquiryHTTPToMerchantDelivery(t *testing.T) {
	runPaymentAcceptance(t, true)
}

func runPaymentAcceptance(t *testing.T, selfService bool) {
	for _, provider := range []string{"binance", "okx"} {
		for _, currency := range []string{"USDT", "USDC"} {
			t.Run(provider+"/"+currency, func(t *testing.T) {
				ctx := context.Background()
				dbpath := filepath.Join(t.TempDir(), "acceptance.db")
				st, err := store.Open(dbpath)
				if err != nil {
					t.Fatal(err)
				}
				cfg := config.Default()
				cfg.BEpusdt.APIToken = "acceptance-only-token"
				cfg.Server.BaseURL = "https://pay.example"
				cfg.Notify.RetryDelays = []int{0}
				cfg.Checkout.Currencies = []string{"USDT", "USDC"}
				ex := &exchange{provider: provider, receiver: "receiver", currency: currency, kind: "PAY"}
				if provider == "okx" {
					ex.kind = "3"
				}
				exchangeServer := httptest.NewServer(http.HandlerFunc(ex.serve))
				defer exchangeServer.Close()
				cfg.Binance.APIKey = "test"
				cfg.Binance.APISecret = "test"
				cfg.Binance.BaseURL = exchangeServer.URL
				cfg.Checkout.Binance.UID = "receiver"
				cfg.OKX.APIKey = "test"
				cfg.OKX.APISecret = "test"
				cfg.OKX.Passphrase = "test"
				cfg.OKX.BaseURL = exchangeServer.URL
				cfg.Checkout.OKX.UID = "receiver"
				mgr := settings.New(cfg, st)
				poll := worker.NewPoller(mgr, st)
				notify := worker.NewNotifier(mgr, st)
				shop := &merchant{token: cfg.BEpusdt.APIToken, dropACK: true}
				shopServer := httptest.NewServer(http.HandlerFunc(shop.serve))
				defer shopServer.Close()
				engine, err := server.New(mgr, st, poll)
				if err != nil {
					t.Fatal(err)
				}
				api := httptest.NewServer(engine)
				defer api.Close()
				post := func(path string, body map[string]any) map[string]any {
					t.Helper()
					raw, _ := json.Marshal(body)
					resp, err := http.Post(api.URL+path, "application/json", bytes.NewReader(raw))
					if err != nil {
						t.Fatal(err)
					}
					defer resp.Body.Close()
					var got map[string]any
					if err := json.NewDecoder(resp.Body).Decode(&got); err != nil {
						t.Fatal(err)
					}
					return got
				}
				request := map[string]any{"order_id": "SHOP-1", "amount": 2.5, "notify_url": shopServer.URL, "redirect_url": "https://shop.example/order/SHOP-1", "fiat": "USD", "reselect": true, "timeout": 600, "currencies": currency}
				request["signature"] = merchantSign(request, shop.token)
				created := post("/api/v1/order/create-order", request)
				if created["status_code"] != float64(200) {
					t.Fatalf("create: %+v", created)
				}
				data := created["data"].(map[string]any)
				shop.trade = data["trade_id"].(string)
				retry := post("/api/v1/order/create-order", request)
				if retry["data"].(map[string]any)["trade_id"] != shop.trade {
					t.Fatal("merchant retry replaced contract")
				}
				resp, err := http.PostForm(api.URL+"/api/order/payment", url.Values{"trade_no": {shop.trade}, "provider": {provider}, "currency": {currency}})
				if err != nil {
					t.Fatal(err)
				}
				var selected struct {
					Code int
					Data struct{ Amount string }
				}
				json.NewDecoder(resp.Body).Decode(&selected)
				resp.Body.Close()
				if selected.Code != 0 || selected.Data.Amount != "2.51" {
					t.Fatalf("selection: %+v", selected)
				}
				order, err := st.GetByTradeNo(ctx, shop.trade)
				if err != nil {
					t.Fatal(err)
				}
				if selfService {
					// Advance only the eligibility clock, never payment or callback state.
					if err := st.DB().Model(order).Updates(map[string]any{"created_at": time.Now().Add(-2 * time.Minute), "selected_at": time.Now().Add(-61 * time.Second)}).Error; err != nil {
						t.Fatal(err)
					}
				}
				// Time changes belong to the fake exchange only: no direct payment-state writes.
				at := time.Now().Add(time.Millisecond)
				for time.Now().Before(at) {
					time.Sleep(time.Millisecond)
				}
				ex.mu.Lock()
				ex.at = at
				ex.amount = selected.Data.Amount
				ex.id = "realistic-id"
				ex.fail = true
				ex.mu.Unlock()
				worker.AcceptanceScan(poll, ctx, cfg, provider)
				order, _ = st.GetByTradeNo(ctx, shop.trade)
				if order.Status != model.StatusReady {
					t.Fatal("failed exchange marked paid")
				}
				ex.mu.Lock()
				ex.fail = false
				ex.mu.Unlock()
				// Rejected incoming types must never mark the order paid.
				ex.mu.Lock()
				originalKind := ex.kind
				if provider == "binance" {
					ex.kind = "PAY_REFUND"
				} else {
					ex.kind = "4"
				}
				ex.id = "filtered-type"
				ex.mu.Unlock()
				worker.AcceptanceScan(poll, ctx, cfg, provider)
				order, _ = st.GetByTradeNo(ctx, shop.trade)
				if order.Status != model.StatusReady {
					t.Fatal("refund/chain deposit credited")
				}
				ex.mu.Lock()
				ex.kind = originalKind
				ex.id = "realistic-id"
				ex.mu.Unlock()
				if provider == "binance" {
					ex.mu.Lock()
					ex.receiver = "another-receiver"
					ex.id = "wrong-receiver"
					ex.mu.Unlock()
					worker.AcceptanceScan(poll, ctx, cfg, provider)
					order, _ = st.GetByTradeNo(ctx, shop.trade)
					if order.Status != model.StatusReady {
						t.Fatal("wrong recipient credited")
					}
					ex.mu.Lock()
					ex.receiver = "receiver"
					ex.amount = "-2.51"
					ex.id = "outgoing"
					ex.mu.Unlock()
					worker.AcceptanceScan(poll, ctx, cfg, provider)
					order, _ = st.GetByTradeNo(ctx, shop.trade)
					if order.Status != model.StatusReady {
						t.Fatal("outgoing transfer credited")
					}
				}
				ex.mu.Lock()
				ex.id = "realistic-id"
				ex.mu.Unlock()
				// Wrong amount first: evidence must persist, and no merchant fulfillment occurs.
				ex.mu.Lock()
				ex.amount = "9.99"
				ex.mu.Unlock()
				worker.AcceptanceScan(poll, ctx, cfg, provider)
				observations, err := st.RecentTxObservations(ctx, 20)
				found := false
				for _, obs := range observations {
					if strings.Contains(obs.TransactionID, "realistic-id") {
						found = true
					}
				}
				if err != nil || !found {
					t.Fatalf("missing unmatched evidence: %+v %v", observations, err)
				}
				ex.mu.Lock()
				ex.amount = selected.Data.Amount
				ex.mu.Unlock()
				if selfService {
					ref := "realistic-id"
					if provider == "okx" {
						ref = "from-" + ref
					}
					for attempt := 0; attempt < 2; attempt++ {
						resp, err := http.PostForm(api.URL+"/api/order/inquiry", url.Values{"trade_no": {shop.trade}, "reference": {ref}})
						if err != nil {
							t.Fatal(err)
						}
						var got map[string]any
						json.NewDecoder(resp.Body).Decode(&got)
						resp.Body.Close()
						if resp.StatusCode != 200 || got["result"] != "paid" {
							t.Fatalf("inquiry failed: %d %+v", resp.StatusCode, got)
						}
					}
					inquiry, err := st.PaymentInquiry(ctx, shop.trade)
					if err != nil || inquiry == nil || inquiry.Result != "paid" || inquiry.Attempts != 1 {
						t.Fatalf("inquiry audit: %+v %v", inquiry, err)
					}
				} else {
					worker.AcceptanceScan(poll, ctx, cfg, provider)
				}
				order, _ = st.GetByTradeNo(ctx, shop.trade)
				if order.Status != model.StatusPaid {
					t.Fatalf("not credited: %+v", order)
				}
				worker.AcceptanceNotify(notify, ctx, http.DefaultTransport)
				shop.mu.Lock()
				paid, deliveries := shop.paid, shop.deliveries
				shop.mu.Unlock()
				if !paid || deliveries != 1 {
					t.Fatal("merchant did not fulfill")
				}
				order, _ = st.GetByTradeNo(ctx, shop.trade)
				if order.NotifyStatus == model.NotifySuccess {
					t.Fatal("lost ACK falsely accepted")
				}
				// Restart storage and workers. Persisted pending callback must be redelivered.
				sqlDB, _ := st.DB().DB()
				sqlDB.Close()
				st, err = store.Open(dbpath)
				if err != nil {
					t.Fatal(err)
				}
				mgr = settings.New(cfg, st)
				poll = worker.NewPoller(mgr, st)
				notify = worker.NewNotifier(mgr, st)
				api.Close()
				engine, err = server.New(mgr, st, poll)
				if err != nil {
					t.Fatal(err)
				}
				api = httptest.NewServer(engine)
				defer api.Close()
				worker.AcceptanceScan(poll, ctx, cfg, provider)
				worker.AcceptanceNotify(notify, ctx, http.DefaultTransport)
				order, _ = st.GetByTradeNo(ctx, shop.trade)
				if order.NotifyStatus != model.NotifySuccess {
					t.Fatalf("notification not recovered: %+v", order)
				}
				shop.mu.Lock()
				deliveries, hits := shop.deliveries, shop.hits
				shop.mu.Unlock()
				if deliveries != 1 || hits != 2 {
					t.Fatalf("duplicate fulfillment: deliveries=%d hits=%d", deliveries, hits)
				}
				var count int64
				st.DB().Model(&model.IncomingTx{}).Count(&count)
				if count != 1 {
					t.Fatalf("transaction credited %d times", count)
				}
				observations, _ = st.RecentTxObservations(ctx, 20)
				for _, obs := range observations {
					if strings.Contains(obs.TransactionID, "realistic-id") {
						t.Fatal("credited payment still unassigned")
					}
				}
				// A second transfer with a different ID is evidence, not another fulfillment.
				ex.mu.Lock()
				ex.id = "second-payment"
				ex.mu.Unlock()
				worker.AcceptanceScan(poll, ctx, cfg, provider)
				observations, _ = st.RecentTxObservations(ctx, 20)
				found = false
				for _, obs := range observations {
					if strings.Contains(obs.TransactionID, "second-payment") {
						found = true
					}
				}
				if !found {
					t.Fatal("duplicate payment evidence missing")
				}
				worker.AcceptanceNotify(notify, ctx, http.DefaultTransport)
				// Another HTTP-created order is canceled via the signed protocol before payment.
				request["order_id"] = "SHOP-2"
				request["signature"] = merchantSign(request, shop.token)
				other := post("/api/v1/order/create-order", request)
				if other["status_code"] != float64(200) {
					t.Fatalf("second create: %+v", other)
				}
				otherTrade := other["data"].(map[string]any)["trade_id"].(string)
				selection, err := http.PostForm(api.URL+"/api/order/payment", url.Values{"trade_no": {otherTrade}, "provider": {provider}, "currency": {currency}})
				if err != nil {
					t.Fatal(err)
				}
				var otherSelected struct {
					Code int
					Data struct{ Amount string }
				}
				json.NewDecoder(selection.Body).Decode(&otherSelected)
				selection.Body.Close()
				if otherSelected.Code != 0 {
					t.Fatal("second payment selection failed")
				}
				cancelBody := map[string]any{"trade_id": otherTrade}
				cancelBody["signature"] = merchantSign(cancelBody, shop.token)
				if canceled := post("/api/v1/order/cancel-transaction", cancelBody); canceled["status_code"] != float64(200) {
					t.Fatal("cancel failed")
				}
				ex.mu.Lock()
				ex.id = "after-cancel"
				ex.amount = otherSelected.Data.Amount
				ex.at = time.Now()
				ex.mu.Unlock()
				worker.AcceptanceScan(poll, ctx, cfg, provider)
				otherOrder, _ := st.GetByTradeNo(ctx, otherTrade)
				if otherOrder.Status != model.StatusCanceled {
					t.Fatal("canceled order reopened")
				}
				observations, _ = st.RecentTxObservations(ctx, 20)
				found = false
				for _, obs := range observations {
					if strings.Contains(obs.TransactionID, "after-cancel") {
						found = true
					}
				}
				if !found {
					t.Fatal("payment after cancel lost")
				}
				// Age only clock fields to exercise API delay beyond grace, using the
				// actual reaper and claim path rather than writing payment statuses.
				request["order_id"] = "SHOP-3"
				request["signature"] = merchantSign(request, shop.token)
				late := post("/api/v1/order/create-order", request)
				if late["status_code"] != float64(200) {
					t.Fatal("late create failed")
				}
				lateTrade := late["data"].(map[string]any)["trade_id"].(string)
				lr, err := http.PostForm(api.URL+"/api/order/payment", url.Values{"trade_no": {lateTrade}, "provider": {provider}, "currency": {currency}})
				if err != nil {
					t.Fatal(err)
				}
				var ls struct {
					Code int
					Data struct{ Amount string }
				}
				json.NewDecoder(lr.Body).Decode(&ls)
				lr.Body.Close()
				if ls.Code != 0 {
					t.Fatal("late selection failed")
				}
				createdAt := time.Now().Add(-5 * time.Minute)
				selectedAt := createdAt.Add(time.Minute)
				expiredAt := createdAt.Add(2 * time.Minute)
				if err := st.DB().Model(&model.Order{}).Where("trade_no = ?", lateTrade).Updates(map[string]any{"created_at": createdAt, "selected_at": selectedAt, "expired_at": expiredAt}).Error; err != nil {
					t.Fatal(err)
				}
				if err := st.DB().Model(&model.AmountSlot{}).Where("trade_no = ?", lateTrade).Update("expires_at", expiredAt.Add(2*time.Minute)).Error; err != nil {
					t.Fatal(err)
				}
				ex.mu.Lock()
				ex.fail = true
				ex.id = "delayed-valid-payment"
				ex.at = expiredAt.Add(-time.Second)
				ex.amount = ls.Data.Amount
				ex.mu.Unlock()
				worker.AcceptanceScan(poll, ctx, cfg, provider)
				worker.AcceptanceReap(worker.NewReaper(st, mgr), ctx)
				lateOrder, _ := st.GetByTradeNo(ctx, lateTrade)
				if lateOrder.ExpireFinalized || lateOrder.Status != model.StatusExpired {
					t.Fatal("API outage prematurely finalized order")
				}
				ex.mu.Lock()
				ex.fail = false
				ex.mu.Unlock()
				worker.AcceptanceScan(poll, ctx, cfg, provider)
				lateOrder, _ = st.GetByTradeNo(ctx, lateTrade)
				if lateOrder.Status != model.StatusPaid {
					t.Fatal("valid payment lost after API recovered beyond grace")
				}
				shop.mu.Lock()
				defer shop.mu.Unlock()
				if shop.deliveries != 1 {
					t.Fatal("duplicate payment delivered again")
				}
				sqlDB, _ = st.DB().DB()
				sqlDB.Close()
			})
		}
	}
}

func TestWorkersStartOnlyAfterGateAndReportRealReadiness(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	st, err := store.Open(filepath.Join(t.TempDir(), "runtime.db"))
	if err != nil {
		t.Fatal(err)
	}
	cfg := config.Default()
	cfg.BEpusdt.APIToken = "runtime-token"
	ex := httptest.NewServer(http.HandlerFunc((&exchange{provider: "binance"}).serve))
	defer ex.Close()
	cfg.Binance.APIKey = "key"
	cfg.Binance.APISecret = "secret"
	cfg.Binance.BaseURL = ex.URL
	cfg.Checkout.Binance.UID = "receiver"
	mgr := settings.New(cfg, st)
	poll := worker.NewPoller(mgr, st)
	engine, err := server.New(mgr, st, poll)
	if err != nil {
		t.Fatal(err)
	}
	gate := filepath.Join(t.TempDir(), "gate")
	t.Setenv("LITEPAY_DEPLOY_GATE", gate)
	if err := os.WriteFile(gate, []byte("hold"), 0600); err != nil {
		t.Fatal(err)
	}
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		if !maintenance.Wait(ctx) {
			return
		}
		wg.Add(3)
		go func() { defer wg.Done(); poll.Run(ctx) }()
		go func() { defer wg.Done(); worker.NewNotifier(mgr, st).Run(ctx) }()
		go func() { defer wg.Done(); worker.NewReaper(st, mgr).Run(ctx) }()
		<-ctx.Done()
	}()
	defer func() { cancel(); wg.Wait(); db, _ := st.DB().DB(); db.Close() }()
	check := func() int {
		w := httptest.NewRecorder()
		engine.ServeHTTP(w, httptest.NewRequest("GET", "/readyz", nil))
		return w.Code
	}
	if check() != 503 {
		t.Fatal("maintenance reported ready")
	}
	if err := os.Remove(gate); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(10 * time.Second)
	for check() != 200 && time.Now().Before(deadline) {
		time.Sleep(50 * time.Millisecond)
	}
	if check() != 200 {
		t.Fatalf("workers or real mock exchange scan never became ready: %+v", poll.StatsFor("binance"))
	}
	if err := os.WriteFile(gate, []byte("hold"), 0600); err != nil {
		t.Fatal(err)
	}
	if check() != 503 {
		t.Fatal("emergency maintenance did not stop readiness")
	}
}

func TestDujiaoStatus3RejectionDoesNotAcknowledgeMerchant(t *testing.T) {
	st, err := store.Open(filepath.Join(t.TempDir(), "expired.db"))
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	cfg := config.Default()
	cfg.BEpusdt.APIToken = "contract-token"
	cfg.Notify.RetryDelays = []int{0}
	// Timer fixture is the only direct construction here; production reaper creates
	// EXPIRED and queues status=3. The primary payment test uses HTTP creation only.
	o, err := st.CreateOrder(ctx, store.CreateOrderInput{Protocol: "bepusdt", Pid: "bepusdt", TradeNo: "EXPIRY", OutTradeNo: "SHOP-1", OriginMoney: "2.50", OriginCurrency: "USD", ExpiredAt: time.Now().Add(-3 * time.Minute)})
	if err != nil {
		t.Fatal(err)
	}
	hits := 0
	shop := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var body map[string]any
		json.NewDecoder(r.Body).Decode(&body)
		if body["status"] != float64(3) || body["signature"] != merchantSign(body, cfg.BEpusdt.APIToken) {
			t.Error("invalid expiry callback")
		}
		hits++
		fmt.Fprint(w, "fail")
	}))
	defer shop.Close()
	if err := st.DB().Model(o).Update("notify_url", shop.URL).Error; err != nil {
		t.Fatal(err)
	}
	mgr := settings.New(cfg, st)
	worker.AcceptanceReap(worker.NewReaper(st, mgr), ctx)
	worker.AcceptanceNotify(worker.NewNotifier(mgr, st), ctx, http.DefaultTransport)
	got, _ := st.GetByTradeNo(ctx, o.TradeNo)
	if hits != 1 || got.NotifyStatus == model.NotifySuccess || got.Status != model.StatusExpired {
		t.Fatalf("merchant expiry rejection hidden: hits=%d order=%+v", hits, got)
	}
}

func TestStoredHistoryGapPreventsBusinessReadiness(t *testing.T) {
	ctx := context.Background()
	st, err := store.Open(filepath.Join(t.TempDir(), "gap-ready.db"))
	if err != nil {
		t.Fatal(err)
	}
	cfg := config.Default()
	ex := httptest.NewServer(http.HandlerFunc((&exchange{provider: "binance"}).serve))
	defer ex.Close()
	cfg.Binance.APIKey = "key"
	cfg.Binance.APISecret = "secret"
	cfg.Binance.BaseURL = ex.URL
	cfg.Checkout.Binance.UID = "receiver"
	mgr := settings.New(cfg, st)
	poll := worker.NewPoller(mgr, st)
	worker.AcceptanceScan(poll, ctx, cfg, "binance")
	st.WorkerResult("notifier", true)
	st.WorkerResult("reaper", true)
	engine, err := server.New(mgr, st, poll)
	if err != nil {
		t.Fatal(err)
	}
	check := func() int {
		w := httptest.NewRecorder()
		engine.ServeHTTP(w, httptest.NewRequest("GET", "/readyz", nil))
		return w.Code
	}
	if check() != 200 {
		t.Fatal("successful worker scan did not become ready")
	}
	if err := st.RecordScanGap(ctx, "binance", time.Now().Add(-10*24*time.Hour), time.Now().Add(-7*24*time.Hour)); err != nil {
		t.Fatal(err)
	}
	if check() != 503 {
		t.Fatal("persistent gap hidden by in-memory success")
	}
}
