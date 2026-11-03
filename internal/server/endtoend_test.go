package server

import (
	"bytes"
	"context"
	"crypto/md5"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"litepay/internal/config"
	"litepay/internal/model"
	"litepay/internal/settings"
	"litepay/internal/sign"
	"litepay/internal/store"
	"litepay/internal/worker"
)

// merchantMock 模拟 BEpusdt 商城：校验回调签名与契约，第一次不确认，重试才确认。
// fulfilled 按商户订单号去重，因此它始终等于"收到过回调的已支付订单数"。
// 验签复刻 dujiao-next BEpusdt 适配器的真实算法，而不是用 LitePay 自己的通用验签。
type merchantMock struct {
	t         *testing.T
	token     *string
	eventID   string
	amount    string
	receiver  string
	mu        sync.Mutex
	received  int
	fulfilled int
	seen      map[string]bool
}

// dujiaoCallback 与 dujiao-next 的 CallbackData 字段一致；金额字段故意保留为
// interface{}，由验签逻辑按适配器行为转成 float64 后用 %v 渲染。
type dujiaoCallback struct {
	TradeID            string      `json:"trade_id"`
	OrderID            string      `json:"order_id"`
	Amount             interface{} `json:"amount"`
	ActualAmount       interface{} `json:"actual_amount"`
	Token              string      `json:"token"`
	BlockTransactionID string      `json:"block_transaction_id"`
	Signature          string      `json:"signature"`
	Status             int         `json:"status"`
}

// dujiaoSign 复刻 dujiao-next：非空非 signature 参数按 ASCII 排序，k=v 用 & 连接，
// 末尾拼接 token 后取 md5 小写。金额已由调用方转成 float64，%v 在 >= 1e6 或
// < 1e-4 时会渲染成指数形式——这正是网关必须拒绝互操作范围外金额的原因。
func dujiaoSign(params map[string]any, token string) string {
	keys := make([]string, 0, len(params))
	for k, v := range params {
		if k == "signature" || dujiaoEmpty(v) {
			continue
		}
		keys = append(keys, k)
	}
	sort.Strings(keys)
	pairs := make([]string, 0, len(keys))
	for _, k := range keys {
		pairs = append(pairs, fmt.Sprintf("%s=%v", k, params[k]))
	}
	sum := md5.Sum([]byte(strings.Join(pairs, "&") + token))
	return hex.EncodeToString(sum[:])
}

func dujiaoEmpty(v any) bool {
	if v == nil {
		return true
	}
	if s, ok := v.(string); ok {
		return strings.TrimSpace(s) == ""
	}
	return false
}

func dujiaoFloat(v any) float64 {
	switch n := v.(type) {
	case float64:
		return n
	case string:
		f, _ := strconv.ParseFloat(n, 64)
		return f
	}
	return 0
}

func (m *merchantMock) handler() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		var cb dujiaoCallback
		if json.Unmarshal(body, &cb) != nil {
			m.t.Error("merchant received undecodable callback")
			w.WriteHeader(400)
			return
		}
		params := map[string]any{
			"trade_id":             cb.TradeID,
			"order_id":             cb.OrderID,
			"amount":               dujiaoFloat(cb.Amount),
			"actual_amount":        dujiaoFloat(cb.ActualAmount),
			"token":                cb.Token,
			"block_transaction_id": cb.BlockTransactionID,
			"status":               cb.Status,
		}
		if !strings.EqualFold(dujiaoSign(params, *m.token), cb.Signature) {
			m.t.Error("merchant signature verification failed (dujiao-next algorithm)")
			w.WriteHeader(400)
			return
		}
		if cb.Status != 2 || cb.BlockTransactionID != m.eventID || dujiaoFloat(cb.ActualAmount) != dujiaoFloat(m.amount) || cb.Token != m.receiver {
			m.t.Error("incorrect callback contract", cb)
		}
		key := cb.OrderID
		m.mu.Lock()
		defer m.mu.Unlock()
		if !m.seen[key] {
			m.seen[key] = true
			m.fulfilled++
		}
		m.received++
		if m.received == 1 {
			w.Write([]byte("not yet"))
		} else {
			w.Write([]byte("success"))
		}
	}
}

func (m *merchantMock) counts() (received, fulfilled int) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.received, m.fulfilled
}

// exchangeMock 记录路径调用次数并按路径返回预设响应。
type exchangeMock struct {
	mu    sync.Mutex
	calls map[string]int
	paths map[string]func(w http.ResponseWriter, r *http.Request) bool
}

func (m *exchangeMock) handle(w http.ResponseWriter, r *http.Request) {
	m.mu.Lock()
	m.calls[r.URL.Path]++
	respond, ok := m.paths[r.URL.Path]
	m.mu.Unlock()
	if !ok {
		w.WriteHeader(404)
		return
	}
	if !respond(w, r) {
		w.WriteHeader(401)
	}
}

func (m *exchangeMock) count(path string) int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.calls[path]
}

// requireCredential 校验私有接口携带了对应交易所的只读凭据（OKX 头或 Binance 签名）。
func requireCredential(r *http.Request) bool {
	if r.Header.Get("OK-ACCESS-KEY") != "" && r.Header.Get("OK-ACCESS-SIGN") != "" {
		return true
	}
	return r.Header.Get("X-MBX-APIKEY") != "" && r.URL.Query().Get("signature") != ""
}

// postForm 以表单请求引擎并返回响应。
func postForm(engine http.Handler, path string, form url.Values) *httptest.ResponseRecorder {
	r := httptest.NewRequest("POST", path, strings.NewReader(form.Encode()))
	r.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	w := httptest.NewRecorder()
	engine.ServeHTTP(w, r)
	return w
}

func getRequest(engine http.Handler, path string) *httptest.ResponseRecorder {
	w := httptest.NewRecorder()
	engine.ServeHTTP(w, httptest.NewRequest("GET", path, nil))
	return w
}

// createOrderViaBEpusdt 用商户签名创建订单，返回网关交易号。
func createOrderViaBEpusdt(t *testing.T, engine http.Handler, token, notifyURL string) string {
	t.Helper()
	payload := map[string]any{"order_id": "shop-order-1", "amount": json.Number("10.001"), "notify_url": notifyURL, "redirect_url": "https://shop.example/return", "fiat": "USD"}
	payload["signature"] = sign.BEpusdtSignGeneric(payload, token)
	body, _ := json.Marshal(payload)
	r := httptest.NewRequest("POST", "/api/v1/order/create-order", bytes.NewReader(body))
	r.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	engine.ServeHTTP(w, r)
	var created struct {
		Status int `json:"status_code"`
		Data   struct {
			ID string `json:"trade_id"`
		} `json:"data"`
	}
	if e := json.Unmarshal(w.Body.Bytes(), &created); e != nil || created.Status != 200 || created.Data.ID == "" {
		t.Fatal(w.Body.String(), e)
	}
	return created.Data.ID
}

// selectPayment 以付款人身份选择支付方式。
func selectPayment(t *testing.T, engine http.Handler, no, provider string) {
	t.Helper()
	w := postForm(engine, "/api/order/payment", url.Values{"trade_no": {no}, "provider": {provider}, "currency": {"USDT"}})
	if w.Code != 200 {
		t.Fatal(w.Body.String())
	}
}

// TestEndToEndOKXPaymentLoop 走通一次完整 OKX 支付闭环：商户签名建单 → 收银台 →
// 选择 OKX → 分配精确金额 → 付款窗口之前的交易不能自动核销 → 交易所未确认与不完整
// 响应不得核销 → 自助查款按转账单号唯一核销 → 自动扫描不重复创建回调 → BEpusdt
// 签名回调失败重试且不重复交付 → 订单状态与商户确认相互独立。
//
// 时间顺序：建单 → 选定付款方式 → 交易发生（选定之后）→ 交易确认 → 查款 → 回调。
// 唯一的时间控制：选定付款方式后立即把付款窗口整体前移两分钟，用于跳过查款一分钟的
// 防滥用延迟；此后不再改动任何业务时间，交易时间落在平移后的窗口之内且晚于选择时刻，
// 全部核销条件依然真实成立。
func TestEndToEndOKXPaymentLoop(t *testing.T) {
	ctx := context.Background()
	st, e := store.Open(":memory:")
	if e != nil {
		t.Fatal(e)
	}
	defer st.Close()

	token := ""
	merchant := &merchantMock{t: t, token: &token, eventID: "dep-1", amount: "10.01", receiver: "receiver", seen: map[string]bool{}}
	merchantSrv := httptest.NewServer(merchant.handler())
	defer merchantSrv.Close()

	exchange := &exchangeMock{calls: map[string]int{}, paths: map[string]func(w http.ResponseWriter, r *http.Request) bool{
		"/api/v5/public/time": func(w http.ResponseWriter, r *http.Request) bool {
			fmt.Fprintf(w, `{"code":"0","data":[{"ts":"%d"}]}`, time.Now().UnixMilli())
			return true
		},
		"/api/v5/account/config": func(w http.ResponseWriter, r *http.Request) bool {
			if !requireCredential(r) {
				return false
			}
			fmt.Fprint(w, `{"code":"0","data":[{"uid":"receiver"}]}`)
			return true
		},
	}}
	setDeposits := func(rows string) {
		exchange.mu.Lock()
		exchange.paths["/api/v5/asset/deposit-history"] = func(w http.ResponseWriter, r *http.Request) bool {
			if !requireCredential(r) {
				return false
			}
			if r.URL.Query().Get("type") != "3" {
				t.Error("deposit query did not filter internal transfers")
			}
			fmt.Fprint(w, rows)
			return true
		}
		exchange.mu.Unlock()
	}

	cfg := config.Default()
	cfg.OKX.APIKey = "key"
	cfg.OKX.APISecret = "secret"
	cfg.OKX.Passphrase = "pass"
	cfg.OKX.BaseURL = exchangeSrvURL(t, exchange)
	cfg.Checkout.OKX.UID = "receiver"
	mgr := settings.New(cfg, st)
	if e = mgr.Load(ctx); e != nil {
		t.Fatal(e)
	}
	token = mgr.Get().BEpusdt.APIToken

	poller := worker.NewPoller(mgr, st)
	poller.UseTestTransports()
	engine, e := New(mgr, st, poller)
	if e != nil {
		t.Fatal(e)
	}

	// 商户创建订单并打开收银台，随后付款人选择 OKX。
	no := createOrderViaBEpusdt(t, engine, token, merchantSrv.URL)
	if w := getRequest(engine, "/pay/"+no); w.Code != 200 {
		t.Fatal("cashier page failed", w.Code)
	}
	selectPayment(t, engine, no, "okx")
	o, e := st.GetByTradeNo(ctx, no)
	if e != nil || o.Status != model.Ready || o.PayAmount() != "10.01" || o.Receiver != "receiver" {
		t.Fatal("payment method selection failed", o, e)
	}

	// 时间控制：窗口整体前移两分钟（查款延迟由此跳过），之后不再改动业务时间。
	selected := time.Now().Add(-2 * time.Minute)
	if e = st.DB().Model(&model.Order{}).Where("trade_no = ?", no).Updates(map[string]any{"selected_at": selected, "expires_at": selected.Add(10 * time.Minute)}).Error; e != nil {
		t.Fatal(e)
	}

	// 交易发生在选定付款方式之后：dep-1 在窗口内，dep-pre 在窗口之前（已确认）。
	depositAt := time.Now()
	depositRow := func(depID, wdID, state string, at time.Time) string {
		return `{"depId":"` + depID + `","fromWdId":"` + wdID + `","ccy":"USDT","amt":"10.01","state":"` + state + `","ts":"` + fmt.Sprint(at.UnixMilli()) + `","txId":"hash-` + depID + `"}`
	}
	setDeposits(`{"code":"0","msg":"","data":[` + depositRow("dep-1", "wd-1", "1", depositAt) + `,` + depositRow("dep-pre", "wd-pre", "2", selected.Add(-time.Minute)) + `]}`)

	// 自动扫描：窗口内的充值未确认只留证据；窗口之前的交易即使已确认也不能核销。
	if e = poller.Scan(ctx, "okx"); e != nil {
		t.Fatal(e)
	}
	var tx model.Transaction
	if e = st.DB().First(&tx, "provider = ? AND event_id = ?", "okx", "dep-1").Error; e != nil {
		t.Fatal(e)
	}
	if tx.State != model.Unconfirmed || tx.OrderID != nil {
		t.Fatal("unconfirmed deposit claimed", tx)
	}
	var early model.Transaction
	if e = st.DB().First(&early, "provider = ? AND event_id = ?", "okx", "dep-pre").Error; e != nil {
		t.Fatal(e)
	}
	if early.State != model.Confirmed || early.OrderID != nil {
		t.Fatal("pre-window deposit was claimed or dropped", early)
	}
	if fresh, _ := st.GetByTradeNo(ctx, no); fresh.Status != model.Ready {
		t.Fatal("unconfirmed deposit paid the order", fresh.Status)
	}

	// 交易所响应不完整（data:null）：扫描失败并留下诊断，游标与订单都不能动。
	exchange.mu.Lock()
	exchange.paths["/api/v5/asset/deposit-history"] = func(w http.ResponseWriter, r *http.Request) bool {
		if !requireCredential(r) {
			return false
		}
		fmt.Fprint(w, `{"code":"0","msg":"","data":null}`)
		return true
	}
	exchange.mu.Unlock()
	if e = poller.Scan(ctx, "okx"); e == nil {
		t.Fatal("incomplete exchange response accepted")
	}
	cursor, _ := st.Cursor(ctx, "okx")
	if cursor.LastError == "" {
		t.Fatal("incomplete scan left no diagnostic")
	}
	if fresh, _ := st.GetByTradeNo(ctx, no); fresh.Status != model.Ready {
		t.Fatal("incomplete response paid the order", fresh.Status)
	}

	// 自助查款：交易所把同一笔充值置为成功后，付款人以转账单号查款核销。
	setDeposits(`{"code":"0","msg":"","data":[` + depositRow("dep-1", "wd-1", "2", depositAt) + `,` + depositRow("dep-pre", "wd-pre", "2", selected.Add(-time.Minute)) + `]}`)
	w := postForm(engine, "/api/order/inquiry", url.Values{"trade_no": {no}, "reference": {"wd-1"}})
	if w.Code != 200 || !strings.Contains(w.Body.String(), `"result":"paid"`) {
		t.Fatal("inquiry did not settle the order", w.Body.String())
	}

	// 自动扫描随后读到同一笔交易：只允许确认已核销，不得重复创建回调。
	if e = poller.Scan(ctx, "okx"); e != nil {
		t.Fatal(e)
	}
	var callbacks int64
	st.DB().Model(&model.Callback{}).Count(&callbacks)
	if callbacks != 1 {
		t.Fatal("duplicate callback after scan", callbacks)
	}
	if fresh, _ := st.GetByTradeNo(ctx, no); fresh.Status != model.Paid {
		t.Fatal("order lost paid status", fresh.Status)
	}

	// 商户通知：第一次商户不确认，订单保持已支付；重试后确认成功，且不重复交付。
	notifier := worker.NewNotifier(mgr, st)
	notifier.UseTestTransport()
	if _, e = notifier.Deliver(ctx); e != nil {
		t.Fatal(e)
	}
	var cb model.Callback
	if e = st.DB().First(&cb, "order_id = ?", o.ID).Error; e != nil || cb.DeliveredAt != nil {
		t.Fatal("HTTP 200 without acknowledgement counted as delivered", cb)
	}
	if fresh, _ := st.GetByTradeNo(ctx, no); fresh.Status != model.Paid {
		t.Fatal("merchant acknowledgement state changed the order", fresh.Status)
	}
	if w = getRequest(engine, "/api/order/status?order_id="+no); !strings.Contains(w.Body.String(), `"status":"PAID"`) || !strings.Contains(w.Body.String(), `"merchant_confirmed":false`) {
		t.Fatal("order status and merchant confirmation are not independent", w.Body.String())
	}
	if e = st.DB().Model(&model.Callback{}).Where("order_id = ?", o.ID).Update("next_at", time.Now().Add(-time.Second)).Error; e != nil {
		t.Fatal(e)
	}
	if _, e = notifier.Deliver(ctx); e != nil {
		t.Fatal(e)
	}
	if _, e = notifier.Deliver(ctx); e != nil {
		t.Fatal(e)
	}
	if received, fulfilled := merchant.counts(); received != 2 || fulfilled != 1 {
		t.Fatal("callback retried incorrectly", received, fulfilled)
	}
	if w = getRequest(engine, "/api/order/status?order_id="+no); !strings.Contains(w.Body.String(), `"merchant_confirmed":true`) {
		t.Fatal("merchant acknowledgement missing", w.Body.String())
	}
	if exchange.count("/api/v5/account/config") == 0 {
		t.Fatal("account UID was never verified against the checkout UID")
	}
}

// exchangeSrvURL 把模拟交易所挂到本地 HTTP 服务上。
func exchangeSrvURL(t *testing.T, mock *exchangeMock) string {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mock.handle(w, r)
	}))
	t.Cleanup(srv.Close)
	return srv.URL
}

// TestEndToEndBinancePaymentLoop 用真实 Binance 适配器走通最小闭环：账户 UID 校验 →
// 商户签名建单 → 收银台选择 Binance USDT → 文档形态的 PAY 收款记录（transactionId
// 既是事件身份也是查款编号）→ 自动扫描唯一核销 → 再次扫描与查款都不能重复核销 →
// BEpusdt 回调只对应这一笔已支付订单。时间顺序与 OKX 闭环相同，且全程不改业务时间
// （自动扫描没有查款的一分钟延迟，无需时间控制）。
func TestEndToEndBinancePaymentLoop(t *testing.T) {
	ctx := context.Background()
	st, e := store.Open(":memory:")
	if e != nil {
		t.Fatal(e)
	}
	defer st.Close()

	const (
		accountUID  = "354937868"
		transaction = "M_P_71505104267788288"
		orderID     = "463524995526164480" // 纯数字订单号，用户在 App 中可复制
	)
	token := ""
	merchant := &merchantMock{t: t, token: &token, eventID: transaction, amount: "10.01", receiver: accountUID, seen: map[string]bool{}}
	merchantSrv := httptest.NewServer(merchant.handler())
	defer merchantSrv.Close()

	// 记录在选定付款方式之后构造：transactionTime 天然落在有效付款窗口内。
	var payPayload string
	exchange := &exchangeMock{calls: map[string]int{}, paths: map[string]func(w http.ResponseWriter, r *http.Request) bool{
		"/api/v3/account": func(w http.ResponseWriter, r *http.Request) bool {
			if r.Header.Get("X-MBX-APIKEY") == "" {
				return false
			}
			// 文档定义 uid 为 int64。
			fmt.Fprint(w, `{"uid":`+accountUID+`}`)
			return true
		},
		"/sapi/v1/pay/transactions": func(w http.ResponseWriter, r *http.Request) bool {
			if r.Header.Get("X-MBX-APIKEY") == "" || r.URL.Query().Get("signature") == "" {
				return false
			}
			fmt.Fprint(w, payPayload)
			return true
		},
	}}
	cfg := config.Default()
	cfg.Binance.APIKey = "key"
	cfg.Binance.APISecret = "secret"
	cfg.Binance.BaseURL = exchangeSrvURL(t, exchange)
	cfg.Checkout.Binance.UID = accountUID
	mgr := settings.New(cfg, st)
	if e = mgr.Load(ctx); e != nil {
		t.Fatal(e)
	}
	token = mgr.Get().BEpusdt.APIToken

	poller := worker.NewPoller(mgr, st)
	poller.UseTestTransports()
	engine, e := New(mgr, st, poller)
	if e != nil {
		t.Fatal(e)
	}

	no := createOrderViaBEpusdt(t, engine, token, merchantSrv.URL)
	selectPayment(t, engine, no, "binance")
	o, e := st.GetByTradeNo(ctx, no)
	if e != nil || o.Status != model.Ready || o.PayAmount() != "10.01" || o.Receiver != accountUID {
		t.Fatal("payment method selection failed", o, e)
	}

	// 交易发生在选定付款方式之后：文档形态的 PAY 收款记录，总额与单资产明细一致。
	// transactionTime 以毫秒返回，取选定时刻后 50ms，保证毫秒精度下仍晚于 selected_at；
	// 随后等待 60ms 再扫描，记录时间因此必然落在扫描窗口 [start, end] 之内。
	payTime := o.SelectedAt.Add(50 * time.Millisecond)
	payPayload = `{"code":"000000","message":"success","success":true,"data":[` +
		`{"orderType":"PAY","transactionId":"` + transaction + `","orderId":"` + orderID + `","transactionTime":` + fmt.Sprint(payTime.UnixMilli()) + `,"amount":"10.01","currency":"USDT","walletTypes":[1],"fundsDetail":[{"currency":"USDT","amount":"10.01"}],"payerInfo":{"name":"Payer"},"receiverInfo":{"name":"Shop","type":"MERCHANT","binanceId":"` + accountUID + `"}}]}`
	time.Sleep(60 * time.Millisecond)

	// 自动扫描唯一核销：transactionId 同时是事件身份与外部查询编号。
	if e = poller.Scan(ctx, "binance"); e != nil {
		t.Fatal(e)
	}
	var tx model.Transaction
	if e = st.DB().First(&tx, "provider = ? AND event_id = ?", "binance", transaction).Error; e != nil {
		t.Fatal(e)
	}
	if tx.Reference != orderID || tx.Receiver != accountUID || tx.Units != 10010000 || tx.OrderID == nil {
		t.Fatal("PAY record not claimed as evidence", tx)
	}
	if fresh, _ := st.GetByTradeNo(ctx, no); fresh.Status != model.Paid {
		t.Fatal("scan did not settle the order", fresh.Status)
	}
	var callbacks int64
	st.DB().Model(&model.Callback{}).Count(&callbacks)
	if callbacks != 1 {
		t.Fatal("claim did not create exactly one callback", callbacks)
	}

	// 再次扫描不能重复核销。
	if e = poller.Scan(ctx, "binance"); e != nil {
		t.Fatal(e)
	}
	st.DB().Model(&model.Callback{}).Count(&callbacks)
	if callbacks != 1 {
		t.Fatal("second scan duplicated the claim", callbacks)
	}

	// 查款不能重复核销：已支付订单不能再查款，且不会触发任何交易所查询。
	payCalls := exchange.count("/sapi/v1/pay/transactions")
	w := postForm(engine, "/api/order/inquiry", url.Values{"trade_no": {no}, "reference": {orderID}})
	if w.Code == 200 {
		t.Fatal("inquiry accepted for a paid order", w.Body.String())
	}
	st.DB().Model(&model.Callback{}).Count(&callbacks)
	if callbacks != 1 || exchange.count("/sapi/v1/pay/transactions") != payCalls {
		t.Fatal("rejected inquiry touched claims or the exchange", callbacks)
	}

	// BEpusdt 回调只对应这一笔已支付订单：首次不确认、重试确认、之后不再投递。
	notifier := worker.NewNotifier(mgr, st)
	notifier.UseTestTransport()
	if _, e = notifier.Deliver(ctx); e != nil {
		t.Fatal(e)
	}
	if e = st.DB().Model(&model.Callback{}).Where("order_id = ?", o.ID).Update("next_at", time.Now().Add(-time.Second)).Error; e != nil {
		t.Fatal(e)
	}
	if _, e = notifier.Deliver(ctx); e != nil {
		t.Fatal(e)
	}
	if _, e = notifier.Deliver(ctx); e != nil {
		t.Fatal(e)
	}
	if received, fulfilled := merchant.counts(); received != 2 || fulfilled != 1 {
		t.Fatal("callback delivery not unique", received, fulfilled)
	}
	if exchange.count("/api/v3/account") == 0 {
		t.Fatal("account UID was never verified against the checkout UID")
	}
}
