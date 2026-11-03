package server

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
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

// okxMock 是只读的模拟 OKX：账号配置、服务器时间与站内充值历史。
// data 载荷由测试步骤切换，用于驱动未确认 → 完整 → 不完整的响应序列。
type okxMock struct {
	mu    sync.Mutex
	data  string
	calls map[string]int
}

func (m *okxMock) set(data string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.data = data
}
func (m *okxMock) count(path string) int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.calls[path]
}
func (m *okxMock) handle(t *testing.T, w http.ResponseWriter, r *http.Request) {
	m.mu.Lock()
	m.calls[r.URL.Path]++
	m.mu.Unlock()
	if r.URL.Path != "/api/v5/public/time" && (r.Header.Get("OK-ACCESS-KEY") == "" || r.Header.Get("OK-ACCESS-SIGN") == "") {
		// 公共时间接口不签名；私有接口必须携带只读凭据。
		t.Error("unsigned private exchange request", r.URL.Path)
		w.WriteHeader(401)
		return
	}
	switch r.URL.Path {
	case "/api/v5/public/time":
		fmt.Fprintf(w, `{"code":"0","data":[{"ts":"%d"}]}`, time.Now().UnixMilli())
	case "/api/v5/account/config":
		fmt.Fprint(w, `{"code":"0","data":[{"uid":"receiver"}]}`)
	case "/api/v5/asset/deposit-history":
		if r.URL.Query().Get("type") != "3" {
			t.Error("deposit query did not filter internal transfers")
		}
		fmt.Fprint(w, m.data)
	default:
		w.WriteHeader(404)
	}
}

// TestEndToEndPaymentLoop 走通一次完整支付闭环：商户签名建单 → 收银台 → 选择 OKX →
// 分配精确金额 → 交易所未确认与不完整响应不得核销 → 自助查款按编号唯一核销 →
// 自动扫描不重复创建回调 → BEpusdt 签名回调失败重试且不重复交付 → 订单状态与商户
// 确认相互独立。交易所侧的金额/UID/币种/时间错误拒绝已由 store 与适配器测试覆盖。
func TestEndToEndPaymentLoop(t *testing.T) {
	ctx := context.Background()
	st, e := store.Open(":memory:")
	if e != nil {
		t.Fatal(e)
	}
	defer st.Close()

	// 模拟商城：校验 BEpusdt 签名与回调契约，第一次不确认，重试才确认。
	token := ""
	var mu sync.Mutex
	received, fulfilled := 0, 0
	seen := map[string]bool{}
	merchant := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var payload map[string]any
		d := json.NewDecoder(r.Body)
		d.UseNumber()
		if d.Decode(&payload) != nil || !sign.BEpusdtVerifyGeneric(payload, token) {
			t.Error("merchant received invalid callback signature")
			w.WriteHeader(400)
			return
		}
		if payload["block_transaction_id"] != "dep-1" || payload["status"] != json.Number("2") || payload["actual_amount"] != json.Number("10.01") || payload["token"] != "receiver" {
			t.Error("incorrect callback contract", payload)
		}
		key := payload["order_id"].(string)
		mu.Lock()
		if !seen[key] {
			seen[key] = true
			fulfilled++
		}
		received++
		first := received == 1
		mu.Unlock()
		if first {
			w.Write([]byte("not yet"))
		} else {
			w.Write([]byte("success"))
		}
	}))
	defer merchant.Close()

	exchange := &okxMock{calls: map[string]int{}}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		exchange.handle(t, w, r)
	}))
	defer srv.Close()

	cfg := config.Default()
	cfg.OKX.APIKey = "key"
	cfg.OKX.APISecret = "secret"
	cfg.OKX.Passphrase = "pass"
	cfg.OKX.BaseURL = srv.URL
	cfg.Checkout.OKX.UID = "receiver"
	mgr := settings.New(cfg, st)
	if e = mgr.Load(ctx); e != nil {
		t.Fatal(e)
	}
	token = mgr.Get().BEpusdt.APIToken
	depositAt := time.Now().Add(-5 * time.Second)
	setDeposit := func(state string) {
		exchange.set(`{"code":"0","msg":"","data":[{"depId":"dep-1","fromWdId":"wd-1","ccy":"USDT","amt":"10.01","state":"` + state + `","ts":"` + fmt.Sprint(depositAt.UnixMilli()) + `","txId":"hash-1"}]}`)
	}

	poller := worker.NewPoller(mgr, st)
	poller.UseTestTransports()
	engine, e := New(mgr, st, poller)
	if e != nil {
		t.Fatal(e)
	}
	do := func(method, path string, body *string) *httptest.ResponseRecorder {
		t.Helper()
		var r *http.Request
		if body != nil {
			r = httptest.NewRequest(method, path, strings.NewReader(*body))
			r.Header.Set("Content-Type", "application/x-www-form-urlencoded")
		} else {
			r = httptest.NewRequest(method, path, nil)
		}
		w := httptest.NewRecorder()
		engine.ServeHTTP(w, r)
		return w
	}

	// 商户创建订单并打开收银台。
	payload := map[string]any{"order_id": "shop-order-1", "amount": json.Number("10.001"), "notify_url": merchant.URL, "redirect_url": "https://shop.example/return", "fiat": "USD"}
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
	if e = json.Unmarshal(w.Body.Bytes(), &created); e != nil || created.Status != 200 || created.Data.ID == "" {
		t.Fatal(w.Body.String(), e)
	}
	no := created.Data.ID
	if w = do("GET", "/pay/"+no, nil); w.Code != 200 {
		t.Fatal("cashier page failed", w.Code)
	}

	// 付款人选择 OKX：分配独占的精确付款金额。
	form := url.Values{"trade_no": {no}, "provider": {"okx"}, "currency": {"USDT"}}
	formBody := form.Encode()
	if w = do("POST", "/api/order/payment", &formBody); w.Code != 200 {
		t.Fatal(w.Body.String())
	}
	o, e := st.GetByTradeNo(ctx, no)
	if e != nil || o.Status != model.Ready || o.PayAmount() != "10.01" || o.Receiver != "receiver" {
		t.Fatal("payment method selection failed", o, e)
	}

	// 交易所尚未确认（state=1 credited）：只允许留下待确认证据，不能核销。
	setDeposit("1")
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
	if fresh, _ := st.GetByTradeNo(ctx, no); fresh.Status != model.Ready {
		t.Fatal("unconfirmed deposit paid the order", fresh.Status)
	}

	// 交易所响应不完整（data:null）：扫描失败并留下诊断，游标与订单都不能动。
	setNull := `{"code":"0","msg":"","data":null}`
	exchange.set(setNull)
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

	// 自助查款：付款人提供转账单号，交易转为成功后由查款核销。
	setDeposit("2")
	if e = st.DB().Model(&model.Order{}).Where("trade_no = ?", no).Update("selected_at", time.Now().Add(-2*time.Minute)).Error; e != nil {
		t.Fatal(e)
	}
	inquiry := url.Values{"trade_no": {no}, "reference": {"wd-1"}}
	inquiryBody := inquiry.Encode()
	w = do("POST", "/api/order/inquiry", &inquiryBody)
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
	mu.Lock()
	deliveredOnce := received == 1 && fulfilled == 1
	mu.Unlock()
	if !deliveredOnce {
		t.Fatal("first callback delivery failed", received, fulfilled)
	}
	var cb model.Callback
	if e = st.DB().First(&cb, "order_id = ?", o.ID).Error; e != nil || cb.DeliveredAt != nil {
		t.Fatal("HTTP 200 without acknowledgement counted as delivered", cb)
	}
	if fresh, _ := st.GetByTradeNo(ctx, no); fresh.Status != model.Paid {
		t.Fatal("merchant acknowledgement state changed the order", fresh.Status)
	}
	w = do("GET", "/api/order/status?order_id="+no, nil)
	if !strings.Contains(w.Body.String(), `"status":"PAID"`) || !strings.Contains(w.Body.String(), `"merchant_confirmed":false`) {
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
	mu.Lock()
	defer mu.Unlock()
	if received != 2 || fulfilled != 1 {
		t.Fatal("callback retried incorrectly", received, fulfilled)
	}
	w = do("GET", "/api/order/status?order_id="+no, nil)
	if !strings.Contains(w.Body.String(), `"merchant_confirmed":true`) {
		t.Fatal("merchant acknowledgement missing", w.Body.String())
	}
	if exchange.count("/api/v5/account/config") == 0 {
		t.Fatal("account UID was never verified against the checkout UID")
	}
}
