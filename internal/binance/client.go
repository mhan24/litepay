package binance

import (
	"context"
	"encoding/json"
	"fmt"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"time"

	"litepay/internal/exchange"
	"litepay/internal/httpx"
	"litepay/internal/model"
)

type flexString = exchange.Text
type Client struct {
	clockOffset      int64
	lastRequestStart time.Time
	backoffUntil     time.Time
	apiKey           string
	apiSecret        string
	baseURL          string
	recvWindow       int
	limit            int
	http             *httpx.Client
	requestMu        sync.Mutex
	nextRequest      time.Time
	requestInterval  time.Duration
}

// New 创建客户端。
func New(apiKey, apiSecret, baseURL string, recvWindow, limit int) *Client {
	if limit <= 0 || limit > 100 {
		limit = 100
	}
	if baseURL == "" {
		baseURL = "https://api.binance.com"
	}
	return &Client{
		apiKey:          apiKey,
		apiSecret:       apiSecret,
		baseURL:         strings.TrimRight(baseURL, "/"),
		recvWindow:      recvWindow,
		limit:           limit,
		http:            httpx.NewClient(15*time.Second, false),
		requestInterval: 2 * time.Second,
	}
}

// UseTestTransport 仅供本包测试访问本地模拟服务；生产入口不会调用。
func (c *Client) UseTestTransport() {
	c.http = httpx.NewClient(15*time.Second, true)
	c.requestInterval = 0
}

func (c *Client) AccountUID(ctx context.Context) (string, error) {
	if c.apiKey == "" || c.apiSecret == "" {
		return "", fmt.Errorf("未配置币安 API Key/Secret")
	}
	ctx, cancel := context.WithTimeout(ctx, 15*time.Second)
	defer cancel()
	body, err := c.signedGet(ctx, "/api/v3/account", nil)
	if err != nil {
		return "", err
	}

	var payload struct {
		UID flexString `json:"uid"`
	}
	if err := json.Unmarshal(body, &payload); err != nil {
		return "", &APIError{Kind: ErrorParse}
	}
	uid := strings.TrimSpace(string(payload.UID))
	if uid == "" {
		return "", &APIError{Kind: ErrorParse}
	}
	return uid, nil
}

// payHistoryRequests 限制单次 History 的请求数。Pay 交易历史按 UID 计 weight 3000，
// 客户端以 2 秒间隔串行请求，30 秒预算内本来就只能发出十几个请求；把上限写成显式
// 常量，避免一次扫描意外吃掉整分钟的调用额度。
const payHistoryRequests = 12

// History splits saturated windows because the Pay endpoint has no page cursor.
// 文档限制：startTime 与 endTime 间隔不得超过 90 天，可查最近 18 个月；调用方窗口远小于该限制。
func (c *Client) History(ctx context.Context, start, end time.Time) ([]model.Transaction, error) {
	ctx, cancel := context.WithTimeout(budgeted(ctx, payHistoryRequests), 30*time.Second)
	defer cancel()
	if start.IsZero() || end.Before(start) {
		return nil, &APIError{Kind: ErrorBusiness}
	}
	type window struct{ a, b int64 }
	pending := []window{{start.UnixMilli(), end.UnixMilli()}}
	out := []model.Transaction{}
	seen := map[string]bool{}
	for requests := 0; len(pending) > 0; requests++ {
		if requests >= payHistoryRequests {
			return nil, &APIError{Kind: ErrorIncomplete}
		}
		w := pending[len(pending)-1]
		pending = pending[:len(pending)-1]
		rows, n, err := c.fetchWindow(ctx, w.a, w.b)
		if err != nil {
			return nil, err
		}
		if n >= c.limit {
			if w.a >= w.b {
				return nil, &APIError{Kind: ErrorIncomplete}
			}
			mid := w.a + (w.b-w.a)/2
			pending = append(pending, window{mid + 1, w.b}, window{w.a, mid})
			continue
		}
		for _, t := range rows {
			if !seen[t.EventID] {
				seen[t.EventID] = true
				out = append(out, t)
			}
		}
	}
	return out, nil
}

type asset struct {
	Currency exchange.Text `json:"currency"`
	Amount   exchange.Text `json:"amount"`
}

func (c *Client) fetchWindow(ctx context.Context, start, end int64) ([]model.Transaction, int, error) {
	body, err := c.signedGet(ctx, "/sapi/v1/pay/transactions", url.Values{"limit": {strconv.Itoa(c.limit)}, "startTime": {strconv.FormatInt(start, 10)}, "endTime": {strconv.FormatInt(end, 10)}})
	if err != nil {
		return nil, 0, err
	}
	var p struct {
		Code    exchange.Text    `json:"code"`
		Success bool             `json:"success"`
		Data    *json.RawMessage `json:"data"`
	}
	if json.Unmarshal(body, &p) != nil {
		return nil, 0, &APIError{Kind: ErrorParse}
	}
	if p.Code != "000000" || !p.Success {
		return nil, 0, &APIError{Kind: ErrorBusiness}
	}
	// 官方 OpenAPI 契约（binance/binance-api-swagger）要求 data 必须存在且为数组。
	// 缺失、null、非数组都不是文档化的空窗口形态：放行任何一种都可能把不完整响应
	// 当成成功扫描而推进游标。宁可让本次扫描失败、游标停在原地，也不臆测语义；
	// data:null 是否真的代表空窗口只能用真实样本确认（见待验证清单）。
	if p.Data == nil || strings.TrimSpace(string(*p.Data)) == "null" {
		return nil, 0, &APIError{Kind: ErrorParse}
	}
	items := []json.RawMessage{}
	if json.Unmarshal(*p.Data, &items) != nil {
		return nil, 0, &APIError{Kind: ErrorParse}
	}
	out := []model.Transaction{}
	seen := map[string]bool{}
	for _, raw := range items {
		var d struct {
			ID       exchange.Text `json:"transactionId"`
			OrderID  exchange.Text `json:"orderId"`
			Type     exchange.Text `json:"orderType"`
			Time     exchange.Text `json:"transactionTime"`
			Amount   exchange.Text `json:"amount"`
			Currency exchange.Text `json:"currency"`
			Details  []asset       `json:"fundsDetail"`
			Receiver struct {
				UID exchange.Text `json:"binanceId"`
			} `json:"receiverInfo"`
		}
		if json.Unmarshal(raw, &d) != nil || d.ID == "" || seen[string(d.ID)] {
			return nil, 0, &APIError{Kind: ErrorParse}
		}
		seen[string(d.ID)] = true
		ms, e := d.Time.Millis()
		if e != nil || ms < start || ms > end {
			return nil, 0, &APIError{Kind: ErrorParse}
		}
		// 交易历史的商户订单号是 orderId（实测为纯数字，如 463524995526164480，即用户在
		// App 账单里可复制的编号）；transactionId 是带字母前缀的事件身份。事件归属只用
		// transactionId；orderId 作为外部查询编号，用于后台核对编号。
		reference := string(d.ID)
		if d.OrderID != "" {
			reference = string(d.OrderID)
		}
		// 官方 OpenAPI 规范把 receiverInfo.binanceId（收款 UID）列为必填；该字段在真实
		// 响应中已验证完整未脱敏（数字形态）。缺失或对不上时只会拒绝核销，不会误认。
		t := model.Transaction{Provider: "binance", EventID: string(d.ID), Reference: reference, Receiver: string(d.Receiver.UID), Currency: strings.ToUpper(string(d.Currency)), OccurredAt: time.UnixMilli(ms), State: model.Confirmed}
		// One parent event is one consumable payment. Preserve every detail, never sum currencies.
		if len(d.Details) > 1 {
			b, _ := json.Marshal(d.Details)
			t.Assets = string(b)
			t.State = model.Ambiguous
			t.Reason = "multiple_assets"
			out = append(out, t)
			continue
		}
		rawAmount := d.Amount
		if len(d.Details) == 1 {
			fund := d.Details[0]
			rawAmount = fund.Amount
			detailCurrency := strings.ToUpper(string(fund.Currency))
			if d.Currency != "" && !strings.EqualFold(string(d.Currency), detailCurrency) {
				t.State = model.Rejected
				t.Reason = "conflicting_currency"
			}
			t.Currency = detailCurrency
			// amount 是订单总额（官方文档允许 8 位小数，实测带方向符号），fundsDetail 是
			// 资产明细（实测恒为正数幅度，出账记录顶层为负）。两者同时提供时：幅度一致
			// 但符号相反是出账（方向拒绝）；幅度不一致——包括文档未定义含义的
			// "总额为 0 而明细非 0"——按 conflicting_amount 拒绝；任一无法精确解析按
			// invalid_amount 拒绝。真实出账样本：amount=-8.02，fundsDetail=8.02。
			top, topErr := d.Amount.SignedUnits()
			fundUnits, fundErr := fund.Amount.SignedUnits()
			switch {
			case topErr != nil || fundErr != nil:
				t.State = model.Rejected
				t.Reason = "invalid_amount"
				t.RawAmount = joinRawAmounts(string(d.Amount), string(fund.Amount))
			case top == -fundUnits && top != 0:
				// 出账转账：顶层带负号、明细为正幅度。方向拒绝，不是金额冲突。
				t.State = model.Rejected
				t.Reason = "not_incoming_payment"
			case top != fundUnits:
				t.State = model.Rejected
				t.Reason = "conflicting_amount"
				t.RawAmount = string(d.Amount)
			}
		}
		n, e := rawAmount.SignedUnits()
		if e != nil {
			// 交易历史允许 8 位小数，支付域只有 6 位。保留原始金额与原因供后台核对；
			// 单条记录被拒绝时照常入库，不阻塞整次扫描。
			t.Units = 0
			t.State = model.Rejected
			t.Reason = "invalid_amount"
			if t.RawAmount == "" {
				t.RawAmount = joinRawAmounts(string(d.Amount))
			}
			out = append(out, t)
			continue
		}
		t.Units = n
		if t.State == model.Confirmed && (n <= 0 || (d.Type != "PAY" && d.Type != "C2C") || t.Receiver == "") {
			// 只认收款方向的 PAY / C2C：付款方向金额为负，退款与提现不是客户付款。
			t.State = model.Rejected
			t.Reason = "not_incoming_payment"
		}
		out = append(out, t)
	}
	return out, len(items), nil
}

// joinRawAmounts 拼接去重后的原始金额文本：被拒绝的资金记录把它留在 RawAmount，
// 供后台用交易所原始证据核对超精度或冲突的金额。
func joinRawAmounts(parts ...string) string {
	seen := map[string]bool{}
	kept := []string{}
	for _, p := range parts {
		p = strings.TrimSpace(p)
		if p == "" || seen[p] {
			continue
		}
		seen[p] = true
		kept = append(kept, p)
	}
	return strings.Join(kept, ",")
}
