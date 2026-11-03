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

// History splits saturated windows because the Pay endpoint has no page cursor.
func (c *Client) History(ctx context.Context, start, end time.Time) ([]model.Transaction, error) {
	ctx, cancel := context.WithTimeout(budgeted(ctx, 64), 30*time.Second)
	defer cancel()
	if start.IsZero() || end.Before(start) {
		return nil, &APIError{Kind: ErrorBusiness}
	}
	type window struct{ a, b int64 }
	pending := []window{{start.UnixMilli(), end.UnixMilli()}}
	out := []model.Transaction{}
	seen := map[string]bool{}
	for requests := 0; len(pending) > 0; requests++ {
		if requests >= 63 {
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
func (c *Client) Lookup(ctx context.Context, ref string, start, end time.Time) ([]model.Transaction, error) {
	rows, err := c.History(ctx, start, end)
	if err != nil {
		return nil, err
	}
	out := []model.Transaction{}
	for _, t := range rows {
		if t.EventID == ref || t.Reference == ref {
			out = append(out, t)
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
		Code    exchange.Text     `json:"code"`
		Success bool              `json:"success"`
		Data    []json.RawMessage `json:"data"`
	}
	if json.Unmarshal(body, &p) != nil || p.Data == nil {
		return nil, 0, &APIError{Kind: ErrorParse}
	}
	if p.Code != "000000" || !p.Success {
		return nil, 0, &APIError{Kind: ErrorBusiness}
	}
	out := []model.Transaction{}
	seen := map[string]bool{}
	for _, raw := range p.Data {
		var d struct {
			OrderID  exchange.Text `json:"orderId"`
			ID       exchange.Text `json:"transactionId"`
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
		t := model.Transaction{Provider: "binance", EventID: string(d.ID), Reference: string(d.OrderID), Receiver: string(d.Receiver.UID), Currency: strings.ToUpper(string(d.Currency)), OccurredAt: time.UnixMilli(ms), State: model.Confirmed}
		if t.Reference == "" {
			t.Reference = t.EventID
		}
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
			t.Currency = strings.ToUpper(string(fund.Currency))
			if d.Amount != "" && d.Amount != "0" {
				top, e := d.Amount.SignedUnits()
				n, fe := fund.Amount.SignedUnits()
				if e != nil || fe != nil || top != n || (d.Currency != "" && !strings.EqualFold(string(d.Currency), t.Currency)) {
					t.State = model.Rejected
					t.Reason = "conflicting_amount"
				}
			}
		}
		n, e := rawAmount.SignedUnits()
		if e != nil {
			return nil, 0, &APIError{Kind: ErrorParse}
		}
		t.Units = n
		if n <= 0 || (d.Type != "PAY" && d.Type != "C2C") || t.Receiver == "" {
			t.State = model.Rejected
			t.Reason = "not_incoming_payment"
		}
		out = append(out, t)
	}
	return out, len(p.Data), nil
}
