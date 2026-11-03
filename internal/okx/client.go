package okx

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"time"

	"litepay/internal/exchange"
	"litepay/internal/httpx"
	"litepay/internal/model"
	"litepay/internal/sign"
)

type flexString = exchange.Text

const defaultBaseURL = "https://www.okx.com"

type Client struct {
	rateMu                                 sync.Mutex
	nextRequest                            time.Time
	requestInterval                        time.Duration
	apiKey, apiSecret, passphrase, baseURL string
	limit                                  int
	http                                   *httpx.Client
	now                                    func() time.Time
	syncTime                               bool
	clockMu                                sync.RWMutex
	receiver                               string
}

func New(key, secret, pass, base string, limit int) *Client {
	if base == "" {
		base = defaultBaseURL
	}
	if limit < 1 || limit > 100 {
		limit = 100
	}
	return &Client{apiKey: key, apiSecret: secret, passphrase: pass, baseURL: strings.TrimRight(base, "/"), limit: limit, http: httpx.NewClient(15*time.Second, false), now: time.Now, syncTime: true, requestInterval: 200 * time.Millisecond}
}
func (c *Client) UseTestTransport() {
	c.http = httpx.NewClient(15*time.Second, true)
	c.requestInterval = 0
}
func (c *Client) AccountUID(ctx context.Context) (string, error) {
	body, err := c.get(ctx, "/api/v5/account/config", nil)
	if err != nil {
		return "", err
	}
	var rows []struct {
		UID exchange.Text `json:"uid"`
	}
	if json.Unmarshal(body, &rows) != nil || len(rows) != 1 || rows[0].UID == "" {
		return "", fmt.Errorf("OKX missing account UID")
	}
	c.receiver = string(rows[0].UID)
	return c.receiver, nil
}

// History uses overlapping timestamp pages. A stalled boundary fails closed.
func (c *Client) History(ctx context.Context, start, end time.Time) ([]model.Transaction, error) {
	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	if c.receiver == "" {
		if _, err := c.AccountUID(ctx); err != nil {
			return nil, err
		}
	}
	out := []model.Transaction{}
	seen := map[string]bool{}
	after := end.UnixMilli() + 1
	for page := 0; page < 20; page++ {
		rows, err := c.page(ctx, after)
		if err != nil {
			return nil, err
		}
		oldest := after
		for _, t := range rows {
			if t.OccurredAt.UnixMilli() < oldest {
				oldest = t.OccurredAt.UnixMilli()
			}
			if t.OccurredAt.Before(start) || t.OccurredAt.After(end) || seen[t.EventID] {
				continue
			}
			seen[t.EventID] = true
			out = append(out, t)
		}
		if len(rows) < c.limit || oldest < start.UnixMilli() {
			return out, nil
		}
		next := oldest + 1
		if next >= after {
			return nil, fmt.Errorf("OKX incomplete timestamp boundary")
		}
		after = next
	}
	return nil, fmt.Errorf("OKX incomplete history: page limit")
}
func (c *Client) Lookup(ctx context.Context, ref string, start, end time.Time) ([]model.Transaction, error) {
	rows, e := c.History(ctx, start, end)
	if e != nil {
		return nil, e
	}
	out := []model.Transaction{}
	for _, t := range rows {
		if t.EventID == ref || t.Reference == ref {
			out = append(out, t)
		}
	}
	return out, nil
}
func (c *Client) page(ctx context.Context, after int64) ([]model.Transaction, error) {
	body, e := c.get(ctx, "/api/v5/asset/deposit-history", url.Values{"type": {"3"}, "limit": {strconv.Itoa(c.limit)}, "after": {strconv.FormatInt(after, 10)}})
	if e != nil {
		return nil, e
	}
	var rows []struct {
		ID        exchange.Text `json:"depId"`
		Reference exchange.Text `json:"fromWdId"`
		Currency  exchange.Text `json:"ccy"`
		Amount    exchange.Text `json:"amt"`
		State     exchange.Text `json:"state"`
		Type      exchange.Text `json:"type"`
		Time      exchange.Text `json:"ts"`
	}
	if json.Unmarshal(body, &rows) != nil || rows == nil {
		return nil, fmt.Errorf("OKX invalid deposit response")
	}
	out := []model.Transaction{}
	seen := map[string]bool{}
	for _, r := range rows {
		ms, e := r.Time.Millis()
		n, ae := r.Amount.SignedUnits()
		if e != nil || ae != nil || r.ID == "" || seen[string(r.ID)] {
			return nil, fmt.Errorf("OKX invalid deposit evidence")
		}
		seen[string(r.ID)] = true
		t := model.Transaction{Provider: "okx", EventID: string(r.ID), Reference: string(r.Reference), Receiver: c.receiver, Currency: strings.ToUpper(string(r.Currency)), Units: n, OccurredAt: time.UnixMilli(ms), State: model.Confirmed}
		// type is a documented request filter; fromWdId is additional internal-transfer evidence.
		if (r.Type != "" && r.Type != "3") || r.Reference == "" || n <= 0 {
			t.State = model.Rejected
			t.Reason = "not_internal_deposit"
		} else if r.State != "2" {
			t.State = model.Unconfirmed
			t.Reason = "deposit_not_completed"
		}
		out = append(out, t)
	}
	return out, nil
}
func (c *Client) get(ctx context.Context, path string, q url.Values) (json.RawMessage, error) {
	c.rateMu.Lock()
	if delay := time.Until(c.nextRequest); delay > 0 {
		timer := time.NewTimer(delay)
		select {
		case <-ctx.Done():
			timer.Stop()
			c.rateMu.Unlock()
			return nil, ctx.Err()
		case <-timer.C:
		}
	}
	c.nextRequest = time.Now().Add(c.requestInterval)
	c.rateMu.Unlock()
	if c.apiKey == "" || c.apiSecret == "" || c.passphrase == "" {
		return nil, fmt.Errorf("未配置 OKX API Key / Secret / Passphrase")
	}

	requestPath := path
	if encoded := q.Encode(); encoded != "" {
		requestPath += "?" + encoded
	}
	c.clockMu.Lock()
	needSync := c.syncTime
	c.syncTime = false
	c.clockMu.Unlock()
	if needSync {
		if err := c.syncServerTime(ctx); err != nil {
			// 时间同步失败不阻断真实请求，后续可能仍能通过 30 秒窗口。
		}
	}
	c.clockMu.RLock()
	nowFn := c.now
	c.clockMu.RUnlock()
	timestamp := nowFn().UTC().Format("2006-01-02T15:04:05.000Z")
	signature := sign.HmacSHA256Base64(timestamp+"GET"+requestPath, c.apiSecret)

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, c.baseURL+requestPath, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Accept", "application/json")
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("OK-ACCESS-KEY", c.apiKey)
	req.Header.Set("OK-ACCESS-SIGN", signature)
	req.Header.Set("OK-ACCESS-TIMESTAMP", timestamp)
	req.Header.Set("OK-ACCESS-PASSPHRASE", c.passphrase)

	status, body, err := c.http.Do(req)
	if err != nil {
		return nil, fmt.Errorf("OKX network request failed")
	}
	if status != http.StatusOK {
		return nil, fmt.Errorf("OKX API 返回 HTTP %d", status)
	}

	var envelope struct {
		Code flexString      `json:"code"`
		Msg  flexString      `json:"msg"`
		Data json.RawMessage `json:"data"`
	}
	if err := json.Unmarshal(body, &envelope); err != nil {
		return nil, fmt.Errorf("解析 OKX 响应失败: %w", err)
	}
	if string(envelope.Code) != "0" {
		return nil, fmt.Errorf("OKX business rejection")
	}
	if len(envelope.Data) == 0 || strings.TrimSpace(string(envelope.Data)) == "null" {
		return nil, fmt.Errorf("OKX 未返回明确的数据数组")
	}
	return envelope.Data, nil
}

// syncServerTime 按 OKX 文档建议在首次私有请求前同步服务器时间。
func (c *Client) syncServerTime(ctx context.Context) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, c.baseURL+"/api/v5/public/time", nil)
	if err != nil {
		return err
	}
	status, body, err := c.http.Do(req)
	if err != nil {
		return err
	}
	if status != http.StatusOK {
		return fmt.Errorf("OKX 时间接口返回 HTTP %d", status)
	}
	var payload struct {
		Code flexString `json:"code"`
		Data []struct {
			TS flexString `json:"ts"`
		} `json:"data"`
	}
	if err := json.Unmarshal(body, &payload); err != nil {
		return err
	}
	if payload.Code != "0" || len(payload.Data) == 0 {
		return fmt.Errorf("OKX 时间接口响应无效")
	}
	ms, err := strconv.ParseInt(string(payload.Data[0].TS), 10, 64)
	if err != nil || ms <= 0 {
		return fmt.Errorf("OKX invalid server timestamp")
	}
	server := time.UnixMilli(ms)
	syncedAt := time.Now()
	c.clockMu.Lock()
	c.now = func() time.Time { return server.Add(time.Since(syncedAt)) }
	c.clockMu.Unlock()
	return nil
}
