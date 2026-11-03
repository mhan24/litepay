// Package okx 封装 OKX API v5 的站内转账入账查询。
//
// 只使用资金账户的充值历史接口：type=3 表示站内转账，type=4 表示链上充值。
// API Key 只需要「读取」权限，并建议绑定服务器 IP 白名单。
package okx

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"time"

	"litepay/internal/httpx"
	"litepay/internal/sign"
)

const (
	defaultBaseURL    = "https://www.okx.com"
	maxStoredRawBytes = 16 << 10
)

// Client OKX API v5 客户端。
type Client struct {
	apiKey     string
	apiSecret  string
	passphrase string
	baseURL    string
	limit      int
	http       *httpx.Client
	now        func() time.Time
	syncTime   bool
	clockMu    sync.RWMutex
}

// New 创建 OKX 客户端。
func New(apiKey, apiSecret, passphrase, baseURL string, limit int) *Client {
	if strings.TrimSpace(baseURL) == "" {
		baseURL = defaultBaseURL
	}
	if limit <= 0 || limit > 100 {
		limit = 100
	}
	return &Client{
		apiKey:     strings.TrimSpace(apiKey),
		apiSecret:  strings.TrimSpace(apiSecret),
		passphrase: strings.TrimSpace(passphrase),
		baseURL:    strings.TrimRight(strings.TrimSpace(baseURL), "/"),
		limit:      limit,
		http:       httpx.NewClient(15*time.Second, false),
		now:        time.Now,
		syncTime:   true,
	}
}

// UseTestTransport 仅供本包测试访问本地模拟服务；生产入口不会调用。
func (c *Client) UseTestTransport() {
	c.http = httpx.NewClient(15*time.Second, true)
}

// Deposit 一条 OKX 充值历史记录。
type Deposit struct {
	DepositID      string
	FromWithdrawID string
	TransactionID  string
	Currency       string
	Amount         float64
	State          string
	Type           string
	Timestamp      time.Time
	From           string
	Raw            string
}

// AccountUID 返回当前 API Key 所属的 OKX 账户 UID。
func (c *Client) AccountUID(ctx context.Context) (string, error) {
	body, err := c.get(ctx, "/api/v5/account/config", nil)
	if err != nil {
		return "", err
	}
	var rows []struct {
		UID flexString `json:"uid"`
	}
	if err := json.Unmarshal(body, &rows); err != nil {
		return "", fmt.Errorf("解析 OKX 账户信息失败: %w", err)
	}
	if len(rows) == 0 || strings.TrimSpace(string(rows[0].UID)) == "" {
		return "", fmt.Errorf("OKX 未返回账户 UID")
	}
	return strings.TrimSpace(string(rows[0].UID)), nil
}

// DepositHistorySince 查询 since 之后、指定币种的全部充值记录。
//
// OKX 按时间倒序返回；官方 after 参数用于继续请求更旧的一页。这里最多
// 翻 20 页（2000 条），并始终保留时间重叠，避免同一毫秒的多条记录漏失。
func (c *Client) DepositHistorySince(ctx context.Context, currency string, since time.Time) ([]Deposit, error) {
	var all []Deposit
	seen := make(map[string]struct{})
	var after int64
	for page := 0; page < 20; page++ {
		rows, err := c.depositHistoryPage(ctx, currency, after)
		if err != nil {
			return nil, err
		}
		if len(rows) == 0 {
			break
		}
		oldest := int64(0)
		for _, row := range rows {
			ts := row.Timestamp.UnixMilli()
			if oldest == 0 || ts < oldest {
				oldest = ts
			}
			if row.Timestamp.Before(since) {
				continue
			}
			key := row.DepositID
			if key == "" {
				key = row.FromWithdrawID + "|" + row.TransactionID
			}
			if _, exists := seen[key]; exists {
				continue
			}
			seen[key] = struct{}{}
			all = append(all, row)
		}
		if len(rows) < c.limit || oldest == 0 || oldest < since.UnixMilli() {
			break
		}
		next := oldest + 1
		if after != 0 && next >= after {
			return nil, fmt.Errorf("OKX 分页游标未前进")
		}
		after = next
		if page == 19 {
			return nil, fmt.Errorf("OKX 充值历史超过分页上限，未完整读取支付窗口")
		}
	}
	return all, nil
}

func (c *Client) depositHistoryPage(ctx context.Context, currency string, after int64) ([]Deposit, error) {
	q := url.Values{}
	if strings.TrimSpace(currency) != "" {
		q.Set("ccy", strings.ToUpper(strings.TrimSpace(currency)))
	}
	q.Set("limit", strconv.Itoa(c.limit))
	q.Set("type", "3")
	if after > 0 {
		q.Set("after", strconv.FormatInt(after, 10))
	}
	body, err := c.get(ctx, "/api/v5/asset/deposit-history", q)
	if err != nil {
		return nil, err
	}

	var rows []struct {
		DepositID      flexString `json:"depId"`
		FromWithdrawID flexString `json:"fromWdId"`
		TransactionID  flexString `json:"txId"`
		Currency       flexString `json:"ccy"`
		Amount         flexString `json:"amt"`
		State          flexString `json:"state"`
		Type           flexString `json:"type"`
		Timestamp      flexInt64  `json:"ts"`
		From           flexString `json:"from"`
	}
	if err := json.Unmarshal(body, &rows); err != nil {
		return nil, fmt.Errorf("解析 OKX 充值记录失败: %w", err)
	}

	out := make([]Deposit, 0, len(rows))
	rawRows, err := rawRows(body)
	if err != nil || len(rawRows) != len(rows) {
		return nil, fmt.Errorf("解析 OKX 充值记录原文失败")
	}
	seen := make(map[string]struct{}, len(rows))
	for i, row := range rows {
		amount, err := strconv.ParseFloat(string(row.Amount), 64)
		if err != nil || math.IsNaN(amount) || math.IsInf(amount, 0) {
			return nil, fmt.Errorf("解析 OKX 充值金额失败: %w", err)
		}
		if row.Timestamp <= 0 {
			return nil, fmt.Errorf("OKX 充值记录 %s 缺少有效时间", row.DepositID)
		}
		id := strings.TrimSpace(string(row.DepositID))
		if id != "" {
			if _, exists := seen[id]; exists {
				return nil, fmt.Errorf("OKX 响应包含重复充值记录 %s", id)
			}
			seen[id] = struct{}{}
		}
		// OKX documents type as a request filter, not a response field.
		// fromWdId is populated only for internal transfers. Keep an explicit
		// conflicting type so the poller still rejects it.
		depositType := strings.TrimSpace(string(row.Type))
		if depositType == "" && strings.TrimSpace(string(row.FromWithdrawID)) != "" {
			depositType = "3"
		}
		out = append(out, Deposit{
			DepositID:      id,
			FromWithdrawID: string(row.FromWithdrawID),
			TransactionID:  string(row.TransactionID),
			Currency:       strings.ToUpper(string(row.Currency)),
			Amount:         amount,
			State:          strings.TrimSpace(string(row.State)),
			Type:           depositType,
			Timestamp:      time.UnixMilli(int64(row.Timestamp)),
			From:           string(row.From),
			Raw:            truncate(string(rawRows[i]), maxStoredRawBytes),
		})
	}
	return out, nil
}

func (c *Client) get(ctx context.Context, path string, q url.Values) (json.RawMessage, error) {
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
		return nil, fmt.Errorf("请求 OKX API 失败: %w", err)
	}
	if status != http.StatusOK {
		return nil, fmt.Errorf("OKX API 返回 HTTP %d: %s", status, truncate(string(body), 300))
	}

	var envelope struct {
		Code flexString      `json:"code"`
		Msg  flexString      `json:"msg"`
		Data json.RawMessage `json:"data"`
	}
	if err := json.Unmarshal(body, &envelope); err != nil {
		return nil, fmt.Errorf("解析 OKX 响应失败: %w (body=%s)", err, truncate(string(body), 200))
	}
	if string(envelope.Code) != "0" {
		return nil, fmt.Errorf("OKX API 业务错误 code=%s message=%s", envelope.Code, envelope.Msg)
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
		return err
	}
	server := time.UnixMilli(ms)
	syncedAt := time.Now()
	c.clockMu.Lock()
	c.now = func() time.Time { return server.Add(time.Since(syncedAt)) }
	c.clockMu.Unlock()
	return nil
}

func rawRows(data json.RawMessage) ([]json.RawMessage, error) {
	var rows []json.RawMessage
	if err := json.Unmarshal(data, &rows); err != nil {
		return nil, err
	}
	return rows, nil
}

type flexString string

func (f *flexString) UnmarshalJSON(b []byte) error {
	s := strings.TrimSpace(string(b))
	if s == "null" || s == "" {
		*f = ""
		return nil
	}
	if len(s) >= 2 && s[0] == '"' && s[len(s)-1] == '"' {
		var value string
		if err := json.Unmarshal(b, &value); err != nil {
			return fmt.Errorf("字符串字段格式无效: %w", err)
		}
		*f = flexString(value)
		return nil
	}
	switch s[0] {
	case '{', '[', 't', 'f':
		return fmt.Errorf("字段必须是字符串或数字")
	}
	*f = flexString(s)
	return nil
}

type flexInt64 int64

func (f *flexInt64) UnmarshalJSON(b []byte) error {
	s := strings.Trim(strings.TrimSpace(string(b)), `"`)
	if s == "" || s == "null" {
		*f = 0
		return nil
	}
	v, err := strconv.ParseInt(s, 10, 64)
	if err != nil {
		fv, err2 := strconv.ParseFloat(s, 64)
		if err2 != nil || math.IsNaN(fv) || math.IsInf(fv, 0) || math.Trunc(fv) != fv ||
			fv < math.MinInt64 || fv > math.MaxInt64 {
			return fmt.Errorf("时间或整数字段格式无效")
		}
		v = int64(fv)
	}
	*f = flexInt64(v)
	return nil
}

func truncate(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n] + "..."
}
