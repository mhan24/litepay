// Package binance 封装 Binance 只读 API（站内转账记录查询）。
//
// 安全提示：该 API Key 只应勾选 "Enable Reading"，并建议配置 IP 白名单，
// 绝对不要开启现货交易 / 提现权限。
package binance

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"math/big"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	money "litepay/internal/amount"
	"litepay/internal/httpx"
	"litepay/internal/sign"
)

const maxStoredRawBytes = 16 << 10

// Client 币安 API 客户端。
type Client struct {
	apiKey     string
	apiSecret  string
	baseURL    string
	recvWindow int
	limit      int
	http       *httpx.Client
}

// New 创建客户端。
func New(apiKey, apiSecret, baseURL string, recvWindow, limit int) *Client {
	if baseURL == "" {
		baseURL = "https://api.binance.com"
	}
	return &Client{
		apiKey:     apiKey,
		apiSecret:  apiSecret,
		baseURL:    strings.TrimRight(baseURL, "/"),
		recvWindow: recvWindow,
		limit:      limit,
		http:       httpx.NewClient(15*time.Second, false),
	}
}

// UseTestTransport 仅供本包测试访问本地模拟服务；生产入口不会调用。
func (c *Client) UseTestTransport() {
	c.http = httpx.NewClient(15*time.Second, true)
}

// Transaction 一条 Binance 站内转账记录（已解析）。
type Transaction struct {
	OrderType       string    `json:"orderType"`
	TransactionID   string    `json:"transactionId"`
	TransactionTime time.Time `json:"transactionTime"`
	AmountText      string    `json:"amount_text"`
	Amount          float64   `json:"amount"` // 绝对值；收支方向由 Incoming 表示
	Incoming        bool      `json:"incoming"`
	Currency        string    `json:"currency"`
	ReceiverID      string    `json:"receiverId"` // 收款方 binanceId
	Raw             string    `json:"raw"`
}

// AccountUID 返回当前 API Key 所属账户的 Binance UID。
// 它与用户填写的收款 UID 必须一致，避免展示一个账户却监听另一个账户。
func (c *Client) AccountUID(ctx context.Context) (string, error) {
	if c.apiKey == "" || c.apiSecret == "" {
		return "", fmt.Errorf("未配置币安 API Key/Secret")
	}
	q := url.Values{}
	q.Set("timestamp", strconv.FormatInt(time.Now().UnixMilli(), 10))
	q.Set("recvWindow", strconv.Itoa(c.recvWindow))
	rawQuery := q.Encode()
	signature := sign.HmacSHA256Hex(rawQuery, c.apiSecret)
	fullURL := fmt.Sprintf("%s/api/v3/account?%s&signature=%s", c.baseURL, rawQuery, signature)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, fullURL, nil)
	if err != nil {
		return "", err
	}
	req.Header.Set("X-MBX-APIKEY", c.apiKey)
	req.Header.Set("Accept", "application/json")
	status, body, err := c.http.Do(req)
	if err != nil {
		return "", fmt.Errorf("请求币安账户信息失败: %w", err)
	}
	if status != http.StatusOK {
		return "", fmt.Errorf("币安账户接口返回 HTTP %d", status)
	}
	var payload struct {
		UID flexString `json:"uid"`
	}
	if err := json.Unmarshal(body, &payload); err != nil {
		return "", fmt.Errorf("解析币安账户信息失败: %w", err)
	}
	uid := strings.TrimSpace(string(payload.UID))
	if uid == "" {
		return "", fmt.Errorf("币安账户接口未返回 UID")
	}
	return uid, nil
}

// PayTransactions 查询 [start, end] 区间内的 Binance 站内转账记录。
//
// 端点：GET /sapi/v1/pay/transactions
// 鉴权：X-MBX-APIKEY 头 + HMAC-SHA256 的 signature 查询参数
func (c *Client) PayTransactions(ctx context.Context, start, end time.Time) ([]Transaction, error) {
	if c.apiKey == "" || c.apiSecret == "" {
		return nil, fmt.Errorf("未配置币安 API Key/Secret")
	}

	if end.IsZero() {
		end = time.Now()
	}
	if start.IsZero() {
		start = end.Add(-89 * 24 * time.Hour)
	}
	if start.After(end) {
		return nil, fmt.Errorf("币安查询时间范围无效")
	}
	// The Pay API has no page parameter. Split saturated time windows until
	// each response is complete, without assuming the response sort order.
	type window struct{ start, end int64 }
	pending := []window{{start.UnixMilli(), end.UnixMilli()}}
	var all []Transaction
	seen := make(map[string]struct{})
	for requests := 0; len(pending) > 0; requests++ {
		if requests >= 63 {
			return nil, fmt.Errorf("币安历史查询超过分窗上限，未完整读取支付窗口")
		}
		w := pending[len(pending)-1]
		pending = pending[:len(pending)-1]
		batch, rawCount, err := c.fetchWindow(ctx, time.UnixMilli(w.start), time.UnixMilli(w.end))
		if err != nil {
			return nil, err
		}
		if rawCount >= c.limit {
			if w.start >= w.end {
				return nil, fmt.Errorf("币安同一毫秒的流水超过单次上限，无法安全确认完整历史")
			}
			mid := w.start + (w.end-w.start)/2
			pending = append(pending, window{mid + 1, w.end}, window{w.start, mid})
			continue
		}
		for _, tx := range batch {
			if _, exists := seen[tx.TransactionID]; !exists {
				seen[tx.TransactionID] = struct{}{}
				all = append(all, tx)
			}
		}
	}
	return all, nil
}

func (c *Client) fetchWindow(ctx context.Context, start, end time.Time) ([]Transaction, int, error) {
	q := url.Values{}
	q.Set("timestamp", strconv.FormatInt(time.Now().UnixMilli(), 10))
	q.Set("recvWindow", strconv.Itoa(c.recvWindow))
	q.Set("limit", strconv.Itoa(c.limit))
	if !start.IsZero() {
		q.Set("startTime", strconv.FormatInt(start.UnixMilli(), 10))
	}
	if !end.IsZero() {
		q.Set("endTime", strconv.FormatInt(end.UnixMilli(), 10))
	}

	// 注意：签名必须基于实际发送的 query string（url.Values.Encode 会按键名排序）
	rawQuery := q.Encode()
	signature := sign.HmacSHA256Hex(rawQuery, c.apiSecret)

	fullURL := fmt.Sprintf("%s/sapi/v1/pay/transactions?%s&signature=%s", c.baseURL, rawQuery, signature)

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, fullURL, nil)
	if err != nil {
		return nil, 0, err
	}
	req.Header.Set("X-MBX-APIKEY", c.apiKey)
	req.Header.Set("Accept", "application/json")

	status, body, err := c.http.Do(req)
	if err != nil {
		return nil, 0, fmt.Errorf("请求币安 API 失败: %w", err)
	}
	if status != http.StatusOK {
		return nil, 0, fmt.Errorf("币安 API 返回 HTTP %d", status)
	}

	var payload struct {
		Code    flexString        `json:"code"`
		Message flexString        `json:"message"`
		Success bool              `json:"success"`
		Data    []json.RawMessage `json:"data"`
	}
	if err := json.Unmarshal(body, &payload); err != nil {
		return nil, 0, fmt.Errorf("解析币安响应失败: %w", err)
	}

	// 币安业务码：000000 表示成功
	if string(payload.Code) != "000000" || !payload.Success {
		return nil, 0, fmt.Errorf("币安 API 业务错误 code=%s", payload.Code)
	}

	if payload.Data == nil {
		return nil, 0, fmt.Errorf("币安未返回明确的历史记录数组")
	}
	out := make([]Transaction, 0, len(payload.Data))
	seenRecords := make(map[string]struct{}, len(payload.Data))
	for _, raw := range payload.Data {
		var d struct {
			OrderType       flexString `json:"orderType"`
			TransactionID   flexString `json:"transactionId"`
			TransactionTime flexInt64  `json:"transactionTime"`
			Amount          flexString `json:"amount"` // 正数收入、负数支出
			Currency        flexString `json:"currency"`
			FundsDetail     []struct {
				Amount   flexString `json:"amount"`
				Currency flexString `json:"currency"`
			} `json:"fundsDetail"`
			ReceiverInfo struct {
				BinanceID flexString `json:"binanceId"`
			} `json:"receiverInfo"`
		}
		if err := json.Unmarshal(raw, &d); err != nil {
			return nil, 0, fmt.Errorf("解析币安转账记录失败: %w", err)
		}
		id := strings.TrimSpace(string(d.TransactionID))
		if id == "" {
			return nil, 0, fmt.Errorf("币安历史记录缺少流水号")
		}
		if _, exists := seenRecords[id]; exists {
			return nil, 0, fmt.Errorf("币安响应包含重复流水 %s", id)
		}
		seenRecords[id] = struct{}{}
		rawAmount := string(d.Amount)
		if rawAmount == "" && len(d.FundsDetail) > 0 {
			rawAmount = "0"
		}
		amountRat, err := money.Decimal(rawAmount)
		if err != nil {
			return nil, 0, err
		}
		amount, _ := amountRat.Float64()
		if math.IsNaN(amount) || math.IsInf(amount, 0) {
			return nil, 0, fmt.Errorf("币安转账 %s 的金额不是有限数字", id)
		}
		if d.TransactionTime <= 0 {
			return nil, 0, fmt.Errorf("币安转账 %s 缺少有效时间", id)
		}
		base := Transaction{
			OrderType:       strings.ToUpper(string(d.OrderType)),
			TransactionID:   id,
			TransactionTime: time.UnixMilli(int64(d.TransactionTime)),
			ReceiverID:      strings.TrimSpace(string(d.ReceiverInfo.BinanceID)),
			Raw:             truncate(string(raw), maxStoredRawBytes),
		}
		if len(d.FundsDetail) > 0 {
			// A non-zero top-level amount is authoritative. Only when it is
			// absent/zero do we infer direction from each detail, because a
			// positive detail under a missing top-level amount is still income.
			detailDirectionKnown := amount != 0
			detailTotal := new(big.Rat)
			for i, fund := range d.FundsDetail {
				fundRat, err := money.Decimal(string(fund.Amount))
				if err != nil {
					return nil, 0, err
				}
				fundAmount, _ := fundRat.Float64()
				fundCurrency := strings.ToUpper(strings.TrimSpace(string(fund.Currency)))
				if math.IsNaN(fundAmount) || math.IsInf(fundAmount, 0) {
					return nil, 0, fmt.Errorf("币安转账 %s 的明细金额不是有限数字", id)
				}
				if fundAmount == 0 || fundCurrency == "" {
					continue
				}
				detailTotal.Add(detailTotal, fundRat)
				item := base
				item.Amount = abs(fundAmount)
				item.AmountText = strings.TrimPrefix(string(fund.Amount), "-")
				if detailDirectionKnown {
					item.Incoming = amount > 0 && fundAmount > 0
				} else {
					item.Incoming = fundAmount > 0
				}
				item.Currency = fundCurrency
				item.TransactionID = fmt.Sprintf("%s:%d", id, i)
				out = append(out, item)
			}
			// Never reinterpret a malformed record as a smaller payment just
			// because one detail happens to match an order.
			if detailDirectionKnown && detailTotal.Cmp(amountRat) != 0 {
				return nil, 0, fmt.Errorf("币安转账 %s 的明细合计与顶层金额不一致", id)
			}
			continue
		}
		if amount == 0 {
			return nil, 0, fmt.Errorf("币安转账 %s 缺少有效金额", id)
		}
		base.Amount = abs(amount)
		base.AmountText = strings.TrimPrefix(string(d.Amount), "-")
		base.Incoming = amount > 0
		base.Currency = strings.ToUpper(strings.TrimSpace(string(d.Currency)))
		out = append(out, base)
	}
	return out, len(payload.Data), nil
}

// -----------------------------------------------------------------------------
// 交易所响应字段可能使用字符串或数字，统一解析后再进入业务层。
// -----------------------------------------------------------------------------

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

type flexFloat float64

func (f *flexFloat) UnmarshalJSON(b []byte) error {
	s := strings.Trim(strings.TrimSpace(string(b)), `"`)
	if s == "" || s == "null" {
		*f = 0
		return nil
	}
	v, err := strconv.ParseFloat(s, 64)
	if err != nil || math.IsNaN(v) || math.IsInf(v, 0) {
		return fmt.Errorf("金额必须是有限数字")
	}
	*f = flexFloat(v)
	return nil
}

func abs(v float64) float64 {
	if v < 0 {
		return -v
	}
	return v
}

func truncate(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n] + "..."
}
