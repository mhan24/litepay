package bybit

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
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

const defaultBaseURL = "https://api.bybit.com"

type Client struct {
	rateMu          sync.Mutex
	nextRequest     time.Time
	requestInterval time.Duration
	apiKey          string
	apiSecret       string
	baseURL         string
	recvWindow      int
	limit           int
	http            *httpx.Client
	now             func() time.Time
	syncTime        bool
	clockMu         sync.RWMutex
	receiver        string
}

func New(apiKey, apiSecret, baseURL string, recvWindow, limit int) *Client {
	if baseURL == "" {
		baseURL = defaultBaseURL
	}
	if recvWindow <= 0 || recvWindow > 60000 {
		recvWindow = 5000
	}
	if limit < 1 || limit > 50 {
		limit = 50
	}
	return &Client{
		apiKey:          apiKey,
		apiSecret:       apiSecret,
		baseURL:         strings.TrimRight(baseURL, "/"),
		recvWindow:      recvWindow,
		limit:           limit,
		http:            httpx.NewClient(15*time.Second, false),
		now:             time.Now,
		syncTime:        true,
		requestInterval: 100 * time.Millisecond,
	}
}

// UseTestTransport 仅供本包测试与本地验收访问模拟或本机代理服务；生产入口不会调用。
func (c *Client) UseTestTransport() {
	c.http = httpx.NewClient(15*time.Second, true)
	c.requestInterval = 0
}

// AccountUID 通过只读接口 /v5/user/query-api 核验当前 API Key 所属账户。
// 外部 Bybit 用户发起的站内转账只能进入主账号，因此显式拒绝子账号 API Key。
func (c *Client) AccountUID(ctx context.Context) (string, error) {
	ctx, cancel := context.WithTimeout(ctx, 15*time.Second)
	defer cancel()
	body, err := c.get(ctx, "/v5/user/query-api", nil)
	if err != nil {
		return "", err
	}
	var payload struct {
		UserID   flexString `json:"userID"`
		IsMaster *bool      `json:"isMaster"`
	}
	if json.Unmarshal(body, &payload) != nil || strings.TrimSpace(string(payload.UserID)) == "" {
		return "", fmt.Errorf("Bybit missing account UID")
	}
	if payload.IsMaster != nil && !*payload.IsMaster {
		return "", fmt.Errorf("Bybit API Key 必须属于主账号（外部站内转账无法直接转入子账号）")
	}
	uid := strings.TrimSpace(string(payload.UserID))
	c.clockMu.Lock()
	c.receiver = uid
	c.clockMu.Unlock()
	return uid, nil
}

// History 通过 /v5/asset/deposit/query-internal-record 分页读取站内转账入账记录。
// 文档规定单次查询时间跨度不超过 30 天、limit 最大 50、通过 nextPageCursor 翻页；
// 任一分页契约不完整或游标重复时整次失败，避免推进扫描游标导致漏单。
func (c *Client) History(ctx context.Context, start, end time.Time) ([]model.Transaction, error) {
	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	if start.IsZero() || end.Before(start) {
		return nil, fmt.Errorf("Bybit invalid history time range")
	}
	c.clockMu.RLock()
	receiver := c.receiver
	c.clockMu.RUnlock()
	if receiver == "" {
		if _, err := c.AccountUID(ctx); err != nil {
			return nil, err
		}
	}
	startMs := start.UnixMilli()
	endMs := end.UnixMilli()
	out := []model.Transaction{}
	seen := map[string]bool{}
	seenCursors := map[string]bool{}
	cursor := ""
	for page := 0; page < 20; page++ {
		rows, nextCursor, err := c.page(ctx, startMs, endMs, cursor)
		if err != nil {
			return nil, err
		}
		for _, t := range rows {
			if seen[t.EventID] {
				continue
			}
			seen[t.EventID] = true
			out = append(out, t)
		}
		if nextCursor == "" {
			return out, nil
		}
		if seenCursors[nextCursor] {
			return nil, fmt.Errorf("Bybit repeated pagination cursor")
		}
		seenCursors[nextCursor] = true
		cursor = nextCursor
	}
	return nil, fmt.Errorf("Bybit incomplete history: page limit")
}

func (c *Client) page(ctx context.Context, startMs, endMs int64, cursor string) ([]model.Transaction, string, error) {
	q := url.Values{
		"startTime": {strconv.FormatInt(startMs, 10)},
		"endTime":   {strconv.FormatInt(endMs, 10)},
		"limit":     {strconv.Itoa(c.limit)},
	}
	if cursor != "" {
		q.Set("cursor", cursor)
	}
	body, err := c.get(ctx, "/v5/asset/deposit/query-internal-record", q)
	if err != nil {
		return nil, "", err
	}
	var res struct {
		Rows           *json.RawMessage `json:"rows"`
		NextPageCursor flexString       `json:"nextPageCursor"`
	}
	if json.Unmarshal(body, &res) != nil || res.Rows == nil || strings.TrimSpace(string(*res.Rows)) == "null" {
		return nil, "", fmt.Errorf("Bybit invalid internal deposit response")
	}
	var rows []struct {
		ID               flexString `json:"id"`
		Type             flexString `json:"type"`
		Coin             flexString `json:"coin"`
		Amount           flexString `json:"amount"`
		Status           flexString `json:"status"`
		CreatedTime      flexString `json:"createdTime"`
		TxID             flexString `json:"txID"`
		ComplianceStatus flexString `json:"complianceStatus"`
	}
	if json.Unmarshal(*res.Rows, &rows) != nil || rows == nil {
		return nil, "", fmt.Errorf("Bybit invalid internal deposit rows")
	}
	c.clockMu.RLock()
	receiver := c.receiver
	c.clockMu.RUnlock()
	out := make([]model.Transaction, 0, len(rows))
	seen := map[string]bool{}
	for _, r := range rows {
		id := strings.TrimSpace(string(r.ID))
		coin := strings.ToUpper(strings.TrimSpace(string(r.Coin)))
		ms, e := r.CreatedTime.Millis()
		if e != nil || ms < startMs || ms > endMs || id == "" || coin == "" || seen[id] {
			return nil, "", fmt.Errorf("Bybit invalid internal deposit evidence")
		}
		seen[id] = true
		n, amountErr := r.Amount.SignedUnits()
		ref := strings.TrimSpace(string(r.TxID))
		if ref == "" {
			ref = id
		}
		t := model.Transaction{
			Provider:   "bybit",
			EventID:    id,
			Reference:  ref,
			Receiver:   receiver,
			Currency:   coin,
			Units:      n,
			OccurredAt: time.UnixMilli(ms),
			State:      model.Confirmed,
		}
		status := strings.TrimSpace(string(r.Status))
		depositType := strings.TrimSpace(string(r.Type))
		compliance := strings.TrimSpace(string(r.ComplianceStatus))
		switch {
		case amountErr != nil:
			// 内转金额可能超过 6 位小数：保留原始金额供后台核对并拒绝核销，不阻塞整次扫描。
			t.Units, t.State, t.Reason = 0, model.Rejected, "invalid_amount"
			t.RawAmount = string(r.Amount)
		case n <= 0 || (depositType != "" && depositType != "1"):
			t.State, t.Reason = model.Rejected, "not_internal_deposit"
		case status == "3":
			t.State, t.Reason = model.Rejected, "deposit_failed"
		case status != "2":
			// 文档状态表：1 = Processing，2 = Success，3 = Deposit failed。
			t.State, t.Reason = model.Unconfirmed, "deposit_not_completed"
		case compliance == "1" || compliance == "2":
			// 地区合规采集尚未完成（如土耳其站 1=Pending collection, 2=Collected），不核销。
			t.State, t.Reason = model.Unconfirmed, "compliance_pending"
		}
		out = append(out, t)
	}
	return out, strings.TrimSpace(string(res.NextPageCursor)), nil
}

func (c *Client) get(ctx context.Context, path string, q url.Values) (json.RawMessage, error) {
	if strings.TrimSpace(c.apiKey) == "" || strings.TrimSpace(c.apiSecret) == "" {
		return nil, fmt.Errorf("未配置 Bybit API Key / Secret")
	}
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

	queryString := q.Encode()
	requestPath := path
	if queryString != "" {
		requestPath += "?" + queryString
	}

	c.clockMu.Lock()
	needSync := c.syncTime
	c.syncTime = false
	c.clockMu.Unlock()
	if needSync {
		if err := c.syncServerTime(ctx); err != nil {
			slog.Debug("Bybit 时间同步失败，继续使用本机时间", "error_type", fmt.Sprintf("%T", err))
		}
	}
	c.clockMu.RLock()
	nowFn := c.now
	c.clockMu.RUnlock()

	timestamp := strconv.FormatInt(nowFn().UnixMilli(), 10)
	recvWindow := strconv.Itoa(c.recvWindow)
	signature := sign.HmacSHA256Hex(timestamp+c.apiKey+recvWindow+queryString, c.apiSecret)

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, c.baseURL+requestPath, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Accept", "application/json")
	req.Header.Set("X-BAPI-API-KEY", c.apiKey)
	req.Header.Set("X-BAPI-SIGN", signature)
	req.Header.Set("X-BAPI-TIMESTAMP", timestamp)
	req.Header.Set("X-BAPI-RECV-WINDOW", recvWindow)

	status, body, err := c.http.Do(req)
	if err != nil {
		return nil, fmt.Errorf("Bybit network request failed")
	}
	var envelope struct {
		RetCode flexString      `json:"retCode"`
		RetMsg  flexString      `json:"retMsg"`
		Result  json.RawMessage `json:"result"`
	}
	parseErr := json.Unmarshal(body, &envelope)
	if status != http.StatusOK {
		return nil, &APIError{HTTPStatus: status, Code: boundedCode(string(envelope.RetCode))}
	}
	if parseErr != nil {
		return nil, fmt.Errorf("Bybit 响应格式无效")
	}
	if string(envelope.RetCode) != "0" {
		return nil, &APIError{HTTPStatus: status, Code: boundedCode(string(envelope.RetCode))}
	}
	if len(envelope.Result) == 0 || strings.TrimSpace(string(envelope.Result)) == "null" {
		return nil, fmt.Errorf("Bybit 未返回明确的结果对象")
	}
	return envelope.Result, nil
}

// syncServerTime 在首次私有请求前通过 /v5/market/time 同步 Bybit 服务器时间。
func (c *Client) syncServerTime(ctx context.Context) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, c.baseURL+"/v5/market/time", nil)
	if err != nil {
		return err
	}
	status, body, err := c.http.Do(req)
	if err != nil {
		return err
	}
	if status != http.StatusOK {
		return fmt.Errorf("Bybit 时间接口返回 HTTP %d", status)
	}
	var payload struct {
		RetCode flexString `json:"retCode"`
		Time    flexString `json:"time"`
		Result  struct {
			TimeSecond flexString `json:"timeSecond"`
			TimeNano   flexString `json:"timeNano"`
		} `json:"result"`
	}
	if err := json.Unmarshal(body, &payload); err != nil {
		return err
	}
	if string(payload.RetCode) != "0" {
		return fmt.Errorf("Bybit 时间接口响应无效")
	}
	var ms int64
	if nano, err := strconv.ParseInt(strings.TrimSpace(string(payload.Result.TimeNano)), 10, 64); err == nil && nano > 0 {
		ms = nano / int64(time.Millisecond)
	} else if t, err := payload.Time.Millis(); err == nil {
		ms = t
	} else if sec, err := strconv.ParseInt(strings.TrimSpace(string(payload.Result.TimeSecond)), 10, 64); err == nil && sec > 0 {
		ms = sec * 1000
	}
	if ms <= 0 {
		return fmt.Errorf("Bybit invalid server timestamp")
	}
	server := time.UnixMilli(ms)
	syncedAt := time.Now()
	c.clockMu.Lock()
	c.now = func() time.Time { return server.Add(time.Since(syncedAt)) }
	c.clockMu.Unlock()
	return nil
}
