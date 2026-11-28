package bitget

import (
	"context"
	"encoding/json"
	"errors"
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

const defaultBaseURL = "https://api.bitget.com"

type Client struct {
	rateMu          sync.Mutex
	nextRequest     time.Time
	requestInterval time.Duration
	apiKey          string
	apiSecret       string
	passphrase      string
	baseURL         string
	limit           int
	http            *httpx.Client
	now             func() time.Time
	syncTime        bool
	clockMu         sync.RWMutex
	receiver        string
}

func New(apiKey, apiSecret, passphrase, baseURL string, limit int) *Client {
	if baseURL == "" {
		baseURL = defaultBaseURL
	}
	if limit < 1 || limit > 100 {
		limit = 100
	}
	return &Client{
		apiKey:          apiKey,
		apiSecret:       apiSecret,
		passphrase:      passphrase,
		baseURL:         strings.TrimRight(baseURL, "/"),
		limit:           limit,
		http:            httpx.NewClient(15*time.Second, false),
		now:             time.Now,
		syncTime:        true,
		requestInterval: 150 * time.Millisecond,
	}
}

// UseTestTransport 仅供本包测试与本地验收访问模拟或本机代理服务；生产入口不会调用。
func (c *Client) UseTestTransport() {
	c.http = httpx.NewClient(15*time.Second, true)
	c.requestInterval = 0
}

// CloseIdleConnections 关闭底层 HTTP 客户端的空闲连接。
func (c *Client) CloseIdleConnections() {
	if c != nil && c.http != nil {
		c.http.CloseIdleConnections()
	}
}

// AccountUID 优先调用 UTA V3 /api/v3/account/info 核验主账号 UID，
// 若账户仍为经典模式（404）则回退 /api/v2/spot/account/info。
// 外部站内转账仅支持转入主账号，因此显式拒绝子账号 API Key。
func (c *Client) AccountUID(ctx context.Context) (string, error) {
	ctx, cancel := context.WithTimeout(ctx, 15*time.Second)
	defer cancel()
	body, err := c.get(ctx, "/api/v3/account/info", nil)
	if err != nil {
		var apiErr *APIError
		if errors.As(err, &apiErr) && (apiErr.HTTPStatus == http.StatusNotFound || apiErr.Code == "40085") {
			body, err = c.get(ctx, "/api/v2/spot/account/info", nil)
		}
		if err != nil {
			return "", err
		}
	}
	var payload struct {
		UserID   flexString `json:"userId"`
		UID      flexString `json:"uid"`
		ParentID flexString `json:"parentId"`
	}
	if json.Unmarshal(body, &payload) != nil {
		return "", fmt.Errorf("Bitget invalid account info response")
	}
	uid := strings.TrimSpace(string(payload.UserID))
	if uid == "" {
		uid = strings.TrimSpace(string(payload.UID))
	}
	if uid == "" {
		return "", fmt.Errorf("Bitget missing account UID")
	}
	parent := strings.TrimSpace(string(payload.ParentID))
	if parent != "" && parent != "0" {
		return "", fmt.Errorf("Bitget API Key 必须属于主账号（外部站内转账无法直接转入子账号）")
	}
	c.clockMu.Lock()
	c.receiver = uid
	c.clockMu.Unlock()
	return uid, nil
}

// History 优先调用 UTA V3 /api/v3/account/deposit-records 分页读取站内转账入账记录，
// 若账户为经典模式（404）则回退 /api/v2/spot/wallet/deposit-records。
// 仅保留站内转账（dest == "internal_transfer"），忽略链上充值（on_chain）。
func (c *Client) History(ctx context.Context, start, end time.Time) ([]model.Transaction, error) {
	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	if start.IsZero() || end.Before(start) {
		return nil, fmt.Errorf("Bitget invalid history time range")
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
	useV3 := true
	out := []model.Transaction{}
	seen := map[string]bool{}
	seenCursors := map[string]bool{}
	cursor := ""
	for page := 0; page < 20; page++ {
		rows, nextCursor, err := c.page(ctx, useV3, startMs, endMs, cursor)
		if err != nil && useV3 && page == 0 {
			var apiErr *APIError
			if errors.As(err, &apiErr) && (apiErr.HTTPStatus == http.StatusNotFound || apiErr.Code == "40085") {
				useV3 = false
				rows, nextCursor, err = c.page(ctx, false, startMs, endMs, "")
			}
		}
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
			return nil, fmt.Errorf("Bitget repeated pagination cursor")
		}
		seenCursors[nextCursor] = true
		cursor = nextCursor
	}
	return nil, fmt.Errorf("Bitget: %w", exchange.ErrHistoryLimit)
}

func (c *Client) page(ctx context.Context, useV3 bool, startMs, endMs int64, cursor string) ([]model.Transaction, string, error) {
	endpoint := "/api/v3/account/deposit-records"
	q := url.Values{
		"startTime": {strconv.FormatInt(startMs, 10)},
		"endTime":   {strconv.FormatInt(endMs, 10)},
		"limit":     {strconv.Itoa(c.limit)},
	}
	if useV3 {
		if cursor != "" {
			q.Set("cursor", cursor)
		}
	} else {
		endpoint = "/api/v2/spot/wallet/deposit-records"
		if cursor != "" {
			q.Set("idLessThan", cursor)
		}
	}
	body, err := c.get(ctx, endpoint, q)
	if err != nil {
		return nil, "", err
	}
	type rawRow struct {
		OrderID     flexString `json:"orderId"`
		ID          flexString `json:"id"`
		TradeID     flexString `json:"tradeId"`
		TxID        flexString `json:"txId"`
		Coin        flexString `json:"coin"`
		Size        flexString `json:"size"`
		Amount      flexString `json:"amount"`
		Status      flexString `json:"status"`
		Dest        flexString `json:"dest"`
		RecordType  flexString `json:"recordType"`
		CTime       flexString `json:"cTime"`
		CreatedTime flexString `json:"createdTime"`
		TS          flexString `json:"ts"`
	}
	var rows []rawRow
	var nextCursor string
	trimmed := strings.TrimSpace(string(body))
	isObject := strings.HasPrefix(trimmed, "{")
	if isObject {
		var container struct {
			List   *json.RawMessage `json:"list"`
			Cursor flexString       `json:"cursor"`
		}
		if json.Unmarshal(body, &container) != nil || container.List == nil || strings.TrimSpace(string(*container.List)) == "null" {
			return nil, "", fmt.Errorf("Bitget invalid deposit records response")
		}
		if json.Unmarshal(*container.List, &rows) != nil || rows == nil {
			return nil, "", fmt.Errorf("Bitget invalid deposit records list")
		}
		nextCursor = strings.TrimSpace(string(container.Cursor))
	} else {
		if json.Unmarshal(body, &rows) != nil || rows == nil {
			return nil, "", fmt.Errorf("Bitget invalid deposit records response")
		}
	}
	c.clockMu.RLock()
	receiver := c.receiver
	c.clockMu.RUnlock()
	out := make([]model.Transaction, 0, len(rows))
	seen := map[string]bool{}
	lastID := ""
	for _, r := range rows {
		id := strings.TrimSpace(string(r.OrderID))
		if id == "" {
			id = strings.TrimSpace(string(r.ID))
		}
		coin := strings.ToUpper(strings.TrimSpace(string(r.Coin)))
		tsField := r.CTime
		if tsField == "" {
			tsField = r.CreatedTime
		}
		if tsField == "" {
			tsField = r.TS
		}
		ms, e := tsField.Millis()
		if e != nil || id == "" || coin == "" || seen[id] {
			return nil, "", fmt.Errorf("Bitget invalid deposit evidence")
		}
		seen[id] = true
		lastID = id
		dest := strings.ToLower(strings.TrimSpace(string(r.Dest)))
		if dest == "" {
			dest = strings.ToLower(strings.TrimSpace(string(r.RecordType)))
		}
		if dest == "on_chain" || dest == "onchain" {
			continue
		}
		if ms < startMs || ms > endMs {
			continue
		}
		rawAmt := r.Size
		if rawAmt == "" {
			rawAmt = r.Amount
		}
		n, amountErr := rawAmt.SignedUnits()
		ref := strings.TrimSpace(string(r.TradeID))
		if ref == "" {
			ref = strings.TrimSpace(string(r.TxID))
		}
		if ref == "" {
			ref = id
		}
		t := model.Transaction{
			Provider:   "bitget",
			EventID:    id,
			Reference:  ref,
			Receiver:   receiver,
			Currency:   coin,
			Units:      n,
			OccurredAt: time.UnixMilli(ms),
			State:      model.Confirmed,
		}
		status := strings.ToLower(strings.TrimSpace(string(r.Status)))
		switch {
		case amountErr != nil:
			t.Units, t.State, t.Reason = 0, model.Rejected, "invalid_amount"
			t.RawAmount = string(rawAmt)
		case n <= 0:
			t.State, t.Reason = model.Rejected, "not_internal_deposit"
		case dest == "":
			t.State, t.Reason = model.Unconfirmed, "internal_transfer_unverified"
		case dest != "internal_transfer" && dest != "internal":
			t.State, t.Reason = model.Rejected, "not_internal_deposit"
		case isFailedDepositStatus(status):
			t.State, t.Reason = model.Rejected, "deposit_failed"
		case status != "success":
			t.State, t.Reason = model.Unconfirmed, "deposit_not_completed"
		}
		out = append(out, t)
	}
	if !isObject && nextCursor == "" && len(rows) >= c.limit {
		nextCursor = lastID
	}
	return out, nextCursor, nil
}

func (c *Client) get(ctx context.Context, path string, q url.Values) (json.RawMessage, error) {
	if strings.TrimSpace(c.apiKey) == "" || strings.TrimSpace(c.apiSecret) == "" || strings.TrimSpace(c.passphrase) == "" {
		return nil, fmt.Errorf("未配置 Bitget API Key / Secret / Passphrase")
	}
	for attempt := 0; attempt < 2; attempt++ {
		data, err := c.getOnce(ctx, path, q)
		var apiErr *APIError
		if attempt == 0 && errors.As(err, &apiErr) && (apiErr.Code == "40005" || apiErr.Code == "40008") {
			if syncErr := c.syncServerTime(ctx); syncErr == nil {
				continue
			}
		}
		return data, err
	}
	return nil, fmt.Errorf("Bitget request failed")
}

func (c *Client) getOnce(ctx context.Context, path string, q url.Values) (json.RawMessage, error) {
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
			slog.Debug("Bitget 时间同步失败，继续使用本机时间", "error_type", fmt.Sprintf("%T", err))
		}
	}
	c.clockMu.RLock()
	nowFn := c.now
	c.clockMu.RUnlock()

	timestamp := strconv.FormatInt(nowFn().UnixMilli(), 10)
	signature := sign.HmacSHA256Base64(timestamp+"GET"+requestPath, c.apiSecret)

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, c.baseURL+requestPath, nil)
	if err != nil {
		return nil, fmt.Errorf("Bitget invalid request")
	}
	req.Header.Set("Accept", "application/json")
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("locale", "en-US")
	req.Header.Set("ACCESS-KEY", c.apiKey)
	req.Header.Set("ACCESS-SIGN", signature)
	req.Header.Set("ACCESS-TIMESTAMP", timestamp)
	req.Header.Set("ACCESS-PASSPHRASE", c.passphrase)

	status, body, err := c.http.Do(req)
	if err != nil {
		if ctxErr := ctx.Err(); ctxErr != nil {
			return nil, fmt.Errorf("Bitget network request failed: %w", ctxErr)
		}
		return nil, fmt.Errorf("Bitget network request failed")
	}
	var envelope struct {
		Code flexString      `json:"code"`
		Msg  flexString      `json:"msg"`
		Data json.RawMessage `json:"data"`
	}
	parseErr := json.Unmarshal(body, &envelope)
	if status != http.StatusOK {
		return nil, &APIError{HTTPStatus: status, Code: boundedCode(string(envelope.Code))}
	}
	if parseErr != nil {
		return nil, fmt.Errorf("Bitget 响应格式无效")
	}
	if string(envelope.Code) != "00000" {
		return nil, &APIError{HTTPStatus: status, Code: boundedCode(string(envelope.Code))}
	}
	if len(envelope.Data) == 0 || strings.TrimSpace(string(envelope.Data)) == "null" {
		return nil, fmt.Errorf("Bitget 未返回明确的数据载荷")
	}
	return envelope.Data, nil
}

func (c *Client) syncServerTime(ctx context.Context) error {
	for _, endpoint := range []string{"/api/v3/public/time", "/api/v2/public/time"} {
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, c.baseURL+endpoint, nil)
		if err != nil {
			return err
		}
		reqStart := time.Now()
		status, body, err := c.http.Do(req)
		if err != nil || status != http.StatusOK {
			continue
		}
		var payload struct {
			Code        flexString `json:"code"`
			RequestTime flexString `json:"requestTime"`
			Data        struct {
				ServerTime flexString `json:"serverTime"`
			} `json:"data"`
		}
		if json.Unmarshal(body, &payload) != nil || string(payload.Code) != "00000" {
			continue
		}
		ms, err := payload.Data.ServerTime.Millis()
		if err != nil {
			ms, err = payload.RequestTime.Millis()
		}
		if err != nil || ms <= 0 {
			continue
		}
		midpoint := reqStart.Add(time.Since(reqStart) / 2)
		server := time.UnixMilli(ms)
		c.clockMu.Lock()
		c.now = func() time.Time { return server.Add(time.Since(midpoint)) }
		c.clockMu.Unlock()
		return nil
	}
	return fmt.Errorf("Bitget 时间接口响应无效")
}

func isFailedDepositStatus(status string) bool {
	switch status {
	case "fail", "failed", "reject", "rejected", "cancel", "canceled", "cancelled",
		"pending_fail", "pending_review_fail", "wallet_fail", "deposit_check_fail":
		return true
	default:
		return false
	}
}
