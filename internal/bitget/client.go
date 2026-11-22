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

// AccountUID 优先调用 UTA V3 /api/v3/account/info 核验主账号 UID，
// 若账户仍为经典模式（404）则回退 /api/v2/spot/account/info。
// 外部站内转账仅支持转入主账号，因此显式拒绝子账号 API Key。
func (c *Client) AccountUID(ctx context.Context) (string, error) {
	ctx, cancel := context.WithTimeout(ctx, 15*time.Second)
	defer cancel()
	body, err := c.get(ctx, "/api/v3/account/info", nil)
	if err != nil {
		var apiErr *APIError
		if errors.As(err, &apiErr) && apiErr.HTTPStatus == http.StatusNotFound {
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

// History 通过 /api/v2/spot/wallet/deposit-records 分页读取站内转账入账记录。
// 仅保留 dest == "internal_transfer" 的站内转账，忽略链上充值（on_chain）。
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
	out := []model.Transaction{}
	seen := map[string]bool{}
	seenCursors := map[string]bool{}
	idLessThan := ""
	for page := 0; page < 20; page++ {
		rows, rawCount, lastID, err := c.page(ctx, startMs, endMs, idLessThan)
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
		if rawCount < c.limit || lastID == "" {
			return out, nil
		}
		if seenCursors[lastID] {
			return nil, fmt.Errorf("Bitget repeated pagination cursor")
		}
		seenCursors[lastID] = true
		idLessThan = lastID
	}
	return nil, fmt.Errorf("Bitget incomplete history: page limit")
}

func (c *Client) page(ctx context.Context, startMs, endMs int64, idLessThan string) ([]model.Transaction, int, string, error) {
	q := url.Values{
		"startTime": {strconv.FormatInt(startMs, 10)},
		"endTime":   {strconv.FormatInt(endMs, 10)},
		"limit":     {strconv.Itoa(c.limit)},
	}
	if idLessThan != "" {
		q.Set("idLessThan", idLessThan)
	}
	body, err := c.get(ctx, "/api/v2/spot/wallet/deposit-records", q)
	if err != nil {
		return nil, 0, "", err
	}
	var rows []struct {
		OrderID flexString `json:"orderId"`
		TradeID flexString `json:"tradeId"`
		Coin    flexString `json:"coin"`
		Size    flexString `json:"size"`
		Status  flexString `json:"status"`
		Dest    flexString `json:"dest"`
		CTime   flexString `json:"cTime"`
	}
	if json.Unmarshal(body, &rows) != nil || rows == nil {
		return nil, 0, "", fmt.Errorf("Bitget invalid deposit records response")
	}
	c.clockMu.RLock()
	receiver := c.receiver
	c.clockMu.RUnlock()
	out := make([]model.Transaction, 0, len(rows))
	seen := map[string]bool{}
	lastID := ""
	for _, r := range rows {
		id := strings.TrimSpace(string(r.OrderID))
		coin := strings.ToUpper(strings.TrimSpace(string(r.Coin)))
		ms, e := r.CTime.Millis()
		if e != nil || id == "" || coin == "" || seen[id] {
			return nil, 0, "", fmt.Errorf("Bitget invalid deposit evidence")
		}
		seen[id] = true
		lastID = id
		dest := strings.ToLower(strings.TrimSpace(string(r.Dest)))
		if dest == "on_chain" {
			continue
		}
		if ms < startMs || ms > endMs {
			continue
		}
		n, amountErr := r.Size.SignedUnits()
		ref := strings.TrimSpace(string(r.TradeID))
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
			t.RawAmount = string(r.Size)
		case n <= 0:
			t.State, t.Reason = model.Rejected, "not_internal_deposit"
		case dest == "":
			t.State, t.Reason = model.Unconfirmed, "internal_transfer_unverified"
		case dest != "internal_transfer":
			t.State, t.Reason = model.Rejected, "not_internal_deposit"
		case status == "fail" || status == "failed":
			t.State, t.Reason = model.Rejected, "deposit_failed"
		case status != "success":
			t.State, t.Reason = model.Unconfirmed, "deposit_not_completed"
		}
		out = append(out, t)
	}
	return out, len(rows), lastID, nil
}

func (c *Client) get(ctx context.Context, path string, q url.Values) (json.RawMessage, error) {
	if strings.TrimSpace(c.apiKey) == "" || strings.TrimSpace(c.apiSecret) == "" || strings.TrimSpace(c.passphrase) == "" {
		return nil, fmt.Errorf("未配置 Bitget API Key / Secret / Passphrase")
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
		return nil, err
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
		server := time.UnixMilli(ms)
		syncedAt := time.Now()
		c.clockMu.Lock()
		c.now = func() time.Time { return server.Add(time.Since(syncedAt)) }
		c.clockMu.Unlock()
		return nil
	}
	return fmt.Errorf("Bitget 时间接口响应无效")
}
