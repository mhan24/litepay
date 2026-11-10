package binance

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/url"
	"strconv"
	"sync/atomic"
	"time"

	"litepay/internal/sign"
)

type ErrorKind string

const (
	ErrorAuth       ErrorKind = "authentication"
	ErrorRate       ErrorKind = "rate_limit"
	ErrorExchange   ErrorKind = "exchange"
	ErrorNetwork    ErrorKind = "network"
	ErrorTimeout    ErrorKind = "timeout"
	ErrorParse      ErrorKind = "parse"
	ErrorBusiness   ErrorKind = "business_rejection"
	ErrorTimestamp  ErrorKind = "timestamp"
	ErrorIncomplete ErrorKind = "incomplete"
	ErrorBudget     ErrorKind = "request_budget"
)

// APIError only exposes bounded categories/numeric codes. Raw exchange bodies and request URLs never escape.
type APIError struct {
	Kind         ErrorKind
	Code, Status int
	cause        error
}

func (e *APIError) Unwrap() error { return e.cause }
func timeoutError(ctx context.Context) *APIError {
	cause := ctx.Err()
	if cause == nil {
		cause = context.DeadlineExceeded
	}
	return &APIError{Kind: ErrorTimeout, cause: cause}
}

func (e *APIError) Error() string {
	return fmt.Sprintf("Binance %s (HTTP %d, code %d)", e.Kind, e.Status, e.Code)
}

type requestBudget struct{ remaining atomic.Int32 }
type budgetKey struct{}

func WithRequestBudget(ctx context.Context, n int) context.Context {
	b := &requestBudget{}
	b.remaining.Store(int32(n))
	return context.WithValue(ctx, budgetKey{}, b)
}
func budgeted(ctx context.Context, n int) context.Context {
	if ctx.Value(budgetKey{}) == nil {
		return WithRequestBudget(ctx, n)
	}
	return ctx
}

func (c *Client) signedGet(ctx context.Context, path string, q url.Values) ([]byte, error) {
	ctx = budgeted(ctx, 3)
	// Waiting for another caller is bounded by the caller's deadline as well.
	for !c.requestMu.TryLock() {
		timer := time.NewTimer(10 * time.Millisecond)
		select {
		case <-ctx.Done():
			timer.Stop()
			return nil, timeoutError(ctx)
		case <-timer.C:
		}
	}
	defer c.requestMu.Unlock()
	for attempt := 0; attempt < 2; attempt++ {
		body, err := c.request(ctx, path, q, true)
		var apiErr *APIError
		if !errors.As(err, &apiErr) || apiErr.Kind != ErrorTimestamp || attempt == 1 {
			return body, err
		}
		clock, err := c.request(ctx, "/api/v3/time", nil, false)
		if err != nil {
			return nil, err
		}
		var stamp struct {
			ServerTime int64 `json:"serverTime"`
		}
		if json.Unmarshal(clock, &stamp) != nil || stamp.ServerTime <= 0 {
			return nil, &APIError{Kind: ErrorParse}
		}
		midpoint := c.lastRequestStart.Add(time.Since(c.lastRequestStart) / 2)
		c.clockOffset = stamp.ServerTime - midpoint.UnixMilli()
	}
	return nil, &APIError{Kind: ErrorTimestamp}
}

func (c *Client) request(ctx context.Context, path string, q url.Values, signed bool) ([]byte, error) {
	if time.Now().Before(c.backoffUntil) {
		return nil, &APIError{Kind: ErrorRate}
	}
	if delay := time.Until(c.nextRequest); delay > 0 {
		timer := time.NewTimer(delay)
		select {
		case <-ctx.Done():
			timer.Stop()
			return nil, timeoutError(ctx)
		case <-timer.C:
		}
	}
	if ctx.Err() != nil {
		return nil, timeoutError(ctx)
	}
	b := ctx.Value(budgetKey{}).(*requestBudget)
	if b.remaining.Add(-1) < 0 {
		return nil, &APIError{Kind: ErrorBudget}
	}
	c.nextRequest = time.Now().Add(c.requestInterval)
	values := url.Values{}
	for k, v := range q {
		values[k] = append([]string(nil), v...)
	}
	if signed {
		values.Set("timestamp", strconv.FormatInt(time.Now().UnixMilli()+c.clockOffset, 10))
		values.Set("recvWindow", strconv.Itoa(c.recvWindow))
		values.Set("signature", sign.HmacSHA256Hex(values.Encode(), c.apiSecret))
	}
	target := c.baseURL + path
	if encoded := values.Encode(); encoded != "" {
		target += "?" + encoded
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, target, nil)
	if err != nil {
		return nil, &APIError{Kind: ErrorBusiness}
	}
	if signed {
		req.Header.Set("X-MBX-APIKEY", c.apiKey)
	}
	req.Header.Set("Accept", "application/json")
	c.lastRequestStart = time.Now()
	if path == "/sapi/v1/pay/transactions" {
		c.usageMu.Lock()
		if time.Since(c.minuteStart) >= time.Minute {
			c.minuteStart = time.Now()
			c.minuteRequests = 0
		}
		c.payRequests++
		c.minuteRequests++
		c.usageMu.Unlock()
	}
	status, body, err := c.http.Do(req)
	if err != nil {
		kind := ErrorNetwork
		var netErr net.Error
		if ctx.Err() != nil || (errors.As(err, &netErr) && netErr.Timeout()) {
			return nil, timeoutError(ctx)
		}
		return nil, &APIError{Kind: kind, Status: status}
	}
	var envelope struct {
		Code flexString `json:"code"`
	}
	jsonErr := json.Unmarshal(body, &envelope)
	code, _ := strconv.Atoi(string(envelope.Code))
	failure := func(kind ErrorKind) ([]byte, error) { return nil, &APIError{Kind: kind, Status: status, Code: code} }
	if status == 429 || status == 418 || code == -1003 || code == -1015 {
		c.backoffUntil = time.Now().Add(time.Minute)
		return failure(ErrorRate)
	}
	if code == -1021 {
		return failure(ErrorTimestamp)
	}
	if status == 401 || status == 403 || code == -2014 || code == -2015 || code == -1022 {
		return failure(ErrorAuth)
	}
	if status >= 500 {
		return failure(ErrorExchange)
	}
	if status != 200 {
		return failure(ErrorBusiness)
	}
	if jsonErr != nil {
		return failure(ErrorParse)
	}
	if code < 0 {
		return failure(ErrorBusiness)
	}
	return body, nil
}

// Usage measures this client's own Pay requests; other applications sharing the UID
// consume separate quota that cannot be inferred from this process.
type Usage struct {
	Requests           uint64 `json:"requests"`
	EstimatedUIDWeight uint64 `json:"estimated_uid_weight"`
	MinuteUIDWeight    uint64 `json:"minute_uid_weight"`
}

func (c *Client) Usage() Usage {
	c.usageMu.Lock()
	defer c.usageMu.Unlock()
	weight := c.minuteRequests * 3000
	if time.Since(c.minuteStart) >= time.Minute {
		weight = 0
	}
	return Usage{c.payRequests, c.payRequests * 3000, weight}
}
