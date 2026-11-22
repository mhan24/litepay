package server

import (
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
)

func TestSelectionEndpointRateLimitAndBoundedMemory(t *testing.T) {
	h := &Handlers{selectionLimit: newSelectionLimiter()}
	r := gin.New()
	r.POST("/api/order/payment", h.LimitPaymentSelection, func(c *gin.Context) { c.JSON(200, gin.H{"code": 0}) })
	accepted := 0
	for range 1000 {
		req := httptest.NewRequest("POST", "/api/order/payment", strings.NewReader(url.Values{"trade_no": {"one-order"}, "provider": {"binance"}, "currency": {"USDT"}}.Encode()))
		req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
		w := httptest.NewRecorder()
		r.ServeHTTP(w, req)
		if w.Code == 200 {
			accepted++
		} else if w.Code != 429 || w.Header().Get("Retry-After") == "" {
			t.Fatal(w.Code, w.Body.String())
		}
	}
	if accepted != 20 {
		t.Fatal("per-order endpoint limit missing", accepted)
	}
	now := time.Now()
	l := newSelectionLimiter()
	for i := range 20000 {
		if ok, _ := l.allow(string(rune(i)), 1, now); !ok {
			t.Fatal(i)
		}
	}
	if ok, _ := l.allow("overflow", 1, now); ok || len(l.entries) > 20000 {
		t.Fatal("unbounded limiter memory")
	}
	if ok, _ := l.allow("after-expiry", 1, now.Add(2*time.Minute)); !ok {
		t.Fatal("expired limiter entries not reclaimed")
	}
	// Even when cleanup interval hasn't elapsed yet, an entry-cap hit reclaims expired keys first.
	l2 := newSelectionLimiter()
	l2.cleanup = now.Add(10 * time.Minute)
	for i := range 20000 {
		l2.entries[string(rune(i+1))] = selectionWindow{count: 1, until: now.Add(-time.Second)}
	}
	if ok, _ := l2.allow("fresh-key", 1, now); !ok {
		t.Fatal("expired entries blocked new key before cleanup interval")
	}
}

func TestSelectionLimiterNormalizesTradeNoWhitespaceAndNilGuards(t *testing.T) {
	h := &Handlers{selectionLimit: newSelectionLimiter()}
	r := gin.New()
	r.POST("/api/order/payment", h.LimitPaymentSelection, func(c *gin.Context) { c.JSON(200, gin.H{"code": 0}) })

	variants := []string{"ws-order", " ws-order", "ws-order ", " ws-order ", "\tws-order\n"}
	accepted := 0
	for i := range 30 {
		no := variants[i%len(variants)]
		req := httptest.NewRequest(http.MethodPost, "/api/order/payment", strings.NewReader(url.Values{"trade_no": {no}, "provider": {"binance"}, "currency": {"USDT"}}.Encode()))
		req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
		req.RemoteAddr = "198.51.100." + string(rune('1'+i%9)) + ":1234"
		w := httptest.NewRecorder()
		r.ServeHTTP(w, req)
		if w.Code == http.StatusOK {
			accepted++
		} else if w.Code != http.StatusTooManyRequests {
			t.Fatalf("unexpected status %d: %s", w.Code, w.Body.String())
		}
	}
	if accepted != 20 {
		t.Fatalf("whitespace variants bypassed per-order rate limit: accepted=%d", accepted)
	}

	// Nil limiters and nil request body must not panic.
	nilH := &Handlers{}
	rNil := gin.New()
	rNil.GET("/api/order/status", nilH.LimitPublicRead, func(c *gin.Context) { c.JSON(200, gin.H{"code": 0}) })
	rNil.POST("/api/order/payment", nilH.LimitPaymentSelection, func(c *gin.Context) { c.JSON(200, gin.H{"code": 0}) })

	w := httptest.NewRecorder()
	rNil.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/api/order/status?order_id=abc", nil))
	if w.Code != http.StatusOK {
		t.Fatalf("expected 200 with nil readLimit, got %d", w.Code)
	}
	w = httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/api/order/payment", nil)
	req.Body = nil
	rNil.ServeHTTP(w, req)
	if w.Code != http.StatusBadRequest {
		t.Fatalf("expected 400 with nil selectionLimit and nil body, got %d", w.Code)
	}
}
