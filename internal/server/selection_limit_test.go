package server

import (
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
}
