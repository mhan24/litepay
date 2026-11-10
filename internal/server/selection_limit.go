package server

import (
	"net/http"
	"strconv"
	"sync"
	"time"

	"github.com/gin-gonic/gin"
)

type selectionWindow struct {
	count int
	until time.Time
}
type selectionLimiter struct {
	mu      sync.Mutex
	entries map[string]selectionWindow
	cleanup time.Time
}

func newSelectionLimiter() *selectionLimiter {
	return &selectionLimiter{entries: map[string]selectionWindow{}}
}
func (l *selectionLimiter) allow(key string, limit int, now time.Time) (bool, int) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if !now.Before(l.cleanup) {
		for k, v := range l.entries {
			if !now.Before(v.until) {
				delete(l.entries, k)
			}
		}
		l.cleanup = now.Add(time.Minute)
	}
	v, exists := l.entries[key]
	if !exists || !now.Before(v.until) {
		if !exists && len(l.entries) >= 20000 {
			return false, 60
		}
		v = selectionWindow{until: now.Add(time.Minute)}
	}
	retry := max(1, int(v.until.Sub(now).Seconds())+1)
	if v.count >= limit {
		return false, retry
	}
	v.count++
	l.entries[key] = v
	return true, 0
}
func (h *Handlers) LimitPaymentSelection(c *gin.Context) {
	reject := func(seconds int) {
		c.Header("Retry-After", strconv.Itoa(seconds))
		c.AbortWithStatusJSON(http.StatusTooManyRequests, gin.H{"code": 1, "msg": "付款方式请求过于频繁，请稍后重试"})
	}
	if ok, retry := h.selectionLimit.allow("ip:"+c.ClientIP(), 120, time.Now()); !ok {
		reject(retry)
		return
	}
	c.Request.Body = http.MaxBytesReader(c.Writer, c.Request.Body, 64<<10)
	if err := c.Request.ParseForm(); err != nil {
		c.AbortWithStatusJSON(400, gin.H{"code": 1, "msg": "付款方式请求格式无效"})
		return
	}
	no := c.PostForm("trade_no")
	if len(no) > 128 {
		c.AbortWithStatusJSON(400, gin.H{"code": 1, "msg": "订单号无效"})
		return
	}
	if ok, retry := h.selectionLimit.allow("order:"+no, 20, time.Now()); !ok {
		reject(retry)
		return
	}
	c.Next()
}
