package server

import (
	"github.com/gin-gonic/gin"
	"strconv"
	"time"
)

func (h *Handlers) LimitPublicRead(c *gin.Context) {
	no := c.Param("trade_no")
	if no == "" {
		no = c.Query("order_id")
	}
	if len(no) > 128 {
		c.AbortWithStatusJSON(400, gin.H{"code": 1, "msg": "订单号无效"})
		return
	}
	ip := c.ClientIP()
	now := time.Now()
	for _, rule := range []struct {
		key   string
		limit int
	}{{"read-ip:" + ip, 3000}, {"read-order:" + ip + ":" + no, 60}} {
		if ok, retry := h.readLimit.allow(rule.key, rule.limit, now); !ok {
			c.Header("Retry-After", strconv.Itoa(retry))
			c.AbortWithStatusJSON(429, gin.H{"code": 1, "msg": "订单状态请求过于频繁，请稍后重试"})
			return
		}
	}
	c.Next()
}
