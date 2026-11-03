package server

import (
	"errors"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
	"litepay/internal/model"
	"litepay/internal/store"
	"litepay/internal/worker"
)

func inquiryDelay(o *model.Order) int {
	if o.SelectedAt == nil {
		return 60
	}
	left := time.Until(o.SelectedAt.Add(time.Minute))
	if left <= 0 {
		return 0
	}
	return int(left.Seconds()) + 1
}

func (h *Handlers) PaymentInquiry(c *gin.Context) {
	if site := c.GetHeader("Sec-Fetch-Site"); site != "" && site != "same-origin" {
		c.JSON(403, gin.H{"msg": "拒绝跨站查款"})
		return
	}
	if origin := c.GetHeader("Origin"); origin != "" {
		u, err := url.Parse(origin)
		base, _ := url.Parse(h.C().Server.BaseURL)
		if err != nil || base == nil || u.Host != base.Host || u.Scheme != base.Scheme {
			c.JSON(403, gin.H{"msg": "拒绝跨站查款"})
			return
		}
	}
	c.Request.Body = http.MaxBytesReader(c.Writer, c.Request.Body, 2048)
	if err := c.Request.ParseForm(); err != nil {
		c.JSON(400, gin.H{"msg": "查款请求无效"})
		return
	}
	tradeNo := strings.TrimSpace(c.PostForm("trade_no"))
	ref := strings.TrimSpace(c.PostForm("reference"))
	if tradeNo == "" || len(tradeNo) > 64 || len(ref) > 128 {
		c.JSON(400, gin.H{"msg": "订单或转账编号无效"})
		return
	}
	result, err := h.poller.CheckPayment(c.Request.Context(), tradeNo, ref)
	if err != nil {
		status := http.StatusServiceUnavailable
		msg := "暂时无法查询交易所，请稍后重试，勿重复付款"
		switch {
		case errors.Is(err, worker.ErrInquiryInvalid):
			status = 400
			msg = err.Error()
		case errors.Is(err, worker.ErrInquiryClosed):
			status = 409
			msg = err.Error()
		case errors.Is(err, worker.ErrInquiryEarly):
			status = 409
			msg = err.Error()
		case errors.Is(err, worker.ErrInquiryBusy), errors.Is(err, store.ErrInquiryLimited):
			status = 429
			msg = "查款繁忙或次数受限，请至少 1 分钟后重试；多次失败请联系商户"
			c.Header("Retry-After", "60")
		}
		c.JSON(status, gin.H{"msg": msg})
		return
	}
	msg := "暂未查到可核销的付款，请核对编号并稍后重试，勿重复转账"
	if result == "needs_review" {
		msg = "该笔付款暂不能自动核销，请联系商户核对，勿重复转账"
	}
	if result == "paid" {
		msg = "付款已确认，请返回商户查看订单；勿重复付款"
	}
	c.JSON(200, gin.H{"result": result, "msg": msg})
}
