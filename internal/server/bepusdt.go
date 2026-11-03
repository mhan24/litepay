package server

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/gin-gonic/gin"

	money "litepay/internal/amount"
	"litepay/internal/config"
	"litepay/internal/model"
	"litepay/internal/sign"
	"litepay/internal/store"
)

type bepusdtCreateRequest struct {
	OrderID     string  `json:"order_id"`
	NotifyURL   string  `json:"notify_url"`
	RedirectURL string  `json:"redirect_url"`
	Signature   string  `json:"signature"`
	Amount      float64 `json:"amount"`
	Name        string  `json:"name"`
	Fiat        string  `json:"fiat"`
	Currencies  string  `json:"currencies"`
	Timeout     int64   `json:"timeout"`
	Reselect    *bool   `json:"reselect"`
}

type bepusdtCancelRequest struct {
	TradeID   string `json:"trade_id"`
	Signature string `json:"signature"`
}

type bepusdtResponse struct {
	StatusCode int         `json:"status_code"`
	Message    string      `json:"message"`
	Data       interface{} `json:"data,omitempty"`
	RequestID  string      `json:"request_id"`
}

// BEpusdtCreateOrder 实现 BEpusdt 收银台模式。
func (h *Handlers) BEpusdtCreateOrder(c *gin.Context) {
	h.createBEpusdtOrder(c)
}

// BEpusdtCancelOrder 实现 /api/v1/order/cancel-transaction。
// 与官方 BEpusdt 一致：停止确认待支付订单并释放占用金额。
func (h *Handlers) BEpusdtCancelOrder(c *gin.Context) {
	raw, err := io.ReadAll(http.MaxBytesReader(c.Writer, c.Request.Body, 1<<20))
	if err != nil {
		h.bepusdtFail(c, "request error")
		return
	}
	generic, err := decodeBEpusdtJSON(raw)
	if err != nil {
		h.bepusdtFail(c, "request json error: "+err.Error())
		return
	}
	if !sign.BEpusdtVerifyGeneric(generic, h.C().BEpusdt.APIToken) {
		h.bepusdtFail(c, "signature verification failed")
		return
	}
	var req bepusdtCancelRequest
	if err := json.Unmarshal(raw, &req); err != nil {
		h.bepusdtFail(c, "request error: "+err.Error())
		return
	}
	req.TradeID = strings.TrimSpace(req.TradeID)
	if req.TradeID == "" {
		h.bepusdtFail(c, "trade_id is required")
		return
	}
	if err := h.st.CancelOrder(c.Request.Context(), req.TradeID); err != nil {
		h.bepusdtFail(c, err.Error())
		return
	}
	h.log.Info("BEpusdt 订单已取消", "trade_id", req.TradeID)
	c.JSON(http.StatusOK, bepusdtResponse{
		StatusCode: 200, Message: "success",
		Data: gin.H{"trade_id": req.TradeID},
	})
}

func (h *Handlers) createBEpusdtOrder(c *gin.Context) {
	cfg := h.C()
	if !cfg.BinanceEnabled() && !cfg.OKXEnabled() {
		h.bepusdtFail(c, "no payment provider is configured")
		return
	}
	raw, err := io.ReadAll(http.MaxBytesReader(c.Writer, c.Request.Body, 1<<20))
	if err != nil {
		h.bepusdtFail(c, "request error")
		return
	}
	generic, err := decodeBEpusdtJSON(raw)
	if err != nil {
		h.bepusdtFail(c, "request json error: "+err.Error())
		return
	}
	if !sign.BEpusdtVerifyGeneric(generic, cfg.BEpusdt.APIToken) {
		h.bepusdtFail(c, "signature verification failed")
		return
	}
	amount, err := jsonNumberString(generic["amount"])
	if err != nil {
		h.bepusdtFail(c, "amount must be a JSON number")
		return
	}
	decimal, decimalErr := money.Decimal(amount)
	if decimalErr != nil || decimal.Sign() <= 0 {
		h.bepusdtFail(c, "amount must be a bounded positive decimal number")
		return
	}
	amountValue, err := strconv.ParseFloat(amount, 64)
	if err != nil || math.IsNaN(amountValue) || math.IsInf(amountValue, 0) || amountValue <= 0 || amountValue > 1e9 {
		h.bepusdtFail(c, "amount must be greater than zero and at most 1000000000")
		return
	}

	var req bepusdtCreateRequest
	if err := json.Unmarshal(raw, &req); err != nil {
		h.bepusdtFail(c, "request error: "+err.Error())
		return
	}
	req.OrderID = strings.TrimSpace(req.OrderID)
	req.NotifyURL = strings.TrimSpace(req.NotifyURL)
	req.RedirectURL = strings.TrimSpace(req.RedirectURL)
	if req.OrderID == "" || req.NotifyURL == "" || req.RedirectURL == "" {
		h.bepusdtFail(c, "order_id, notify_url, redirect_url and amount are required")
		return
	}
	if len(req.OrderID) > 128 || len(req.Name) > 255 || len(req.Currencies) > 64 ||
		len(req.NotifyURL) > 512 || len(req.RedirectURL) > 512 {
		h.bepusdtFail(c, "request field exceeds maximum length")
		return
	}
	if externalURL(req.NotifyURL) == "" {
		h.bepusdtFail(c, "notify_url must be an absolute http(s) URL")
		return
	}
	if externalURL(req.RedirectURL) == "" {
		h.bepusdtFail(c, "redirect_url must be an absolute http(s) URL")
		return
	}
	fiat := strings.ToUpper(strings.TrimSpace(req.Fiat))
	if fiat == "" {
		fiat = "USD"
	}
	if fiat != "USD" {
		h.bepusdtFail(c, "unsupported fiat: "+fiat+" (supported: USD)")
		return
	}

	if err := bepusdtCurrency(cfg, req); err != nil {
		h.bepusdtFail(c, err.Error())
		return
	}
	reselect := bepusdtReselect(cfg, req.Reselect)
	// 最低应付金额属于稳定币金额，必须在用户选定通道和币种后再校验。
	ttl := time.Duration(cfg.Order.ExpireMinutes) * time.Minute
	if req.Timeout != 0 && (req.Timeout < 120 || req.Timeout > 86400) {
		h.bepusdtFail(c, "timeout must be 0 or between 120 and 86400 seconds")
		return
	}
	if req.Timeout >= 120 {
		ttl = time.Duration(req.Timeout) * time.Second
	}
	tradeNo := generateTradeNo()
	order, err := h.st.CreateOrReuseCashierOrder(c.Request.Context(), store.CreateOrderInput{
		Protocol:       "bepusdt",
		Pid:            "bepusdt",
		TradeNo:        tradeNo,
		OutTradeNo:     req.OrderID,
		Name:           strings.TrimSpace(req.Name),
		OriginMoney:    amount,
		OriginCurrency: fiat,
		NotifyURL:      req.NotifyURL,
		ReturnURL:      req.RedirectURL,
		ClientIP:       c.ClientIP(),
		ExpiredAt:      time.Now().Add(ttl),
		CurrencyLimit:  req.Currencies,
		Reselect:       reselect,
		ReplaceReady:   true,
	})
	if err != nil {
		h.bepusdtFail(c, "order create failed: "+err.Error())
		return
	}
	h.log.Info("BEpusdt 收银台订单已处理", "trade_id", order.TradeNo, "order_id", req.OrderID)
	h.bepusdtCreated(c, cfg, order, reselect)
}

func bepusdtCurrency(cfg *config.Config, req bepusdtCreateRequest) error {
	for _, currency := range cfg.Checkout.Currencies {
		if config.IsSupportedCurrency(currency) && currencyAllowedByLimit(req.Currencies, currency) {
			return nil
		}
	}
	return fmt.Errorf("no supported currency matches %q", req.Currencies)
}

// bepusdtReselect 把后台开关作为全局主开关；只有后台允许时，
// 商户订单请求才可以通过 reselect=false 进一步收紧。
func bepusdtReselect(cfg *config.Config, requested *bool) bool {
	if !cfg.BEpusdt.Reselect {
		return false
	}
	return requested == nil || *requested
}

func (h *Handlers) bepusdtCreated(c *gin.Context, cfg *config.Config, order *model.Order, reselect bool) {
	data := gin.H{
		"fiat": order.OriginCurrency, "trade_id": order.TradeNo, "order_id": order.OutTradeNo,
		"name": order.Name, "status": orderStatusNumber(order), "amount": order.OriginMoney,
		"expiration_time": max(0, int(time.Until(order.ExpiredAt).Seconds())),
		"payment_url":     cfg.Server.BaseURL + "/pay/" + order.TradeNo,
		"reselect":        reselect && order.Reselect && order.Status != model.StatusPaid,
		"network":         bepusdtPaymentMethods(cfg, order),
	}
	c.JSON(http.StatusOK, bepusdtResponse{StatusCode: 200, Message: "success", Data: data, RequestID: ""})
}

// bepusdtPaymentMethods mirrors the cashier method list returned by upstream
// BEpusdt. Dujiao Next currently ignores this field, but returning the complete
// contract keeps the cashier response interoperable with other clients.
func bepusdtPaymentMethods(cfg *config.Config, order *model.Order) []gin.H {
	out := make([]gin.H, 0, 4)
	for _, provider := range []string{config.ProviderBinance, config.ProviderOKX} {
		if !cfg.ProviderEnabled(provider) {
			continue
		}
		network := "Binance"
		if provider == config.ProviderOKX {
			network = "OKX"
		}
		for _, currency := range cfg.Checkout.Currencies {
			if !config.IsSupportedCurrency(currency) ||
				!currencyAllowedByLimit(order.CurrencyLimit, currency) {
				continue
			}
			out = append(out, gin.H{
				"amount":            order.OriginMoney,
				"actual_amount":     order.OriginMoney,
				"fiat":              order.OriginCurrency,
				"exchange_rate":     "1",
				"currency":          currency,
				"network":           network,
				"token_net_name":    network,
				"token_custom_name": "",
				"is_popular":        false,
			})
		}
	}
	return out
}

func orderStatusNumber(order *model.Order) int {
	switch order.Status {
	case model.StatusPaid:
		return 2
	case model.StatusExpired:
		return 3
	case model.StatusCanceled:
		return 4
	default:
		return 1
	}
}

func (h *Handlers) bepusdtFail(c *gin.Context, message string) {
	h.log.Warn("BEpusdt API 返回业务错误", "ip", c.ClientIP(), "message", message)
	c.JSON(http.StatusOK, bepusdtResponse{StatusCode: 400, Message: message, RequestID: ""})
}

func decodeBEpusdtJSON(raw []byte) (map[string]interface{}, error) {
	var generic map[string]interface{}
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.UseNumber()
	if err := decoder.Decode(&generic); err != nil {
		return nil, err
	}
	if generic == nil {
		return nil, fmt.Errorf("expected JSON object")
	}
	if err := decoder.Decode(new(interface{})); err != io.EOF {
		return nil, fmt.Errorf("unexpected trailing JSON")
	}
	return generic, nil
}

func jsonNumberString(value interface{}) (string, error) {
	number, ok := value.(json.Number)
	if !ok {
		return "", fmt.Errorf("not a number")
	}
	return number.String(), nil
}
