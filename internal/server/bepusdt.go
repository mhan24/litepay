package server

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/gin-gonic/gin"

	money "litepay/internal/amount"
	"litepay/internal/config"
	"litepay/internal/model"
	"litepay/internal/sign"
)

const (
	bepusdtSigFailLimit   = 10
	replayCacheMaxEntries = 20000
	replayCacheWindow     = 75 * time.Second
)

type replayCache struct {
	mu      sync.Mutex
	entries map[string]time.Time
	cleanup time.Time
}

func newReplayCache() *replayCache {
	return &replayCache{entries: make(map[string]time.Time)}
}

func (r *replayCache) accept(endpoint string, body map[string]interface{}, now time.Time) bool {
	sig, _ := body["signature"].(string)
	nonce, _ := body["nonce"].(string)
	sig = strings.ToLower(strings.TrimSpace(sig))
	nonce = strings.TrimSpace(nonce)
	if sig == "" {
		return false
	}
	key := endpoint + "\x00" + sig + "\x00" + nonce

	r.mu.Lock()
	defer r.mu.Unlock()
	if !now.Before(r.cleanup) || len(r.entries) >= replayCacheMaxEntries {
		for k, until := range r.entries {
			if !now.Before(until) {
				delete(r.entries, k)
			}
		}
		r.cleanup = now.Add(time.Minute)
	}
	if until, exists := r.entries[key]; exists && now.Before(until) {
		return false
	}
	if len(r.entries) >= replayCacheMaxEntries {
		return false
	}
	r.entries[key] = now.Add(replayCacheWindow)
	return true
}

type bepusdtCreateRequest struct {
	OrderID     string      `json:"order_id"`
	NotifyURL   string      `json:"notify_url"`
	RedirectURL string      `json:"redirect_url"`
	Signature   string      `json:"signature"`
	Amount      json.Number `json:"amount"`
	Name        string      `json:"name"`
	Fiat        string      `json:"fiat"`
	Currencies  string      `json:"currencies"`
	Timeout     int64       `json:"timeout"`
	Reselect    *bool       `json:"reselect"`
}

type bepusdtCancelRequest struct {
	OrderID   string `json:"order_id"`
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
// 注意：由于上游 BEpusdt 协议（及独角数卡 BEpusdt 适配器）建单请求固定字段不包含 timestamp / nonce，
// 为保持协议对外 JSON 字段与签名算法完全兼容，create-order 不强制要求时间戳，
// 而是依靠 store.Create 按商户订单号（out_trade_no / order_id）的严格幂等约束，以及按 IP 的签名失败限流来防护重放与暴力猜解。
func (h *Handlers) BEpusdtCreateOrder(c *gin.Context) {
	h.createBEpusdtOrder(c)
}

// BEpusdtCancelOrder 实现 /api/v1/order/cancel-transaction。
// 取消后停止认领付款；原金额档位继续保留到冷却期结束。
func (h *Handlers) BEpusdtCancelOrder(c *gin.Context) {
	raw, err := io.ReadAll(http.MaxBytesReader(c.Writer, c.Request.Body, 1<<20))
	if err != nil {
		var maxErr *http.MaxBytesError
		if errors.As(err, &maxErr) {
			h.bepusdtFail(c, "request body too large")
			return
		}
		h.bepusdtFail(c, "request error")
		return
	}
	generic, err := decodeBEpusdtJSON(raw)
	if err != nil {
		h.bepusdtFail(c, "request json error: "+err.Error())
		return
	}
	if !sign.BEpusdtVerifyGeneric(generic, h.C().BEpusdt.APIToken) {
		h.bepusdtSigFail(c, "invalid_signature", "signature verification failed")
		return
	}
	var req bepusdtCancelRequest
	if err := json.Unmarshal(raw, &req); err != nil {
		h.bepusdtFail(c, "request error: "+err.Error())
		return
	}
	req.TradeID = strings.TrimSpace(req.TradeID)
	req.OrderID = strings.TrimSpace(req.OrderID)
	if len(req.TradeID) > 128 || len(req.OrderID) > 128 {
		h.bepusdtFail(c, "order reference is too long")
		return
	}
	if req.TradeID == "" && req.OrderID == "" {
		h.bepusdtFail(c, "trade_id or order_id is required")
		return
	}
	o, err := h.st.CancelReference(c.Request.Context(), req.TradeID, req.OrderID)
	if err != nil {
		h.bepusdtFail(c, err.Error())
		return
	}
	h.logger().Info("BEpusdt 订单已取消", "trade_id", o.TradeNo)
	c.JSON(http.StatusOK, bepusdtResponse{
		StatusCode: 200, Message: "success",
		Data: gin.H{"trade_id": o.TradeNo, "order_id": o.OutTradeNo},
	})
}

func (h *Handlers) createBEpusdtOrder(c *gin.Context) {
	if h.paymentOperationsBlocked(c) {
		h.bepusdtFail(c, "商城取消同步异常或核对未完成，暂停接单")
		return
	}
	cfg := h.C()
	if !cfg.AnyProviderEnabled() {
		h.bepusdtFail(c, "no payment provider is configured")
		return
	}
	raw, err := io.ReadAll(http.MaxBytesReader(c.Writer, c.Request.Body, 1<<20))
	if err != nil {
		var maxErr *http.MaxBytesError
		if errors.As(err, &maxErr) {
			h.bepusdtFail(c, "request body too large")
			return
		}
		h.bepusdtFail(c, "request error")
		return
	}
	generic, err := decodeBEpusdtJSON(raw)
	if err != nil {
		h.bepusdtFail(c, "request json error: "+err.Error())
		return
	}
	if !sign.BEpusdtVerifyGeneric(generic, cfg.BEpusdt.APIToken) {
		h.bepusdtSigFail(c, "invalid_signature", "signature verification failed")
		return
	}
	amount, err := jsonNumberString(generic["amount"])
	if err != nil {
		h.bepusdtFail(c, "amount must be a JSON number")
		return
	}
	units, err := money.Parse(amount)
	if err != nil || units <= 0 {
		h.bepusdtFail(c, "amount must be a positive exact decimal with at most six significant fractional digits")
		return
	}
	// 商城侧（dujiao-next BEpusdt 适配器）回调验签用 %v(float64) 重新渲染金额：
	// 金额 >= 1000000 或 < 0.0001 会被渲染成指数形式（1e+06 / 1e-05），与这里按
	// 十进制字符串生成的签名永远不一致，回调会被商城静默拒绝。在创建订单时就显式
	// 拒绝互操作范围之外的金额，避免付款后无法交付。
	if !money.MallCompatible(units) {
		h.bepusdtFail(c, "amount outside the upstream mall interoperable range [0.0001, 1000000)")
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
	req.Name = strings.TrimSpace(req.Name)
	req.Currencies = strings.TrimSpace(req.Currencies)
	if req.OrderID == "" || req.NotifyURL == "" || req.RedirectURL == "" {
		h.bepusdtFail(c, "order_id, notify_url, redirect_url and amount are required")
		return
	}
	if len(req.OrderID) > 128 || len(req.Name) > 255 || len(req.Currencies) > 64 ||
		len(req.NotifyURL) > 512 || len(req.RedirectURL) > 512 {
		h.bepusdtFail(c, "request field exceeds maximum length")
		return
	}
	if err := cfg.ValidateNotifyURL(req.NotifyURL); err != nil {
		h.bepusdtFail(c, err.Error())
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
	if h.paymentOperationsBlocked(c) {
		h.bepusdtFail(c, "商城取消同步异常或核对未完成，暂停接单")
		return
	}
	if !h.anyProviderAccepting(c, cfg, &model.Order{CurrencyLimit: req.Currencies}) {
		h.bepusdtFail(c, "收款扫描尚未就绪或行情不可用，暂停接单")
		return
	}
	tradeNo, err := generateTradeNo()
	if err != nil {
		h.bepusdtFail(c, "order create failed: "+err.Error())
		return
	}
	order, err := h.st.Create(c.Request.Context(), model.Order{SigningKeyID: cfg.BEpusdt.TokenVersion, TradeNo: tradeNo, OutTradeNo: req.OrderID, Name: req.Name, OriginUnits: units, NotifyURL: req.NotifyURL, ReturnURL: req.RedirectURL, ExpiresAt: time.Now().Add(ttl), CurrencyLimit: req.Currencies, Reselect: reselect})
	if err != nil {
		h.bepusdtFail(c, "order create failed: "+err.Error())
		return
	}
	h.logger().Info("BEpusdt 收银台订单已处理", "trade_id", order.TradeNo, "order_id", req.OrderID)
	h.bepusdtCreated(c, cfg, order, reselect)
}

func bepusdtCurrency(cfg *config.Config, req bepusdtCreateRequest) error {
	for _, currency := range cfg.Checkout.Currencies {
		if config.IsSupportedCurrency(currency) && currencyAllowedByLimit(req.Currencies, currency) {
			for _, provider := range []string{config.ProviderBinance, config.ProviderOKX, config.ProviderBybit, config.ProviderBitget} {
				if cfg.ProviderEnabled(provider) && cfg.ProviderCurrency(provider, currency) {
					return nil
				}
			}
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
		"fiat": "USD", "trade_id": order.TradeNo, "order_id": order.OutTradeNo,
		"name": order.Name, "status": orderStatusNumber(order), "amount": order.Original(),
		"expiration_time": max(0, int(time.Until(order.ExpiresAt).Seconds())),
		"payment_url":     cfg.Server.BaseURL + "/pay/" + order.TradeNo,
		"reselect":        reselect && order.Reselect && order.Status != model.Paid,
		"network":         bepusdtPaymentMethods(cfg, order),
	}
	c.JSON(http.StatusOK, bepusdtResponse{StatusCode: 200, Message: "success", Data: data, RequestID: ""})
}

// bepusdtPaymentMethods mirrors the cashier method list returned by upstream
// BEpusdt. Dujiao Next currently ignores this field, but returning the complete
// contract keeps the cashier response interoperable with other clients.
func bepusdtPaymentMethods(cfg *config.Config, order *model.Order) []gin.H {
	out := make([]gin.H, 0, 8)
	for _, provider := range cfg.OrderedProviders() {
		if !cfg.ProviderEnabled(provider) {
			continue
		}
		network := "Binance"
		if provider == config.ProviderOKX {
			network = "OKX"
		}
		if provider == config.ProviderBybit {
			network = "Bybit"
		}
		if provider == config.ProviderBitget {
			network = "Bitget"
		}
		for _, currency := range cfg.Checkout.Currencies {
			if !cfg.ProviderCurrency(provider, currency) ||
				!currencyAllowedByLimit(order.CurrencyLimit, currency) {
				continue
			}
			actual, rate := order.Original(), "1"
			out = append(out, gin.H{
				"amount":            order.Original(),
				"actual_amount":     actual,
				"fiat":              "USD",
				"exchange_rate":     rate,
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
	case model.Paid:
		return 2
	case model.Expired:
		return 3
	case model.Canceled:
		return 4
	default:
		return 1
	}
}

func (h *Handlers) bepusdtSigFail(c *gin.Context, reason, message string) {
	now := time.Now()
	h.logOrderSigFail(c, reason, message, now)
	if h.selectionLimit != nil {
		if ok, retry := h.selectionLimit.allow("order-sig-fail:"+c.ClientIP(), bepusdtSigFailLimit, now); !ok {
			waitForRequest(c, 100*time.Millisecond)
			c.Header("Retry-After", strconv.Itoa(retry))
			c.JSON(http.StatusTooManyRequests, bepusdtResponse{StatusCode: http.StatusTooManyRequests, Message: message, RequestID: ""})
			return
		}
	}
	c.JSON(http.StatusOK, bepusdtResponse{StatusCode: 400, Message: message, RequestID: ""})
}

func (h *Handlers) logOrderSigFail(c *gin.Context, reason, message string, now time.Time) {
	if h.selectionLimit != nil {
		if ok, _ := h.selectionLimit.allow("order-sig-log:"+c.ClientIP(), 1, now); !ok {
			return
		}
	}
	h.logger().Warn("BEpusdt API 签名校验被拒", "ip", c.ClientIP(), "path", c.Request.URL.Path, "reason", reason, "message", message)
}

func (h *Handlers) bepusdtFail(c *gin.Context, message string) {
	h.logger().Warn("BEpusdt API 返回业务错误", "ip", c.ClientIP(), "message", message)
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

// waitForRequest bounds throttling work and releases canceled requests promptly.
func waitForRequest(c *gin.Context, delay time.Duration) {
	if delay <= 0 {
		return
	}
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-timer.C:
	case <-c.Request.Context().Done():
	}
}
