package server

import (
	"crypto/rand"
	"encoding/hex"
	"log/slog"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/gin-gonic/gin"

	"litepay/internal/amount"
	"litepay/internal/config"
	"litepay/internal/model"
	"litepay/internal/settings"
	"litepay/internal/store"
	"litepay/internal/worker"
)

// Handlers 持有全部依赖。
type Handlers struct {
	cfg     *settings.Manager
	st      *store.Store
	log     *slog.Logger
	adminMu sync.Mutex
	admin   *adminAuth
	poller  *worker.Poller
}

// C 返回当前生效配置的快照（后台修改后立即反映）。
func (h *Handlers) C() *config.Config { return h.cfg.Get() }

// CashierData 收银台模板数据。
type CashierData struct {
	Title            string
	Provider         string
	ProviderName     string
	TradeNo          string
	OutTradeNo       string
	Name             string
	OriginAmount     string // 下单 USD 金额，两步选择阶段也会展示
	Amount           string // 精确应付金额，如 10.01
	Currency         string
	OriginMoney      string
	OriginCurrency   string
	UID              string
	HasPayee         bool
	Note             string
	RemainSeconds    int
	TotalSeconds     int
	ReturnURL        string
	Status           string
	CreatedAt        string
	ExpiredAt        string
	Currencies       []string
	Providers        []providerOption
	CurrencyOptions  []currencyOption
	CanReselect      bool
	NeedsProvider    bool
	NeedsCurrency    bool
	ProviderSubtitle string
}

// -----------------------------------------------------------------------------
// 收银台页面
// -----------------------------------------------------------------------------

// CashierPage GET /pay/:trade_no —— 供用户刷新/回看订单。
func (h *Handlers) CashierPage(c *gin.Context) {
	tradeNo := c.Param("trade_no")
	o, err := h.st.GetByTradeNo(c.Request.Context(), tradeNo)
	if err != nil || o == nil {
		h.renderError(c, http.StatusNotFound, "未找到订单", "该订单不存在，可能已被删除。请返回商户页面重新下单。")
		return
	}
	if o.Status == model.StatusPaid {
		h.renderResult(c, o)
		return
	}
	if o.Status == model.StatusCanceled || o.Status == model.StatusExpired ||
		(o.Status != model.StatusPending && o.Status != model.StatusReady) ||
		time.Now().After(o.ExpiredAt) {
		h.renderResult(c, o)
		return
	}
	h.renderCashier(c, o)
}

func (h *Handlers) renderCashier(c *gin.Context, o *model.Order) {
	cfg := h.C()
	if o.OriginCurrency != "USD" {
		h.renderResult(c, o)
		return
	}
	remain := int(time.Until(o.ExpiredAt).Seconds())
	if remain < 0 {
		remain = 0
	}
	// 商户可以传入自定义 timeout；订单创建后修改全局有效期也不能改变
	// 这张订单的进度条总时长。
	total := int(o.ExpiredAt.Sub(o.CreatedAt).Seconds())
	if total <= 0 {
		total = cfg.Order.ExpireMinutes * 60
	}
	provider := strings.ToLower(o.Collection)
	providerName := "选择收款平台"
	uid := o.PayAddress
	note := cfg.Checkout.Note

	reselectRequested := cfg.BEpusdt.Reselect && c.Query("reselect") == "1" && o.Reselect &&
		(o.Status == model.StatusPending || o.Status == model.StatusReady)
	selectedProvider := strings.ToLower(strings.TrimSpace(c.Query("provider")))
	if selectedProvider != config.ProviderBinance && selectedProvider != config.ProviderOKX {
		selectedProvider = ""
	}
	// 第一步选择收款平台是纯前端过渡页，不写数据库；只有第二步选择币种
	// 才真正锁定 order.collection。这样未选币种时订单不会被错误确认。
	needsProvider := o.Collection == "" || reselectRequested
	needsCurrency := !needsProvider && o.Currency == ""
	if selectedProvider != "" && (o.Collection == "" || reselectRequested) {
		needsProvider = false
		needsCurrency = true
		provider = selectedProvider
	}
	if needsProvider {
		uid = ""
		note = ""
	} else if needsCurrency {
		uid = ""
		// 当前后台提示是按最终精确金额设计的。锁定前展示会与随后
		// 分配的尾差金额不一致，因此选择阶段隐藏该提示。
		note = ""
		if provider == config.ProviderBinance {
			providerName = "Binance 站内转账"
		} else if provider == config.ProviderOKX {
			providerName = "OKX 站内转账"
		}
	} else if provider == config.ProviderBinance {
		providerName = "Binance 站内转账"
	} else if provider == config.ProviderOKX {
		providerName = "OKX 站内转账"
		uid = o.PayAddress
	}
	providerSubtitle := "先选择收款平台，再选择币种"
	if needsCurrency && provider == config.ProviderBinance {
		providerSubtitle = "选择 Binance 收款币种"
	} else if needsCurrency && provider == config.ProviderOKX {
		providerSubtitle = "选择 OKX 收款币种"
	} else if provider == config.ProviderBinance {
		providerSubtitle = "UID 站内转账 · 自动确认到账"
	} else if provider == config.ProviderOKX {
		providerSubtitle = "UID 站内转账 · 自动确认到账"
	}
	hasPayee := uid != ""
	canReselect := cfg.BEpusdt.Reselect && o.Protocol == "bepusdt" && o.Reselect &&
		(o.Status == model.StatusPending || o.Status == model.StatusReady) && provider != "" &&
		!reselectRequested && cfg.ProviderEnabled(provider)

	currencyOptions := currencyOptionsFor(cfg, o, provider)

	data := CashierData{
		Title:            providerName,
		Provider:         provider,
		ProviderName:     providerName,
		TradeNo:          o.TradeNo,
		OutTradeNo:       o.OutTradeNo,
		Name:             o.Name,
		OriginAmount:     o.OriginMoney,
		Amount:           amount.FormatPrecision(o.PayAmount, o.AmountPrecision),
		Currency:         o.Currency,
		OriginMoney:      o.OriginMoney,
		OriginCurrency:   o.OriginCurrency,
		UID:              uid,
		HasPayee:         hasPayee,
		Note:             note,
		RemainSeconds:    remain,
		TotalSeconds:     total,
		ReturnURL:        o.ReturnURL,
		Status:           o.Status,
		CreatedAt:        o.CreatedAt.Format("2006-01-02 15:04:05"),
		ExpiredAt:        o.ExpiredAt.Format("15:04:05"),
		Currencies:       append([]string(nil), cfg.Checkout.Currencies...),
		Providers:        providerOptionForQuery(providerOptions(cfg, o), selectedProvider),
		CurrencyOptions:  currencyOptions,
		CanReselect:      canReselect,
		NeedsProvider:    needsProvider,
		NeedsCurrency:    needsCurrency,
		ProviderSubtitle: providerSubtitle,
	}
	c.HTML(http.StatusOK, "cashier.html", data)
}

type providerOption struct {
	Key      string
	Name     string
	Subtitle string
	Logo     string
	Enabled  bool
	Selected bool
}

type currencyOption struct {
	Currency string
	Icon     string
	Enabled  bool
	Selected bool
}

func providerOptions(cfg *config.Config, o *model.Order) []providerOption {
	return []providerOption{
		{
			Key: config.ProviderBinance, Name: "Binance", Logo: "/assets/brand/binance.svg",
			Subtitle: "UID 站内转账 · USDT / USDC",
			Enabled:  cfg.BinanceEnabled() && orderCurrencyAllowed(cfg, o, "USDT", "USDC"),
			Selected: strings.EqualFold(o.Collection, config.ProviderBinance),
		},
		{
			Key: config.ProviderOKX, Name: "OKX", Logo: "/assets/brand/okx.svg",
			Subtitle: "UID 站内转账 · 按精确金额自动确认",
			Enabled:  cfg.OKXEnabled() && orderCurrencyAllowed(cfg, o, okxCurrencies(cfg)...),
			Selected: strings.EqualFold(o.Collection, config.ProviderOKX),
		},
	}
}

// providerOptionForQuery 让收银台在选择收款平台的过渡页上先高亮目标平台，
// 真正的订单仍保持未绑定状态，只有选币种时才原子锁定付款信息。
func providerOptionForQuery(options []providerOption, selected string) []providerOption {
	selected = strings.ToLower(strings.TrimSpace(selected))
	if selected == "" {
		return options
	}
	for i := range options {
		options[i].Selected = strings.EqualFold(options[i].Key, selected)
	}
	return options
}

func currencyOptionsFor(cfg *config.Config, o *model.Order, provider string) []currencyOption {
	provider = strings.ToLower(strings.TrimSpace(provider))
	out := make([]currencyOption, 0, len(cfg.Checkout.Currencies))
	for _, currency := range cfg.Checkout.Currencies {
		currency = strings.ToUpper(strings.TrimSpace(currency))
		if !orderCurrencyAllowed(cfg, o, currency) || !providerAllowsCurrency(provider, currency) {
			continue
		}
		switch provider {
		case config.ProviderBinance:
			out = append(out, currencyOption{
				Currency: currency, Icon: currencyIcon(currency), Enabled: cfg.BinanceEnabled(),
				Selected: strings.EqualFold(o.Currency, currency),
			})
		case config.ProviderOKX:
			out = append(out, currencyOption{
				Currency: currency, Icon: currencyIcon(currency), Enabled: cfg.OKXEnabled(),
				Selected: strings.EqualFold(o.Currency, currency),
			})
		default:
			out = append(out, currencyOption{Currency: currency, Icon: currencyIcon(currency), Enabled: true})
		}
	}
	return out
}

func currencyIcon(currency string) string {
	switch strings.ToUpper(strings.TrimSpace(currency)) {
	case "USDT":
		return "/assets/brand/usdt.svg"
	case "USDC":
		return "/assets/brand/usdc.svg"
	default:
		return ""
	}
}

func providerAllowsCurrency(provider, currency string) bool {
	switch strings.ToLower(strings.TrimSpace(provider)) {
	case config.ProviderOKX:
		return currency == "USDT" || currency == "USDC"
	case config.ProviderBinance:
		return currency == "USDT" || currency == "USDC"
	default:
		return false
	}
}

func orderCurrencyAllowed(cfg *config.Config, o *model.Order, currencies ...string) bool {
	for _, wanted := range currencies {
		wanted = strings.ToUpper(strings.TrimSpace(wanted))
		if cfg.CurrencyEnabled(wanted) && currencyAllowedByLimit(o.CurrencyLimit, wanted) {
			return true
		}
	}
	return false
}

func currencyAllowedByLimit(limit string, currency string) bool {
	limit = strings.TrimSpace(limit)
	if limit == "" {
		return true
	}
	include := make(map[string]bool)
	exclude := make(map[string]bool)
	for _, raw := range strings.FieldsFunc(strings.ToUpper(limit), func(r rune) bool {
		return r == ',' || r == '，' || r == ' ' || r == '/' || r == '|' || r == ';'
	}) {
		raw = strings.TrimSpace(raw)
		if raw == "" {
			continue
		}
		if strings.HasPrefix(raw, "-") {
			exclude[strings.TrimPrefix(raw, "-")] = true
		} else {
			include[raw] = true
		}
	}
	if exclude[currency] {
		return false
	}
	return len(include) == 0 || include[currency]
}

func okxCurrencies(cfg *config.Config) []string {
	out := make([]string, 0, len(cfg.Checkout.Currencies))
	for _, currency := range cfg.Checkout.Currencies {
		if currency == "USDT" || currency == "USDC" {
			out = append(out, currency)
		}
	}
	return out
}

// SelectPayment POST /api/order/payment —— 选择通道与币种。
func (h *Handlers) SelectPayment(c *gin.Context) {
	// 订单号是收银台的 bearer 凭据；仅允许本站页面提交，防止第三方页面
	// 在付款人不知情时替换收款平台或币种。该接口不依赖 Cookie，因此不能
	// 用 SameSite 代替来源检查。
	if site := c.GetHeader("Sec-Fetch-Site"); site != "" && site != "same-origin" {
		c.JSON(http.StatusForbidden, gin.H{"code": 1, "msg": "拒绝跨站修改收款方式"})
		return
	}
	h.cfg.WithPaymentConfig(func() { h.selectPayment(c) })
}

func (h *Handlers) selectPayment(c *gin.Context) {
	cfg := h.C()
	tradeNo := strings.TrimSpace(c.PostForm("trade_no"))
	provider := strings.ToLower(strings.TrimSpace(c.PostForm("provider")))
	currency := strings.ToUpper(strings.TrimSpace(c.PostForm("currency")))
	if tradeNo == "" || !cfg.CurrencyEnabled(currency) || !cfg.ProviderEnabled(provider) {
		c.JSON(http.StatusBadRequest, gin.H{"code": 1, "msg": "订单、收款平台或币种无效，请刷新页面后重试"})
		return
	}
	o, err := h.st.GetByTradeNo(c.Request.Context(), tradeNo)
	if err != nil || o == nil {
		c.JSON(http.StatusNotFound, gin.H{"code": 1, "msg": "未找到订单，请返回商户页面重新下单"})
		return
	}
	if (o.Status != model.StatusPending && o.Status != model.StatusReady) ||
		(o.Status == model.StatusReady && (!o.Reselect || !cfg.BEpusdt.Reselect)) ||
		time.Now().After(o.ExpiredAt) {
		c.JSON(http.StatusConflict, gin.H{"code": 1, "msg": "该订单当前无法更改收款方式，请返回商户页面查看订单状态"})
		return
	}
	if provider == config.ProviderBinance && currency != "USDT" && currency != "USDC" {
		c.JSON(http.StatusBadRequest, gin.H{"code": 1, "msg": "Binance 收款仅支持 USDT 或 USDC"})
		return
	}
	if !providerAllowsCurrency(provider, currency) {
		c.JSON(http.StatusBadRequest, gin.H{"code": 1, "msg": "该收款平台不支持所选币种，请重新选择"})
		return
	}
	if !orderCurrencyAllowed(cfg, o, currency) {
		c.JSON(http.StatusBadRequest, gin.H{"code": 1, "msg": "商户未开放该币种，请选择其他币种"})
		return
	}
	origin, err := strconv.ParseFloat(o.OriginMoney, 64)
	if err != nil || origin <= 0 {
		c.JSON(http.StatusBadRequest, gin.H{"code": 1, "msg": "订单金额不正确，请返回商户页面重新下单"})
		return
	}
	if o.OriginCurrency != "USD" {
		c.JSON(http.StatusBadRequest, gin.H{"code": 1, "msg": "订单法币不受支持，请返回商户页面重新下单"})
		return
	}
	var payAddress string
	if provider == config.ProviderOKX {
		payAddress = cfg.Checkout.OKX.UID
	} else {
		payAddress = cfg.Checkout.Binance.UID
	}
	updated, err := h.st.SelectPayment(c.Request.Context(), tradeNo, provider, currency,
		payAddress, origin,
		cfg.Order.AmountStep, cfg.Order.MaxOffset, cfg.Order.MinAmount,
		cfg.Order.AmountPrecision,
		time.Duration(cfg.Order.SlotGraceSeconds)*time.Second)
	if err != nil {
		c.JSON(http.StatusConflict, gin.H{"code": 1, "msg": err.Error()})
		return
	}
	c.JSON(http.StatusOK, gin.H{"code": 0, "data": gin.H{
		"provider": updated.Collection, "currency": updated.Currency,
		"amount": amount.FormatPrecision(updated.PayAmount, updated.AmountPrecision),
	}})
}

func (h *Handlers) renderResult(c *gin.Context, o *model.Order) {
	data := CashierData{
		Title:       "支付结果",
		TradeNo:     o.TradeNo,
		OutTradeNo:  o.OutTradeNo,
		Name:        o.Name,
		Amount:      amount.FormatPrecision(o.PayAmount, o.AmountPrecision),
		Currency:    o.Currency,
		OriginMoney: o.OriginMoney,
		ReturnURL:   o.ReturnURL,
		Status:      o.Status,
		CreatedAt:   o.CreatedAt.Format("2006-01-02 15:04:05"),
	}
	c.HTML(http.StatusOK, "result.html", data)
}

func (h *Handlers) renderError(c *gin.Context, status int, title, detail string) {
	c.HTML(status, "error.html", gin.H{
		"Title":  title,
		"Detail": detail,
	})
}

// -----------------------------------------------------------------------------
// 前端订单状态查询接口
// -----------------------------------------------------------------------------

// OrderStatus GET /api/order/status?order_id=<trade_no>
func (h *Handlers) OrderStatus(c *gin.Context) {
	tradeNo := strings.TrimSpace(c.Query("order_id"))
	if tradeNo == "" {
		c.JSON(http.StatusBadRequest, gin.H{"code": 1, "msg": "缺少订单号"})
		return
	}

	o, err := h.st.GetByTradeNo(c.Request.Context(), tradeNo)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"code": 1, "msg": "订单状态查询失败，请稍后重试"})
		return
	}
	if o == nil {
		c.JSON(http.StatusNotFound, gin.H{"code": 1, "msg": "未找到订单"})
		return
	}
	remain := int(time.Until(o.ExpiredAt).Seconds())
	if remain < 0 {
		remain = 0
	}

	c.JSON(http.StatusOK, gin.H{
		"code": 0,
		"msg":  "ok",
		"data": gin.H{
			"order_id":       o.TradeNo,
			"out_trade_no":   o.OutTradeNo,
			"status":         o.Status,
			"amount":         amount.FormatPrecision(o.PayAmount, o.AmountPrecision),
			"currency":       o.Currency,
			"remain_seconds": remain,
			"paid_at":        o.PaidAt,
			"return_url":     externalURL(o.ReturnURL),
			"provider":       o.Collection,
			"reselect":       o.Reselect && h.C().BEpusdt.Reselect,
		},
	})
}

// -----------------------------------------------------------------------------
// 运维 / 首页
// -----------------------------------------------------------------------------

// Health GET /healthz
func (h *Handlers) Health(c *gin.Context) {
	if err := h.st.Ping(c.Request.Context()); err != nil {
		h.log.Error("健康检查数据库访问失败", "err", err)
		c.JSON(http.StatusServiceUnavailable, gin.H{"ok": false})
		return
	}
	c.JSON(http.StatusOK, gin.H{
		"ok":   true,
		"time": time.Now().Format(time.RFC3339),
	})
}

// Index GET /
func (h *Handlers) Index(c *gin.Context) {
	c.HTML(http.StatusOK, "index.html", gin.H{
		"Currency":    h.C().DefaultCurrency(),
		"Provider":    "Binance / OKX",
		"Expire":      h.C().Order.ExpireMinutes,
		"PollSeconds": min(h.C().Binance.PollInterval, h.C().OKX.PollInterval),
	})
}

// -----------------------------------------------------------------------------
// 工具函数
// -----------------------------------------------------------------------------

// generateTradeNo is also the public bearer credential for a cashier order.
func generateTradeNo() string {
	var buf [16]byte
	if _, err := rand.Read(buf[:]); err != nil {
		panic("secure random source unavailable")
	}
	return "LP" + hex.EncodeToString(buf[:])
}
