package server

import (
	"crypto/rand"
	"encoding/hex"
	"log/slog"
	"net/http"
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
	CanInquire       bool
	InquiryDelay     int
	NotifyStatus     string
	Title            string
	Provider         string
	ProviderName     string
	TradeNo          string
	OutTradeNo       string
	Name             string
	Amount           string // 精确应付金额，如 10.01
	Currency         string
	OriginMoney      string
	UID              string
	HasPayee         bool
	Note             string
	RemainSeconds    int
	TotalSeconds     int
	ReturnURL        string
	Status           string
	CreatedAt        string
	ExpiredAt        string
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

func (h *Handlers) renderCashier(c *gin.Context, o *model.Order) {
	cfg := h.C()
	remain := int(time.Until(o.ExpiresAt).Seconds())
	if remain < 0 {
		remain = 0
	}
	// 商户可以传入自定义 timeout；订单创建后修改全局有效期也不能改变
	// 这张订单的进度条总时长。
	total := int(o.ExpiresAt.Sub(o.CreatedAt).Seconds())
	if total <= 0 {
		total = cfg.Order.ExpireMinutes * 60
	}
	provider := strings.ToLower(o.Provider)
	providerName := "选择收款平台"
	uid := o.Receiver
	note := cfg.Checkout.Note

	reselectRequested := cfg.BEpusdt.Reselect && c.Query("reselect") == "1" && o.Reselect &&
		(o.Status == model.Pending || o.Status == model.Ready)
	selectedProvider := strings.ToLower(strings.TrimSpace(c.Query("provider")))
	if selectedProvider != config.ProviderBinance && selectedProvider != config.ProviderOKX {
		selectedProvider = ""
	}
	// 第一步选择收款平台是纯前端过渡页，不写数据库；只有第二步选择币种
	// 才真正锁定 订单付款方式。这样未选币种时订单不会被错误确认。
	needsProvider := o.Provider == "" || reselectRequested
	needsCurrency := !needsProvider && o.Currency == ""
	if selectedProvider != "" && (o.Provider == "" || reselectRequested) {
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
		uid = o.Receiver
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
	canReselect := cfg.BEpusdt.Reselect && o.Reselect &&
		(o.Status == model.Pending || o.Status == model.Ready) && provider != "" &&
		!reselectRequested && cfg.ProviderEnabled(provider)

	currencyOptions := currencyOptionsFor(cfg, o, provider)

	data := CashierData{
		CanInquire:       !needsProvider && !needsCurrency && o.Status == model.Ready,
		InquiryDelay:     inquiryDelay(o),
		Title:            providerName,
		Provider:         provider,
		ProviderName:     providerName,
		TradeNo:          o.TradeNo,
		OutTradeNo:       o.OutTradeNo,
		Name:             o.Name,
		Amount:           o.PayAmount(),
		Currency:         o.Currency,
		OriginMoney:      o.Original(),
		UID:              uid,
		HasPayee:         hasPayee,
		Note:             note,
		RemainSeconds:    remain,
		TotalSeconds:     total,
		ReturnURL:        o.ReturnURL,
		Status:           o.Status,
		CreatedAt:        o.CreatedAt.Format("2006-01-02 15:04:05"),
		ExpiredAt:        o.ExpiresAt.Format("15:04:05"),
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
			Selected: strings.EqualFold(o.Provider, config.ProviderBinance),
		},
		{
			Key: config.ProviderOKX, Name: "OKX", Logo: "/assets/brand/okx.svg",
			Subtitle: "UID 站内转账 · 按精确金额自动确认",
			Enabled:  cfg.OKXEnabled() && orderCurrencyAllowed(cfg, o, cfg.Checkout.Currencies...),
			Selected: strings.EqualFold(o.Provider, config.ProviderOKX),
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
	var out []currencyOption
	for _, currency := range cfg.Checkout.Currencies {
		if orderCurrencyAllowed(cfg, o, currency) {
			out = append(out, currencyOption{Currency: currency, Icon: currencyIcon(currency), Enabled: cfg.ProviderEnabled(provider), Selected: o.Currency == currency})
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

func (h *Handlers) CashierPage(c *gin.Context) {
	o, err := h.st.GetByTradeNo(c.Request.Context(), c.Param("trade_no"))
	if err != nil {
		h.renderError(c, 404, "未找到订单", "请返回商户页面核对订单。")
		return
	}
	if o.Status == model.Paid || o.Status == model.Expired || o.Status == model.Canceled || time.Now().After(o.ExpiresAt) {
		h.renderResult(c, o)
		return
	}
	h.renderCashier(c, o)
}
func inquiryDelay(o *model.Order) int {
	if o.SelectedAt == nil {
		return 60
	}
	return max(0, int(time.Until(o.SelectedAt.Add(time.Minute)).Seconds())+1)
}
func (h *Handlers) SelectPayment(c *gin.Context) {
	h.cfg.WithPaymentConfig(func() {
		cfg := h.C()
		no := c.PostForm("trade_no")
		provider := strings.ToLower(c.PostForm("provider"))
		currency := strings.ToUpper(c.PostForm("currency"))
		o, err := h.st.GetByTradeNo(c.Request.Context(), no)
		if err != nil || !cfg.ProviderEnabled(provider) || !cfg.CurrencyEnabled(currency) || !currencyAllowedByLimit(o.CurrencyLimit, currency) {
			c.JSON(400, gin.H{"code": 1, "msg": "付款方式不可用"})
			return
		}
		receiver := cfg.Checkout.Binance.UID
		if provider == config.ProviderOKX {
			receiver = cfg.Checkout.OKX.UID
		}
		step, e := amount.Parse(cfg.Order.AmountStep)
		minimum, me := amount.Parse(cfg.Order.MinAmount)
		if e != nil || me != nil {
			c.JSON(500, gin.H{"code": 1, "msg": "金额配置无效"})
			return
		}
		_, err = h.st.Select(c.Request.Context(), no, provider, currency, receiver, cfg.Order.AmountPrecision, step, minimum, cfg.Order.MaxOffset, cfg.Order.SlotGraceSeconds)
		if err != nil {
			c.JSON(400, gin.H{"code": 1, "msg": err.Error()})
			return
		}
		c.JSON(200, gin.H{"code": 0})
	})
}
func (h *Handlers) PaymentInquiry(c *gin.Context) {
	msg, err := h.poller.Inquiry(c.Request.Context(), c.PostForm("trade_no"), strings.TrimSpace(c.PostForm("reference")))
	if err != nil {
		if msg == "" {
			msg = err.Error()
		}
		c.JSON(400, gin.H{"code": 1, "msg": msg})
		return
	}
	result := "unconfirmed"
	if o, e := h.st.GetByTradeNo(c.Request.Context(), c.PostForm("trade_no")); e == nil && o.Status == model.Paid {
		result = "paid"
	}
	c.JSON(200, gin.H{"code": 0, "msg": msg, "result": result})
}
func (h *Handlers) renderResult(c *gin.Context, o *model.Order) {
	var cb model.Callback
	ack := "PENDING"
	if err := h.st.DB().WithContext(c.Request.Context()).First(&cb, "order_id = ?", o.ID).Error; err == nil && cb.DeliveredAt != nil {
		ack = "SUCCESS"
	}
	c.HTML(200, "result.html", CashierData{Title: "付款结果", TradeNo: o.TradeNo, OutTradeNo: o.OutTradeNo, Name: o.Name, Amount: o.PayAmount(), Currency: o.Currency, OriginMoney: o.Original(), ReturnURL: o.ReturnURL, Status: o.Status, CreatedAt: o.CreatedAt.Format("2006-01-02 15:04:05"), CanInquire: o.Status == model.Ready, InquiryDelay: inquiryDelay(o), Provider: o.Provider, NotifyStatus: ack})
}
func (h *Handlers) renderError(c *gin.Context, status int, title, detail string) {
	c.HTML(status, "error.html", gin.H{"Title": title, "Detail": detail})
}
func (h *Handlers) OrderStatus(c *gin.Context) {
	o, err := h.st.GetByTradeNo(c.Request.Context(), c.Query("order_id"))
	if err != nil {
		c.JSON(404, gin.H{"code": 1, "msg": "未找到订单"})
		return
	}
	var cb model.Callback
	ack := false
	if err = h.st.DB().WithContext(c.Request.Context()).First(&cb, "order_id = ?", o.ID).Error; err == nil {
		ack = cb.DeliveredAt != nil
	}
	c.JSON(200, gin.H{"code": 0, "data": gin.H{"status": o.Status, "amount": o.PayAmount(), "currency": o.Currency, "provider": o.Provider, "remain_seconds": max(0, int(time.Until(o.ExpiresAt).Seconds())), "return_url": externalURL(o.ReturnURL), "merchant_confirmed": ack}})
}
func (h *Handlers) Health(c *gin.Context) {
	if err := h.st.Ping(c.Request.Context()); err != nil {
		c.JSON(503, gin.H{"ok": false})
		return
	}
	c.JSON(200, gin.H{"ok": true})
}
func (h *Handlers) Ready(c *gin.Context) {
	cfg := h.C()
	if !cfg.BinanceEnabled() && !cfg.OKXEnabled() {
		c.JSON(503, gin.H{"ready": false, "reason": "未配置收款账户"})
		return
	}
	for _, provider := range []string{config.ProviderBinance, config.ProviderOKX} {
		if !cfg.ProviderEnabled(provider) {
			continue
		}
		cursor, err := h.st.Cursor(c.Request.Context(), provider)
		if err != nil || cursor.LastSuccess.IsZero() || cursor.LastError != "" || time.Since(cursor.LastSuccess) > 2*time.Minute || time.Since(cursor.ScannedUntil) > 3*time.Minute {
			c.JSON(503, gin.H{"ready": false, "reason": "收款扫描尚未就绪"})
			return
		}
	}
	h.Health(c)
}
func (h *Handlers) Index(c *gin.Context) {
	if target := externalURL(h.C().Server.StoreURL); target != "" {
		c.Redirect(302, target)
		return
	}
	c.Status(404)
}
func generateTradeNo() string {
	var b [20]byte
	if _, err := rand.Read(b[:]); err != nil {
		panic(err)
	}
	return hex.EncodeToString(b[:])
}
