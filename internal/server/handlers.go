package server

import (
	"crypto/rand"
	"encoding/base64"
	"encoding/hex"
	"errors"
	"fmt"
	"html/template"
	"log/slog"
	"math"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/skip2/go-qrcode"

	"litepay/internal/amount"
	"litepay/internal/config"
	"litepay/internal/gram"
	"litepay/internal/model"
	"litepay/internal/settings"
	"litepay/internal/store"
	"litepay/internal/worker"
)

// Handlers 持有全部依赖。
type Handlers struct {
	readLimit      *selectionLimiter
	selectionLimit *selectionLimiter
	replays        *replayCache
	cfg            *settings.Manager
	st             *store.Store
	log            *slog.Logger
	proxyWarnOnce  sync.Once
	adminMu        sync.Mutex
	admin          *adminAuth
	poller         *worker.Poller
}

// C 返回当前生效配置的快照（后台修改后立即反映）。
func (h *Handlers) C() *config.Config { return h.cfg.Get() }

// CashierData 收银台模板数据。
type CashierData struct {
	Nonce              string
	ReviewAmounts      []string
	CancellationReview bool
	RefundRecorded     bool
	RefundRecheck      bool
	NotifyStatus       string
	SettlementPending  bool
	MerchantReview     bool
	Title              string
	Provider           string
	ProviderName       string
	TradeNo            string
	OutTradeNo         string
	Name               string
	Amount             string // 精确应付金额，如 10.01
	Currency           string
	RateUSD            string
	TransferURL        string
	PaymentQR          template.URL
	OriginMoney        string
	UID                string
	HasPayee           bool
	Note               string
	RemainSeconds      int
	TotalSeconds       int
	ReturnURL          string
	Status             string
	CreatedAt          string
	ExpiredAt          string
	Providers          []providerOption
	CurrencyOptions    []currencyOption
	CanReselect        bool
	NeedsProvider      bool
	NeedsCurrency      bool
	ProviderSubtitle   string
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
	if selectedProvider != config.ProviderBinance && selectedProvider != config.ProviderOKX && selectedProvider != config.ProviderBybit && selectedProvider != config.ProviderBitget && selectedProvider != config.ProviderTON {
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
	if needsProvider && !h.anyProviderAccepting(c, cfg, o) || !needsProvider && (!h.providerAccepting(c, cfg, provider) || !needsCurrency && !h.orderProviderAccepting(c, cfg, o)) {
		h.renderError(c, 503, "收款平台暂不可用", "当前平台账户、收款扫描或行情暂不可用。请稍后刷新重试，或返回商户页面选择其他付款方式，请勿继续转账。")
		return
	}
	currencyOptions := currencyOptionsFor(cfg, o, provider)
	if needsCurrency && len(currencyOptions) == 0 {
		h.renderError(c, 503, "收款平台暂不可用", "当前平台暂不支持此订单允许的付款币种。请返回选择其他收款平台，或联系商户处理。")
		return
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
		} else if provider == config.ProviderBybit {
			providerName = "Bybit 站内转账"
		} else if provider == config.ProviderBitget {
			providerName = "Bitget 站内转账"
		}
	} else if provider == config.ProviderBinance {
		providerName = "Binance 站内转账"
	} else if provider == config.ProviderOKX {
		providerName = "OKX 站内转账"
		uid = o.Receiver
	} else if provider == config.ProviderBybit {
		providerName = "Bybit 站内转账"
		uid = o.Receiver
	} else if provider == config.ProviderBitget {
		providerName = "Bitget 站内转账"
		uid = o.Receiver
	}
	if provider == config.ProviderTON {
		providerName = "TON 主网 · GRAM"
	}
	providerSubtitle := "先选择收款平台，再选择币种"
	if needsCurrency && provider == config.ProviderBinance {
		providerSubtitle = "选择 Binance 收款币种"
	} else if needsCurrency && provider == config.ProviderOKX {
		providerSubtitle = "选择 OKX 收款币种"
	} else if needsCurrency && provider == config.ProviderBybit {
		providerSubtitle = "选择 Bybit 收款币种"
	} else if needsCurrency && provider == config.ProviderBitget {
		providerSubtitle = "选择 Bitget 收款币种"
	} else if provider == config.ProviderBinance {
		providerSubtitle = "UID 站内转账（0 手续费）· 按精确金额自动确认"
	} else if provider == config.ProviderOKX {
		providerSubtitle = "UID 站内转账（0 手续费）· 按精确金额自动确认"
	} else if provider == config.ProviderBybit {
		providerSubtitle = "UID 站内转账（0 手续费）· 按精确金额自动确认"
	} else if provider == config.ProviderBitget {
		providerSubtitle = "UID 站内转账（0 手续费）· 按精确金额自动确认"
	}
	if provider == config.ProviderTON {
		providerSubtitle = "TON 主网链上转账 · 按精确金额自动确认"
	}
	hasPayee := uid != ""
	canReselect := cfg.BEpusdt.Reselect && o.Reselect &&
		(o.Status == model.Pending || o.Status == model.Ready) && provider != "" &&
		!reselectRequested && cfg.ProviderEnabled(provider)

	options := providerOptions(cfg, o)
	stats, _ := h.st.ProviderSuccessStats(c.Request.Context())
	for i := range options {
		options[i].Enabled = options[i].Enabled && h.providerAccepting(c, cfg, options[i].Key)
		stat := stats[options[i].Key]
		total := stat.Paid + stat.Failed
		if total > 0 {
			pct := math.Round(float64(stat.Paid) * 100 / float64(total))
			options[i].SuccessLabel = fmt.Sprintf("成功率 %.0f%%", pct)
			options[i].HasSuccessRate = true
		} else {
			options[i].SuccessLabel = "等待第一笔测试"
		}
	}

	transferURL := ""
	var paymentQR template.URL
	if provider == config.ProviderTON && uid != "" && !needsProvider && !needsCurrency {
		transferURL = "ton://transfer/" + uid + "?amount=" + strconv.FormatInt(o.PayUnits*1000, 10)
		png, err := qrcode.Encode(transferURL, qrcode.Medium, 480)
		if err != nil {
			h.renderError(c, http.StatusInternalServerError, "暂时无法生成收款二维码", "二维码生成失败，请刷新订单页面后重试。")
			return
		}
		// Only locally encoded PNG bytes enter this trusted data URL.
		paymentQR = template.URL("data:image/png;base64," + base64.StdEncoding.EncodeToString(png)) // #nosec G203 -- The fixed image/png data URL contains only locally encoded PNG bytes in Base64.
	}
	data := CashierData{
		RateUSD: strings.TrimRight(strings.TrimRight(o.RateUSD, "0"), "."), TransferURL: transferURL, PaymentQR: paymentQR,
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
		Providers:        providerOptionForQuery(options, selectedProvider),
		CurrencyOptions:  currencyOptions,
		CanReselect:      canReselect,
		NeedsProvider:    needsProvider,
		NeedsCurrency:    needsCurrency,
		ProviderSubtitle: providerSubtitle,
	}
	renderHTML(c, http.StatusOK, "cashier.html", data)
}

type providerOption struct {
	Key            string
	Name           string
	Subtitle       string
	Logo           string
	Enabled        bool
	Selected       bool
	SuccessLabel   string
	HasSuccessRate bool
	RegisterURL    string
}

type currencyOption struct {
	Currency string
	Icon     string
	Enabled  bool
	Selected bool
}

func internalTransferSubtitle(currencies []string) string {
	if len(currencies) == 0 {
		return "UID 站内转账（0 手续费）"
	}
	return "UID 站内转账（0 手续费）· " + strings.Join(currencies, " / ")
}

func providerOptions(cfg *config.Config, o *model.Order) []providerOption {
	all := map[string]providerOption{
		config.ProviderBinance: {
			Key: config.ProviderBinance, Name: "Binance", Logo: "/assets/brand/binance.svg",
			Subtitle:    internalTransferSubtitle(cfg.ProviderCurrencies(config.ProviderBinance)),
			Enabled:     cfg.BinanceEnabled() && orderCurrencyAllowed(cfg, o, cfg.ProviderCurrencies(config.ProviderBinance)...),
			Selected:    strings.EqualFold(o.Provider, config.ProviderBinance),
			RegisterURL: externalURL(cfg.Checkout.Binance.RegisterURL),
		},
		config.ProviderOKX: {
			Key: config.ProviderOKX, Name: "OKX", Logo: "/assets/brand/okx.svg",
			Subtitle:    internalTransferSubtitle(cfg.ProviderCurrencies(config.ProviderOKX)),
			Enabled:     cfg.OKXEnabled() && orderCurrencyAllowed(cfg, o, cfg.ProviderCurrencies(config.ProviderOKX)...),
			Selected:    strings.EqualFold(o.Provider, config.ProviderOKX),
			RegisterURL: externalURL(cfg.Checkout.OKX.RegisterURL),
		},
		config.ProviderBybit: {
			Key: config.ProviderBybit, Name: "Bybit", Logo: "/assets/brand/bybit.svg",
			Subtitle:    internalTransferSubtitle(cfg.ProviderCurrencies(config.ProviderBybit)),
			Enabled:     cfg.BybitEnabled() && orderCurrencyAllowed(cfg, o, cfg.ProviderCurrencies(config.ProviderBybit)...),
			Selected:    strings.EqualFold(o.Provider, config.ProviderBybit),
			RegisterURL: externalURL(cfg.Checkout.Bybit.RegisterURL),
		},
		config.ProviderBitget: {
			Key: config.ProviderBitget, Name: "Bitget", Logo: "/assets/brand/bitget.svg",
			Subtitle:    internalTransferSubtitle(cfg.ProviderCurrencies(config.ProviderBitget)),
			Enabled:     cfg.BitgetEnabled() && orderCurrencyAllowed(cfg, o, cfg.ProviderCurrencies(config.ProviderBitget)...),
			Selected:    strings.EqualFold(o.Provider, config.ProviderBitget),
			RegisterURL: externalURL(cfg.Checkout.Bitget.RegisterURL),
		},
		config.ProviderTON: {
			Key: config.ProviderTON, Name: "TON / GRAM", Logo: "/assets/brand/gram.svg",
			Subtitle: "TON 主网转账 · GRAM · 实时 USD 报价",
			Enabled:  cfg.TONEnabled() && orderCurrencyAllowed(cfg, o, "GRAM"),
			Selected: o.Provider == config.ProviderTON,
		},
	}
	ordered := cfg.OrderedProviders()
	out := make([]providerOption, 0, len(ordered))
	for _, key := range ordered {
		if opt, ok := all[key]; ok {
			out = append(out, opt)
		}
	}
	return out
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
		if cfg.ProviderCurrency(provider, currency) && orderCurrencyAllowed(cfg, o, currency) {
			out = append(out, currencyOption{Currency: currency, Icon: currencyIcon(currency), Enabled: cfg.ProviderEnabled(provider), Selected: o.Currency == currency})
		}
	}
	return out
}

func currencyIcon(currency string) string {
	switch strings.ToUpper(strings.TrimSpace(currency)) {
	case "GRAM":
		return "/assets/brand/gram.svg"
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
		h.renderError(c, 404, "未找到订单", "该订单不存在或链接有误，请返回商户页面核对后重试。")
		return
	}
	if o.Status == model.Paid || o.Status == model.Expired || o.Status == model.Canceled || time.Now().After(o.ExpiresAt) {
		h.renderResult(c, o)
		return
	}
	if h.paymentOperationsBlocked(c) {
		h.renderError(c, 503, "收款服务暂不可用", "商城取消同步异常或恢复核对尚未完成，请稍后刷新页面重试，请勿继续转账。")
		return
	}
	h.renderCashier(c, o)
}
func (h *Handlers) SelectPayment(c *gin.Context) {
	if h.paymentOperationsBlocked(c) {
		c.JSON(503, gin.H{"code": 1, "msg": "商城取消同步异常或恢复核对未完成，暂不接受付款，请稍后重试"})
		return
	}
	h.cfg.WithPaymentConfig(func() {
		cfg := h.C()
		no := strings.TrimSpace(c.PostForm("trade_no"))
		provider := strings.ToLower(strings.TrimSpace(c.PostForm("provider")))
		currency := strings.ToUpper(strings.TrimSpace(c.PostForm("currency")))
		o, err := h.st.GetByTradeNo(c.Request.Context(), no)
		if err != nil || !cfg.ProviderEnabled(provider) || !cfg.ProviderCurrency(provider, currency) || !currencyAllowedByLimit(o.CurrencyLimit, currency) {
			c.JSON(400, gin.H{"code": 1, "msg": "当前付款方式不可用，请刷新页面后重新选择"})
			return
		}
		if o.Provider != "" && (!cfg.BEpusdt.Reselect || !o.Reselect) && !(o.Provider == provider && o.Currency == currency) {
			c.JSON(400, gin.H{"code": 1, "msg": "订单不允许更换付款方式"})
			return
		}
		if state := h.providerState(c, cfg, provider); !state.Ready {
			c.JSON(503, gin.H{"code": 1, "msg": state.Reason + "，暂不接受付款，请稍后重试或更换付款方式"})
			return
		}
		receiver := cfg.Checkout.Binance.UID
		if provider == config.ProviderOKX {
			receiver = cfg.Checkout.OKX.UID
		}
		if provider == config.ProviderBybit {
			receiver = cfg.Checkout.Bybit.UID
		}
		if provider == config.ProviderBitget {
			receiver = cfg.Checkout.Bitget.UID
		}
		step, e := amount.Parse(cfg.Order.AmountStep)
		minimum, me := amount.Parse(cfg.Order.MinAmount)
		if e != nil || me != nil {
			c.JSON(500, gin.H{"code": 1, "msg": "系统金额配置无效，请联系管理员检查订单规则设置"})
			return
		}
		if provider == config.ProviderTON {
			if h.poller == nil {
				c.JSON(503, gin.H{"code": 1, "msg": "GRAM 行情检测器尚未启动，请稍后重试"})
				return
			}
			receiver = cfg.TON.Address
			_, err = h.st.SelectGRAM(c.Request.Context(), no, receiver, h.poller.GRAM.Snapshot(), cfg.Order.SlotGraceSeconds)
		} else {
			_, err = h.st.Select(c.Request.Context(), no, provider, currency, receiver, cfg.Order.AmountPrecision, step, minimum, cfg.Order.MaxOffset, cfg.Order.SlotGraceSeconds)
		}
		if err != nil {
			status := 400
			if errors.Is(err, store.ErrSelectionThrottled) {
				status = 429
				c.Header("Retry-After", "2")
			}
			c.JSON(status, gin.H{"code": 1, "msg": err.Error()})
			return
		}
		c.JSON(200, gin.H{"code": 0})
	})
}
func (h *Handlers) renderResult(c *gin.Context, o *model.Order) {
	var cb model.Callback
	ack := "PENDING"
	if err := h.st.DB().WithContext(c.Request.Context()).First(&cb, "order_id = ?", o.ID).Error; err == nil && cb.DeliveredAt != nil {
		ack = "SUCCESS"
	}
	review, reviewErr := h.st.CancellationReviewSummary(c.Request.Context(), o.ID)
	if reviewErr != nil {
		h.renderError(c, 503, "暂时无法读取到账核对记录", "请稍后刷新页面重试，或联系商户客服处理。")
		return
	}
	var receipts []model.Transaction
	var reviewAmounts []string
	if review.Present() {
		if err := h.st.DB().WithContext(c.Request.Context()).Table("transactions t").Select("t.*").Joins("JOIN cancellation_reviews r ON r.provider=t.provider AND r.event_id=t.event_id").Where("r.order_id=? AND t.state=?", o.ID, model.Confirmed).Order("r.id").Find(&receipts).Error; err != nil {
			h.renderError(c, 503, "暂时无法读取到账核对记录", "请稍后刷新页面重试，或联系商户客服处理。")
			return
		}
		for _, receipt := range receipts {
			reviewAmounts = append(reviewAmounts, amount.String(receipt.Units)+" "+receipt.Currency)
		}
	}

	payAmount := o.PayAmount()
	if o.PayUnits <= 0 {
		payAmount = ""
	}
	renderHTML(c, 200, "result.html", CashierData{ReviewAmounts: reviewAmounts, CancellationReview: review.Present(), RefundRecorded: review.Current(), RefundRecheck: review.Status() == "recheck_required", Title: "付款结果", TradeNo: o.TradeNo, OutTradeNo: o.OutTradeNo, Name: o.Name, Amount: payAmount, Currency: o.Currency, OriginMoney: o.Original(), ReturnURL: o.ReturnURL, Status: o.Status, SettlementPending: o.PaymentPhase(time.Now()) == "settling", MerchantReview: cb.State == "merchant_review", CreatedAt: o.CreatedAt.Format("2006-01-02 15:04:05"), Provider: o.Provider, NotifyStatus: ack})
}
func (h *Handlers) renderError(c *gin.Context, status int, title, detail string) {
	renderHTML(c, status, "error.html", gin.H{"Title": title, "Detail": detail})
}
func (h *Handlers) OrderStatus(c *gin.Context) {
	o, err := h.st.GetByTradeNo(c.Request.Context(), c.Query("order_id"))
	if err != nil {
		c.JSON(404, gin.H{"code": 1, "msg": "未找到订单，请返回商户页面核对"})
		return
	}
	var cb model.Callback
	ack := false
	if err = h.st.DB().WithContext(c.Request.Context()).First(&cb, "order_id = ?", o.ID).Error; err == nil {
		ack = cb.DeliveredAt != nil
	}
	review, err := h.st.CancellationReviewSummary(c.Request.Context(), o.ID)
	if err != nil {
		c.JSON(503, gin.H{"code": 1, "msg": "暂时无法读取到账核对记录，请稍后重试"})
		return
	}
	phase := o.PaymentPhase(time.Now())
	accepting := !h.paymentOperationsBlocked(c) && time.Now().Before(o.ExpiresAt) && (o.Status == model.Pending || o.Status == model.Ready) && h.orderProviderAccepting(c, h.C(), o)
	if !accepting && (o.Status == model.Pending || o.Status == model.Ready) {
		phase = "settling"
	}
	payAmount := o.PayAmount()
	if o.PayUnits <= 0 {
		payAmount = ""
	}
	c.JSON(200, gin.H{"code": 0, "data": gin.H{"status": o.Status, "cancellation_review_status": review.Status(), "refund_review_generation_current": review.Current(), "amount": payAmount, "currency": o.Currency, "provider": o.Provider, "remain_seconds": max(0, int(time.Until(o.ExpiresAt).Seconds())), "return_url": externalURL(o.ReturnURL), "merchant_confirmed": ack, "payment_phase": phase, "accepting_payment": accepting, "merchant_review_required": cb.State == "merchant_review"}})
}
func (h *Handlers) Health(c *gin.Context) {
	if err := h.st.Ping(c.Request.Context()); err != nil {
		c.JSON(503, gin.H{"ok": false})
		return
	}
	c.JSON(200, gin.H{"ok": true})
}

// Ready uses the same provider admission policy as new payment instructions.
// Detailed providers, degraded flag and failure reasons are restricted to authenticated
// administrators or callers presenting the read-only readiness probe token.
func (h *Handlers) Ready(c *gin.Context) {
	ctx := c.Request.Context()
	cfg := h.C()
	detailed := h.canReadReadinessDetails(c)
	respondUnready := func(reason string, providers []providerHealth) {
		if !detailed {
			c.JSON(503, gin.H{"ready": false})
			return
		}
		body := gin.H{"ready": false, "reason": reason}
		if providers != nil {
			body["providers"] = providers
		}
		c.JSON(503, body)
	}
	if err := h.st.Ping(ctx); err != nil {
		respondUnready("数据库暂不可用", nil)
		return
	}
	if cfg.Server.CancelSyncRequired {
		if h.paymentOperationsBlocked(c) {
			respondUnready("商城取消同步异常或心跳过期", nil)
			return
		}
	} else if health, err := h.st.IntegrationHealth(ctx); err == nil && health.Conflicts > 0 {
		respondUnready("商城取消同步存在未解决的已支付冲突", nil)
		return
	}

	states := h.providerHealths(c)
	available, failed := 0, 0
	for _, state := range states {
		if state.Ready {
			available++
		} else if state.Configured {
			failed++
		}
	}
	if available == 0 {
		respondUnready("无可用收款平台", states)
		return
	}
	if !detailed {
		c.JSON(200, gin.H{"ready": true})
		return
	}
	c.JSON(200, gin.H{"ok": true, "ready": true, "degraded": failed > 0, "providers": states})
}
func (h *Handlers) Index(c *gin.Context) {
	if target := externalURL(h.C().Server.StoreURL); target != "" {
		c.Redirect(302, target)
		return
	}
	c.Status(404)
}
func generateTradeNo() (string, error) {
	var b [20]byte
	if _, err := rand.Read(b[:]); err != nil {
		return "", fmt.Errorf("生成订单号失败: %w", err)
	}
	return hex.EncodeToString(b[:]), nil
}

func (h *Handlers) GRAMStatus(c *gin.Context) {
	if h.poller == nil {
		c.JSON(503, gin.H{"code": 1, "msg": "GRAM 状态检测器尚未启动"})
		return
	}
	s := h.poller.GRAM.Snapshot()
	if s.Address != h.C().TON.Address {
		s.Balance = ""
		s.BalanceUSD = ""
		s.BalanceFresh = false
		s.BalanceAt = time.Time{}
		s.BalanceError = "等待检测新地址余额"
		s.Address = h.C().TON.Address
	}
	c.JSON(200, gin.H{"code": 0, "data": s, "price_interval_seconds": int(gram.PriceInterval / time.Second), "balance_interval_seconds": int(gram.BalanceInterval / time.Second)})
}
