package server

import (
	"crypto/hmac"
	"crypto/pbkdf2"
	"crypto/rand"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"golang.org/x/crypto/bcrypt"
	"log/slog"
	"math"
	"net/http"
	"net/url"
	"path"
	"strconv"
	"strings"
	"sync"
	"time"
	"unicode/utf8"

	"github.com/gin-gonic/gin"

	"litepay/internal/amount"
	"litepay/internal/config"
	"litepay/internal/model"
	"litepay/internal/settings"
	"litepay/internal/store"
)

func (h *Handlers) currentAdmin() *adminAuth {
	h.adminMu.Lock()
	defer h.adminMu.Unlock()
	cfg := h.C()
	if h.admin == nil || h.admin.user != cfg.Admin.Username || h.admin.pass != cfg.Admin.Password {
		h.admin = newAdminAuth(cfg)
	}
	return h.admin
}

func formatOrderAmount(value float64, precision int) string {
	if value <= 0 {
		return "—"
	}
	return amount.FormatPrecision(value, precision)
}

// -----------------------------------------------------------------------------
// 会话与鉴权
// -----------------------------------------------------------------------------

const adminCookieName = "lp_admin"

const (
	adminSessionSalt       = "litepay-admin-session-v1"
	adminSessionIterations = 210000
	adminSessionKeyBytes   = 32
	adminOrdersMaxPage     = 100000
	adminOrdersMaxKeyword  = 256
)

// adminSession 会话载荷（Base64(JSON) + "." + HMAC 签名）。
type adminSession struct {
	User string `json:"u"`
	Exp  int64  `json:"e"`
	CSRF string `json:"c"`
}

type adminAuth struct {
	user string
	pass string
	key  []byte
	ttl  time.Duration

	mu       sync.Mutex
	attempts map[string]*loginAttempt
}

type loginAttempt struct {
	count    int // reserved attempts, including requests still in flight
	inflight int
	until    time.Time
}

func newAdminAuth(cfg *config.Config) *adminAuth {
	// PBKDF2 降低密码泄漏后离线猜测会话签名密钥的速度。修改账号或密码
	// 会派生不同密钥，所有旧会话随之失效。
	key, err := pbkdf2.Key(sha256.New, cfg.Admin.Username+"\x00"+cfg.Admin.Password,
		[]byte(adminSessionSalt), adminSessionIterations, adminSessionKeyBytes)
	if err != nil {
		panic(fmt.Sprintf("派生后台会话密钥失败: %v", err))
	}
	ttl := time.Duration(cfg.Admin.SessionHours) * time.Hour
	if ttl <= 0 {
		ttl = 12 * time.Hour
	}
	return &adminAuth{
		user:     cfg.Admin.Username,
		pass:     cfg.Admin.Password,
		key:      key,
		ttl:      ttl,
		attempts: make(map[string]*loginAttempt),
	}
}

// issue 生成新会话 token 与 CSRF token。
func (a *adminAuth) issue() (token, csrf string, err error) {
	buf := make([]byte, 16)
	if _, err := rand.Read(buf); err != nil {
		return "", "", fmt.Errorf("生成后台会话随机数失败: %w", err)
	}
	csrf = hex.EncodeToString(buf)

	s := adminSession{User: a.user, Exp: time.Now().Add(a.ttl).Unix(), CSRF: csrf}
	payload, err := json.Marshal(s)
	if err != nil {
		return "", "", fmt.Errorf("生成后台会话失败: %w", err)
	}
	raw := base64.RawURLEncoding.EncodeToString(payload)

	mac := hmac.New(sha256.New, a.key)
	mac.Write([]byte(raw))
	return raw + "." + hex.EncodeToString(mac.Sum(nil)), csrf, nil
}

// parse 校验并解析会话。
func (a *adminAuth) parse(token string) (*adminSession, bool) {
	raw, sig, ok := strings.Cut(token, ".")
	if !ok || raw == "" || sig == "" {
		return nil, false
	}
	mac := hmac.New(sha256.New, a.key)
	mac.Write([]byte(raw))
	want := hex.EncodeToString(mac.Sum(nil))
	if subtle.ConstantTimeCompare([]byte(sig), []byte(want)) != 1 {
		return nil, false
	}
	payload, err := base64.RawURLEncoding.DecodeString(raw)
	if err != nil {
		return nil, false
	}
	var s adminSession
	if err := json.Unmarshal(payload, &s); err != nil {
		return nil, false
	}
	if time.Now().Unix() > s.Exp {
		return nil, false
	}
	return &s, true
}

// checkCredentials 校验账号密码（常量时间比较）。
func (a *adminAuth) checkCredentials(user, pass string) bool {
	uOK := subtle.ConstantTimeCompare([]byte(user), []byte(a.user)) == 1
	pOK := subtle.ConstantTimeCompare([]byte(pass), []byte(a.pass)) == 1
	if strings.HasPrefix(a.pass, "$2a$") || strings.HasPrefix(a.pass, "$2b$") {
		pOK = bcrypt.CompareHashAndPassword([]byte(a.pass), []byte(pass)) == nil
	}
	return uOK && pOK
}

// allow 同一 IP 10 分钟内最多 6 次登录尝试，预先计数以限制并发请求。
func (a *adminAuth) allow(ip string) bool {
	a.mu.Lock()
	defer a.mu.Unlock()

	now := time.Now()
	for key, attempt := range a.attempts {
		if now.After(attempt.until) {
			delete(a.attempts, key)
		}
	}
	if len(a.attempts) >= 10000 {
		if _, exists := a.attempts[ip]; !exists {
			return false
		}
	}
	at, ok := a.attempts[ip]
	if !ok || now.After(at.until) {
		a.attempts[ip] = &loginAttempt{count: 1, inflight: 1, until: now.Add(10 * time.Minute)}
		return true
	}
	if at.count >= 6 {
		return false
	}
	at.count++
	at.inflight++
	return true
}

// succeeded 清除已成功登录 IP 的失败计数，同时保留其他并发请求的预留。
// failed 保留本次失败尝试，直到 10 分钟窗口结束。
func (a *adminAuth) succeeded(ip string) {
	a.mu.Lock()
	defer a.mu.Unlock()
	at, ok := a.attempts[ip]
	if !ok {
		return
	}
	if at.inflight > 0 {
		at.inflight--
	}
	// A successful login clears earlier failures, but requests that are
	// already in flight remain counted so they cannot be used to bypass the
	// limit during the same window.
	at.count = at.inflight
	if at.count == 0 {
		delete(a.attempts, ip)
	}
}

func (a *adminAuth) failed(ip string) {
	a.mu.Lock()
	defer a.mu.Unlock()
	if at, ok := a.attempts[ip]; ok && at.inflight > 0 {
		at.inflight--
	}
}

// AdminAuth 后台鉴权中间件。
func (h *Handlers) AdminAuth() gin.HandlerFunc {
	return func(c *gin.Context) {
		c.Header("X-Frame-Options", "DENY")
		c.Header("Cache-Control", "no-store")

		cookie, err := c.Cookie(adminCookieName)
		if err != nil {
			h.redirectLogin(c)
			return
		}
		sess, ok := h.currentAdmin().parse(cookie)
		if !ok {
			h.redirectLogin(c)
			return
		}

		// CSRF：所有写操作必须携带会话内的 token
		if c.Request.Method == http.MethodPost {
			token := c.PostForm("csrf")
			if token == "" {
				token = c.GetHeader("X-CSRF-Token")
			}
			if subtle.ConstantTimeCompare([]byte(token), []byte(sess.CSRF)) != 1 {
				c.Abort()
				c.String(http.StatusForbidden, "CSRF 校验失败，请刷新页面后重试")
				return
			}
		}

		c.Set("admin_user", sess.User)
		c.Set("admin_csrf", sess.CSRF)
		c.Next()
	}
}

func (h *Handlers) redirectLogin(c *gin.Context) {
	next := c.Request.URL.RequestURI()
	c.Redirect(http.StatusSeeOther, h.adminURL("/admin/login?next=")+url.QueryEscape(next))
	c.Abort()
}

// -----------------------------------------------------------------------------
// 页面数据结构
// -----------------------------------------------------------------------------

// AdminBase 所有后台页面共用的头部数据。
type AdminBase struct {
	Active  string
	User    string
	CSRF    string
	Flash   string
	FlashOK bool
	Title   string
}

func (h *Handlers) adminBase(c *gin.Context, active, title string) AdminBase {
	flash := c.Query("msg")
	return AdminBase{
		Active:  active,
		Title:   title,
		User:    c.GetString("admin_user"),
		CSRF:    c.GetString("admin_csrf"),
		Flash:   flash,
		FlashOK: c.Query("t") != "err",
	}
}

// -----------------------------------------------------------------------------
// 登录 / 退出
// -----------------------------------------------------------------------------

// AdminLoginPage GET /admin/login
func (h *Handlers) AdminLoginPage(c *gin.Context) {
	if cookie, err := c.Cookie(adminCookieName); err == nil {
		if _, ok := h.currentAdmin().parse(cookie); ok {
			c.Redirect(http.StatusSeeOther, h.adminURL("/admin"))
			return
		}
	}
	c.Header("Cache-Control", "no-store")
	c.HTML(http.StatusOK, "admin_login.html", gin.H{
		"Next":     c.Query("next"),
		"Error":    c.Query("err"),
		"SiteName": "LitePay 管理后台",
	})
}

// AdminLoginSubmit POST /admin/login
func (h *Handlers) AdminLoginSubmit(c *gin.Context) {
	auth := h.currentAdmin()
	ip := c.ClientIP()
	if !auth.allow(ip) {
		c.Redirect(http.StatusSeeOther, h.adminURL("/admin/login?err=")+url.QueryEscape("尝试过于频繁，请 10 分钟后再试"))
		return
	}

	user := strings.TrimSpace(c.PostForm("username"))
	pass := c.PostForm("password")
	next := c.PostForm("next")

	if !auth.checkCredentials(user, pass) {
		// Attempts are reserved atomically in allow, including concurrent requests.
		auth.failed(ip)
		c.Redirect(http.StatusSeeOther, h.adminURL("/admin/login?err=")+url.QueryEscape("账号或密码错误"))
		return
	}
	auth.succeeded(ip)

	token, _, err := auth.issue()
	if err != nil {
		slog.Error("创建后台会话失败", "err", err)
		c.Redirect(http.StatusSeeOther, h.adminURL("/admin/login?err=")+url.QueryEscape("登录服务暂时不可用，请稍后重试"))
		return
	}
	c.SetSameSite(http.SameSiteStrictMode)
	c.SetCookie(adminCookieName, token, int(auth.ttl.Seconds()), h.C().AdminPath(), "", strings.HasPrefix(h.C().Server.BaseURL, "https://"), true)

	if !safeAdminNext(next, h.C().AdminPath()) {
		next = h.adminURL("/admin")
	}
	c.Redirect(http.StatusSeeOther, next)
}

// AdminLogout POST /admin/logout
func (h *Handlers) AdminLogout(c *gin.Context) {
	c.SetSameSite(http.SameSiteStrictMode)
	// Remove legacy root-scoped sessions as well as the current mount cookie.
	c.SetCookie(adminCookieName, "", -1, "/", "", strings.HasPrefix(h.C().Server.BaseURL, "https://"), true)
	c.SetCookie(adminCookieName, "", -1, h.C().AdminPath(), "", strings.HasPrefix(h.C().Server.BaseURL, "https://"), true)
	c.Redirect(http.StatusSeeOther, h.adminURL("/admin/login"))
}

// -----------------------------------------------------------------------------
// 概览
// -----------------------------------------------------------------------------

// AdminDashboard GET /admin
func (h *Handlers) AdminDashboard(c *gin.Context) {
	ctx := c.Request.Context()
	now := time.Now()
	midnight := time.Date(now.Year(), now.Month(), now.Day(), 0, 0, 0, 0, now.Location())

	today, err := h.st.StatsSince(ctx, midnight)
	if err != nil {
		h.renderAdminError(c, err)
		return
	}
	all, _ := h.st.StatsSince(ctx, time.Unix(0, 0))
	counts, _ := h.st.CountByStatus(ctx)
	notifyFail, _ := h.st.NotifyFailureCount(ctx)
	expireNotifyFail, _ := h.st.ExpiredNotifyFailureCount(ctx)
	recent, _ := h.st.RecentOrders(ctx, 8)
	volumes, _ := h.st.VolumeByCurrency(ctx, midnight)
	ops, err := h.st.Operations(ctx)
	if err != nil {
		h.renderAdminError(c, err)
		return
	}
	observations, _ := h.st.RecentTxObservations(ctx, 8)
	observationCount, _ := h.st.CountOpenTxObservations(ctx)

	c.HTML(http.StatusOK, "admin_dashboard.html", gin.H{
		"Base":             h.adminBase(c, "dashboard", "概览"),
		"Today":            today,
		"Volumes":          volumes,
		"Currencies":       h.C().Checkout.Currencies,
		"All":              all,
		"Counts":           counts,
		"PendingCount":     counts[model.StatusPending] + counts[model.StatusReady],
		"NotifyFail":       notifyFail,
		"ExpireNotifyFail": expireNotifyFail,
		"Recent":           recent,
		"Operations":       ops,
		"Observations":     observations,
		"ObservationCount": observationCount,
		"Poll":             h.poller.StatsFor(config.ProviderBinance),
		"OKXPoll":          h.poller.StatsFor(config.ProviderOKX),
		"BinanceOn":        h.C().BinanceEnabled(),
		"OKXOn":            h.C().OKXEnabled(),
		"Currency":         h.C().DefaultCurrency(),
		"DBSize":           humanSize(h.st.DatabaseSize()),
		"Now":              now.Format("2006-01-02 15:04:05"),
		"ExpireMin":        h.C().Order.ExpireMinutes,
		"PollSec":          h.C().Binance.PollInterval,
		"AmountPrecision":  h.C().Order.AmountPrecision,
		"BaseURL":          h.C().Server.BaseURL,
		"PayeeReady":       h.C().BinanceEnabled() || h.C().OKXEnabled(),
	})
}

// -----------------------------------------------------------------------------
// 订单列表
// -----------------------------------------------------------------------------

// AdminOrders GET /admin/orders
func (h *Handlers) AdminOrders(c *gin.Context) {
	page := adminOrdersPage(c.Query("page"))
	status := adminOrderStatus(c.Query("status"))
	keyword := strings.TrimSpace(c.Query("q"))
	if len(keyword) > adminOrdersMaxKeyword {
		keyword = keyword[:adminOrdersMaxKeyword]
	}
	f := store.OrderFilter{
		Status:   status,
		Keyword:  keyword,
		Page:     page,
		PageSize: 20,
	}

	orders, total, err := h.st.ListOrders(c.Request.Context(), f)
	if err != nil {
		h.renderAdminError(c, err)
		return
	}

	totalPages := int((total + int64(f.PageSize) - 1) / int64(f.PageSize))
	if totalPages < 1 {
		totalPages = 1
	}

	c.HTML(http.StatusOK, "admin_orders.html", gin.H{
		"Base":            h.adminBase(c, "orders", "订单"),
		"Orders":          orders,
		"Total":           total,
		"Page":            f.Page,
		"TotalPages":      totalPages,
		"Status":          f.Status,
		"Keyword":         f.Keyword,
		"Currency":        h.C().DefaultCurrency(),
		"HasPrev":         f.Page > 1,
		"HasNext":         f.Page < totalPages,
		"PrevPage":        f.Page - 1,
		"NextPage":        f.Page + 1,
		"AmountPrecision": h.C().Order.AmountPrecision,
	})
}

func adminOrdersPage(raw string) int {
	page, err := strconv.Atoi(strings.TrimSpace(raw))
	if err != nil || page < 1 {
		return 1
	}
	if page > adminOrdersMaxPage {
		return adminOrdersMaxPage
	}
	return page
}

func adminOrderStatus(raw string) string {
	status := strings.ToUpper(strings.TrimSpace(raw))
	switch status {
	case model.StatusPending, model.StatusReady, model.StatusPaid, model.StatusExpired,
		model.StatusCanceled, model.StatusSuperseded:
		return status
	default:
		return ""
	}
}

// -----------------------------------------------------------------------------
// 订单详情
// -----------------------------------------------------------------------------

// AdminOrderDetail GET /admin/orders/:trade_no
func (h *Handlers) AdminOrderDetail(c *gin.Context) {
	tradeNo := c.Param("trade_no")
	o, err := h.st.GetByTradeNo(c.Request.Context(), tradeNo)
	if err != nil || o == nil {
		h.renderAdminError(c, fmt.Errorf("订单 %s 不存在", tradeNo))
		return
	}
	logs, _ := h.st.NotifyLogs(c.Request.Context(), o.ID, 30)
	canPay := o.PayAmount > 0 && !o.ExpireFinalized && (o.Status == model.StatusReady || o.Status == model.StatusExpired)
	canExpire := o.Status == model.StatusPending || o.Status == model.StatusReady

	c.HTML(http.StatusOK, "admin_order.html", gin.H{
		"Base":   h.adminBase(c, "orders", "订单详情"),
		"Order":  o,
		"Logs":   logs,
		"Amount": amount.FormatPrecision(o.PayAmount, o.AmountPrecision),
		"CanPay": canPay,
		"CanRetry": o.Status == model.StatusPaid ||
			(o.Status == model.StatusExpired && o.ExpireFinalized),
		"CanExpire": canExpire,
	})
}

// AdminOrderAction POST /admin/orders/:trade_no/action
func (h *Handlers) AdminOrderAction(c *gin.Context) {
	tradeNo := c.Param("trade_no")
	action := c.PostForm("action")
	back := h.adminURL("/admin/orders/") + tradeNo

	var err error
	var msg string

	switch action {
	case "pay":
		_, err = h.st.ReconcileObservation(c.Request.Context(), tradeNo, strings.TrimSpace(c.PostForm("transaction_id")), h.C().Order.AmountPrecision)
		msg = "已人工确认收款，支付结果通知即将发送"
	case "notify":
		err = h.st.ResetNotify(c.Request.Context(), tradeNo)
		msg = "已重置结果通知，系统稍后会自动重发"
	case "expire":
		err = h.st.ForceExpire(c.Request.Context(), tradeNo)
		msg = "收银台已立即结束；宽限期内仍会确认已发生的付款，随后发送过期通知"
	default:
		err = fmt.Errorf("无法识别该操作，请刷新页面后重试")
	}

	if err != nil {
		c.Redirect(http.StatusSeeOther, back+"?msg="+url.QueryEscape(err.Error())+"&t=err")
		return
	}
	c.Redirect(http.StatusSeeOther, back+"?msg="+url.QueryEscape(msg))
}

// -----------------------------------------------------------------------------
// 设置
// -----------------------------------------------------------------------------

// AdminSettings GET /admin/settings
func (h *Handlers) AdminSettings(c *gin.Context) {
	cfg := h.C()
	updated := h.cfg.UpdatedAt(c.Request.Context())

	c.HTML(http.StatusOK, "admin_settings.html", gin.H{
		"Base":            h.adminBase(c, "settings", "设置"),
		"HasSaved":        !updated.IsZero(),
		"UpdatedAt":       updated.Format("2006-01-02 15:04:05"),
		"Cfg":             cfg,
		"Currencies":      cfg.Checkout.Currencies,
		"KnownCurrencies": config.SupportedCurrencies,
		"DelaysJoined":    joinInts(cfg.Notify.RetryDelays),
		"BEpusdtKeyMask":  maskSecret(cfg.BEpusdt.APIToken),
		"BinanceKey":      maskSecret(cfg.Binance.APIKey),
		"BinanceOn":       cfg.BinanceEnabled(),
		"OKXOn":           cfg.OKXEnabled(),
		"CollectionOn":    cfg.BinanceEnabled() || cfg.OKXEnabled(),
		"Provider":        "multi",
		"OKXKey":          maskSecret(cfg.OKX.APIKey),
		"DBPath":          cfg.Database.Path,
		"DBSize":          humanSize(h.st.DatabaseSize()),
		"BaseURL":         cfg.Server.BaseURL,
	})
}

// AdminSettingsSave POST /admin/settings
func (h *Handlers) AdminSettingsSave(c *gin.Context) {
	p, errs := parseSettingsForm(c)
	if len(errs) > 0 {
		c.Redirect(http.StatusSeeOther, h.adminURL("/admin/settings?msg=")+url.QueryEscape(strings.Join(errs, "；"))+"&t=err")
		return
	}

	operator := c.GetString("admin_user")
	if err := h.cfg.Update(c.Request.Context(), p, operator); err != nil {
		c.Redirect(http.StatusSeeOther, h.adminURL("/admin/settings?msg=")+url.QueryEscape(err.Error())+"&t=err")
		return
	}

	slog.Info("后台配置已更新", "operator", operator)
	if p.VerifyAccounts {
		c.Redirect(http.StatusSeeOther, h.adminURL("/admin/settings?msg=")+url.QueryEscape("已完整配置的平台 API Key 与收款 UID 核验通过，配置已保存"))
		return
	}
	c.Redirect(http.StatusSeeOther, h.adminURL("/admin/settings?msg=")+url.QueryEscape("配置已保存并立即生效"))
}

// AdminAccountSave updates credentials after reauthentication.
func (h *Handlers) AdminAccountSave(c *gin.Context) {
	fail := func(msg string) {
		c.Redirect(http.StatusSeeOther, h.adminURL("/admin/settings?msg=")+url.QueryEscape(msg)+"&t=err")
	}
	auth := h.currentAdmin()
	ip := c.ClientIP()
	if !auth.allow(ip) {
		fail("尝试过于频繁，请 10 分钟后再试")
		return
	}
	if !auth.checkCredentials(c.GetString("admin_user"), c.PostForm("current_password")) {
		auth.failed(ip)
		fail("当前密码错误")
		return
	}
	auth.succeeded(ip)
	user := strings.TrimSpace(c.PostForm("admin_username"))
	pass := c.PostForm("new_password")
	if user == "" || utf8.RuneCountInString(user) > 64 {
		fail("用户名需为 1~64 个字符")
		return
	}
	p := settings.Patch{AdminUsername: &user}
	if pass != "" {
		if utf8.RuneCountInString(pass) < 6 || len(pass) > 72 {
			fail("新密码至少 6 个字符，且不超过 72 字节")
			return
		}
		if pass != c.PostForm("confirm_password") {
			fail("两次新密码不一致")
			return
		}
		hash, err := bcrypt.GenerateFromPassword([]byte(pass), bcrypt.DefaultCost)
		if err != nil {
			fail("密码处理失败")
			return
		}
		value := string(hash)
		p.AdminPasswordHash = &value
	}
	if err := h.cfg.Update(c.Request.Context(), p, auth.user); err != nil {
		fail(err.Error())
		return
	}
	h.AdminLogout(c)
}

// AdminRotateToken POST /admin/settings/token —— 服务端生成新 Token，
// 新值只在本次跳转的接入页展示，不在设置表单中回填。
func (h *Handlers) AdminRotateToken(c *gin.Context) {
	_, err := h.cfg.RotateAPIToken(c.Request.Context(), c.GetString("admin_user"))
	if err != nil {
		c.Redirect(http.StatusSeeOther,
			h.adminURL("/admin/integration?msg=")+url.QueryEscape("API Token 生成失败，请重试")+"&t=err")
		return
	}
	slog.Info("BEpusdt API Token 已轮换", "operator", c.GetString("admin_user"))
	c.Redirect(http.StatusSeeOther,
		h.adminURL("/admin/integration?msg=")+url.QueryEscape("API Token 已轮换；请立即更新独角数卡")+
			"&token_rotated=1")
}

// parseSettingsForm 解析设置表单，返回补丁与校验错误。
func parseSettingsForm(c *gin.Context) (settings.Patch, []string) {
	var p settings.Patch
	p.VerifyAccounts = c.PostForm("verify_accounts") == "on"
	var errs []string

	// A blank field keeps the current value unless its explicit clear checkbox
	// is selected. This avoids treating "I left it blank" as a destructive edit.
	editableString := func(key string) *string {
		v := strings.TrimSpace(c.PostForm(key))
		if v == "" && c.PostForm("clear_"+key) != "on" {
			return nil
		}
		return &v
	}
	optInt := func(key string, min, max int) *int {
		raw := strings.TrimSpace(c.PostForm(key))
		if raw == "" {
			return nil
		}
		v, err := strconv.Atoi(raw)
		if err != nil || v < min || v > max {
			errs = append(errs, fmt.Sprintf("%s 需为 %d~%d 的整数", key, min, max))
			return nil
		}
		return &v
	}
	optFloat := func(key, label string, min, max float64) *float64 {
		raw := strings.TrimSpace(c.PostForm(key))
		if raw == "" {
			return nil
		}
		v, err := strconv.ParseFloat(raw, 64)
		if err != nil || math.IsNaN(v) || math.IsInf(v, 0) || v < min || v > max {
			errs = append(errs, fmt.Sprintf("%s需为 %g~%g 之间的数字", label, min, max))
			return nil
		}
		return &v
	}
	reselect := c.PostForm("bepusdt_reselect") == "on"
	p.BEpusdtReselect = &reselect

	{
		curs := c.PostFormArray("checkout_currencies")
		p.CheckoutCurrencies = &curs
	}
	p.BinanceUID = editableString("binance_uid")
	p.CheckoutNote = editableString("checkout_note")

	p.OrderExpireMinutes = optInt("order_expire_minutes", 1, 1440)
	p.OrderAmountStep = optFloat("order_amount_step", "金额步长", 0.000001, 1)
	p.OrderMaxOffset = optInt("order_max_offset", 1, 900)
	p.OrderMinAmount = optFloat("order_min_amount", "最低金额", 0.0001, 1000)
	p.OrderSlotGrace = optInt("order_slot_grace", 0, 3600)
	p.OrderAmountPrecision = optInt("order_amount_precision", 2, 6)

	p.BinancePollInterval = optInt("binance_poll_interval", 2, 60)
	p.BinanceLookback = optInt("binance_lookback", 1, 60)
	p.BinanceAPIKey = editableString("binance_api_key")
	p.BinanceAPISecret = editableString("binance_api_secret")

	p.OKXAPIKey = editableString("okx_api_key")
	p.OKXAPISecret = editableString("okx_api_secret")
	p.OKXPassphrase = editableString("okx_passphrase")
	p.OKXUID = editableString("okx_uid")
	p.OKXPollInterval = optInt("okx_poll_interval", 2, 60)
	p.OKXLookback = optInt("okx_lookback", 1, 60)

	p.NotifyRetryDelays = func() *[]int {
		raw := strings.TrimSpace(c.PostForm("notify_retry_delays"))
		if raw == "" {
			return nil
		}
		parts := strings.FieldsFunc(raw, func(r rune) bool { return r == ',' || r == '，' || r == ' ' })
		out := make([]int, 0, len(parts))
		for _, v := range parts {
			n, err := strconv.Atoi(strings.TrimSpace(v))
			if err != nil || n < 0 || n > 3600 {
				errs = append(errs, "重试序列需为 0~3600 的整数，用逗号分隔")
				return nil
			}
			out = append(out, n)
		}
		return &out
	}()
	p.NotifyMaxAttempts = optInt("notify_max_attempts", 1, 10)
	p.NotifyTimeout = optInt("notify_timeout", 3, 60)

	return p, errs
}

func joinInts(v []int) string {
	parts := make([]string, len(v))
	for i, n := range v {
		parts[i] = strconv.Itoa(n)
	}
	return strings.Join(parts, ",")
}

// -----------------------------------------------------------------------------
// 接入信息
// -----------------------------------------------------------------------------

// AdminIntegration GET /admin/integration
func (h *Handlers) AdminIntegration(c *gin.Context) {
	cfg := h.C()
	base := cfg.Server.BaseURL
	key := cfg.BEpusdt.APIToken

	currencies := cfg.Checkout.Currencies
	if len(currencies) == 0 {
		currencies = []string{cfg.DefaultCurrency()}
	}

	c.HTML(http.StatusOK, "admin_integration.html", gin.H{
		"Base":       h.adminBase(c, "integration", "接入信息"),
		"BaseURL":    base,
		"SubmitURL":  base + "/api/v1/order/create-order",
		"Key":        key,
		"KeyMask":    maskSecret(cfg.BEpusdt.APIToken),
		"TokenFresh": c.Query("token_rotated") == "1",
		"Currencies": currencies,
		"Expire":     cfg.Order.ExpireMinutes,
	})
}

// -----------------------------------------------------------------------------
// 工具
// -----------------------------------------------------------------------------

func (h *Handlers) renderAdminError(c *gin.Context, err error) {
	c.HTML(http.StatusOK, "admin_error.html", gin.H{
		"Base":  h.adminBase(c, "orders", "操作失败"),
		"Error": err.Error(),
	})
}

func humanSize(n int64) string {
	switch {
	case n >= 1<<30:
		return fmt.Sprintf("%.1f GB", float64(n)/(1<<30))
	case n >= 1<<20:
		return fmt.Sprintf("%.1f MB", float64(n)/(1<<20))
	case n >= 1<<10:
		return fmt.Sprintf("%.1f KB", float64(n)/(1<<10))
	}
	return fmt.Sprintf("%d B", n)
}

func maskSecret(s string) string {
	if s == "" {
		return "（未配置）"
	}
	if len(s) <= 8 {
		return "********"
	}
	return s[:4] + "********" + s[len(s)-4:]
}

// adminURL maps internal destinations onto the configured mount point.
func (h *Handlers) adminURL(route string) string {
	return h.C().AdminPath() + strings.TrimPrefix(route, "/admin")
}

func safeAdminNext(raw, root string) bool {
	u, err := url.Parse(raw)
	if err != nil || u.IsAbs() || u.Host != "" || u.Opaque != "" || strings.ContainsAny(raw, "\\\r\n") || strings.ContainsAny(u.Path, "\\\r\n") || path.Clean(u.Path) != u.Path {
		return false
	}
	return u.Path == root || strings.HasPrefix(u.Path, root+"/")
}
