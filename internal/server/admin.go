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
	"log/slog"
	"net/http"
	"net/url"
	"path"
	"strconv"
	"strings"
	"sync"
	"time"
	"unicode/utf8"

	"github.com/gin-gonic/gin"
	"golang.org/x/crypto/bcrypt"

	"litepay/internal/amount"
	"litepay/internal/config"
	"litepay/internal/model"
	"litepay/internal/settings"
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

// -----------------------------------------------------------------------------
// 会话与鉴权
// -----------------------------------------------------------------------------

const adminCookieName = "lp_admin"

const (
	adminSessionSalt       = "litepay-admin-session-v1"
	adminSessionIterations = 210000
	adminSessionKeyBytes   = 32
	adminOrdersMaxPage     = 100000
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
	Active    string
	User      string
	CSRF      string
	Flash     string
	FlashOK   bool
	FlashWarn bool
	Title     string
}

func (h *Handlers) adminBase(c *gin.Context, active, title string) AdminBase {
	flash := c.Query("msg")
	return AdminBase{
		Active:    active,
		Title:     title,
		User:      c.GetString("admin_user"),
		CSRF:      c.GetString("admin_csrf"),
		Flash:     flash,
		FlashOK:   c.Query("t") != "err",
		FlashWarn: c.Query("t") == "warn",
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
	c.SetCookie(adminCookieName, "", -1, h.C().AdminPath(), "", strings.HasPrefix(h.C().Server.BaseURL, "https://"), true)
	c.Redirect(http.StatusSeeOther, h.adminURL("/admin/login"))
}

// -----------------------------------------------------------------------------
// 概览
// -----------------------------------------------------------------------------

// AdminSettings GET /admin/settings
func (h *Handlers) AdminSettings(c *gin.Context) {
	cfg := h.C()
	updated := h.cfg.UpdatedAt(c.Request.Context())

	c.HTML(http.StatusOK, "admin_settings.html", gin.H{
		"Base":               h.adminBase(c, "settings", "设置"),
		"HasSaved":           !updated.IsZero(),
		"UpdatedAt":          updated.Format("2006-01-02 15:04:05"),
		"Cfg":                cfg,
		"PendingNotifyCount": h.pendingNotifyCount(c),
		"CanRotateToken":     !cfg.Server.Production || cfg.Server.MerchantKeyVersions,
		"Currencies":         cfg.Checkout.Currencies,
		"KnownCurrencies":    []string{"USDT", "USDC"},
		"BinanceCurrencies":  cfg.ProviderCurrencies(config.ProviderBinance),
		"OKXCurrencies":      cfg.ProviderCurrencies(config.ProviderOKX),
		"TONCurrencies":      cfg.ProviderCurrencies(config.ProviderTON),
		"DelaysJoined":       joinInts(cfg.Notify.RetryDelays),
		"BEpusdtKeyMask":     maskSecret(cfg.BEpusdt.APIToken),
		"BinanceKey":         maskSecret(cfg.Binance.APIKey),
		"BinanceOn":          cfg.BinanceEnabled(),
		"OKXOn":              cfg.OKXEnabled(),
		"CollectionOn":       cfg.AnyProviderEnabled(),
		"Provider":           "multi",
		"OKXKey":             maskSecret(cfg.OKX.APIKey),
		"TONKey":             maskSecret(cfg.TON.APIKey),
		"DBPath":             cfg.Database.Path,

		"BaseURL": cfg.Server.BaseURL,
	})
}

// AdminSettingsSave POST /admin/settings
func (h *Handlers) AdminSettingsSave(c *gin.Context) {
	p, errs := parseSettingsForm(c)
	destination := h.adminURL("/admin/settings?msg=")
	scope := c.PostForm("settings_scope")
	for _, valid := range []string{"binance", "okx", "ton", "checkout", "orders", "merchant"} {
		if scope == valid {
			destination = h.adminURL("/admin/settings?view=" + scope + "&msg=")
			break
		}
	}
	if len(errs) > 0 {
		c.Redirect(http.StatusSeeOther, destination+url.QueryEscape(strings.Join(errs, "；"))+"&t=err")
		return
	}

	operator := c.GetString("admin_user")
	checks, err := h.cfg.UpdateWithVerification(c.Request.Context(), p, operator)
	if err != nil {
		c.Redirect(http.StatusSeeOther, destination+url.QueryEscape(err.Error())+"&t=err")
		return
	}

	slog.Info("后台配置已更新", "operator", operator)
	if p.VerifyAccounts {
		message, partial := verificationMessage(checks)
		suffix := ""
		if partial {
			suffix = "&t=warn"
		}
		c.Redirect(http.StatusSeeOther, destination+url.QueryEscape(message)+suffix)
		return
	}
	c.Redirect(http.StatusSeeOther, destination+url.QueryEscape("配置已保存并立即生效"))
}

func verificationMessage(checks []settings.AccountVerification) (string, bool) {
	names := map[string]string{config.ProviderBinance: "Binance", config.ProviderOKX: "OKX", config.ProviderTON: "GRAM 余额接口"}
	parts := []string{"配置已保存"}
	partial := false
	for _, check := range checks {
		name := names[check.Provider]
		if check.Verified {
			parts = append(parts, name+" 核验通过")
			continue
		}
		partial = true
		detail := " 未核验"
		if len(check.Missing) > 0 {
			detail += "，需配置：" + strings.Join(check.Missing, "、")
		}
		parts = append(parts, name+detail)
	}
	return strings.Join(parts, "；"), partial
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

// AdminSiteSave changes the public redirect and admin entry after reauthentication.
func (h *Handlers) AdminSiteSave(c *gin.Context) {
	fail := func(msg string) {
		c.Redirect(http.StatusSeeOther, h.adminURL("/admin/settings?view=account&msg=")+url.QueryEscape(msg)+"&t=err")
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
	root := strings.TrimSpace(c.PostForm("admin_path"))
	storeURL := strings.TrimSpace(c.PostForm("store_url"))
	if root == "" {
		fail("请填写后台入口，以 / 开头")
		return
	}
	oldRoot := h.C().AdminPath()
	if err := h.cfg.Update(c.Request.Context(), settings.Patch{AdminPath: &root, StoreURL: &storeURL}, auth.user); err != nil {
		fail(err.Error())
		return
	}
	c.SetSameSite(http.SameSiteStrictMode)
	c.SetCookie(adminCookieName, "", -1, oldRoot, "", strings.HasPrefix(h.C().Server.BaseURL, "https://"), true)
	h.AdminLogout(c)
}

// AdminRotateToken POST /admin/settings/token —— 服务端生成新 Token，
// 新值只在本次跳转的接入页展示，不在设置表单中回填。
func (h *Handlers) AdminRotateToken(c *gin.Context) {
	_, err := h.cfg.RotateAPIToken(c.Request.Context(), c.GetString("admin_user"))
	if err != nil {
		c.Redirect(http.StatusSeeOther,
			h.adminURL("/admin/integration?msg=")+url.QueryEscape(err.Error())+"&t=err")
		return
	}
	slog.Info("BEpusdt API Token 已轮换", "operator", c.GetString("admin_user"))
	c.Redirect(http.StatusSeeOther,
		h.adminURL("/admin/integration?msg=")+url.QueryEscape("API Token 已轮换；历史订单通知保留原密钥版本，商城须兼容旧版本。请更新新订单使用的 Token")+
			"&token_rotated=1")
}

// parseSettingsForm 解析设置表单，返回补丁与校验错误。
func parseSettingsForm(c *gin.Context) (settings.Patch, []string) {
	scope := c.PostForm("settings_scope")
	p := settings.Patch{}
	var errs []string
	text := func(key string, secret bool) *string {
		raw, exists := c.GetPostForm(key)
		if !exists {
			return nil
		}
		raw = strings.TrimSpace(raw)
		if secret && raw == "" {
			return nil
		}
		return &raw
	}
	integer := func(key string, low, high int) *int {
		raw := strings.TrimSpace(c.PostForm(key))
		if raw == "" {
			return nil
		}
		n, err := strconv.Atoi(raw)
		if err != nil || n < low || n > high {
			errs = append(errs, fmt.Sprintf("%s 需为 %d~%d 的整数", key, low, high))
			return nil
		}
		return &n
	}
	money := func(key string) *string {
		raw := strings.TrimSpace(c.PostForm(key))
		if raw == "" {
			return nil
		}
		if _, err := amount.Parse(raw); err != nil {
			errs = append(errs, key+" 金额无效")
			return nil
		}
		return &raw
	}
	currencies := func(key string) *[]string {
		v := c.PostFormArray(key)
		if v == nil {
			v = []string{}
		}
		return &v
	}
	clear := c.PostForm("clear_section") == "on"
	empty := ""
	none := []string{}
	defaults := config.Default()
	switch scope {
	case config.ProviderBinance:
		p.BinanceUID = text("binance_uid", false)
		p.BinanceAPIKey = text("binance_api_key", true)
		p.BinanceAPISecret = text("binance_api_secret", true)
		p.BinanceCurrencies = currencies("binance_currencies")
		p.BinancePollInterval = integer("binance_poll_interval", 2, 60)
		p.BinanceLookback = integer("binance_lookback", 1, 60)
		if clear {
			p.BinanceUID = &empty
			p.BinanceAPIKey = &empty
			p.BinanceAPISecret = &empty
			p.BinanceCurrencies = &none
			p.BinancePollInterval = &defaults.Binance.PollInterval
			p.BinanceLookback = &defaults.Binance.LookbackBuffer
			errs = nil
		}
	case config.ProviderOKX:
		p.OKXUID = text("okx_uid", false)
		p.OKXAPIKey = text("okx_api_key", true)
		p.OKXAPISecret = text("okx_api_secret", true)
		p.OKXPassphrase = text("okx_passphrase", true)
		p.OKXCurrencies = currencies("okx_currencies")
		p.OKXPollInterval = integer("okx_poll_interval", 2, 60)
		p.OKXLookback = integer("okx_lookback", 1, 60)
		if clear {
			p.OKXUID = &empty
			p.OKXAPIKey = &empty
			p.OKXAPISecret = &empty
			p.OKXPassphrase = &empty
			p.OKXCurrencies = &none
			p.OKXPollInterval = &defaults.OKX.PollInterval
			p.OKXLookback = &defaults.OKX.LookbackBuffer
			errs = nil
		}
	case config.ProviderTON:
		p.TONAddress = text("ton_address", false)
		p.TONAPIKey = text("ton_api_key", true)
		p.TONCurrencies = currencies("ton_currencies")
		p.TONPollInterval = integer("ton_poll_interval", 2, 60)
		if clear {
			p.TONAddress = &empty
			p.TONAPIKey = &empty
			p.TONCurrencies = &none
			p.TONPollInterval = &defaults.TON.PollInterval
			errs = nil
		}
	case "checkout":
		p.CheckoutNote = text("checkout_note", false)
	case "orders":
		p.OrderExpireMinutes = integer("order_expire_minutes", 1, 1440)
		p.OrderAmountStep = money("order_amount_step")
		p.OrderMaxOffset = integer("order_max_offset", 1, 900)
		p.OrderMinAmount = money("order_min_amount")
		p.OrderSlotGrace = integer("order_slot_grace", 0, 3600)
		p.OrderAmountPrecision = integer("order_amount_precision", 2, 6)
	case "merchant":
		value := c.PostForm("bepusdt_reselect") == "on"
		p.BEpusdtReselect = &value
		raw := strings.TrimSpace(c.PostForm("notify_retry_delays"))
		if raw != "" {
			var delays []int
			for _, part := range strings.FieldsFunc(raw, func(r rune) bool { return r == ',' || r == '，' || r == ' ' }) {
				n, err := strconv.Atoi(part)
				if err != nil || n < 0 || n > 3600 {
					errs = append(errs, "重试间隔需为 0~3600 的整数，用逗号分隔")
					break
				}
				delays = append(delays, n)
			}
			p.NotifyRetryDelays = &delays
		}
		p.NotifyMaxAttempts = integer("notify_max_attempts", 1, 10)
		p.NotifyTimeout = integer("notify_timeout", 3, 60)
	default:
		errs = append(errs, "设置分区无效，请刷新设置页后重试")
	}
	if scope == config.ProviderBinance || scope == config.ProviderOKX || scope == config.ProviderTON {
		p.VerifyProvider = scope
		p.VerifyAccounts = !clear && c.PostForm("verify_accounts") == "on"
	}
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

// -----------------------------------------------------------------------------
// 工具
// -----------------------------------------------------------------------------

func (h *Handlers) renderAdminError(c *gin.Context, err error) {
	c.HTML(http.StatusOK, "admin_error.html", gin.H{
		"Base":  h.adminBase(c, "orders", "操作失败"),
		"Error": err.Error(),
	})
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

func (h *Handlers) AdminDashboard(c *gin.Context) {
	queues, err := h.st.QueueStats(c.Request.Context())
	if err != nil {
		h.renderAdminError(c, err)
		return
	}
	cancelHealth, cancelErr := h.st.IntegrationHealth(c.Request.Context())
	cancelWarning := h.C().Server.CancelSyncRequired && (cancelErr != nil || !cancelHealth.Healthy || cancelHealth.Failures > 0 || time.Since(cancelHealth.ReceivedAt) > 45*time.Second)

	var exhausted, review, merchantReview int64
	var orders []model.Order
	var evidence []model.Transaction
	var cursors []model.ScanCursor
	var callbacks []pendingCallbackView
	var ton []model.TONScanCheckpoint
	var reviews []model.ConfirmationTask
	db := h.st.DB().WithContext(c.Request.Context())
	for _, query := range []error{db.Model(&model.Callback{}).Where("state = ? AND delivered_at IS NULL", "merchant_review").Count(&merchantReview).Error, db.Model(&model.Callback{}).Where("delivered_at IS NULL AND (state = ? OR (attempts >= attempt_base + ? AND next_at <= ?))", "exhausted", h.C().Notify.MaxAttempts, time.Now()).Count(&exhausted).Error, db.Model(&model.ConfirmationTask{}).Where("state = ?", "manual_review").Count(&review).Error} {
		if query != nil {
			h.renderAdminError(c, query)
			return
		}
	}
	for _, err := range []error{db.Order("id DESC").Limit(8).Find(&orders).Error, db.Where("order_id IS NULL").Order("updated_at DESC").Limit(20).Find(&evidence).Error, db.Find(&cursors).Error, db.Table("callbacks c").Select("c.*,o.trade_no,o.out_trade_no").Joins("JOIN orders o ON o.id=c.order_id").Where("c.delivered_at IS NULL").Order("c.next_at").Limit(20).Scan(&callbacks).Error, db.Where("active = ?", true).Find(&ton).Error, db.Where("state = ?", "manual_review").Order("deadline").Limit(20).Find(&reviews).Error} {
		if err != nil {
			h.renderAdminError(c, err)
			return
		}
	}
	c.HTML(200, "admin_dashboard.html", gin.H{"Base": h.adminBase(c, "dashboard", "收款概览"), "Orders": orders, "Evidence": evidence, "Scans": cursors, "Callbacks": callbacks, "ExhaustedCallbacks": exhausted, "Queues": queues, "CancelHealth": cancelHealth, "CancelWarning": cancelWarning, "CancelRequired": h.C().Server.CancelSyncRequired, "BinanceUsage": h.poller.BinanceUsage(), "MerchantReviewCallbacks": merchantReview, "ReviewConfirmations": review, "TONCheckpoints": ton, "ConfirmationReviews": reviews})
}
func (h *Handlers) AdminOrders(c *gin.Context) {
	page, _ := strconv.Atoi(c.Query("page"))
	if page < 1 {
		page = 1
	}
	if page > adminOrdersMaxPage {
		page = adminOrdersMaxPage
	}
	var orders []model.Order
	db := h.st.DB().WithContext(c.Request.Context()).Order("id DESC")
	if q := strings.TrimSpace(c.Query("q")); q != "" {
		db = db.Where("trade_no = ? OR out_trade_no = ?", q, q)
	}
	if err := db.Offset((page - 1) * 50).Limit(50).Find(&orders).Error; err != nil {
		h.renderAdminError(c, err)
		return
	}
	c.HTML(200, "admin_orders.html", gin.H{"Base": h.adminBase(c, "orders", "订单"), "Orders": orders, "Page": page, "Prev": max(1, page-1), "Next": page + 1, "Query": c.Query("q")})
}
func (h *Handlers) AdminCloseOrder(c *gin.Context) {
	no := c.Param("trade_no")
	target := h.adminURL("/admin/orders/") + url.PathEscape(no) + "?msg="
	if err := h.st.Cancel(c.Request.Context(), no); err != nil {
		c.Redirect(http.StatusSeeOther, target+url.QueryEscape(err.Error())+"&t=err")
		return
	}
	h.log.Info("管理员关闭订单", "trade_id", no, "operator", c.GetString("admin_user"))
	c.Redirect(http.StatusSeeOther, target+url.QueryEscape("订单已关闭"))
}

func (h *Handlers) AdminOrderDetail(c *gin.Context) {
	o, err := h.st.GetByTradeNo(c.Request.Context(), c.Param("trade_no"))
	if err != nil {
		h.renderAdminError(c, err)
		return
	}
	db := h.st.DB().WithContext(c.Request.Context())
	var tx []model.Transaction
	var logs []model.NotifyLog
	var tasks []model.Callback
	var recoveries []model.CallbackRecovery
	var checks []model.ConfirmationTask
	var attempts []model.PaymentAttempt
	var paymentRecoveries []model.PaymentRecovery
	var history []model.HistoricalScan
	for _, err := range []error{db.Where("order_id = ?", o.ID).Find(&tx).Error, db.Where("order_id = ?", o.ID).Order("id DESC").Find(&logs).Error, db.Where("order_id = ?", o.ID).Find(&tasks).Error, db.Where("order_id = ?", o.ID).Order("id DESC").Find(&recoveries).Error, db.Where("order_id = ?", o.ID).Find(&checks).Error, db.Where("order_id = ?", o.ID).Order("id").Find(&attempts).Error, db.Where("order_id = ?", o.ID).Order("id DESC").Find(&paymentRecoveries).Error, db.Where("order_id = ?", o.ID).Order("id DESC").Find(&history).Error} {
		if err != nil {
			h.renderAdminError(c, err)
			return
		}
	}
	c.HTML(200, "admin_order.html", gin.H{"Base": h.adminBase(c, "orders", "订单详情"), "Order": o, "Transactions": tx, "Callbacks": tasks, "Logs": logs, "Recoveries": recoveries, "Confirmations": checks, "NotifyMaxAttempts": h.C().Notify.MaxAttempts, "PaymentAttempts": attempts, "PaymentRecoveries": paymentRecoveries, "HistoricalScans": history})
}
func (h *Handlers) AdminIntegration(c *gin.Context) {
	c.HTML(200, "admin_integration.html", gin.H{"Base": h.adminBase(c, "integration", "接入信息"), "BaseURL": h.C().Server.BaseURL, "Token": h.C().BEpusdt.APIToken, "PendingNotifyCount": h.pendingNotifyCount(c), "CanRotateToken": !h.C().Server.Production || h.C().Server.MerchantKeyVersions})
}

func (h *Handlers) AdminRetryNotify(c *gin.Context) {
	no := c.Param("trade_no")
	target := h.adminURL("/admin/orders/") + url.PathEscape(no) + "?msg="
	if err := h.st.RetryCallback(c.Request.Context(), no, c.GetString("admin_user"), c.PostForm("reason"), h.C().Notify.MaxAttempts); err != nil {
		c.Redirect(http.StatusSeeOther, target+url.QueryEscape(err.Error())+"&t=err")
		return
	}
	h.log.Warn("管理员重新通知商城", "trade_id", no, "operator", c.GetString("admin_user"))
	c.Redirect(http.StatusSeeOther, target+url.QueryEscape("通知已重新入队，将使用原订单号发送"))
}

func (h *Handlers) pendingNotifyCount(c *gin.Context) int64 {
	var n int64
	if err := h.st.DB().WithContext(c.Request.Context()).Model(&model.Callback{}).Where("delivered_at IS NULL").Count(&n).Error; err != nil {
		return -1
	}
	return n
}

func (h *Handlers) AdminReconcilePayment(c *gin.Context) {
	no := c.Param("trade_no")
	target := h.adminURL("/admin/orders/") + url.PathEscape(no) + "?msg="
	err := h.st.ReconcilePayment(c.Request.Context(), no, c.PostForm("provider"), strings.TrimSpace(c.PostForm("event_id")), c.GetString("admin_user"), c.PostForm("reason"))
	if err != nil {
		c.Redirect(http.StatusSeeOther, target+url.QueryEscape(err.Error())+"&t=err")
		return
	}
	h.log.Warn("管理员按已验证到账证据补单", "trade_id", no, "operator", c.GetString("admin_user"))
	c.Redirect(http.StatusSeeOther, target+url.QueryEscape("补单已核销并创建通知任务"))
}

func (h *Handlers) AdminHistoricalScan(c *gin.Context) {
	no := c.Param("trade_no")
	target := h.adminURL("/admin/orders/") + url.PathEscape(no) + "?msg="
	if err := h.st.RequestHistoricalScan(c.Request.Context(), no, c.GetString("admin_user"), c.PostForm("reason")); err != nil {
		c.Redirect(http.StatusSeeOther, target+url.QueryEscape(err.Error())+"&t=err")
		return
	}
	c.Redirect(http.StatusSeeOther, target+url.QueryEscape("历史到账补扫已排队，关闭订单只记录证据，不会自动补单"))
}

type pendingCallbackView struct {
	model.Callback
	TradeNo    string
	OutTradeNo string
}
