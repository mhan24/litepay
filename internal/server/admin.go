package server

import (
	"context"
	"crypto/hmac"
	"crypto/pbkdf2"
	"crypto/rand"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net"
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
	"litepay/internal/store"
)

func (h *Handlers) currentAdmin() *adminAuth {
	h.adminMu.Lock()
	defer h.adminMu.Unlock()
	cfg := h.C()
	if h.admin == nil {
		h.admin = newAdminAuth(cfg)
		if h.st != nil {
			if err := h.admin.loadEpoch(h.st); err != nil {
				return h.admin
			}
		}
	} else if h.admin.user != cfg.Admin.Username || h.admin.pass != cfg.Admin.Password || h.admin.ttl != adminSessionTTL(cfg) {
		next := newAdminAuth(cfg)
		h.admin.mu.Lock()
		next.epoch, next.st, next.epochRetryAt = h.admin.epoch, h.admin.st, h.admin.epochRetryAt
		h.admin.mu.Unlock()
		h.admin = next
	}
	return h.admin
}

// -----------------------------------------------------------------------------
// 会话与鉴权
// -----------------------------------------------------------------------------

const adminCookieName = "lp_admin"

var errAdminEpochUnavailable = errors.New("后台会话版本不可用")

const (
	adminSessionSalt        = "litepay-admin-session-v1"
	adminSessionIterations  = 210000
	adminSessionKeyBytes    = 32
	adminOrdersMaxPage      = 100000
	adminUserAttemptLimit   = 6
	adminIPAttemptLimit     = 30
	adminGlobalAttemptLimit = 300
	adminMaxAttemptKeys     = 10000
	adminAttemptWindow      = 10 * time.Minute
	adminEpochRetryInterval = 5 * time.Second
)

// adminSession 会话载荷（Base64(JSON) + "." + HMAC 签名）。
type adminSession struct {
	ID    string `json:"sid"`
	Epoch uint64 `json:"ep"`
	User  string `json:"u"`
	Exp   int64  `json:"e"`
	CSRF  string `json:"c"`
}

type adminAuth struct {
	user string
	pass string
	key  []byte
	ttl  time.Duration

	mu             sync.Mutex
	epoch          uint64
	epochRetryAt   time.Time
	st             *store.Store
	reauth         *adminAuth
	hardLimits     bool
	attempts       map[string]*loginAttempt
	ipAttempts     map[string]*loginAttempt
	globalCount    int
	globalInflight int
	globalUntil    time.Time
}

type loginAttempt struct {
	count    int // reserved attempts, including requests still in flight
	inflight int
	until    time.Time
}

func adminSessionTTL(cfg *config.Config) time.Duration {
	hours := cfg.Admin.SessionHours
	if cfg.Server.Production && hours > 72 {
		hours = 72
	}
	ttl := time.Duration(hours) * time.Hour
	if ttl <= 0 {
		ttl = 12 * time.Hour
	}
	return ttl
}

func newAdminAuth(cfg *config.Config) *adminAuth {
	// PBKDF2 降低密码泄漏后离线猜测会话签名密钥的速度。修改账号或密码
	// 会派生不同密钥，所有旧会话随之失效。
	key, err := pbkdf2.Key(sha256.New, cfg.Admin.Username+"\x00"+cfg.Admin.Password,
		[]byte(adminSessionSalt), adminSessionIterations, adminSessionKeyBytes)
	if err != nil {
		panic(fmt.Sprintf("派生后台会话密钥失败: %v", err))
	}
	return &adminAuth{
		user:       cfg.Admin.Username,
		pass:       cfg.Admin.Password,
		key:        key,
		ttl:        adminSessionTTL(cfg),
		epoch:      1,
		attempts:   make(map[string]*loginAttempt),
		ipAttempts: make(map[string]*loginAttempt),
	}
}

func (a *adminAuth) currentEpoch() uint64 {
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.epoch == 0 && a.st != nil {
		_ = a.loadEpochLocked(a.st, false) // Failure stays closed; the helper limits retries and logs.
	}
	return a.epoch
}

func (a *adminAuth) loadEpoch(st *store.Store) error {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.loadEpochLocked(st, false)
}

// The caller holds mu. Recovery advances the durable epoch so a failed revocation
// cannot revive cookies signed before storage failed.
func (a *adminAuth) loadEpochLocked(st *store.Store, bump bool) error {
	a.st = st
	if a.epoch == 0 && time.Now().Before(a.epochRetryAt) {
		return errAdminEpochUnavailable
	}
	epoch, err := st.AdminSessionEpoch(context.Background(), bump || a.epoch == 0)
	if err != nil {
		a.epoch = 0
		a.epochRetryAt = time.Now().Add(adminEpochRetryInterval)
		slog.Error("加载或推进后台会话版本失败，后台会话暂不可用", "err", err)
		return err
	}
	a.epoch, a.epochRetryAt = epoch, time.Time{}
	return nil
}

func (a *adminAuth) bumpEpoch() (uint64, error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.st != nil {
		if err := a.loadEpochLocked(a.st, true); err != nil {
			return 0, err
		}
	} else {
		a.epoch++
	}
	return a.epoch, nil
}

// reauthenticate reserves by session ID and username, with a separate global budget.
func (h *Handlers) reauthenticate(c *gin.Context) bool {
	a := h.currentAdmin()
	a.mu.Lock()
	if a.reauth == nil {
		a.reauth = &adminAuth{hardLimits: true, attempts: make(map[string]*loginAttempt), ipAttempts: make(map[string]*loginAttempt)}
	}
	limiter := a.reauth
	a.mu.Unlock()
	sid, user := c.GetString("admin_sid"), c.GetString("admin_user")
	allowed := sid != "" && limiter.allow(sid, user)
	if allowed && a.checkCredentials(user, c.PostForm("current_password")) {
		limiter.succeeded(sid, user)
		return true
	}
	exhausted := !allowed
	if allowed {
		limiter.failed(sid, user)
		limiter.mu.Lock()
		_, key := adminAttemptKeys(sid, user)
		attempt := limiter.attempts[key]
		exhausted = (attempt != nil && attempt.count >= adminUserAttemptLimit) || limiter.globalCount >= adminGlobalAttemptLimit
		limiter.mu.Unlock()
	}
	if exhausted {
		_, err := a.bumpEpoch()
		slog.Warn("后台密码重新验证达到上限，已吊销会话", "operator", user, "session_id", sid, "err", err)
	}
	return false
}

// issue 生成新会话 token 与 CSRF token。
func (a *adminAuth) issue() (token, csrf string, err error) {
	epoch := a.currentEpoch()
	if epoch == 0 {
		return "", "", errAdminEpochUnavailable
	}
	sidBuf := make([]byte, 16)
	if _, err := rand.Read(sidBuf); err != nil {
		return "", "", fmt.Errorf("生成后台会话 ID 失败: %w", err)
	}
	buf := make([]byte, 16)
	if _, err := rand.Read(buf); err != nil {
		return "", "", fmt.Errorf("生成后台会话随机数失败: %w", err)
	}
	sid := hex.EncodeToString(sidBuf)
	csrf = hex.EncodeToString(buf)

	s := adminSession{
		ID:    sid,
		Epoch: epoch,
		User:  a.user,
		Exp:   time.Now().Add(a.ttl).Unix(),
		CSRF:  csrf,
	}
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
	if s.Epoch == 0 || s.ID == "" || s.User != a.user || s.Epoch != a.currentEpoch() {
		return nil, false
	}
	if time.Now().Unix() > s.Exp {
		return nil, false
	}
	return &s, true
}

func constantTimeEqualSHA256(a, b string) bool {
	ha := sha256.Sum256([]byte(a))
	hb := sha256.Sum256([]byte(b))
	return subtle.ConstantTimeCompare(ha[:], hb[:]) == 1
}

// checkCredentials 校验账号密码（先做 SHA-256 再常量时间比较，避免因长度不同提前返回）。
func (a *adminAuth) checkCredentials(user, pass string) bool {
	uOK := constantTimeEqualSHA256(user, a.user)
	pOK := constantTimeEqualSHA256(pass, a.pass)
	if config.IsBcryptHash(a.pass) {
		pOK = bcrypt.CompareHashAndPassword([]byte(a.pass), []byte(pass)) == nil
	}
	return uOK && pOK
}

func adminAttemptKeys(ip, user string) (string, string) {
	ip = strings.TrimSpace(ip)
	user = strings.TrimSpace(user)
	if len(user) > 64 {
		sum := sha256.Sum256([]byte(user))
		user = hex.EncodeToString(sum[:8])
	}
	return ip, ip + "\x00" + user
}

// allow reserves attempts atomically; login IP/global budgets delay, while reauthentication uses hard limits.
func (a *adminAuth) allow(ip string, users ...string) bool {
	a.mu.Lock()
	defer a.mu.Unlock()

	user := ""
	if len(users) > 0 {
		user = users[0]
	}
	ipKey, userKey := adminAttemptKeys(ip, user)

	now := time.Now()
	for key, attempt := range a.attempts {
		if now.After(attempt.until) {
			delete(a.attempts, key)
		}
	}
	for key, attempt := range a.ipAttempts {
		if now.After(attempt.until) {
			delete(a.ipAttempts, key)
		}
	}
	if now.After(a.globalUntil) {
		a.globalCount = 0
		a.globalInflight = 0
		a.globalUntil = now.Add(adminAttemptWindow)
	}
	if a.hardLimits && a.globalCount >= adminGlobalAttemptLimit {
		return false
	}
	if len(a.attempts) >= adminMaxAttemptKeys {
		if _, exists := a.attempts[userKey]; !exists {
			return false
		}
	}
	if len(a.ipAttempts) >= adminMaxAttemptKeys {
		if _, exists := a.ipAttempts[ipKey]; !exists {
			return false
		}
	}

	ipAt, ipOK := a.ipAttempts[ipKey]
	if a.hardLimits && ipOK && !now.After(ipAt.until) && ipAt.count >= adminIPAttemptLimit {
		return false
	}
	uAt, uOK := a.attempts[userKey]
	if uOK && !now.After(uAt.until) && uAt.count >= adminUserAttemptLimit {
		return false
	}

	if !ipOK || now.After(ipAt.until) {
		a.ipAttempts[ipKey] = &loginAttempt{count: 1, inflight: 1, until: now.Add(adminAttemptWindow)}
	} else {
		ipAt.count++
		ipAt.inflight++
	}
	if !uOK || now.After(uAt.until) {
		a.attempts[userKey] = &loginAttempt{count: 1, inflight: 1, until: now.Add(adminAttemptWindow)}
	} else {
		uAt.count++
		uAt.inflight++
	}
	a.globalCount++
	a.globalInflight++
	return true
}

// succeeded 清除已成功登录 (IP, 用户名) 的失败计数，同时保留其他并发请求的预留。
// failed 保留本次失败尝试，直到 10 分钟窗口结束。
func (a *adminAuth) succeeded(ip string, users ...string) {
	a.mu.Lock()
	defer a.mu.Unlock()
	user := ""
	if len(users) > 0 {
		user = users[0]
	}
	ipKey, userKey := adminAttemptKeys(ip, user)

	if a.globalInflight > 0 {
		a.globalInflight--
	}
	if a.globalCount > a.globalInflight {
		a.globalCount--
	}
	if ipAt, ok := a.ipAttempts[ipKey]; ok {
		if ipAt.inflight > 0 {
			ipAt.inflight--
		}
		if ipAt.count > ipAt.inflight {
			ipAt.count--
		}
		if ipAt.count == 0 {
			delete(a.ipAttempts, ipKey)
		}
	}
	at, ok := a.attempts[userKey]
	if !ok {
		return
	}
	if at.inflight > 0 {
		at.inflight--
	}
	// A successful login clears earlier failures for this (IP, user), while
	// requests still in flight remain counted.
	at.count = at.inflight
	if at.count == 0 {
		delete(a.attempts, userKey)
	}
}

func (a *adminAuth) failed(ip string, users ...string) {
	a.mu.Lock()
	defer a.mu.Unlock()
	user := ""
	if len(users) > 0 {
		user = users[0]
	}
	ipKey, userKey := adminAttemptKeys(ip, user)

	if a.globalInflight > 0 {
		a.globalInflight--
	}
	if ipAt, ok := a.ipAttempts[ipKey]; ok && ipAt.inflight > 0 {
		ipAt.inflight--
	}
	if at, ok := a.attempts[userKey]; ok && at.inflight > 0 {
		at.inflight--
	}
}

func (h *Handlers) hasValidAdminSession(c *gin.Context) bool {
	cookie, err := c.Cookie(adminCookieName)
	if err != nil || cookie == "" {
		return false
	}
	_, ok := h.currentAdmin().parse(cookie)
	return ok
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
				c.String(http.StatusForbidden, "页面会话校验失败（CSRF），请返回上一页刷新后重试")
				return
			}
		}

		c.Set("admin_sid", sess.ID)
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
	Active       string
	User         string
	CSRF         string
	Flash        string
	FlashOK      bool
	FlashWarn    bool
	ProxyWarning bool
	Title        string
}

func (h *Handlers) adminBase(c *gin.Context, active, title string) AdminBase {
	flash := c.Query("msg")
	cfg := h.C()
	return AdminBase{
		Active:       active,
		Title:        title,
		User:         c.GetString("admin_user"),
		CSRF:         c.GetString("admin_csrf"),
		Flash:        flash,
		FlashOK:      c.Query("t") != "err",
		FlashWarn:    c.Query("t") == "warn",
		ProxyWarning: cfg.Server.Production && len(cfg.Server.TrustedProxies) == 0,
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
	next := c.Query("next")
	if !safeAdminNext(next, h.C().AdminPath()) {
		next = ""
	}
	c.Header("Cache-Control", "no-store")
	renderHTML(c, http.StatusOK, "admin_login.html", gin.H{
		"Next":     next,
		"Error":    c.Query("err"),
		"SiteName": "LitePay 管理后台",
	})
}

// AdminLoginSubmit POST /admin/login
func (h *Handlers) AdminLoginSubmit(c *gin.Context) {
	auth := h.currentAdmin()
	ip := c.ClientIP()
	user := strings.TrimSpace(c.PostForm("username"))
	pass := c.PostForm("password")
	next := c.PostForm("next")

	hasSession := h.hasValidAdminSession(c)
	if !hasSession && !auth.allow(ip, user) {
		c.Redirect(http.StatusSeeOther, h.adminURL("/admin/login?err=")+url.QueryEscape("登录尝试过于频繁，请 10 分钟后再试"))
		return
	}

	if !hasSession {
		waitForRequest(c, auth.loginDelay(ip))
	}
	if !auth.checkCredentials(user, pass) {
		if !hasSession {
			// Attempts are reserved atomically in allow, including concurrent requests.
			auth.failed(ip, user)
		}
		errTarget := h.adminURL("/admin/login?err=") + url.QueryEscape("管理员账号或密码错误，请核对后重试")
		if safeAdminNext(next, h.C().AdminPath()) {
			errTarget += "&next=" + url.QueryEscape(next)
		}
		c.Redirect(http.StatusSeeOther, errTarget)
		return
	}
	if !hasSession {
		auth.succeeded(ip, user)
	}

	token, _, err := auth.issue()
	if err != nil {
		if !errors.Is(err, errAdminEpochUnavailable) {
			slog.Error("创建后台会话失败", "err", err)
		}
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

func (h *Handlers) logoutAdmin(c *gin.Context, cookiePath string) {
	if _, err := h.currentAdmin().bumpEpoch(); err != nil {
		c.String(http.StatusInternalServerError, "会话吊销失败，请稍后重试")
		return
	}
	if cookiePath == "" {
		cookiePath = h.C().AdminPath()
	}
	c.SetSameSite(http.SameSiteStrictMode)
	c.SetCookie(adminCookieName, "", -1, cookiePath, "", strings.HasPrefix(h.C().Server.BaseURL, "https://"), true)
	c.Redirect(http.StatusSeeOther, h.adminURL("/admin/login"))
}

// AdminLogout POST /admin/logout
func (h *Handlers) AdminLogout(c *gin.Context) {
	h.logoutAdmin(c, h.C().AdminPath())
}

// -----------------------------------------------------------------------------
// 概览
// -----------------------------------------------------------------------------

// AdminSettings GET /admin/settings
func (h *Handlers) AdminSettings(c *gin.Context) {
	cfg := h.C()
	updated := h.cfg.UpdatedAt(c.Request.Context())

	renderHTML(c, http.StatusOK, "admin_settings.html", gin.H{
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
		"BybitCurrencies":    cfg.ProviderCurrencies(config.ProviderBybit),
		"BitgetCurrencies":   cfg.ProviderCurrencies(config.ProviderBitget),
		"DelaysJoined":       joinInts(cfg.Notify.RetryDelays),
		"BEpusdtKeyMask":     maskSecret(cfg.BEpusdt.APIToken),
		"BinanceKey":         maskSecret(cfg.Binance.APIKey),
		"BinanceOn":          cfg.BinanceEnabled(),
		"OKXOn":              cfg.OKXEnabled(),
		"BybitOn":            cfg.BybitEnabled(),
		"BitgetOn":           cfg.BitgetEnabled(),
		"CollectionOn":       cfg.AnyProviderEnabled(),
		"Provider":           "multi",
		"ProviderOrder":      cfg.OrderedProviders(),
		"ProviderOrderCSV":   strings.Join(cfg.OrderedProviders(), ","),
		"ProviderNames":      map[string]string{config.ProviderBinance: "币安 · Binance", config.ProviderOKX: "欧易 · OKX", config.ProviderBybit: "Bybit", config.ProviderBitget: "Bitget"},
		"OKXKey":             maskSecret(cfg.OKX.APIKey),
		"BybitKey":           maskSecret(cfg.Bybit.APIKey),
		"BitgetKey":          maskSecret(cfg.Bitget.APIKey),
		"DBPath":             cfg.Database.Path,

		"BaseURL": cfg.Server.BaseURL,
	})
}

// AdminSettingsSave POST /admin/settings
func (h *Handlers) AdminSettingsSave(c *gin.Context) {
	p, errs := parseSettingsForm(c)
	destination := h.adminURL("/admin/settings?msg=")
	scope := c.PostForm("settings_scope")
	for _, valid := range []string{"binance", "okx", "bybit", "bitget", "checkout", "orders", "merchant"} {
		if scope == valid {
			destination = h.adminURL("/admin/settings?view=" + scope + "&msg=")
			break
		}
	}
	if len(errs) > 0 {
		c.Redirect(http.StatusSeeOther, destination+url.QueryEscape(strings.Join(errs, "；"))+"&t=err")
		return
	}

	// Deployment controls (notify_hosts, base_url, production) are startup-only;
	// parseSettingsForm never exposes them as editable settings.
	cfg := h.C()
	sensitive := p.BinanceAPIKey != nil || p.BinanceAPISecret != nil || p.OKXAPIKey != nil || p.OKXAPISecret != nil || p.OKXPassphrase != nil || p.BybitAPIKey != nil || p.BybitAPISecret != nil || p.BitgetAPIKey != nil || p.BitgetAPISecret != nil || p.BitgetPassphrase != nil ||
		(p.BinanceUID != nil && *p.BinanceUID != cfg.Checkout.Binance.UID) || (p.OKXUID != nil && *p.OKXUID != cfg.Checkout.OKX.UID) || (p.BybitUID != nil && *p.BybitUID != cfg.Checkout.Bybit.UID) || (p.BitgetUID != nil && *p.BitgetUID != cfg.Checkout.Bitget.UID)
	if sensitive && !h.reauthenticate(c) {
		c.Redirect(http.StatusSeeOther, destination+url.QueryEscape("当前密码错误或验证过于频繁，请重新登录后重试")+"&t=err")
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
	names := map[string]string{config.ProviderBinance: "Binance", config.ProviderOKX: "OKX", config.ProviderBybit: "Bybit", config.ProviderBitget: "Bitget"}
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
		c.Redirect(http.StatusSeeOther, h.adminURL("/admin/settings?view=account&msg=")+url.QueryEscape(msg)+"&t=err")
	}
	if !h.reauthenticate(c) {
		fail("当前密码错误或验证过于频繁，请重新登录后重试")
		return
	}

	user := strings.TrimSpace(c.PostForm("admin_username"))
	pass := c.PostForm("new_password")
	if user == "" || utf8.RuneCountInString(user) > 64 {
		fail("用户名需为 1~64 个字符，请修改后重试")
		return
	}
	p := settings.Patch{AdminUsername: &user}
	if pass != "" {
		if utf8.RuneCountInString(pass) < 12 || len(pass) > 72 {
			fail("新密码需至少 12 个字符且不超过 72 字节，请重新输入")
			return
		}
		if pass != c.PostForm("confirm_password") {
			fail("两次输入的新密码不一致，请重新确认")
			return
		}
		hash, err := bcrypt.GenerateFromPassword([]byte(pass), bcrypt.DefaultCost)
		if err != nil {
			fail("密码加密处理失败，请稍后重试")
			return
		}
		value := string(hash)
		p.AdminPasswordHash = &value
	}
	if err := h.cfg.Update(c.Request.Context(), p, c.GetString("admin_user")); err != nil {
		fail(err.Error())
		return
	}
	h.logoutAdmin(c, "")
}

// AdminSiteSave changes the public redirect and admin entry after reauthentication.
func (h *Handlers) AdminSiteSave(c *gin.Context) {
	fail := func(msg string) {
		c.Redirect(http.StatusSeeOther, h.adminURL("/admin/settings?view=account&msg=")+url.QueryEscape(msg)+"&t=err")
	}
	if !h.reauthenticate(c) {
		fail("当前密码错误或验证过于频繁，请重新登录后重试")
		return
	}

	root := strings.TrimSpace(c.PostForm("admin_path"))
	storeURL := strings.TrimSpace(c.PostForm("store_url"))
	if root == "" {
		fail("请填写以 / 开头的后台入口路径")
		return
	}
	oldRoot := h.C().AdminPath()
	if err := h.cfg.Update(c.Request.Context(), settings.Patch{AdminPath: &root, StoreURL: &storeURL}, c.GetString("admin_user")); err != nil {
		fail(err.Error())
		return
	}
	h.logoutAdmin(c, oldRoot)
}

// AdminRotateToken POST /admin/settings/token —— 服务端生成新 Token，
// 新值只在本次跳转的接入页展示，不在设置表单中回填。
func (h *Handlers) AdminRotateToken(c *gin.Context) {
	if !h.reauthenticate(c) {
		c.String(http.StatusForbidden, "当前密码错误或验证过于频繁，请重新登录后重试")
		return
	}

	_, err := h.cfg.RotateAPIToken(c.Request.Context(), c.GetString("admin_user"))
	if err != nil {
		c.Redirect(http.StatusSeeOther,
			h.adminURL("/admin/integration?msg=")+url.QueryEscape(err.Error())+"&t=err")
		return
	}
	slog.Info("BEpusdt API Token 已轮换", "operator", c.GetString("admin_user"))
	c.Redirect(http.StatusSeeOther,
		h.adminURL("/admin/integration?msg=")+url.QueryEscape("API Token 已轮换。新订单将使用新 Token，历史订单回调仍使用原密钥版本，请在商城后台同步更新 Token")+
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
		p.BinanceRegisterURL = text("binance_register_url", false)
		p.BinanceAPIKey = text("binance_api_key", true)
		p.BinanceAPISecret = text("binance_api_secret", true)
		p.BinanceCurrencies = currencies("binance_currencies")
		p.BinancePollInterval = integer("binance_poll_interval", 2, 60)
		p.BinanceLookback = integer("binance_lookback", 1, 60)
		if clear {
			p.BinanceUID = &empty
			p.BinanceRegisterURL = &empty
			p.BinanceAPIKey = &empty
			p.BinanceAPISecret = &empty
			p.BinanceCurrencies = &none
			p.BinancePollInterval = &defaults.Binance.PollInterval
			p.BinanceLookback = &defaults.Binance.LookbackBuffer
			errs = nil
		}
	case config.ProviderOKX:
		p.OKXUID = text("okx_uid", false)
		p.OKXRegisterURL = text("okx_register_url", false)
		p.OKXAPIKey = text("okx_api_key", true)
		p.OKXAPISecret = text("okx_api_secret", true)
		p.OKXPassphrase = text("okx_passphrase", true)
		p.OKXCurrencies = currencies("okx_currencies")
		p.OKXPollInterval = integer("okx_poll_interval", 2, 60)
		p.OKXLookback = integer("okx_lookback", 1, 60)
		if clear {
			p.OKXUID = &empty
			p.OKXRegisterURL = &empty
			p.OKXAPIKey = &empty
			p.OKXAPISecret = &empty
			p.OKXPassphrase = &empty
			p.OKXCurrencies = &none
			p.OKXPollInterval = &defaults.OKX.PollInterval
			p.OKXLookback = &defaults.OKX.LookbackBuffer
			errs = nil
		}
	case config.ProviderBybit:
		p.BybitUID = text("bybit_uid", false)
		p.BybitRegisterURL = text("bybit_register_url", false)
		p.BybitAPIKey = text("bybit_api_key", true)
		p.BybitAPISecret = text("bybit_api_secret", true)
		p.BybitCurrencies = currencies("bybit_currencies")
		p.BybitPollInterval = integer("bybit_poll_interval", 2, 60)
		p.BybitLookback = integer("bybit_lookback", 1, 60)
		if clear {
			p.BybitUID = &empty
			p.BybitRegisterURL = &empty
			p.BybitAPIKey = &empty
			p.BybitAPISecret = &empty
			p.BybitCurrencies = &none
			p.BybitPollInterval = &defaults.Bybit.PollInterval
			p.BybitLookback = &defaults.Bybit.LookbackBuffer
			errs = nil
		}
	case config.ProviderBitget:
		p.BitgetUID = text("bitget_uid", false)
		p.BitgetRegisterURL = text("bitget_register_url", false)
		p.BitgetAPIKey = text("bitget_api_key", true)
		p.BitgetAPISecret = text("bitget_api_secret", true)
		p.BitgetPassphrase = text("bitget_passphrase", true)
		p.BitgetCurrencies = currencies("bitget_currencies")
		p.BitgetPollInterval = integer("bitget_poll_interval", 2, 60)
		p.BitgetLookback = integer("bitget_lookback", 1, 60)
		if clear {
			p.BitgetUID = &empty
			p.BitgetRegisterURL = &empty
			p.BitgetAPIKey = &empty
			p.BitgetAPISecret = &empty
			p.BitgetPassphrase = &empty
			p.BitgetCurrencies = &none
			p.BitgetPollInterval = &defaults.Bitget.PollInterval
			p.BitgetLookback = &defaults.Bitget.LookbackBuffer
			errs = nil
		}
	case "checkout":
		p.CheckoutNote = text("checkout_note", false)
		rawOrder := strings.TrimSpace(c.PostForm("provider_order"))
		if rawOrder != "" {
			parts := strings.Split(rawOrder, ",")
			normalized := config.NormalizeProviderOrder(parts)
			p.ProviderOrder = &normalized
		}
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
	if scope == config.ProviderBinance || scope == config.ProviderOKX || scope == config.ProviderBybit || scope == config.ProviderBitget {
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
	renderHTML(c, http.StatusOK, "admin_error.html", gin.H{
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
	cancelWarning := cancelHealth.Conflicts > 0 || cancelHealth.DiscoveryPending > 0 || h.C().Server.CancelSyncRequired && (cancelErr != nil || h.paymentOperationsBlocked(c))

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
	cancellationQueue, err := h.st.CancellationReviewQueue(c.Request.Context())
	if err != nil {
		h.renderAdminError(c, err)
		return
	}
	var binanceUsage any
	if h.poller != nil {
		binanceUsage = h.poller.BinanceUsage()
	}
	renderHTML(c, 200, "admin_dashboard.html", gin.H{"CancellationReviewQueue": cancellationQueue, "Base": h.adminBase(c, "dashboard", "收款概览"), "Orders": orders, "Evidence": evidence, "Scans": cursors, "Callbacks": callbacks, "ExhaustedCallbacks": exhausted, "Queues": queues, "CancelHealth": cancelHealth, "CancelWarning": cancelWarning, "CancelRequired": h.C().Server.CancelSyncRequired, "ProviderHealths": h.providerHealths(c), "BinanceUsage": binanceUsage, "MerchantReviewCallbacks": merchantReview, "ReviewConfirmations": review, "TONCheckpoints": ton, "ConfirmationReviews": reviews})
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
	renderHTML(c, 200, "admin_orders.html", gin.H{"Base": h.adminBase(c, "orders", "订单"), "Orders": orders, "Page": page, "Prev": max(1, page-1), "Next": page + 1, "Query": c.Query("q")})
}
// logger returns the configured logger or slog.Default when unset (e.g., direct handler tests).
func (h *Handlers) logger() *slog.Logger {
	if h.log != nil {
		return h.log
	}
	return slog.Default()
}

func (h *Handlers) AdminCloseOrder(c *gin.Context) {
	no := c.Param("trade_no")
	target := h.adminURL("/admin/orders/") + url.PathEscape(no) + "?msg="
	if err := h.st.Cancel(c.Request.Context(), no); err != nil {
		c.Redirect(http.StatusSeeOther, target+url.QueryEscape(err.Error())+"&t=err")
		return
	}
	h.logger().Info("管理员关闭订单", "trade_id", no, "operator", c.GetString("admin_user"))
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
	var reviews []model.CancellationReview
	var reviewAudits []model.CancellationReviewAudit
	for _, err := range []error{db.Where("order_id = ?", o.ID).Find(&reviews).Error, db.Where("review_id IN (SELECT id FROM cancellation_reviews WHERE order_id=?)", o.ID).Order("id").Find(&reviewAudits).Error, db.Where("order_id = ? OR attempt_id IN (SELECT id FROM payment_attempts WHERE order_id=?)", o.ID, o.ID).Find(&tx).Error, db.Where("order_id = ?", o.ID).Order("id DESC").Find(&logs).Error, db.Where("order_id = ?", o.ID).Find(&tasks).Error, db.Where("order_id = ?", o.ID).Order("id DESC").Find(&recoveries).Error, db.Where("order_id = ?", o.ID).Find(&checks).Error, db.Where("order_id = ?", o.ID).Order("id").Find(&attempts).Error, db.Where("order_id = ?", o.ID).Order("id DESC").Find(&paymentRecoveries).Error, db.Where("order_id = ?", o.ID).Order("id DESC").Find(&history).Error} {
		if err != nil {
			h.renderAdminError(c, err)
			return
		}
	}
	type reviewView struct {
		model.CancellationReview
		Amount        string
		Currency      string
		EvidenceState string
		Receiver      string
		Current       bool
	}
	views := make([]reviewView, 0, len(reviews))
	for _, review := range reviews {
		var receipt model.Transaction
		if err := db.First(&receipt, "provider=? AND event_id=?", review.Provider, review.EventID).Error; err != nil {
			h.renderAdminError(c, err)
			return
		}
		current, err := h.st.CancellationReviewCurrent(c.Request.Context(), review.ID)
		if err != nil {
			h.renderAdminError(c, err)
			return
		}
		views = append(views, reviewView{CancellationReview: review, Amount: amount.String(receipt.Units), Currency: receipt.Currency, EvidenceState: receipt.State, Receiver: receipt.Receiver, Current: current})
	}
	summary, summaryErr := h.st.CancellationReviewSummary(c.Request.Context(), o.ID)
	if summaryErr != nil {
		h.renderAdminError(c, summaryErr)
		return
	}
	renderHTML(c, 200, "admin_order.html", gin.H{"RefundReviewGenerationCurrent": summary.Current(), "UncoveredCancellationReceipts": summary.Uncovered, "CancellationReviews": views, "CancellationReviewAudits": reviewAudits, "Base": h.adminBase(c, "orders", "订单详情"), "Order": o, "Transactions": tx, "Callbacks": tasks, "Logs": logs, "Recoveries": recoveries, "Confirmations": checks, "NotifyMaxAttempts": h.C().Notify.MaxAttempts, "PaymentAttempts": attempts, "PaymentRecoveries": paymentRecoveries, "HistoricalScans": history})
}
func (h *Handlers) AdminIntegration(c *gin.Context) {
	renderHTML(c, 200, "admin_integration.html", gin.H{
		"Base":               h.adminBase(c, "integration", "接入信息"),
		"BaseURL":            h.C().Server.BaseURL,
		"TokenMask":          maskSecret(h.C().BEpusdt.APIToken),
		"Token":              "",
		"PendingNotifyCount": h.pendingNotifyCount(c),
		"CanRotateToken":     !h.C().Server.Production || h.C().Server.MerchantKeyVersions,
	})
}

func (h *Handlers) AdminIntegrationReveal(c *gin.Context) {
	if !h.reauthenticate(c) {
		c.String(http.StatusForbidden, "当前密码错误或验证过于频繁，请重新登录后重试")
		return
	}

	operator := c.GetString("admin_user")
	h.logger().Warn("管理员查看完整 BEpusdt API Token", "operator", operator, "ip", c.ClientIP())
	renderHTML(c, 200, "admin_integration.html", gin.H{
		"Base":               h.adminBase(c, "integration", "接入信息"),
		"BaseURL":            h.C().Server.BaseURL,
		"TokenMask":          maskSecret(h.C().BEpusdt.APIToken),
		"Token":              h.C().BEpusdt.APIToken,
		"PendingNotifyCount": h.pendingNotifyCount(c),
		"CanRotateToken":     !h.C().Server.Production || h.C().Server.MerchantKeyVersions,
	})
}

func (h *Handlers) AdminRetryNotify(c *gin.Context) {
	no := c.Param("trade_no")
	target := h.adminURL("/admin/orders/") + url.PathEscape(no) + "?msg="
	if err := h.st.RetryCallback(c.Request.Context(), no, c.GetString("admin_user"), c.PostForm("reason"), h.C().Notify.MaxAttempts); err != nil {
		c.Redirect(http.StatusSeeOther, target+url.QueryEscape(err.Error())+"&t=err")
		return
	}
	h.logger().Warn("管理员重新通知商城", "trade_id", no, "operator", c.GetString("admin_user"))
	c.Redirect(http.StatusSeeOther, target+url.QueryEscape("商户通知已重新入队，将按原订单号发送"))
}

func (h *Handlers) pendingNotifyCount(c *gin.Context) int64 {
	var n int64
	if err := h.st.DB().WithContext(c.Request.Context()).Model(&model.Callback{}).Where("delivered_at IS NULL").Count(&n).Error; err != nil {
		return -1
	}
	return n
}

func (h *Handlers) AdminReconcilePayment(c *gin.Context) {
	if !h.reauthenticate(c) {
		c.String(http.StatusForbidden, "当前密码错误或验证过于频繁，请重新登录后重试")
		return
	}

	no := c.Param("trade_no")
	target := h.adminURL("/admin/orders/") + url.PathEscape(no) + "?msg="
	err := h.st.ReconcilePayment(c.Request.Context(), no, c.PostForm("provider"), strings.TrimSpace(c.PostForm("event_id")), c.GetString("admin_user"), c.PostForm("reason"))
	if err != nil {
		c.Redirect(http.StatusSeeOther, target+url.QueryEscape(err.Error())+"&t=err")
		return
	}
	h.logger().Warn("管理员按已验证到账证据补单", "trade_id", no, "operator", c.GetString("admin_user"))
	c.Redirect(http.StatusSeeOther, target+url.QueryEscape("补单核销已完成，并已创建商户通知任务"))
}

func (h *Handlers) AdminHistoricalScan(c *gin.Context) {
	no := c.Param("trade_no")
	target := h.adminURL("/admin/orders/") + url.PathEscape(no) + "?msg="
	if err := h.st.RequestHistoricalScan(c.Request.Context(), no, c.GetString("admin_user"), c.PostForm("reason")); err != nil {
		c.Redirect(http.StatusSeeOther, target+url.QueryEscape(err.Error())+"&t=err")
		return
	}
	c.Redirect(http.StatusSeeOther, target+url.QueryEscape("历史到账补扫任务已加入队列。待补扫完成后请核对下方到账证据；已取消订单仅记录证据，不会自动补单"))
}

type pendingCallbackView struct {
	model.Callback
	TradeNo    string
	OutTradeNo string
}

func (h *Handlers) AdminRecordRefund(c *gin.Context) {
	if !h.reauthenticate(c) {
		c.String(http.StatusForbidden, "当前密码错误或验证过于频繁，请重新登录后重试")
		return
	}

	no := c.Param("trade_no")
	target := h.adminURL("/admin/orders/") + url.PathEscape(no) + "?msg="
	err := h.st.RecordCancellationRefund(c.Request.Context(), no, c.PostForm("provider"), c.PostForm("event_id"), c.GetString("admin_user"), c.PostForm("reference"), c.PostForm("reason"))
	if err != nil {
		c.Redirect(http.StatusSeeOther, target+url.QueryEscape(err.Error())+"&t=err")
		return
	}
	h.logger().Warn("管理员登记取消到账的外部退款凭证", "trade_id", no, "operator", c.GetString("admin_user"))
	c.Redirect(http.StatusSeeOther, target+url.QueryEscape("退款凭证已保存，请确认原付款账户已收到退款"))
}

func (a *adminAuth) loginDelay(ip string) time.Duration {
	a.mu.Lock()
	defer a.mu.Unlock()
	excess := max(0, a.globalCount-adminGlobalAttemptLimit+1)
	if at := a.ipAttempts[strings.TrimSpace(ip)]; at != nil {
		excess = max(excess, at.count-adminIPAttemptLimit+1)
	}
	return time.Duration(min(excess, 50)) * 10 * time.Millisecond
}

func trustedProxyPeer(remote string, proxies []string) bool {
	host, _, err := net.SplitHostPort(remote)
	if err != nil {
		return false
	}
	ip := net.ParseIP(host)
	for _, raw := range proxies {
		if peer := net.ParseIP(raw); peer != nil && peer.Equal(ip) {
			return true
		}
		if _, network, err := net.ParseCIDR(raw); err == nil && network.Contains(ip) {
			return true
		}
	}
	return false
}

func (h *Handlers) warnUntrustedProxy(c *gin.Context) {
	if (c.GetHeader("X-Forwarded-For") != "" || c.GetHeader("X-Real-IP") != "") && !trustedProxyPeer(c.Request.RemoteAddr, h.C().Server.TrustedProxies) {
		h.proxyWarnOnce.Do(func() {
			slog.Warn("检测到未受信任对端的转发头，已忽略；反向代理部署请配置 server.trusted_proxies，否则客户端共用 IP 登录延迟桶")
		})
	}
	c.Next()
}
