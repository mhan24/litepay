// Package server 提供 HTTP 路由与页面渲染。
package server

import (
	"crypto/sha256"
	"embed"
	"encoding/hex"
	"html/template"
	"io/fs"
	"log/slog"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"

	"github.com/gin-gonic/gin"

	"litepay/internal/settings"
	"litepay/internal/store"
	"litepay/internal/worker"
)

//go:embed templates/*.html
var templateFS embed.FS

//go:embed static/brand/*
var staticFS embed.FS

var ginModeOnce sync.Once

// New 构建 gin 引擎并注册全部路由。
func New(mgr *settings.Manager, st *store.Store, poller *worker.Poller) (*gin.Engine, error) {
	if err := mgr.Get().ValidateWebRoutes(); err != nil {
		return nil, err
	}
	ginModeOnce.Do(func() {
		gin.SetMode(gin.ReleaseMode)
	})

	tmpl, err := template.New("").Funcs(template.FuncMap{
		"externalURL": externalURL,
		"tonURL": func(raw string) template.URL {
			if strings.HasPrefix(raw, "ton://transfer/") {
				return template.URL(raw) // #nosec G203 -- Only the fixed ton://transfer/ scheme passes; html/template still escapes attribute delimiters.
			}
			return ""
		},
		"adminPath": func() string { return mgr.Get().AdminPath() },
		"hasStr": func(list []string, v string) bool {
			for _, item := range list {
				if strings.EqualFold(item, v) {
					return true
				}
			}
			return false
		},
		"urlEscape":   url.PathEscape,
		"queryEscape": url.QueryEscape,
	}).ParseFS(templateFS, "templates/*.html")
	if err != nil {
		return nil, err
	}

	r := gin.New()
	// Do not reveal or redirect retired admin paths before the dynamic guard runs.
	r.RedirectTrailingSlash = false
	r.RedirectFixedPath = false
	// Proxy headers are ignored unless the deployment explicitly trusts their source.
	if err := r.SetTrustedProxies(mgr.Get().Server.TrustedProxies); err != nil {
		return nil, err
	}
	r.Use(gin.Recovery())
	r.Use(contentSecurityPolicy)
	r.Use(func(c *gin.Context) {
		c.Header("Referrer-Policy", "no-referrer")
		c.Header("X-Content-Type-Options", "nosniff")
		c.Header("X-Frame-Options", "DENY")
		c.Header("Permissions-Policy", "camera=(), microphone=(), geolocation=(), payment=()")
		c.Header("Cache-Control", "no-store")
		if isHTTPS(c, mgr.Get()) {
			c.Header("Strict-Transport-Security", "max-age=31536000; includeSubDomains")
		}
		if c.Request.Body != nil {
			c.Request.Body = http.MaxBytesReader(c.Writer, c.Request.Body, 1<<20)
		}
		c.Next()
	})
	r.Use(requestLogger())
	r.SetHTMLTemplate(tmpl)
	brandFS, err := fs.Sub(staticFS, "static/brand")
	if err != nil {
		return nil, err
	}
	r.GET("/assets/brand/:file", func(c *gin.Context) {
		name := c.Param("file")
		if strings.Contains(name, "..") || !strings.HasSuffix(name, ".svg") {
			c.Status(http.StatusNotFound)
			return
		}
		data, readErr := fs.ReadFile(brandFS, name)
		if readErr != nil {
			c.Status(http.StatusNotFound)
			return
		}
		c.Header("Cache-Control", "public, max-age=86400")
		c.Data(http.StatusOK, "image/svg+xml; charset=utf-8", data)
	})

	cfg := mgr.Get()
	h := &Handlers{
		cfg:            mgr,
		st:             st,
		log:            slog.Default(),
		poller:         poller,
		selectionLimit: newSelectionLimiter(),
		readLimit:      newSelectionLimiter(),
		replays:        newReplayCache(),
	}
	if cfg.Server.Production && len(cfg.Server.TrustedProxies) == 0 {
		slog.Warn("production 模式下 server.trusted_proxies 为空，反向代理场景下将共用客户端 IP")
	}
	if cfg.AdminEnabled() {
		h.admin = newAdminAuth(cfg)
		if err := h.admin.loadEpoch(st); err != nil {
			return nil, err
		}
	}

	r.Use(h.warnUntrustedProxy)

	// ---- BEpusdt API（独角数卡收银台模式）----
	r.POST("/api/v1/order/create-order", h.BEpusdtCreateOrder)
	r.POST("/api/v1/order/cancel-transaction", h.BEpusdtCancelOrder)
	r.POST("/api/v1/order/cancel-sync-status", h.CancelSyncHeartbeat)
	r.POST("/api/v1/order/cancel-sync-generation", h.CancelSyncGeneration)
	r.POST("/api/v1/order/cancellation-review", h.CancellationReviewStatus)

	// ---- 收银台 ----
	r.GET("/pay/:trade_no", h.LimitPublicRead, h.CashierPage)

	// ---- 前端订单状态查询接口 ----
	r.GET("/api/order/status", h.LimitPublicRead, h.OrderStatus)
	r.POST("/api/order/payment", h.LimitPaymentSelection, h.SelectPayment)

	// ---- 运维 ----
	r.GET("/healthz", h.Health)
	r.GET("/readyz", h.Ready)
	r.GET("/readyz/providers/:provider", h.ProviderReady)
	r.GET("/", h.Index)

	// ---- 后台管理 ----
	if cfg.AdminEnabled() {
		adminRoot := r.Group("/:admin_entry", func(c *gin.Context) {
			if "/"+c.Param("admin_entry") != mgr.Get().AdminPath() {
				c.AbortWithStatus(http.StatusNotFound)
				return
			}
			c.Set("admin_route", true)
			c.Next()
		})
		adminRoot.GET("/login", h.AdminLoginPage)
		adminRoot.POST("/login", h.AdminLoginSubmit)
		adminRoot.POST("/logout", h.AdminAuth(), h.AdminLogout)

		adm := adminRoot.Group("", h.AdminAuth())
		adm.GET("", h.AdminDashboard)
		adm.GET("/", h.AdminDashboard)
		adm.GET("/operations", h.AdminOperations)
		adm.GET("/orders", h.AdminOrders)
		adm.GET("/orders/:trade_no", h.AdminOrderDetail)
		adm.POST("/orders/:trade_no/close", h.AdminCloseOrder)
		adm.POST("/orders/:trade_no/retry-notify", h.AdminRetryNotify)
		adm.POST("/orders/:trade_no/reconcile", h.AdminReconcilePayment)
		adm.POST("/orders/:trade_no/rescan", h.AdminHistoricalScan)
		adm.POST("/orders/:trade_no/refund-record", h.AdminRecordRefund)
		adm.GET("/integration", h.AdminIntegration)
		adm.POST("/integration", h.AdminIntegrationReveal)
		adm.POST("/integration/token", h.AdminIntegrationReveal)
		adm.POST("/integration/reveal", h.AdminIntegrationReveal)
		adm.GET("/settings", h.AdminSettings)
		adm.POST("/settings", h.AdminSettingsSave)
		adm.POST("/settings/token", h.AdminRotateToken)
		adm.POST("/settings/account", h.AdminAccountSave)
		adm.POST("/settings/site", h.AdminSiteSave)
		slog.Info("后台管理已启用")
	} else {
		slog.Warn("config.yaml 未配置 admin.password，后台管理未启用")
	}

	return r, nil
}

// externalURL 只允许绝对 HTTP(S) 地址进入页面链接和跳转脚本。
func externalURL(raw string) string {
	raw = strings.TrimSpace(raw)
	if strings.ContainsAny(raw, "\r\n\t") {
		return ""
	}
	u, err := url.Parse(raw)
	if err != nil || (u.Scheme != "http" && u.Scheme != "https") || u.Hostname() == "" ||
		u.User != nil || strings.Contains(u.Host, "\\") {
		return ""
	}
	return u.String()
}

// Keep order actions correlatable without recording the cashier access credential.
func tradeLogRef(tradeNo string) string {
	sum := sha256.Sum256([]byte(tradeNo))
	return hex.EncodeToString(sum[:8])
}

// requestLogger 结构化访问日志。
func requestLogger() gin.HandlerFunc {
	return func(c *gin.Context) {
		start := time.Now()
		c.Next()
		if c.Request.URL.Path == "/healthz" {
			return
		}
		slog.Info("HTTP",
			"method", c.Request.Method,
			"path", redactedPath(c),
			"status", c.Writer.Status(),
			"ip", c.ClientIP(),
			"cost", time.Since(start).String(),
		)
	}
}

// redactedPath 收银台 URL 的订单号是访问凭据，不能原样写入日志。
// 已匹配路由记录路由模板；未匹配路径只保留首段。
func redactedPath(c *gin.Context) string {
	if c.GetBool("admin_route") {
		return "/[admin]"
	}
	if pattern := c.FullPath(); pattern != "" {
		return pattern
	}
	return "/[unmatched]"
}
