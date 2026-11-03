// Package server 提供 HTTP 路由与页面渲染。
package server

import (
	"embed"
	"html/template"
	"io/fs"
	"litepay/internal/maintenance"
	"log/slog"
	"net/http"
	"net/url"
	"strings"
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

// New 构建 gin 引擎并注册全部路由。
func New(mgr *settings.Manager, st *store.Store, poller *worker.Poller) (*gin.Engine, error) {
	gin.SetMode(gin.ReleaseMode)

	tmpl, err := template.New("").Funcs(template.FuncMap{
		"externalURL": externalURL,
		"hasStr": func(list []string, v string) bool {
			for _, item := range list {
				if strings.EqualFold(item, v) {
					return true
				}
			}
			return false
		},
		"fmtOrderAmount": formatOrderAmount,
	}).ParseFS(templateFS, "templates/*.html")
	if err != nil {
		return nil, err
	}

	r := gin.New()
	// Proxy headers are ignored unless the deployment explicitly trusts their source.
	if err := r.SetTrustedProxies(mgr.Get().Server.TrustedProxies); err != nil {
		return nil, err
	}
	r.Use(gin.Recovery())
	r.Use(func(c *gin.Context) {
		c.Header("Referrer-Policy", "no-referrer")
		c.Header("X-Content-Type-Options", "nosniff")
		c.Header("X-Frame-Options", "DENY")
		c.Header("Permissions-Policy", "camera=(), microphone=(), geolocation=(), payment=()")
		c.Header("Cache-Control", "no-store")
		if strings.HasPrefix(mgr.Get().Server.BaseURL, "https://") {
			c.Header("Strict-Transport-Security", "max-age=31536000; includeSubDomains")
		}
		c.Request.Body = http.MaxBytesReader(c.Writer, c.Request.Body, 1<<20)
		c.Next()
	})
	r.Use(requestLogger())
	r.SetHTMLTemplate(tmpl)
	brandFS, err := fs.Sub(staticFS, "static/brand")
	if err != nil {
		return nil, err
	}
	r.Use(func(c *gin.Context) {
		if c.Request.URL.Path != "/healthz" && c.Request.URL.Path != "/readyz" && maintenance.Blocked() {
			c.AbortWithStatusJSON(http.StatusServiceUnavailable, gin.H{"error": "正在维护，请稍后重试"})
			return
		}
		c.Next()
	})
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
		cfg:    mgr,
		st:     st,
		log:    slog.Default(),
		poller: poller,
	}
	if cfg.AdminEnabled() {
		h.admin = newAdminAuth(cfg)
	}

	// ---- BEpusdt API（独角数卡收银台模式）----
	r.POST("/api/v1/order/create-order", h.BEpusdtCreateOrder)
	r.POST("/api/v1/order/cancel-transaction", h.BEpusdtCancelOrder)

	// ---- 收银台 ----
	r.GET("/pay/:trade_no", h.CashierPage)

	// ---- 前端订单状态查询接口 ----
	r.GET("/api/order/status", h.OrderStatus)
	r.POST("/api/order/payment", h.SelectPayment)

	// ---- 运维 ----
	r.GET("/healthz", h.Health)
	r.GET("/readyz", h.Ready)
	r.GET("/", h.Index)

	// ---- 后台管理 ----
	if cfg.AdminEnabled() {
		r.GET("/admin/login", h.AdminLoginPage)
		r.POST("/admin/login", h.AdminLoginSubmit)
		r.POST("/admin/logout", h.AdminLogout)

		adm := r.Group("/admin", h.AdminAuth())
		adm.GET("", h.AdminDashboard)
		adm.GET("/orders", h.AdminOrders)
		adm.GET("/orders/:trade_no", h.AdminOrderDetail)
		adm.POST("/orders/:trade_no/action", h.AdminOrderAction)
		adm.GET("/integration", h.AdminIntegration)
		adm.GET("/settings", h.AdminSettings)
		adm.POST("/settings", h.AdminSettingsSave)
		adm.POST("/settings/token", h.AdminRotateToken)
		adm.POST("/settings/account", h.AdminAccountSave)
		slog.Info("后台管理已启用", "url", "/admin", "user", cfg.Admin.Username)
	} else {
		slog.Warn("config.yaml 未配置 admin.password，后台管理未启用")
	}

	return r, nil
}

// externalURL 只允许绝对 HTTP(S) 地址进入页面链接和跳转脚本。
func externalURL(raw string) string {
	raw = strings.TrimSpace(raw)
	u, err := url.Parse(raw)
	if err != nil || (u.Scheme != "http" && u.Scheme != "https") || u.Host == "" ||
		u.User != nil {
		return ""
	}
	return u.String()
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
	if pattern := c.FullPath(); pattern != "" {
		return pattern
	}
	parts := strings.Split(strings.Trim(c.Request.URL.Path, "/"), "/")
	if len(parts) == 1 && parts[0] != "" {
		return "/" + parts[0]
	}
	return "/" + parts[0] + "/*"
}
