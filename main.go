// LitePay —— Binance / OKX 站内转账的 BEpusdt 收银台网关
//
// 作用：
//
//	对 Dujiao Next 暴露 BEpusdt JSON API 和收银台，
//	后台持续确认 Binance 或 OKX 站内转账，按精确金额确认到账，
//	到账后自动向独角数卡发送支付结果通知。
//
// 安全：
//
//	币安 / OKX API Key 仅需读取权限 + IP 白名单，切勿开启交易/提现。
//
// 启动：
//
//	go run . -config config.yaml
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"litepay/internal/config"
	"litepay/internal/server"
	"litepay/internal/settings"
	"litepay/internal/store"
	"litepay/internal/worker"
)

func main() {
	cfgPath := flag.String("config", "config.yaml", "配置文件路径（yaml）")
	flag.Parse()

	cfg, err := config.Load(*cfgPath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "[FATAL] 配置加载失败: %v\n", err)
		os.Exit(1)
	}

	setupLogger(cfg.LogLevel)

	st, err := store.Open(cfg.Database.Path)
	if err != nil {
		slog.Error("数据库初始化失败", "err", err)
		os.Exit(1)
	}

	// 优雅退出：收到 SIGINT/SIGTERM 时取消所有后台协程
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	// 业务配置以 SQLite 为唯一来源，YAML 只提供启动参数。
	mgr := settings.New(cfg, st)
	if err := mgr.Load(context.Background()); err != nil {
		slog.Error("加载后台配置失败", "err", err)
		os.Exit(1)
	}
	if !mgr.Get().BinanceEnabled() && !mgr.Get().OKXEnabled() {
		slog.Warn("收款平台尚未配置完整，请管理后台配置 Binance 或 OKX")
	}
	// 启动横幅必须打印数据库加载后的有效配置；否则后台已配置 OKX 时，
	// 仍会错误显示 okx=false，给排障造成误导。
	banner(mgr.Get())
	poller := worker.NewPoller(mgr, st)
	notifier := worker.NewNotifier(mgr, st)
	reaper := worker.NewReaper(st, mgr)

	go poller.Run(ctx)   // Binance / OKX 到账确认
	go notifier.Run(ctx) // BEpusdt 支付结果通知（含退避重试）
	go reaper.Run(ctx)   // 过期订单与金额占用清理

	engine, err := server.New(mgr, st, poller)
	if err != nil {
		slog.Error("HTTP 服务初始化失败", "err", err)
		os.Exit(1)
	}

	httpSrv := &http.Server{
		Addr:              cfg.Server.Addr,
		Handler:           engine,
		ReadHeaderTimeout: 15 * time.Second,
		ReadTimeout:       30 * time.Second,
		WriteTimeout:      90 * time.Second,
		IdleTimeout:       60 * time.Second,
		MaxHeaderBytes:    64 << 10,
	}

	go func() {
		slog.Info("HTTP 服务已监听", "addr", cfg.Server.Addr, "base_url", cfg.Server.BaseURL)
		if err := httpSrv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			slog.Error("HTTP 服务异常退出", "err", err)
			stop()
		}
	}()

	<-ctx.Done()
	slog.Info("正在关闭网关…")

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := httpSrv.Shutdown(shutdownCtx); err != nil {
		slog.Warn("HTTP 服务关闭超时", "err", err)
	}
	slog.Info("网关已退出")
}

func setupLogger(level string) {
	lv := slog.LevelInfo
	switch strings.ToLower(level) {
	case "debug":
		lv = slog.LevelDebug
	case "warn":
		lv = slog.LevelWarn
	case "error":
		lv = slog.LevelError
	}
	slog.SetDefault(slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: lv})))
}

// banner 打印启动信息（密钥脱敏）。
func banner(cfg *config.Config) {
	slog.Info("========================================")
	slog.Info("LitePay · Binance / OKX 站内转账 BEpusdt 收银台")
	slog.Info("监听地址", "addr", cfg.Server.Addr)
	slog.Info("对外地址", "base_url", cfg.Server.BaseURL)
	slog.Info("BEpusdt API", "mode", "cashier", "token", mask(cfg.BEpusdt.APIToken))
	slog.Info("收款币种", "currencies", strings.Join(cfg.Checkout.Currencies, ","), "fiat", "USD")
	slog.Info("收款平台", "binance", cfg.BinanceEnabled(), "okx", cfg.OKXEnabled())
	if cfg.BinanceEnabled() {
		slog.Info("币安 API", "base", cfg.Binance.BaseURL, "key", mask(cfg.Binance.APIKey),
			"poll_seconds", cfg.ProviderInterval(config.ProviderBinance))
	}
	if cfg.OKXEnabled() {
		slog.Info("OKX API", "base", cfg.OKX.BaseURL, "key", mask(cfg.OKX.APIKey),
			"poll_seconds", cfg.ProviderInterval(config.ProviderOKX),
			"receiver_uid", mask(cfg.Checkout.OKX.UID))
	}
	slog.Info("数据库", "path", cfg.Database.Path)
	slog.Info("========================================")
}

func mask(s string) string {
	if s == "" {
		return "(未配置)"
	}
	if len(s) <= 8 {
		return "****"
	}
	return s[:4] + "****" + s[len(s)-4:]
}
