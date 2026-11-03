// LitePay: a development-stage BEpusdt gateway for Binance / OKX internal payments.
package main

import (
	"context"
	"errors"
	"flag"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"time"

	"litepay/internal/config"
	"litepay/internal/server"
	"litepay/internal/settings"
	"litepay/internal/store"
	"litepay/internal/worker"
)

func main() {
	if err := run(); err != nil {
		slog.Error("LitePay 停止", "error", err)
		os.Exit(1)
	}
}
func run() error {
	cfgPath := flag.String("config", "config.yaml", "启动配置路径")
	flag.Parse()
	// SQLite and configuration files created by this process are private by default.
	syscall.Umask(0077)
	cfg, err := config.Load(*cfgPath)
	if err != nil {
		return err
	}
	level := slog.LevelInfo
	switch strings.ToLower(cfg.LogLevel) {
	case "debug":
		level = slog.LevelDebug
	case "warn":
		level = slog.LevelWarn
	case "error":
		level = slog.LevelError
	}
	slog.SetDefault(slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: level})))
	st, err := store.Open(cfg.Database.Path)
	if err != nil {
		return err
	}
	defer st.Close()
	mgr := settings.New(cfg, st)
	if err = mgr.Load(context.Background()); err != nil {
		return err
	}
	poller := worker.NewPoller(mgr, st)
	notifier := worker.NewNotifier(mgr, st)
	engine, err := server.New(mgr, st, poller)
	if err != nil {
		return err
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	var workers sync.WaitGroup
	for _, run := range []func(context.Context){poller.Run, notifier.Run} {
		workers.Go(func() { run(ctx) })
	}
	srv := &http.Server{Addr: cfg.Server.Addr, Handler: engine, ReadHeaderTimeout: 10 * time.Second, ReadTimeout: 20 * time.Second, WriteTimeout: 45 * time.Second, IdleTimeout: 60 * time.Second, MaxHeaderBytes: 64 << 10}
	failures := make(chan error, 1)
	go func() {
		err := srv.ListenAndServe()
		if !errors.Is(err, http.ErrServerClosed) {
			failures <- err
		}
		stop()
	}()
	<-ctx.Done()
	shutdown, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	err = srv.Shutdown(shutdown)
	workers.Wait()
	select {
	case failure := <-failures:
		return failure
	default:
		return err
	}
}
