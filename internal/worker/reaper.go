package worker

import (
	"context"
	"log/slog"
	"time"

	"litepay/internal/settings"
	"litepay/internal/store"
)

// Reaper 负责订单过期与金额占用清理。
type Reaper struct {
	store *store.Store
	cfg   *settings.Manager
}

// NewReaper 创建清理器。
func NewReaper(st *store.Store, cfg *settings.Manager) *Reaper {
	return &Reaper{store: st, cfg: cfg}
}

// Run 每 20 秒执行一次清理。
func (r *Reaper) Run(ctx context.Context) {
	ticker := time.NewTicker(20 * time.Second)
	defer ticker.Stop()

	r.once(ctx)
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			r.once(ctx)
		}
	}
}

func (r *Reaper) once(ctx context.Context) {
	if n, err := r.store.MarkExpired(ctx); err != nil {
		slog.Error("标记过期订单失败", "err", err)
	} else if n > 0 {
		slog.Info("已将超时订单标记为过期", "count", n)
	}

	grace := time.Duration(r.cfg.Get().Order.SlotGraceSeconds) * time.Second
	if n, err := r.store.FinalizeExpired(ctx, grace); err != nil {
		slog.Error("终结过期订单失败", "err", err)
	} else if n > 0 {
		slog.Info("已结束过期订单，停止确认到账并安排过期通知", "count", n)
	}
}
