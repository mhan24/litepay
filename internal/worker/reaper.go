package worker

import (
	"context"
	"log/slog"
	"time"

	"litepay/internal/maintenance"
	"litepay/internal/settings"
	"litepay/internal/store"
)

// Reaper 负责订单过期与金额占用清理。
type Reaper struct {
	store *store.Store
	cfg   *settings.Manager

	// lastPrune 记录上次清理观察账本的时间，避免每轮都执行删除。
	lastPrune time.Time
}

// observationRetention 未归属流水账本的保留期。账本只服务于近期漏单排查，
// 长期运行时不能无限增长；已归属记录同样到期清理。
const observationRetention = 30 * 24 * time.Hour

// observationPruneInterval 清理观察账本的最小间隔。
const observationPruneInterval = time.Hour

// NewReaper 创建清理器。
func NewReaper(st *store.Store, cfg *settings.Manager) *Reaper {
	return &Reaper{store: st, cfg: cfg}
}

// Run 每 20 秒执行一次清理。
func (r *Reaper) Run(ctx context.Context) {
	ticker := time.NewTicker(20 * time.Second)
	defer ticker.Stop()

	if !maintenance.Blocked() {
		r.once(ctx)
	}
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if maintenance.Blocked() {
				continue
			}
			r.once(ctx)
		}
	}
}

func (r *Reaper) once(ctx context.Context) {
	ok := true
	defer func() { r.store.WorkerResult("reaper", ok) }()
	if n, err := r.store.MarkExpired(ctx); err != nil {
		ok = false
		slog.Error("标记过期订单失败", "err", err)
	} else if n > 0 {
		slog.Info("已将超时订单标记为过期", "count", n)
	}

	grace := time.Duration(r.cfg.Get().Order.SlotGraceSeconds) * time.Second
	if n, err := r.store.FinalizeExpired(ctx, grace); err != nil {
		ok = false
		slog.Error("终结过期订单失败", "err", err)
	} else if n > 0 {
		slog.Info("已结束过期订单，停止确认到账并安排过期通知", "count", n)
	}

	r.pruneObservations(ctx)
}

// pruneObservations 按保留期清理未归属流水账本，每小时最多执行一次。
func (r *Reaper) pruneObservations(ctx context.Context) {
	if !r.lastPrune.IsZero() && time.Since(r.lastPrune) < observationPruneInterval {
		return
	}
	n, err := r.store.PruneTxObservations(ctx, time.Now().Add(-observationRetention))
	if err != nil {
		slog.Error("清理未归属流水账本失败", "err", err)
		return
	}
	r.lastPrune = time.Now()
	if n > 0 {
		slog.Info("已清理超过保留期的未归属流水记录", "count", n,
			"retention_days", int(observationRetention.Hours()/24))
	}
}
