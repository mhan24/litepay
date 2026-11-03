package worker

import (
	"context"
	"errors"
	"log/slog"
	"sync"
	"time"

	"litepay/internal/binance"
	"litepay/internal/config"
	"litepay/internal/model"
	"litepay/internal/okx"
	"litepay/internal/settings"
	"litepay/internal/store"
)

type source interface {
	AccountUID(context.Context) (string, error)
	History(context.Context, time.Time, time.Time) ([]model.Transaction, error)
	Lookup(context.Context, string, time.Time, time.Time) ([]model.Transaction, error)
}
type connection struct {
	config config.Config
	source source
	uid    string
}

// One scheduler owns the shared exchange clients, budgets and account verification.
// An inquiry never queues ahead of an active scan.
type Poller struct {
	cfg     *settings.Manager
	st      *store.Store
	mu      sync.Mutex
	clients map[string]*connection
}

func NewPoller(cfg *settings.Manager, st *store.Store) *Poller {
	return &Poller{cfg: cfg, st: st, clients: map[string]*connection{}}
}
func (p *Poller) client(ctx context.Context, cfg *config.Config, provider string) (source, error) {
	if !cfg.ProviderEnabled(provider) {
		return nil, errors.New("付款平台不可用")
	}

	cached := p.clients[provider]
	if cached == nil || cached.config.Binance != cfg.Binance || cached.config.OKX != cfg.OKX || cached.config.Checkout.Binance != cfg.Checkout.Binance || cached.config.Checkout.OKX != cfg.Checkout.OKX {
		var cli source
		if provider == config.ProviderBinance {
			cli = binance.New(cfg.Binance.APIKey, cfg.Binance.APISecret, cfg.Binance.BaseURL, cfg.Binance.RecvWindow, cfg.Binance.Limit)
		} else {
			cli = okx.New(cfg.OKX.APIKey, cfg.OKX.APISecret, cfg.OKX.Passphrase, cfg.OKX.BaseURL, cfg.OKX.Limit)
		}
		cached = &connection{config: *cfg, source: cli}
		p.clients[provider] = cached
	}
	if cached.uid == "" {
		uid, err := cached.source.AccountUID(ctx)
		if err != nil {
			return nil, err
		}
		want := cfg.Checkout.Binance.UID
		if provider == config.ProviderOKX {
			want = cfg.Checkout.OKX.UID
		}
		if uid != want {
			return nil, errors.New("API 账户与收款 UID 不一致")
		}
		cached.uid = uid
	}
	return cached.source, nil
}

// duplicate external references are evidence of ambiguity, not separate payable orders.
func markAmbiguous(rows []model.Transaction) {
	refs := map[string]int{}
	for _, t := range rows {
		if t.Reference != "" {
			refs[t.Reference]++
		}
	}
	for i := range rows {
		if refs[rows[i].Reference] > 1 {
			rows[i].State = model.Ambiguous
			rows[i].Reason = "duplicate_reference"
		}
	}
}
func (p *Poller) Scan(ctx context.Context, provider string) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	cfg := p.cfg.Get()
	if !cfg.ProviderEnabled(provider) {
		return nil
	}
	ctx, cancel := context.WithTimeout(ctx, 35*time.Second)
	defer cancel()
	end := time.Now()
	cli, err := p.client(ctx, cfg, provider)
	if err == nil {
		var cursor model.ScanCursor
		cursor, err = p.st.Cursor(ctx, provider)
		if err == nil {
			start := cursor.ScannedUntil.Add(-time.Duration(cfg.ProviderLookback(provider)) * time.Minute)
			if cursor.ScannedUntil.IsZero() {
				start = end.Add(-time.Duration(cfg.Order.ExpireMinutes+2) * time.Minute)
				var earliest model.Order
				if e := p.st.DB().WithContext(ctx).Where("status = ? AND provider = ?", model.Ready, provider).Order("selected_at").First(&earliest).Error; e == nil && earliest.SelectedAt != nil && earliest.SelectedAt.Before(start) {
					start = earliest.SelectedAt.Add(-time.Minute)
				}
			}
			if end.Sub(start) > time.Hour {
				end = start.Add(time.Hour)
			}
			var rows []model.Transaction
			rows, err = cli.History(ctx, start, end)
			if err == nil {
				markAmbiguous(rows)
				for _, t := range rows {
					if _, err = p.st.Process(ctx, t, ""); err != nil {
						break
					}
				}
			}
		}
	}
	// Use the parent deadline-independent write to retain a safe diagnostic even after API timeout.
	saveCtx, saveCancel := context.WithTimeout(context.WithoutCancel(ctx), 2*time.Second)
	defer saveCancel()
	if e := p.st.ScanResult(saveCtx, provider, end, err); e != nil {
		return e
	}
	return err
}
func (p *Poller) Inquiry(ctx context.Context, no, ref string) (string, error) {
	if len(ref) < 1 || len(ref) > 128 {
		return "", errors.New("付款编号长度无效")
	}
	if !p.mu.TryLock() {
		return "", errors.New("正在扫描，请稍后查款")
	}
	defer p.mu.Unlock()
	o, err := p.st.ReserveInquiry(ctx, no, ref)
	if err != nil {
		return "", err
	}
	result := "无法确认该编号，请核对收款平台、编号和订单信息"
	defer func() {
		save, cancel := context.WithTimeout(context.WithoutCancel(ctx), 2*time.Second)
		defer cancel()
		_ = p.st.InquiryResult(save, no, result)
	}()
	ctx, cancel := context.WithTimeout(ctx, 8*time.Second)
	defer cancel()
	cli, err := p.client(ctx, p.cfg.Get(), o.Provider)
	if err != nil {
		result = "交易所查询失败，未确认付款"
		return result, err
	}
	rows, err := cli.Lookup(ctx, ref, *o.SelectedAt, minTime(o.ExpiresAt, time.Now()))
	if err != nil {
		result = "交易所查询未完整完成，未确认付款"
		return result, err
	}
	if len(rows) != 1 {
		if len(rows) > 1 {
			for _, t := range rows {
				t.State = model.Ambiguous
				t.Reason = "multiple_reference_candidates"
				if _, e := p.st.Process(ctx, t, no); e != nil {
					return result, e
				}
			}
			result = "编号对应多笔资金记录，需要人工核对"
		}
		return result, nil
	}
	outcome, err := p.st.Process(ctx, rows[0], no)
	if err != nil {
		return result, err
	}
	switch outcome {
	case "paid", "already_claimed":
		result = "已确认到账，正在通知商户"
	case "other_order":
		result = "该交易不属于当前订单"
	default:
		result = "交易未满足当前订单的付款条件，需要核对"
	}
	return result, nil
}
func minTime(a, b time.Time) time.Time {
	if a.Before(b) {
		return a
	}
	return b
}
func (p *Poller) Run(ctx context.Context) {
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	next := map[string]time.Time{}
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			for _, provider := range []string{config.ProviderBinance, config.ProviderOKX} {
				if !time.Now().Before(next[provider]) {
					if err := p.Scan(ctx, provider); err != nil && ctx.Err() == nil {
						slog.Warn("收款扫描未完成", "provider", provider)
					}
					interval := p.cfg.Get().ProviderInterval(provider)
					var active int64
					if e := p.st.DB().WithContext(ctx).Model(&model.Order{}).Where("provider = ? AND status = ?", provider, model.Ready).Count(&active).Error; e == nil && active == 0 {
						interval = max(interval, 30)
					}
					next[provider] = time.Now().Add(time.Duration(interval) * time.Second)
				}
			}
			if err := p.st.Expire(ctx, p.cfg.Get().Order.SlotGraceSeconds); err != nil && ctx.Err() == nil {
				slog.Error("订单过期状态写入失败")
			}
		}
	}
}
