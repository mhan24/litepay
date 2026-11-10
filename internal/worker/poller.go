package worker

import (
	"context"
	"errors"
	"log/slog"
	"sync"
	"time"

	"litepay/internal/binance"
	"litepay/internal/config"
	"litepay/internal/gram"
	"litepay/internal/model"
	"litepay/internal/okx"
	"litepay/internal/settings"
	"litepay/internal/store"
)

type source interface {
	AccountUID(context.Context) (string, error)
	History(context.Context, time.Time, time.Time) ([]model.Transaction, error)
}
type connection struct {
	config config.Config
	source source
	uid    string
}

// One scheduler owns the shared exchange clients, budgets and account verification.
type Poller struct {
	cfg     *settings.Manager
	st      *store.Store
	mu      sync.Mutex
	clients map[string]*connection
	// 每个平台下一轮补查未确认到账的偏移量，用于有界轮转。
	pendingOffset map[string]int
	GRAM          *gram.Monitor
	// testTransports 仅由端到端测试置位：创建的交易所客户端访问本地模拟服务。
	testTransports bool
}

func NewPoller(cfg *settings.Manager, st *store.Store) *Poller {
	return &Poller{GRAM: gram.NewMonitor(), cfg: cfg, st: st, clients: map[string]*connection{}, pendingOffset: map[string]int{}}
}

// UseTestTransports 只用于端到端测试：调度器创建的交易所客户端只访问本地模拟服务，
// 已缓存的连接一并作废。生产代码不得调用。
func (p *Poller) UseTestTransports() {
	p.mu.Lock()
	p.testTransports = true
	p.clients = map[string]*connection{}
	p.mu.Unlock()
}
func (p *Poller) client(ctx context.Context, cfg *config.Config, provider string) (source, error) {
	if !cfg.ProviderEnabled(provider) {
		return nil, errors.New("付款平台不可用")
	}

	cached := p.clients[provider]
	if cached == nil || cached.config.TON != cfg.TON || cached.config.Binance != cfg.Binance || cached.config.OKX != cfg.OKX || cached.config.Checkout.Binance != cfg.Checkout.Binance || cached.config.Checkout.OKX != cfg.Checkout.OKX {
		var cli source
		if provider == config.ProviderBinance {
			cli = binance.New(cfg.Binance.APIKey, cfg.Binance.APISecret, cfg.Binance.BaseURL, cfg.Binance.RecvWindow, cfg.Binance.Limit)
		} else if provider == config.ProviderTON {
			cli = gram.New(cfg.TON.BaseURL, cfg.TON.APIKey, cfg.TON.Address)
		} else {
			cli = okx.New(cfg.OKX.APIKey, cfg.OKX.APISecret, cfg.OKX.Passphrase, cfg.OKX.BaseURL, cfg.OKX.Limit)
		}
		cached = &connection{config: *cfg, source: cli}
		if p.testTransports {
			if u, ok := cli.(interface{ UseTestTransport() }); ok {
				u.UseTestTransport()
			}
		}
		p.clients[provider] = cached
	}
	if cached.uid == "" {
		uid, err := cached.source.AccountUID(ctx)
		if err != nil {
			return nil, err
		}
		want := cfg.Checkout.Binance.UID
		if provider == config.ProviderTON {
			want = cfg.TON.Address
		}
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
	// Nothing is waiting for a payment, so a history query could only rediscover evidence no
	// order can claim. Skipping keeps the cursor honest: the next scan that is actually due
	// catches up from the last position a successful query really read.
	due, err := p.st.ScanDue(ctx, provider, cfg.ScanRetention())
	if err != nil || !due {
		return err
	}
	ctx, cancel := context.WithTimeout(ctx, 35*time.Second)
	defer cancel()
	end := time.Now()
	coveredFrom := end
	cli, err := p.client(ctx, cfg, provider)
	if err == nil {
		coveredFrom, end, err = p.scanIncremental(ctx, cfg, provider, cli, end)
	}
	// 未确认到账按各自原始创建时间单独补查：它只补充事实、不推进游标，因此旧记录既不会
	// 把正常增量扫描拖回旧窗口，也不会被增量游标越过之后彻底丢失。
	var pendingErr error
	rechecked := err == nil
	if rechecked {
		pendingErr = p.recheckPending(ctx, cfg, provider, cli, coveredFrom, end)
	}
	// Use the parent deadline-independent write to retain a safe diagnostic even after API timeout.
	saveCtx, saveCancel := context.WithTimeout(context.WithoutCancel(ctx), 2*time.Second)
	defer saveCancel()
	if e := p.st.ScanResult(saveCtx, provider, end, err); e != nil {
		return e
	}
	if rechecked {
		// 补查是独立的一路：结果单独记录，不影响扫描游标与订单过期判断。
		if e := p.st.PendingResult(saveCtx, provider, pendingErr); e != nil {
			return e
		}
	}
	if err != nil {
		return err
	}
	return pendingErr
}

// scanIncremental 正常增量扫描：从游标继续读到当前时刻，返回本次真正覆盖到的时间区间。
// 只有完整成功的查询才推进游标；不完整响应返回错误，游标保持原位。
func (p *Poller) scanIncremental(ctx context.Context, cfg *config.Config, provider string, cli source, end time.Time) (time.Time, time.Time, error) {
	cursor, err := p.st.Cursor(ctx, provider)
	if err != nil {
		return end, end, err
	}
	lookback := time.Duration(cfg.ProviderLookback(provider)) * time.Minute
	first := cursor.ScannedUntil.IsZero()
	start := cursor.ScannedUntil.Add(-lookback)
	if first {
		start = end.Add(-cfg.ScanRetention())
	}
	// 仍在等待付款的订单必须被完整覆盖：首次扫描没有任何覆盖，起点可能比最早一张订单的
	// 窗口更晚，必须回溯；平时则跳过更早的空白历史，不重读已有覆盖的部分。
	var earliest model.Order
	if e := p.st.DB().WithContext(ctx).Where("status = ? AND provider = ?", model.Ready, provider).Order("selected_at").First(&earliest).Error; e == nil && earliest.SelectedAt != nil {
		floor := earliest.SelectedAt.Add(-lookback)
		if (first && floor.Before(start)) || (!first && floor.After(start)) {
			start = floor
		}
	}
	if end.Sub(start) > time.Hour {
		end = start.Add(time.Hour)
	}
	rows, err := cli.History(ctx, start, end)
	if err != nil {
		return start, end, err
	}
	markAmbiguous(rows)
	for _, t := range rows {
		if _, err = p.st.Process(ctx, t, ""); err != nil {
			return start, end, err
		}
	}
	return start, end, nil
}

// pendingRecheckLimit 限制单轮补查的未确认到账数量，避免历史堆积拖慢每一次扫描。
const pendingRecheckLimit = 10

// recheckPending 按原始创建时间重读仍处于补查期内的未确认到账。交易所历史按创建时间
// 分页、状态变化不会移动记录，增量游标越过之后就再也读不到，所以这里按记录自身的时间
// 单独查询：窗口固定在这条记录附近，不重扫整段历史，也不推进扫描游标。
// 每轮只取 pendingRecheckLimit 条并轮转偏移量，超出的记录在后续轮次补上，不会被永久跳过。
// 已经落在本轮增量窗口里的记录不再重复请求。
func (p *Poller) recheckPending(ctx context.Context, cfg *config.Config, provider string, cli source, from, to time.Time) error {
	retention := cfg.ScanRetention()
	offset := p.pendingOffset[provider]
	times, err := p.st.PendingTimes(ctx, provider, retention, offset, pendingRecheckLimit)
	if err != nil {
		return err
	}
	if len(times) == 0 && offset > 0 {
		// 上一轮已经读到末尾：回到开头继续轮转。
		offset = 0
		if times, err = p.st.PendingTimes(ctx, provider, retention, 0, pendingRecheckLimit); err != nil {
			return err
		}
	}
	p.pendingOffset[provider] = offset + len(times)
	lookback := time.Duration(cfg.ProviderLookback(provider)) * time.Minute
	var covered time.Time
	for _, at := range times {
		if !at.Before(from) && !at.After(to) {
			continue // 本轮增量扫描已经读过这条记录
		}
		start, stop := at.Add(-lookback), at.Add(lookback)
		if !covered.IsZero() && !start.After(covered) {
			continue // 与上一笔的补查窗口重叠，已在上一次查询中读到
		}
		rows, err := cli.History(ctx, start, stop)
		if err != nil {
			return err
		}
		markAmbiguous(rows)
		for _, t := range rows {
			if _, err = p.st.Process(ctx, t, ""); err != nil {
				return err
			}
		}
		covered = stop
	}
	return nil
}

// Run 每秒推进付款窗口，并按各平台的查询间隔调度扫描。是否需要请求交易所由 Scan
// 自身判断，这里不再重复粗略的“有没有订单”判断。
func (p *Poller) Run(ctx context.Context) {
	monitorCtx, stopMonitor := context.WithCancel(ctx)
	var monitorWG sync.WaitGroup
	monitorWG.Go(func() { p.monitorGRAM(monitorCtx) })
	defer func() { stopMonitor(); monitorWG.Wait() }()

	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	next := map[string]time.Time{}
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			cfg := p.cfg.Get()
			for _, provider := range []string{config.ProviderBinance, config.ProviderOKX, config.ProviderTON} {
				if !time.Now().Before(next[provider]) {
					if err := p.Scan(ctx, provider); err != nil && ctx.Err() == nil {
						slog.Warn("收款扫描未完成", "provider", provider)
					}
					next[provider] = time.Now().Add(time.Duration(cfg.ProviderInterval(provider)) * time.Second)
				}
			}
			if err := p.st.Expire(ctx, cfg.Order.SlotGraceSeconds, cfg.ScanRetention()); err != nil && ctx.Err() == nil {
				slog.Error("订单过期状态写入失败")
			}
		}
	}
}

// Prices and balances keep updating even when no checkout is waiting for payment.
func (p *Poller) monitorGRAM(ctx context.Context) {
	var wg sync.WaitGroup
	for _, balance := range []bool{false, true} {
		wg.Go(func() {
			interval := gram.PriceInterval
			if balance {
				interval = gram.BalanceInterval
			}
			ticker := time.NewTicker(interval)
			defer ticker.Stop()
			var balanceClient *gram.Client
			var balanceConfig config.TONConfig
			for {
				cfg := p.cfg.Get()
				if cfg.TONEnabled() {
					if balance {
						if balanceClient == nil || balanceConfig != cfg.TON {
							balanceClient = gram.New(cfg.TON.BaseURL, cfg.TON.APIKey, cfg.TON.Address)
							balanceConfig = cfg.TON
						}
						_ = p.GRAM.CheckBalance(ctx, balanceClient)
					} else {
						_ = p.GRAM.CheckPrice(ctx)
					}
				}
				select {
				case <-ctx.Done():
					return
				case <-ticker.C:
				}
			}
		})
	}
	wg.Wait()
}
