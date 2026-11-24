package worker

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"runtime/debug"
	"sync"
	"time"

	"gorm.io/gorm"
	"litepay/internal/binance"
	"litepay/internal/bitget"
	"litepay/internal/bybit"
	"litepay/internal/config"
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

// Each provider serializes its own clients, request budgets and account verification.
type Poller struct {
	cfg          *settings.Manager
	st           *store.Store
	mu           sync.Mutex
	scanLocks    map[string]*sync.Mutex
	clients      map[string]*connection
	scannedScope map[string]string
	// 每个平台下一轮补查未确认到账的偏移量，用于有界轮转。
	pendingOffset map[string]int
	// testTransports 仅由端到端测试置位：创建的交易所客户端访问本地模拟服务。
	testTransports bool
}

func NewPoller(cfg *settings.Manager, st *store.Store) *Poller {
	return &Poller{cfg: cfg, st: st, clients: map[string]*connection{}, scannedScope: map[string]string{}, scanLocks: map[string]*sync.Mutex{"binance": {}, "okx": {}, "bybit": {}, "bitget": {}}, pendingOffset: map[string]int{}}
}

// UseTestTransports 只用于端到端测试：调度器创建的交易所客户端只访问本地模拟服务，
// 已缓存的连接一并作废。生产代码不得调用。
func (p *Poller) UseTestTransports() {
	p.mu.Lock()
	p.testTransports = true
	p.clients = map[string]*connection{}
	p.scannedScope = map[string]string{}
	p.mu.Unlock()
}
func providerUID(cfg *config.Config, provider string) string {
	switch provider {
	case config.ProviderBinance:
		return cfg.Checkout.Binance.UID
	case config.ProviderOKX:
		return cfg.Checkout.OKX.UID
	case config.ProviderBybit:
		return cfg.Checkout.Bybit.UID
	case config.ProviderBitget:
		return cfg.Checkout.Bitget.UID
	default:
		return ""
	}
}

func sameProviderClientConfig(provider string, a, b config.Config) bool {
	switch provider {
	case config.ProviderBinance:
		return a.Binance.APIKey == b.Binance.APIKey &&
			a.Binance.APISecret == b.Binance.APISecret &&
			a.Binance.BaseURL == b.Binance.BaseURL &&
			a.Binance.RecvWindow == b.Binance.RecvWindow &&
			a.Binance.Limit == b.Binance.Limit &&
			a.Checkout.Binance.UID == b.Checkout.Binance.UID
	case config.ProviderOKX:
		return a.OKX.APIKey == b.OKX.APIKey &&
			a.OKX.APISecret == b.OKX.APISecret &&
			a.OKX.Passphrase == b.OKX.Passphrase &&
			a.OKX.BaseURL == b.OKX.BaseURL &&
			a.OKX.Limit == b.OKX.Limit &&
			a.Checkout.OKX.UID == b.Checkout.OKX.UID
	case config.ProviderBybit:
		return a.Bybit.APIKey == b.Bybit.APIKey &&
			a.Bybit.APISecret == b.Bybit.APISecret &&
			a.Bybit.BaseURL == b.Bybit.BaseURL &&
			a.Bybit.RecvWindow == b.Bybit.RecvWindow &&
			a.Bybit.Limit == b.Bybit.Limit &&
			a.Checkout.Bybit.UID == b.Checkout.Bybit.UID
	case config.ProviderBitget:
		return a.Bitget.APIKey == b.Bitget.APIKey &&
			a.Bitget.APISecret == b.Bitget.APISecret &&
			a.Bitget.Passphrase == b.Bitget.Passphrase &&
			a.Bitget.BaseURL == b.Bitget.BaseURL &&
			a.Bitget.Limit == b.Bitget.Limit &&
			a.Checkout.Bitget.UID == b.Checkout.Bitget.UID
	default:
		return false
	}
}

func (p *Poller) client(ctx context.Context, cfg *config.Config, provider string) (source, error) {
	if !cfg.ProviderEnabled(provider) {
		return nil, errors.New("付款平台不可用")
	}

	p.mu.Lock()
	cached := p.clients[provider]
	if cached == nil || !sameProviderClientConfig(provider, cached.config, *cfg) {
		var cli source
		switch provider {
		case config.ProviderBinance:
			cli = binance.New(cfg.Binance.APIKey, cfg.Binance.APISecret, cfg.Binance.BaseURL, cfg.Binance.RecvWindow, cfg.Binance.Limit)
		case config.ProviderBybit:
			cli = bybit.New(cfg.Bybit.APIKey, cfg.Bybit.APISecret, cfg.Bybit.BaseURL, cfg.Bybit.RecvWindow, cfg.Bybit.Limit)
		case config.ProviderBitget:
			cli = bitget.New(cfg.Bitget.APIKey, cfg.Bitget.APISecret, cfg.Bitget.Passphrase, cfg.Bitget.BaseURL, cfg.Bitget.Limit)
		case config.ProviderOKX:
			cli = okx.New(cfg.OKX.APIKey, cfg.OKX.APISecret, cfg.OKX.Passphrase, cfg.OKX.BaseURL, cfg.OKX.Limit)
		default:
			p.mu.Unlock()
			return nil, errors.New("付款平台不可用")
		}
		cached = &connection{config: *cfg, source: cli}
		if p.testTransports {
			if u, ok := cli.(interface{ UseTestTransport() }); ok {
				u.UseTestTransport()
			}
		}
		p.clients[provider] = cached
	}
	uid := cached.uid
	src := cached.source
	p.mu.Unlock()
	if uid == "" {
		uid, err := src.AccountUID(ctx)
		if err != nil {
			return nil, err
		}
		want := providerUID(cfg, provider)
		if uid != want {
			return nil, errors.New("API 账户与收款 UID 不一致")
		}
		p.mu.Lock()
		cached.uid = uid
		p.mu.Unlock()
	}
	return src, nil
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
	lock := p.scanLocks[provider]
	if lock == nil {
		return nil
	}
	lock.Lock()
	defer lock.Unlock()
	cfg := p.cfg.Get()
	if cfg.Server.CancelSyncRequired {
		// Runtime heartbeat failures pause instructions and callbacks, but scanning must
		// continue collecting and attributing deposits once restore replay has completed.
		ready, err := p.st.RecoveryReady(ctx)
		if err != nil || !ready {
			return err
		}
	}
	if !cfg.ProviderEnabled(provider) {
		return nil
	}
	historyParent := ctx
	defer func() {
		if historyParent.Err() != nil {
			return
		}
		latest := p.cfg.Get()
		if !latest.ProviderEnabled(provider) {
			return
		}
		if err := p.scanHistorical(historyParent, latest, provider); err != nil && historyParent.Err() == nil {
			slog.Warn("管理员历史补扫未完成", "provider", provider, "error", err)
		}
	}()
	// Active orders and durable confirmation tasks scan frequently; idle accounts still reconcile.
	due, err := p.st.ScanDue(ctx, provider, "")
	if err == nil {
		ready, readyErr := p.ProviderReady(ctx, cfg, provider)
		if readyErr != nil {
			return readyErr
		}
		if !ready {
			due = true
		}
	}
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
	if parentErr := historyParent.Err(); parentErr != nil && err != nil {
		return parentErr
	}
	// Use the parent deadline-independent write to retain a safe diagnostic even after API timeout.
	saveCtx, saveCancel := context.WithTimeout(context.WithoutCancel(ctx), 2*time.Second)
	defer saveCancel()
	if e := p.st.ScanResult(saveCtx, provider, end, err); e != nil {
		return e
	}
	if err == nil {
		p.recordScanScope(cfg, provider)
	}
	if rechecked {
		if parentErr := historyParent.Err(); parentErr != nil && pendingErr != nil {
			return parentErr
		}
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
// Binance Pay 历史无游标、按时间窗口二分拆分且有请求预算：窗口过密时一次 History 会耗尽
// 预算返回 incomplete。这里减半窗口重试，成功后游标只推进到本次真正读完的位置；被跳过的
// 后半段留在游标之后，下一轮从该位置继续扫，不丢记录。
func (p *Poller) scanIncremental(ctx context.Context, cfg *config.Config, provider string, cli source, end time.Time) (time.Time, time.Time, error) {
	start, end, scannedUntil, err := p.scanBounds(ctx, cfg, provider, end)
	if err != nil {
		return start, end, err
	}
	origEnd := end
	for {
		rows, err := cli.History(ctx, start, end)
		if err == nil {
			markAmbiguous(rows)
			for _, t := range rows {
				if _, err = p.st.Process(ctx, t, ""); err != nil {
					return start, end, err
				}
			}
			return start, end, nil
		}
		if !scanOverflow(err) {
			return start, end, err
		}
		if end.Sub(start) <= time.Second {
			if !scannedUntil.IsZero() && start.Before(scannedUntil) && origEnd.After(scannedUntil) {
				start = scannedUntil
				end = origEnd
				continue
			}
			return start, end, err
		}
		nextEnd := start.Add(end.Sub(start) / 2)
		if !scannedUntil.IsZero() && start.Before(scannedUntil) && !nextEnd.After(scannedUntil) && origEnd.After(scannedUntil) {
			start = scannedUntil
			end = origEnd
			continue
		}
		end = nextEnd
	}
}

// scanOverflow 判断是否是“本次窗口数据太密、受请求预算限制”可重试的失败，而不是网络或账户错误。
func scanOverflow(err error) bool {
	var apiErr *binance.APIError
	return errors.As(err, &apiErr) && (apiErr.Kind == binance.ErrorIncomplete || apiErr.Kind == binance.ErrorBudget)
}

// pendingRecheckLimit 限制单轮补查的未确认到账数量，避免历史堆积拖慢每一次扫描。
const pendingRecheckLimit = 10

// recheckPending 按原始创建时间重读仍处于补查期内的未确认到账。交易所历史按创建时间
// 分页、状态变化不会移动记录，增量游标越过之后就再也读不到，所以这里按记录自身的时间
// 单独查询：窗口固定在这条记录附近，不重扫整段历史，也不推进扫描游标。
// 每轮只取 pendingRecheckLimit 条并轮转偏移量，超出的记录在后续轮次补上，不会被永久跳过。
// 已经落在本轮增量窗口里的记录不再重复请求。
func (p *Poller) recheckPending(ctx context.Context, cfg *config.Config, provider string, cli source, from, to time.Time) error {
	p.mu.Lock()
	offset := p.pendingOffset[provider]
	p.mu.Unlock()
	times, err := p.st.PendingTimes(ctx, provider, offset, pendingRecheckLimit)
	if err != nil {
		return err
	}
	if len(times) == 0 && offset > 0 {
		// 上一轮已经读到末尾：回到开头继续轮转。
		offset = 0
		if times, err = p.st.PendingTimes(ctx, provider, 0, pendingRecheckLimit); err != nil {
			return err
		}
	}
	processed := 0
	defer func() {
		p.mu.Lock()
		p.pendingOffset[provider] = offset + processed
		p.mu.Unlock()
	}()
	lookback := time.Duration(cfg.ProviderLookback(provider)) * time.Minute
	var firstErr error
	for _, at := range times {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if !at.Before(from) && !at.After(to) {
			processed++
			continue // 本轮增量扫描已经读过这条记录
		}
		radius := lookback
		var rows []model.Transaction
		var itemErr error
		for {
			start, stop := at.Add(-radius), at.Add(radius)
			rows, itemErr = cli.History(ctx, start, stop)
			if itemErr == nil {
				break
			}
			if ctx.Err() != nil {
				return ctx.Err()
			}
			if !scanOverflow(itemErr) || radius <= time.Second {
				break
			}
			radius /= 2
			if radius < time.Second {
				radius = time.Second
			}
		}
		if itemErr != nil {
			if firstErr == nil {
				firstErr = itemErr
			}
			processed++
			continue
		}
		markAmbiguous(rows)
		for _, t := range rows {
			if _, procErr := p.st.Process(ctx, t, ""); procErr != nil {
				if ctx.Err() != nil {
					return ctx.Err()
				}
				if firstErr == nil {
					firstErr = procErr
				}
				break
			}
		}
		processed++
	}
	return firstErr
}

// Run 每秒推进付款窗口，并按各平台的查询间隔调度扫描。是否需要请求交易所由 Scan
// 自身判断，这里不再重复粗略的“有没有订单”判断。
func (p *Poller) Run(ctx context.Context) {
	var workers sync.WaitGroup
	for _, provider := range []string{config.ProviderBinance, config.ProviderOKX, config.ProviderBybit, config.ProviderBitget} {
		workers.Go(func() {
			for {
				if ctx.Err() != nil {
					return
				}
				if err := p.scanOnceSafe(ctx, provider); err != nil && ctx.Err() == nil {
					slog.Warn("收款扫描未完成", "provider", provider, "error", err)
				}
				interval := max(p.cfg.Get().ProviderInterval(provider), 1)
				timer := time.NewTimer(time.Duration(interval) * time.Second)
				select {
				case <-ctx.Done():
					timer.Stop()
					return
				case <-timer.C:
				}
			}
		})
	}
	defer workers.Wait()
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if err := p.expireOnceSafe(ctx); err != nil && ctx.Err() == nil {
				slog.Error("订单过期状态写入失败", "error", err)
			}
		}
	}
}

func (p *Poller) scanOnceSafe(ctx context.Context, provider string) (err error) {
	defer func() {
		if r := recover(); r != nil {
			slog.Error("收款扫描发生异常", "provider", provider, "panic", fmt.Sprint(r), "stack", string(debug.Stack()))
			err = fmt.Errorf("scan panic: %v", r)
		}
	}()
	return p.Scan(ctx, provider)
}

func (p *Poller) expireOnceSafe(ctx context.Context) (err error) {
	defer func() {
		if r := recover(); r != nil {
			slog.Error("订单过期检查发生异常", "panic", fmt.Sprint(r), "stack", string(debug.Stack()))
			err = fmt.Errorf("expire panic: %v", r)
		}
	}()
	if p.cfg.Get().Server.CancelSyncRequired {
		ready, err := p.st.RecoveryReady(ctx)
		if err != nil || !ready {
			return err
		}
	}
	return p.st.Expire(ctx, p.cfg.Get().Order.SlotGraceSeconds)
}

// scanBounds never skips a previously unscanned gap, including time after cancellation.
func (p *Poller) scanBounds(ctx context.Context, cfg *config.Config, provider string, end time.Time) (time.Time, time.Time, time.Time, error) {
	cursor, err := p.st.Cursor(ctx, provider)
	if err != nil {
		return end, end, time.Time{}, err
	}
	lookback := time.Duration(cfg.ProviderLookback(provider)) * time.Minute
	first := cursor.ScannedUntil.IsZero()
	start := cursor.ScannedUntil.Add(-lookback)
	if first {
		start = end.Add(-cfg.InitialScanLookback())
	}
	// 仍在等待付款的订单必须被完整覆盖：首次扫描没有任何覆盖，起点可能比最早一张订单的
	// 窗口更晚，必须回溯；已有游标时始终覆盖其后的全部对账历史。
	var earliest model.PaymentAttempt
	e := p.st.DB().WithContext(ctx).Table("payment_attempts a").Select("a.*").Joins("JOIN orders o ON o.id=a.order_id").Where("o.status = ? AND a.provider = ?", model.Ready, provider).Order("a.selected_at").First(&earliest).Error
	if e != nil && !errors.Is(e, gorm.ErrRecordNotFound) {
		return end, end, cursor.ScannedUntil, e
	}
	if e == nil {
		floor := earliest.SelectedAt.Add(-lookback)
		if first && floor.Before(start) {
			start = floor
		}
	}
	if start.After(end) {
		start = end.Add(-lookback)
	}
	if first || cursor.ScannedUntil.After(end) {
		if end.Sub(start) > time.Hour {
			end = start.Add(time.Hour)
		}
	} else if end.Sub(cursor.ScannedUntil) > time.Hour {
		end = cursor.ScannedUntil.Add(time.Hour)
	}
	return start, end, cursor.ScannedUntil, nil
}

func (p *Poller) BinanceUsage() binance.Usage {
	p.mu.Lock()
	defer p.mu.Unlock()
	if conn := p.clients[config.ProviderBinance]; conn != nil {
		if cli, ok := conn.source.(*binance.Client); ok {
			return cli.Usage()
		}
	}
	return binance.Usage{}
}
