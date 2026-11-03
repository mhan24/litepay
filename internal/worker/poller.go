package worker

import (
	"context"
	"fmt"
	"log/slog"
	"net"
	"net/url"
	"sort"
	"strings"
	"sync"
	"time"

	"litepay/internal/amount"
	"litepay/internal/binance"
	"litepay/internal/config"
	"litepay/internal/model"
	"litepay/internal/okx"
	"litepay/internal/settings"
	"litepay/internal/store"
)

// PollStats 到账确认运行状态快照（供后台展示）。
type PollStats struct {
	Enabled       bool      `json:"enabled"`
	Provider      string    `json:"provider"`
	LastRunAt     time.Time `json:"last_run_at"`
	LastOKAt      time.Time `json:"last_ok_at"`
	LastError     string    `json:"last_error"`
	LastFetched   int       `json:"last_fetched"`
	LastMatched   int       `json:"last_matched"`
	SkipOutgoing  int       `json:"skip_outgoing"`
	SkipType      int       `json:"skip_type"`
	SkipReceiver  int       `json:"skip_receiver"`
	SkipAmount    int       `json:"skip_amount"`
	SkipDuplicate int       `json:"skip_duplicate"` // 已入账过的流水再次出现在历史窗口
	SkipCurrency  int       `json:"skip_currency"`
	SkipState     int       `json:"skip_state"`
	TotalMatched  int64     `json:"total_matched"`
	TotalRuns     int64     `json:"total_runs"`
	LastIntervalS int       `json:"last_interval_s"`
}

// PollerStats 两条通道各自的运行状态。
type PollerStats struct {
	Binance PollStats `json:"binance"`
	OKX     PollStats `json:"okx"`
}

type incomingTransaction struct {
	ID           string
	Type         string
	Amount       float64
	Currency     string
	Time         time.Time
	ReceiverID   string
	ReceiverAddr string
	Incoming     bool
	Raw          string
}

type transactionRejectReason int

const (
	transactionAccepted transactionRejectReason = iota
	transactionOutgoing
	transactionType
	transactionCurrency
	transactionReceiver
)

// Poller 同时确认 Binance 和 OKX 到账；每笔订单只按自己选择的平台确认。
type Poller struct {
	cfg   *settings.Manager
	store *store.Store

	lastRuns map[string]time.Time

	binanceClient      *binance.Client
	binanceKey         string
	binanceSecret      string
	binanceBase        string
	binanceUID         string
	binanceVerifiedUID string
	binanceUIDVerified bool
	binanceUIDRetryAt  time.Time

	okxClient      *okx.Client
	okxKey         string
	okxSecret      string
	okxPassphrase  string
	okxBase        string
	okxVerifiedUID string
	okxUIDVerified bool
	okxUIDRetryAt  time.Time

	statsMu sync.RWMutex
	stats   PollerStats
}

// NewPoller 创建到账确认器。
func NewPoller(cfg *settings.Manager, st *store.Store) *Poller {
	return &Poller{
		cfg: cfg, store: st,
		lastRuns: make(map[string]time.Time),
	}
}

// StatsFor 返回指定通道状态。
func (p *Poller) StatsFor(provider string) PollStats {
	p.statsMu.RLock()
	defer p.statsMu.RUnlock()
	cfg := p.cfg.Get()
	if strings.EqualFold(provider, config.ProviderOKX) {
		s := p.stats.OKX
		s.Enabled, s.Provider = cfg.OKXEnabled(), config.ProviderOKX
		return s
	}
	s := p.stats.Binance
	s.Enabled, s.Provider = cfg.BinanceEnabled(), config.ProviderBinance
	return s
}

func (p *Poller) setStats(provider string, fn func(*PollStats)) {
	p.statsMu.Lock()
	if strings.EqualFold(provider, config.ProviderOKX) {
		fn(&p.stats.OKX)
	} else {
		fn(&p.stats.Binance)
	}
	p.statsMu.Unlock()
}

// Run 启动到账确认循环，两个收款平台各自按配置间隔查询。
func (p *Poller) Run(ctx context.Context) {
	c0 := p.cfg.Get()
	slog.Info("到账确认已启动", "binance", c0.BinanceEnabled(),
		"okx", c0.OKXEnabled(), "currencies", strings.Join(c0.Checkout.Currencies, ","))

	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			slog.Info("到账确认已停止")
			return
		case now := <-ticker.C:
			cfg := p.cfg.Get()
			for _, provider := range []string{config.ProviderBinance, config.ProviderOKX} {
				if !cfg.ProviderEnabled(provider) {
					continue
				}
				orders, err := p.store.MatchableOrders(ctx, provider,
					time.Duration(cfg.Order.SlotGraceSeconds)*time.Second)
				if err != nil {
					slog.Error("查询待确认订单失败", "err", err)
					continue
				}
				// 没有待确认订单时降频扫描，而不是完全停扫：宕机或交易所
				// API 延迟期间的迟到流水也要能被发现并留痕。
				interval := time.Duration(cfg.ProviderInterval(provider)) * time.Second
				if !hasOrdersForProvider(orders, provider) {
					interval = idleScanInterval
				}
				if now.Sub(p.lastRuns[provider]) < interval {
					continue
				}
				p.lastRuns[provider] = now
				p.tick(ctx, cfg, provider)
			}
		}
	}
}

func hasOrdersForProvider(orders []model.Order, provider string) bool {
	for i := range orders {
		if strings.EqualFold(orders[i].Collection, provider) {
			return true
		}
	}
	return false
}

const (
	// scanOverlap 相邻两次扫描的重叠量，覆盖交易所侧延迟入账的流水。
	scanOverlap = 2 * time.Minute
	// maxScanWindow 单轮补扫的最大跨度；更久的历史按轮次分块追赶。
	maxScanWindow = 6 * time.Hour
	// maxScanBackfill 补扫的最远回溯，避免游标损坏后拉取过久历史。
	maxScanBackfill = 7 * 24 * time.Hour
	// idleScanInterval 没有待确认订单时的扫描间隔，用于发现迟到流水。
	idleScanInterval = 2 * time.Minute
	// scanClockSkew 容忍交易所时间略超前于本机时钟，避免把刚到的流水
	// 判成“未来”而丢弃。
	scanClockSkew = 2 * time.Minute
)

// scanWindow 是一次交易所查询覆盖的时间区间。
type scanWindow struct{ start, end time.Time }

// scanPlan 是一轮扫描要覆盖的窗口。
type scanPlan struct {
	recent  scanWindow
	backlog *scanWindow
	gap     *scanWindow
}

// newScanPlan 依据待确认订单和持久化游标计算本轮扫描窗口。
//
// 待确认订单给出“现在必须实时确认”的区间；游标给出“上次已经扫到哪里”。
// 两者取更早者，重启或交易所 API 故障后才不会留下永久空洞。
func newScanPlan(orders []model.Order, cursor *model.ScanCursor, lookback time.Duration, now time.Time) scanPlan {
	recentStart := now.Add(-lookback)
	if len(orders) > 0 {
		recentStart = orders[0].CreatedAt.Add(-lookback)
	}
	// Bound realtime API work even when an unresolved order is very old.
	oldestNeeded := recentStart
	if floor := now.Add(-maxScanWindow); recentStart.Before(floor) {
		recentStart = floor
	}
	start := oldestNeeded
	if cursor != nil && !cursor.ScannedUntil.IsZero() {
		if from := cursor.ScannedUntil.Add(-scanOverlap); from.Before(start) {
			start = from
		}
	}
	plan := scanPlan{recent: scanWindow{start: recentStart, end: now}}
	if floor := now.Add(-maxScanBackfill); start.Before(floor) {
		plan.gap = &scanWindow{start: start, end: floor}
		start = floor
	}
	if end := start.Add(maxScanWindow); end.Before(recentStart) {
		// 落后超过一个窗口的跨度：先补最老的一段，剩下的交给后续轮次。
		plan.backlog = &scanWindow{start: start, end: end}
	} else if start.Before(recentStart) {
		plan.recent.start = start
	}
	return plan
}

// windows 返回本轮要扫描的窗口，积压窗口在前。
func (p scanPlan) windows() []scanWindow {
	if p.backlog != nil {
		return []scanWindow{*p.backlog, p.recent}
	}
	return []scanWindow{p.recent}
}

// advance 返回扫描成功后游标应推进到的时间点。
//
// 存在积压时只推进到积压窗口末尾：否则中间那段未扫描的时间会被永久跳过。
func (p scanPlan) advance() time.Time {
	if p.backlog != nil {
		return p.backlog.end
	}
	return p.recent.end
}

// scanTotals 汇总一轮扫描（可能覆盖多个窗口）的计数。
type scanTotals struct {
	fetched  int
	matched  int
	skipOut  int
	skipType int
	skipRecv int
	skipAmt  int
	skipCur  int
	skipDup  int
}

func (p *Poller) tick(ctx context.Context, cfg *config.Config, provider string) {
	p.setStats(provider, func(s *PollStats) {
		s.LastRunAt = time.Now()
		s.TotalRuns++
		s.LastIntervalS = cfg.ProviderInterval(provider)
		s.Provider = provider
	})

	grace := time.Duration(cfg.Order.SlotGraceSeconds) * time.Second
	orders, err := p.store.MatchableOrders(ctx, provider, grace)
	if err != nil {
		slog.Error("查询待确认订单失败", "err", err)
		return
	}
	// 只保留当前通道已选中的订单。收银台尚未选择的订单不参与任何通道。
	filtered := orders[:0]
	for _, o := range orders {
		if strings.EqualFold(o.Collection, provider) {
			filtered = append(filtered, o)
		}
	}
	orders = filtered

	cursor, err := p.store.ScanCursor(ctx, provider)
	if err != nil {
		p.failScan(ctx, provider, fmt.Errorf("读取扫描进度失败: %w", err))
		return
	}

	lookback := time.Duration(cfg.ProviderLookback(provider)) * time.Minute
	plan := newScanPlan(orders, cursor, lookback, time.Now())

	if plan.gap != nil {
		if err := p.store.RecordScanGap(ctx, provider, plan.gap.start, plan.gap.end); err != nil {
			p.failScan(ctx, provider, err)
			return
		}
	}
	book := newTxObservationBook(provider)
	var totals scanTotals
	for _, w := range plan.windows() {
		if err := p.scanWindow(ctx, cfg, provider, orders, w, book, &totals); err != nil {
			p.failScan(ctx, provider, err)
			return
		}
	}

	for _, obs := range book.pending() {
		if err := p.store.RecordTxObservation(ctx, obs); err != nil {
			p.failScan(ctx, provider, fmt.Errorf("保存交易观察失败: %w", err))
			return
		}
	}
	// 只有整轮扫描全部成功才推进游标，失败时保持原地等待下一轮补扫。
	if err := p.store.SaveScanCursor(ctx, provider, plan.advance()); err != nil {
		p.failScan(ctx, provider, fmt.Errorf("保存扫描进度失败: %w", err))
		return
	}

	p.setStats(provider, func(s *PollStats) {
		s.LastOKAt = time.Now()
		s.LastError = ""
		if plan.gap != nil || (cursor != nil && cursor.GapEnd != nil) {
			s.LastError = "存在超出自动回溯范围的历史缺口，需人工对账"
		}
		s.Provider = provider
		s.LastFetched = totals.fetched
		s.LastMatched = totals.matched
		s.TotalMatched += int64(totals.matched)
		s.SkipOutgoing = totals.skipOut
		s.SkipType = totals.skipType
		s.SkipReceiver = totals.skipRecv
		s.SkipAmount = totals.skipAmt
		s.SkipCurrency = totals.skipCur
		s.SkipDuplicate = totals.skipDup
	})

	if totals.fetched > 0 {
		slog.Info("到账扫描完成", "provider", provider, "fetched", totals.fetched,
			"matched", totals.matched, "skip_outgoing", totals.skipOut,
			"skip_type", totals.skipType, "skip_currency", totals.skipCur,
			"skip_receiver", totals.skipRecv, "skip_amount", totals.skipAmt,
			"skip_duplicate", totals.skipDup)
	}
}

// failScan 记录一次失败的扫描：游标不推进，失败原因落库供后台排查。
func (p *Poller) failScan(ctx context.Context, provider string, err error) {
	p.setStats(provider, func(s *PollStats) { s.LastError = err.Error(); s.Provider = provider })
	slog.Warn("查询到账记录失败", "provider", provider, "err", err)
	if saveErr := p.store.SaveScanError(ctx, provider, err.Error()); saveErr != nil {
		slog.Warn("记录扫描失败原因失败", "provider", provider, "err", saveErr)
	}
}

// scanWindow 查询并处理一个时间窗口内的流水。
func (p *Poller) scanWindow(ctx context.Context, cfg *config.Config, provider string,
	orders []model.Order, w scanWindow, book *txObservationBook, totals *scanTotals) error {

	var txs []incomingTransaction
	var err error
	if provider == config.ProviderOKX {
		txs, err = p.okxTransactions(ctx, cfg, w.start, w.end)
	} else {
		txs, err = p.binanceTransactions(ctx, cfg, w.start, w.end)
	}
	if err != nil {
		return err
	}

	for _, tx := range txs {
		// 交易所可能忽略时间参数返回窗口外的流水，不能算进本轮结论。
		// 上界放宽一个时钟偏移量，避免交易所时间略超前时丢掉刚到的付款。
		if tx.Time.Before(w.start) || tx.Time.After(w.end.Add(scanClockSkew)) {
			continue
		}
		totals.fetched++

		reject := rejectTransaction(provider, cfg, &tx)
		if reject != transactionAccepted {
			switch reject {
			case transactionOutgoing:
				totals.skipOut++
			case transactionType:
				totals.skipType++
			case transactionCurrency:
				totals.skipCur++
			case transactionReceiver:
				totals.skipRecv++
			}
			// 支出类流水不属于潜在漏单证据，不入账本；类型/币种/收款人被拒
			// 的流水（如走了链上充值、付错币种）必须留痕。
			if reject != transactionOutgoing {
				book.observe(&tx, observationReason(reject), "")
			}
			continue
		}

		hit := false
		resolvedTradeNo := ""
		for i := range orders {
			o := &orders[i]
			if o.Status == model.StatusPaid {
				continue
			}
			if !strings.EqualFold(o.Currency, tx.Currency) {
				continue
			}
			if !orderMatchesTransaction(o, &tx, cfg.Order.AmountPrecision) {
				continue
			}
			if tx.Time.Before(o.CreatedAt) || tx.Time.After(o.ExpiredAt) {
				continue
			}

			ok, err := p.store.ClaimAndPay(ctx, o, model.IncomingTx{
				TransactionID:   tx.ID,
				Provider:        provider,
				Type:            tx.Type,
				Amount:          tx.Amount,
				Currency:        tx.Currency,
				ReceiverID:      tx.ReceiverID,
				ReceiverAddr:    tx.ReceiverAddr,
				TransactionTime: tx.Time,
				Raw:             tx.Raw,
			}, cfg.Order.AmountPrecision)
			if err != nil {
				if !store.IsClaimRejection(err) {
					return fmt.Errorf("核销事务失败: %w", err)
				}
				book.observe(&tx, model.ObservationReasonClaimFailed, o.TradeNo)
				slog.Error("订单入账失败", "trade_no", o.TradeNo, "tx", tx.ID, "err", err)
				continue
			}
			if !ok {
				// 幂等闸门拦下：这笔流水已经入账到某个订单，不是未归属流水。
				// 继续尝试其它订单只会重复触发同一个守卫，直接结束匹配。
				slog.Debug("流水或订单已被处理，跳过", "tx", tx.ID, "trade_no", o.TradeNo)
				totals.skipDup++
				hit = true
				break
			}

			totals.matched++
			hit = true
			resolvedTradeNo = o.TradeNo
			o.Status = model.StatusPaid
			slog.Info("已确认到账，订单已支付",
				"provider", provider,
				"trade_no", o.TradeNo,
				"out_trade_no", o.OutTradeNo,
				"amount", amount.FormatPrecision(tx.Amount, cfg.Order.AmountPrecision),
				"currency", tx.Currency,
				"receiver_uid", tx.ReceiverID,
				"transaction_id", tx.ID,
			)
			break
		}
		if hit {
			// 已入账的流水不能继续留在“未归属”里：撤销本轮观察，并把此前
			// 累积的观察记录标记为 resolved。
			book.retract(tx.ID)
			if err := p.store.ResolveTxObservation(ctx, provider, tx.ID, resolvedTradeNo); err != nil {
				return fmt.Errorf("标记交易观察已归属失败: %w", err)
			}
			continue
		}
		totals.skipAmt++
		book.observe(&tx, model.ObservationReasonNoMatch, "")
	}
	return nil
}

// rejectTransaction 只放行订单所选平台可确认的站内收入。
// 该函数不依赖数据库，便于覆盖退款、支出、链上充值和收款 UID 错误等边界。
func rejectTransaction(provider string, cfg *config.Config, tx *incomingTransaction) transactionRejectReason {
	if !tx.Incoming || tx.Amount <= 0 {
		return transactionOutgoing
	}

	switch provider {
	case config.ProviderOKX:
		// OKX type=3 是站内转账，type=4 是链上充值；后者永不参与匹配。
		if tx.Type != "3" {
			return transactionType
		}
	case config.ProviderBinance:
		if !isIncomingType(tx.Type) {
			return transactionType
		}
		if tx.ReceiverID == "" ||
			!strings.EqualFold(strings.TrimSpace(tx.ReceiverID), strings.TrimSpace(cfg.Checkout.Binance.UID)) {
			return transactionReceiver
		}
	default:
		return transactionType
	}

	if !cfg.CurrencyEnabled(tx.Currency) {
		return transactionCurrency
	}
	return transactionAccepted
}

// observationReason 把拒绝原因映射为观察账本里的稳定标记。
func observationReason(r transactionRejectReason) string {
	switch r {
	case transactionOutgoing:
		return model.ObservationReasonOutgoing
	case transactionType:
		return model.ObservationReasonType
	case transactionCurrency:
		return model.ObservationReasonCurrency
	case transactionReceiver:
		return model.ObservationReasonReceiver
	}
	return model.ObservationReasonUnknown
}

// txObservationBook 汇总一轮扫描中每笔未归属流水的失败原因。
//
// 同一笔流水在一轮扫描里可能先被某个订单拒绝入账（claim_failed），又不命中
// 其它订单而被标成 no_match。逐条写库会让后写的原因覆盖前面的原因，所以先
// 在内存里只保留优先级最高的一条，扫描结束再统一落库。
type txObservationBook struct {
	provider string
	items    map[string]model.TxObservation
}

func newTxObservationBook(provider string) *txObservationBook {
	return &txObservationBook{provider: provider, items: make(map[string]model.TxObservation)}
}

// observe 记录一次观察；已存在更高优先级的原因时保留原值。
func (b *txObservationBook) observe(tx *incomingTransaction, reason, orderTradeNo string) {
	reason = model.NormalizeTxObservationReason(reason)
	rank := model.TxObservationRank(reason)
	if cur, ok := b.items[tx.ID]; ok && cur.ReasonRank > rank {
		return
	}
	b.items[tx.ID] = model.TxObservation{
		Provider: b.provider, TransactionID: tx.ID, Type: tx.Type,
		Amount: tx.Amount, Currency: tx.Currency, ReceiverID: tx.ReceiverID,
		TransactionTime: tx.Time, Reason: reason, ReasonRank: rank,
		OrderTradeNo: orderTradeNo, Raw: tx.Raw,
	}
}

// retract 撤销一笔流水本轮的未归属结论（它其实已经入账）。
func (b *txObservationBook) retract(transactionID string) {
	delete(b.items, transactionID)
}

// pending 返回待落库的观察记录，按流水号排序便于测试与日志稳定。
func (b *txObservationBook) pending() []model.TxObservation {
	out := make([]model.TxObservation, 0, len(b.items))
	for _, obs := range b.items {
		out = append(out, obs)
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].Provider != out[j].Provider {
			return out[i].Provider < out[j].Provider
		}
		return out[i].TransactionID < out[j].TransactionID
	})
	return out
}

func orderMatchesTransaction(o *model.Order, tx *incomingTransaction, amountPrecision int) bool {
	return amount.MatchRounded(amount.FormatPrecision(o.PayAmount, o.AmountPrecision), tx.Amount)
}

func (p *Poller) binanceTransactions(ctx context.Context, cfg *config.Config,
	start, end time.Time) ([]incomingTransaction, error) {
	cli := p.binanceClientFor(cfg)
	uid := strings.TrimSpace(cfg.Checkout.Binance.UID)
	if uid == "" {
		return nil, fmt.Errorf("未配置 Binance 收款 UID")
	}
	if (!p.binanceUIDVerified || p.binanceVerifiedUID != uid) &&
		(p.binanceUIDRetryAt.IsZero() || time.Now().After(p.binanceUIDRetryAt)) {
		accountUID, err := cli.AccountUID(ctx)
		if err != nil {
			p.binanceUIDRetryAt = time.Now().Add(time.Minute)
			return nil, fmt.Errorf("校验 Binance 收款账户失败: %w", err)
		}
		if accountUID != uid {
			p.binanceUIDRetryAt = time.Now().Add(5 * time.Minute)
			return nil, fmt.Errorf("Binance API 账户 UID %s 与收款 UID %s 不一致",
				mask(accountUID), mask(uid))
		}
		p.binanceVerifiedUID = uid
		p.binanceUIDVerified = true
		p.binanceUIDRetryAt = time.Time{}
	}
	if !p.binanceUIDVerified || p.binanceVerifiedUID != uid {
		return nil, fmt.Errorf("Binance 收款账户 UID 尚未通过校验，等待重试")
	}
	rows, err := cli.PayTransactions(ctx, start, end)
	if err != nil {
		return nil, err
	}
	out := make([]incomingTransaction, 0, len(rows))
	for _, row := range rows {
		out = append(out, incomingTransaction{
			ID: row.TransactionID, Type: strings.ToUpper(row.OrderType),
			Amount: row.Amount, Currency: strings.ToUpper(row.Currency),
			Time: row.TransactionTime, ReceiverID: row.ReceiverID,
			ReceiverAddr: row.ReceiverID, Incoming: row.Incoming, Raw: row.Raw,
		})
	}
	return out, nil
}

func (p *Poller) okxTransactions(ctx context.Context, cfg *config.Config, since, until time.Time) ([]incomingTransaction, error) {
	cli := p.okxClientFor(cfg)
	uid := strings.TrimSpace(cfg.Checkout.OKX.UID)
	if uid == "" {
		return nil, fmt.Errorf("未配置 OKX 收款 UID")
	}
	if (!p.okxUIDVerified || p.okxVerifiedUID != uid) &&
		(p.okxUIDRetryAt.IsZero() || time.Now().After(p.okxUIDRetryAt)) {
		accountUID, err := cli.AccountUID(ctx)
		if err != nil {
			p.okxUIDRetryAt = time.Now().Add(time.Minute)
			return nil, fmt.Errorf("校验 OKX 收款账户失败: %w", err)
		}
		if accountUID != uid {
			p.okxUIDRetryAt = time.Now().Add(5 * time.Minute)
			return nil, fmt.Errorf("OKX API 账户 UID %s 与收款 UID %s 不一致", mask(accountUID), mask(uid))
		}
		p.okxVerifiedUID = uid
		p.okxUIDVerified = true
		p.okxUIDRetryAt = time.Time{}
	}
	if !p.okxUIDVerified || p.okxVerifiedUID != uid {
		return nil, fmt.Errorf("OKX 收款账户 UID 尚未通过校验，等待重试")
	}
	seen := make(map[string]struct{})
	var out []incomingTransaction
	for _, currency := range cfg.Checkout.Currencies {
		if currency != "USDT" && currency != "USDC" {
			continue
		}
		rows, err := cli.DepositHistorySince(ctx, currency, since)
		if err != nil {
			return nil, err
		}
		for _, row := range rows {
			// OKX 的 after 只用来翻页，返回结果可能超出一轮扫描的窗口，
			// 越界流水交给对应窗口的那一轮处理。
			if row.Timestamp.After(until) || row.Timestamp.Before(since) {
				continue
			}
			// 官方状态：0 等待确认、1 确认到账、2 充值成功。
			// 仅 state=2 允许入账，避免未完成确认的充值提前发货。
			// 客户端用 fromWdId 识别未返回 type 的站内转账。
			// 显式 type=4 的链上充值绝不能混入。
			if row.State != "2" || row.Type != "3" ||
				strings.TrimSpace(row.FromWithdrawID) == "" {
				continue
			}
			id := strings.TrimSpace(row.DepositID)
			if id == "" {
				id = strings.TrimSpace(row.FromWithdrawID)
			}
			if id == "" {
				id = strings.TrimSpace(row.TransactionID)
			}
			if id == "" {
				continue
			}
			key := "okx:" + id
			if _, exists := seen[key]; exists {
				continue
			}
			seen[key] = struct{}{}
			out = append(out, incomingTransaction{
				ID: key, Type: "3", Amount: row.Amount,
				Currency: row.Currency, Time: row.Timestamp,
				ReceiverID: uid, ReceiverAddr: uid, Incoming: true, Raw: row.Raw,
			})
		}
	}
	return out, nil
}

func (p *Poller) binanceClientFor(cfg *config.Config) *binance.Client {
	if p.binanceClient != nil &&
		p.binanceKey == cfg.Binance.APIKey &&
		p.binanceSecret == cfg.Binance.APISecret &&
		p.binanceBase == cfg.Binance.BaseURL {
		return p.binanceClient
	}
	p.binanceClient = binance.New(cfg.Binance.APIKey, cfg.Binance.APISecret, cfg.Binance.BaseURL,
		cfg.Binance.RecvWindow, cfg.Binance.Limit)
	if isLoopbackBaseURL(cfg.Binance.BaseURL) {
		p.binanceClient.UseTestTransport()
	}
	p.binanceKey, p.binanceSecret, p.binanceBase = cfg.Binance.APIKey, cfg.Binance.APISecret, cfg.Binance.BaseURL
	p.binanceVerifiedUID = ""
	p.binanceUIDVerified = false
	p.binanceUIDRetryAt = time.Time{}
	slog.Info("币安客户端已按新配置重建", "base", cfg.Binance.BaseURL, "key", mask(cfg.Binance.APIKey))
	return p.binanceClient
}

func (p *Poller) okxClientFor(cfg *config.Config) *okx.Client {
	if p.okxClient != nil &&
		p.okxKey == cfg.OKX.APIKey &&
		p.okxSecret == cfg.OKX.APISecret &&
		p.okxPassphrase == cfg.OKX.Passphrase &&
		p.okxBase == cfg.OKX.BaseURL {
		return p.okxClient
	}
	p.okxClient = okx.New(cfg.OKX.APIKey, cfg.OKX.APISecret, cfg.OKX.Passphrase,
		cfg.OKX.BaseURL, cfg.OKX.Limit)
	if isLoopbackBaseURL(cfg.OKX.BaseURL) {
		p.okxClient.UseTestTransport()
	}
	p.okxVerifiedUID = ""
	p.okxUIDVerified = false
	p.okxUIDRetryAt = time.Time{}
	p.okxKey, p.okxSecret, p.okxPassphrase, p.okxBase =
		cfg.OKX.APIKey, cfg.OKX.APISecret, cfg.OKX.Passphrase, cfg.OKX.BaseURL
	slog.Info("OKX 客户端已按新配置重建", "base", cfg.OKX.BaseURL, "key", mask(cfg.OKX.APIKey))
	return p.okxClient
}

// isIncomingType 判断是否属于币安 Pay 交易中可作为个人站内转账的
// 订单类型。PAY / C2C 覆盖普通 UID 转账；其它类型（退款、支付出款、
// Crypto Box 等）一律不进入入账匹配。金额正负号另行校验。
func isIncomingType(t string) bool {
	t = strings.ToUpper(strings.TrimSpace(t))
	return t == "PAY" || t == "C2C"
}

func mask(s string) string {
	if len(s) <= 8 {
		return "****"
	}
	return s[:4] + "****" + s[len(s)-4:]
}

func isLoopbackBaseURL(raw string) bool {
	u, err := url.Parse(raw)
	if err != nil {
		return false
	}
	host := u.Hostname()
	ip := net.ParseIP(host)
	return strings.EqualFold(host, "localhost") || (ip != nil && ip.IsLoopback())
}
