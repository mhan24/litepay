// Package settings 管理「后台可编辑的运行时配置」。
//
// 设计要点：
//   - 所有业务字段集中在 Patch 中，保存后写入 settings 表（JSON），重启后自动恢复；
//   - 内存中以原子指针持有 *config.Config 快照：更新时先深拷贝、再改、再原子替换，
//     读者始终拿到一份自洽的快照，无需加锁也不会读到半更新状态；
//   - SQLite 是业务配置的唯一来源；config.yaml 只保存启动参数。
package settings

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"time"
	"unicode/utf8"

	"litepay/internal/amount"
	"litepay/internal/config"
	"litepay/internal/gram"
	"litepay/internal/model"
	"litepay/internal/store"
)

// Patch 配置补丁：字段为指针，nil 表示“不修改”。
type Patch struct {
	AdminPath         *string `json:"admin_path,omitempty"`
	StoreURL          *string `json:"store_url,omitempty"`
	VerifyAccounts    bool    `json:"-"`
	VerifyProvider    string  `json:"-"`
	AdminUsername     *string `json:"admin_username,omitempty"`
	AdminPasswordHash *string `json:"admin_password_hash,omitempty"`
	BEpusdtReselect   *bool   `json:"bepusdt_reselect,omitempty"`

	// 收款信息
	BinanceCurrencies  *[]string `json:"binance_currencies,omitempty"`
	OKXCurrencies      *[]string `json:"okx_currencies,omitempty"`
	TONCurrencies      *[]string `json:"ton_currencies,omitempty"`
	CheckoutCurrencies *[]string `json:"checkout_currencies,omitempty"`
	CheckoutNote       *string   `json:"checkout_note,omitempty"`
	BinanceUID         *string   `json:"binance_uid,omitempty"`
	OKXUID             *string   `json:"okx_uid,omitempty"`

	// 订单
	OrderExpireMinutes   *int    `json:"order_expire_minutes,omitempty"`
	OrderAmountStep      *string `json:"order_amount_step,omitempty"`
	OrderMaxOffset       *int    `json:"order_max_offset,omitempty"`
	OrderMinAmount       *string `json:"order_min_amount,omitempty"`
	OrderSlotGrace       *int    `json:"order_slot_grace,omitempty"`
	OrderAmountPrecision *int    `json:"order_amount_precision,omitempty"`

	// 币安
	BinancePollInterval *int    `json:"binance_poll_interval,omitempty"`
	BinanceLookback     *int    `json:"binance_lookback,omitempty"`
	BinanceAPIKey       *string `json:"binance_api_key,omitempty"`
	BinanceAPISecret    *string `json:"binance_api_secret,omitempty"`

	// OKX
	OKXAPIKey       *string `json:"okx_api_key,omitempty"`
	OKXAPISecret    *string `json:"okx_api_secret,omitempty"`
	OKXPassphrase   *string `json:"okx_passphrase,omitempty"`
	OKXPollInterval *int    `json:"okx_poll_interval,omitempty"`
	OKXLookback     *int    `json:"okx_lookback,omitempty"`

	// TON 主网
	TONAddress      *string `json:"ton_address,omitempty"`
	TONAPIKey       *string `json:"ton_api_key,omitempty"`
	TONPollInterval *int    `json:"ton_poll_interval,omitempty"`

	// 回调
	NotifyRetryDelays *[]int `json:"notify_retry_delays,omitempty"`
	NotifyMaxAttempts *int   `json:"notify_max_attempts,omitempty"`
	NotifyTimeout     *int   `json:"notify_timeout,omitempty"`
}

// Apply 把补丁应用到配置副本。
func (p Patch) Apply(c *config.Config) {
	if p.AdminPath != nil {
		c.Admin.Path = strings.TrimSpace(*p.AdminPath)
	}
	if p.StoreURL != nil {
		c.Server.StoreURL = strings.TrimSpace(*p.StoreURL)
	}
	if p.AdminUsername != nil {
		c.Admin.Username = strings.TrimSpace(*p.AdminUsername)
	}
	if p.AdminPasswordHash != nil {
		c.Admin.Password = *p.AdminPasswordHash
	}
	if p.BEpusdtReselect != nil {
		c.BEpusdt.Reselect = *p.BEpusdtReselect
	}
	if p.CheckoutCurrencies != nil {
		c.Checkout.Currencies = config.NormalizeCurrencies(*p.CheckoutCurrencies)
		if p.BinanceCurrencies == nil {
			c.Checkout.BinanceCurrencies = nil
		}
		if p.OKXCurrencies == nil {
			c.Checkout.OKXCurrencies = nil
		}
		if p.TONCurrencies == nil {
			c.Checkout.TONCurrencies = nil
		}
	}
	if p.BinanceCurrencies != nil {
		c.Checkout.BinanceCurrencies = append([]string{}, (*p.BinanceCurrencies)...)
	}
	if p.OKXCurrencies != nil {
		c.Checkout.OKXCurrencies = append([]string{}, (*p.OKXCurrencies)...)
	}
	if p.TONCurrencies != nil {
		c.Checkout.TONCurrencies = append([]string{}, (*p.TONCurrencies)...)
	}
	if p.CheckoutNote != nil {
		c.Checkout.Note = strings.TrimSpace(*p.CheckoutNote)
	}
	if p.BinanceUID != nil {
		c.Checkout.Binance.UID = strings.TrimSpace(*p.BinanceUID)
	}
	if p.OKXUID != nil {
		c.Checkout.OKX.UID = strings.TrimSpace(*p.OKXUID)
	}

	if p.OrderExpireMinutes != nil {
		c.Order.ExpireMinutes = *p.OrderExpireMinutes
	}
	if p.OrderAmountStep != nil {
		c.Order.AmountStep = *p.OrderAmountStep
	}
	if p.OrderMaxOffset != nil {
		c.Order.MaxOffset = *p.OrderMaxOffset
	}
	if p.OrderMinAmount != nil {
		c.Order.MinAmount = *p.OrderMinAmount
	}
	if p.OrderSlotGrace != nil {
		c.Order.SlotGraceSeconds = *p.OrderSlotGrace
	}
	if p.OrderAmountPrecision != nil {
		c.Order.AmountPrecision = *p.OrderAmountPrecision
	}

	if p.BinancePollInterval != nil {
		c.Binance.PollInterval = *p.BinancePollInterval
	}
	if p.BinanceLookback != nil {
		c.Binance.LookbackBuffer = *p.BinanceLookback
	}
	if p.BinanceAPIKey != nil {
		c.Binance.APIKey = strings.TrimSpace(*p.BinanceAPIKey)
	}
	if p.BinanceAPISecret != nil {
		c.Binance.APISecret = strings.TrimSpace(*p.BinanceAPISecret)
	}

	if p.OKXAPIKey != nil {
		c.OKX.APIKey = strings.TrimSpace(*p.OKXAPIKey)
	}
	if p.OKXAPISecret != nil {
		c.OKX.APISecret = strings.TrimSpace(*p.OKXAPISecret)
	}
	if p.OKXPassphrase != nil {
		c.OKX.Passphrase = strings.TrimSpace(*p.OKXPassphrase)
	}
	if p.OKXPollInterval != nil {
		c.OKX.PollInterval = *p.OKXPollInterval
	}
	if p.OKXLookback != nil {
		c.OKX.LookbackBuffer = *p.OKXLookback
	}
	if p.TONAddress != nil {
		c.TON.Address = strings.TrimSpace(*p.TONAddress)
	}
	if p.TONAPIKey != nil {
		c.TON.APIKey = strings.TrimSpace(*p.TONAPIKey)
	}
	if p.TONPollInterval != nil {
		c.TON.PollInterval = *p.TONPollInterval
	}
	if p.NotifyRetryDelays != nil {
		c.Notify.RetryDelays = append([]int(nil), (*p.NotifyRetryDelays)...)
	}
	if p.NotifyMaxAttempts != nil {
		c.Notify.MaxAttempts = *p.NotifyMaxAttempts
	}
	if p.NotifyTimeout != nil {
		c.Notify.TimeoutSeconds = *p.NotifyTimeout
	}
}

// Snapshot 把当前配置的可变部分导出为补丁（用于持久化）。
func Snapshot(c *config.Config) Patch {
	s := func(v string) *string { return &v }
	i := func(v int) *int { return &v }

	b := func(v bool) *bool { return &v }

	return Patch{
		AdminPath:          s(c.AdminPath()),
		StoreURL:           s(c.Server.StoreURL),
		AdminUsername:      s(c.Admin.Username),
		BEpusdtReselect:    b(c.BEpusdt.Reselect),
		CheckoutCurrencies: &[]string{},
		BinanceCurrencies:  &c.Checkout.BinanceCurrencies, OKXCurrencies: &c.Checkout.OKXCurrencies, TONCurrencies: &c.Checkout.TONCurrencies,
		CheckoutNote: s(c.Checkout.Note),
		BinanceUID:   s(c.Checkout.Binance.UID),
		OKXUID:       s(c.Checkout.OKX.UID),

		OrderExpireMinutes:   i(c.Order.ExpireMinutes),
		OrderAmountStep:      s(c.Order.AmountStep),
		OrderMaxOffset:       i(c.Order.MaxOffset),
		OrderMinAmount:       s(c.Order.MinAmount),
		OrderSlotGrace:       i(c.Order.SlotGraceSeconds),
		OrderAmountPrecision: i(c.Order.AmountPrecision),

		BinancePollInterval: i(c.Binance.PollInterval),
		BinanceLookback:     i(c.Binance.LookbackBuffer),
		BinanceAPIKey:       s(c.Binance.APIKey),
		BinanceAPISecret:    s(c.Binance.APISecret),

		OKXAPIKey:       s(c.OKX.APIKey),
		OKXAPISecret:    s(c.OKX.APISecret),
		OKXPassphrase:   s(c.OKX.Passphrase),
		OKXPollInterval: i(c.OKX.PollInterval),
		OKXLookback:     i(c.OKX.LookbackBuffer),

		TONAddress: s(c.TON.Address), TONAPIKey: s(c.TON.APIKey), TONPollInterval: i(c.TON.PollInterval),
		NotifyRetryDelays: &[]int{},
		NotifyMaxAttempts: i(c.Notify.MaxAttempts),
		NotifyTimeout:     i(c.Notify.TimeoutSeconds),
	}
}

// Manager 持有当前生效配置。
type Manager struct {
	cur           atomic.Pointer[config.Config]
	st            *store.Store
	mu            sync.Mutex
	verifyAccount func(context.Context, *config.Config, string) error
}

// New 创建配置管理器。
func New(cfg *config.Config, st *store.Store) *Manager {
	base := clone(cfg)
	normalize(base)
	m := &Manager{st: st, verifyAccount: verifyCollectionAccount}
	m.cur.Store(clone(base))
	return m
}

// Get 返回当前配置快照（只读，调用方不得修改）。
func (m *Manager) Get() *config.Config { return m.cur.Load() }

// Store 返回配置管理器绑定的存储，仅供同一进程的审计与测试使用。
func (m *Manager) Store() *store.Store { return m.st }

// Load 启动时应用数据库中的覆盖项。
func (m *Manager) Load(ctx context.Context) error {
	payload, err := m.st.LoadSettingPayload(ctx)
	if err != nil {
		return err
	}
	if err := m.ensureAPIToken(ctx); err != nil {
		return err
	}
	if strings.TrimSpace(payload) == "" {
		return nil
	}
	var p Patch
	if err := json.Unmarshal([]byte(payload), &p); err != nil {
		return fmt.Errorf("解析后台配置失败: %w", err)
	}
	next := clone(m.cur.Load())
	p.Apply(next)
	normalize(next)
	if err := Validate(next); err != nil {
		return fmt.Errorf("后台配置无效: %w", err)
	}
	m.cur.Store(next)
	return nil
}

// ensureAPIToken 读取系统生成的 Token；仅在数据库中没有 Token 时生成。
func (m *Manager) ensureAPIToken(ctx context.Context) error {
	stored, err := m.st.LoadCredential(ctx, "bepusdt_api_token")
	if err != nil {
		return fmt.Errorf("读取 API Token 失败: %w", err)
	}
	if stored != nil && strings.TrimSpace(stored.Value) != "" {
		next := clone(m.cur.Load())
		next.BEpusdt.APIToken = stored.Value
		m.cur.Store(next)
		return nil
	}
	token, err := newAPIToken()
	if err != nil {
		return err
	}
	if err := m.st.SaveCredential(ctx, model.Credential{
		Name: "bepusdt_api_token", Value: token, UpdatedBy: "startup",
	}); err != nil {
		return fmt.Errorf("保存系统生成的 API Token 失败: %w", err)
	}
	next := clone(m.cur.Load())
	next.BEpusdt.APIToken = token
	m.cur.Store(next)
	return nil
}

// RotateAPIToken 生成并持久化新的 API Token。
func (m *Manager) RotateAPIToken(ctx context.Context, operator string) (string, error) {
	token, err := newAPIToken()
	if err != nil {
		return "", err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if err := m.st.SaveCredential(ctx, model.Credential{
		Name: "bepusdt_api_token", Value: token, UpdatedBy: operator,
	}); err != nil {
		return "", fmt.Errorf("保存新 API Token 失败: %w", err)
	}
	next := clone(m.cur.Load())
	next.BEpusdt.APIToken = token
	m.cur.Store(next)
	return token, nil
}

func newAPIToken() (string, error) {
	buf := make([]byte, 24)
	if _, err := rand.Read(buf); err != nil {
		return "", err
	}
	return hex.EncodeToString(buf), nil
}

// Update 应用补丁：校验 → 持久化 → 原子替换。
func (m *Manager) Update(ctx context.Context, p Patch, operator string) error {
	return m.update(ctx, p, operator, nil)
}

// UpdateWithVerification returns only checks completed for the configuration that was saved.
func (m *Manager) UpdateWithVerification(ctx context.Context, p Patch, operator string) ([]AccountVerification, error) {
	var report []AccountVerification
	err := m.update(ctx, p, operator, &report)
	return report, err
}

func (m *Manager) update(ctx context.Context, p Patch, operator string, report *[]AccountVerification) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	next := clone(m.cur.Load())
	p.Apply(next)
	normalize(next)
	if err := Validate(next); err != nil {
		return err
	}

	current := m.cur.Load()
	// 只有会改变到账归属或付款窗口的字段需要在待付款期间冻结。API 凭据不改变
	// 归属（换 Key 仍须通过 UID 核验），必须允许在扫描失败时随时修正。
	sensitive := current.TON.Address != next.TON.Address || current.Checkout.Binance.UID != next.Checkout.Binance.UID || current.Checkout.OKX.UID != next.Checkout.OKX.UID ||
		current.Order.SlotGraceSeconds != next.Order.SlotGraceSeconds || !reflect.DeepEqual(current.Checkout.Currencies, next.Checkout.Currencies) ||
		!reflect.DeepEqual(current.Checkout.BinanceCurrencies, next.Checkout.BinanceCurrencies) || !reflect.DeepEqual(current.Checkout.OKXCurrencies, next.Checkout.OKXCurrencies) || !reflect.DeepEqual(current.Checkout.TONCurrencies, next.Checkout.TONCurrencies)
	if sensitive {
		active, err := m.st.HasActivePayments(ctx)
		if err != nil {
			return err
		}
		if active {
			return fmt.Errorf("仍有待确认支付的订单，暂不能更换收款账户、币种或宽限期")
		}
	}

	if p.VerifyProvider != "" && p.VerifyProvider != config.ProviderBinance && p.VerifyProvider != config.ProviderOKX && p.VerifyProvider != config.ProviderTON {
		return fmt.Errorf("核验平台无效")
	}
	if p.VerifyAccounts && p.VerifyProvider != "" && !next.ProviderEnabled(p.VerifyProvider) {
		for _, check := range verificationTargets(next) {
			if check.Provider == p.VerifyProvider {
				return fmt.Errorf("该平台尚未完成收款配置：%s", strings.Join(check.Missing, "、"))
			}
		}
	}
	if p.VerifyAccounts && !next.AnyProviderEnabled() {
		return fmt.Errorf("请先完整填写至少一个平台的 API 凭据和收款 UID，或 GRAM 收款地址")
	}
	// Verify changed collection accounts before publishing or persisting credentials.
	changed := map[string]bool{
		config.ProviderBinance: current.Binance.APIKey != next.Binance.APIKey || current.Binance.APISecret != next.Binance.APISecret || current.Checkout.Binance.UID != next.Checkout.Binance.UID,
		config.ProviderOKX:     current.OKX.APIKey != next.OKX.APIKey || current.OKX.APISecret != next.OKX.APISecret || current.OKX.Passphrase != next.OKX.Passphrase || current.Checkout.OKX.UID != next.Checkout.OKX.UID,
		config.ProviderTON:     !current.TONEnabled() || current.TON.Address != next.TON.Address || current.TON.APIKey != next.TON.APIKey,
	}
	checks := verificationTargets(next)
	for i := range checks {
		check := &checks[i]
		if !next.ProviderEnabled(check.Provider) || (!(p.VerifyAccounts && (p.VerifyProvider == "" || p.VerifyProvider == check.Provider)) && !changed[check.Provider]) {
			continue
		}
		if err := m.verifyAccount(ctx, next, check.Provider); err != nil {
			return err
		}
		check.Verified = true
	}
	// 持久化完整可变配置，保证重启后一致
	snap := Snapshot(next)
	if strings.HasPrefix(next.Admin.Password, "$2a$") || strings.HasPrefix(next.Admin.Password, "$2b$") {
		v := next.Admin.Password
		snap.AdminPasswordHash = &v
	}
	snap.CheckoutCurrencies = &next.Checkout.Currencies
	snap.NotifyRetryDelays = &next.Notify.RetryDelays

	payload, err := json.Marshal(snap)
	if err != nil {
		return err
	}
	if err := m.st.SaveSettingPayload(ctx, string(payload), operator); err != nil {
		return fmt.Errorf("保存配置失败: %w", err)
	}
	m.cur.Store(next)
	if report != nil {
		*report = checks
		if p.VerifyProvider != "" {
			for _, check := range checks {
				if check.Provider == p.VerifyProvider {
					*report = []AccountVerification{check}
					break
				}
			}
		}
	}
	return nil
}

// normalize 统一收款币种。
func normalize(c *config.Config) {
	c.NormalizePaymentCurrencies()
}

// Validate 校验配置合法性。
func Validate(c *config.Config) error {
	if err := c.ValidateWebRoutes(); err != nil {
		return err
	}
	if strings.TrimSpace(c.Admin.Username) == "" || utf8.RuneCountInString(c.Admin.Username) > 64 {
		return fmt.Errorf("管理员用户名需为 1~64 个字符")
	}
	if len(strings.TrimSpace(c.BEpusdt.APIToken)) < 16 {
		return fmt.Errorf("BEpusdt API Token 至少 16 位（建议 32 位随机字符串）")
	}
	if (c.BinanceEnabled() && strings.TrimSpace(c.Checkout.Binance.UID) == "") ||
		(c.OKXEnabled() && strings.TrimSpace(c.Checkout.OKX.UID) == "") {
		return fmt.Errorf("已配置凭据的收款平台必须填写收款 UID")
	}
	for _, cur := range c.Checkout.Currencies {
		if !config.IsSupportedCurrency(cur) {
			return fmt.Errorf("不支持币种 %s，仅支持 USDT / USDC / GRAM", cur)
		}
	}
	if c.Order.ExpireMinutes < 1 || c.Order.ExpireMinutes > 1440 {
		return fmt.Errorf("订单有效期需在 1~1440 分钟之间")
	}
	step, err := amount.Parse(c.Order.AmountStep)
	if err != nil || step <= 0 || step > amount.Scale {
		return fmt.Errorf("金额步长必须大于零且不超过 1")
	}
	if c.Order.MaxOffset < 1 || c.Order.MaxOffset > 900 {
		return fmt.Errorf("最大金额调整次数需在 1~900 之间")
	}
	minUnits, err := amount.Parse(c.Order.MinAmount)
	if err != nil || minUnits <= 0 {
		return fmt.Errorf("最小金额必须大于零")
	}
	if c.Order.SlotGraceSeconds < 0 || c.Order.SlotGraceSeconds > 3600 {
		return fmt.Errorf("过期宽限需在 0~3600 秒之间")
	}
	if c.Order.AmountPrecision < 2 || c.Order.AmountPrecision > 6 {
		return fmt.Errorf("金额精度需在 2~6 位之间")
	}
	if step%amount.Quantum(c.Order.AmountPrecision) != 0 {
		return fmt.Errorf("金额步长必须符合显示精度")
	}
	if c.Binance.PollInterval < 2 || c.Binance.PollInterval > 60 {
		return fmt.Errorf("查询间隔需在 2~60 秒之间")
	}
	if c.Binance.LookbackBuffer < 1 || c.Binance.LookbackBuffer > 60 {
		return fmt.Errorf("回看缓冲需在 1~60 分钟之间")
	}
	if c.OKX.PollInterval < 2 || c.OKX.PollInterval > 60 {
		return fmt.Errorf("OKX 查询间隔需在 2~60 秒之间")
	}
	if c.OKX.LookbackBuffer < 1 || c.OKX.LookbackBuffer > 60 {
		return fmt.Errorf("OKX 回看缓冲需在 1~60 分钟之间")
	}
	if c.TON.Address != "" {
		if _, err := gram.ParseAddress(c.TON.Address); err != nil {
			return err
		}
	}
	if c.TON.PollInterval < 2 || c.TON.PollInterval > 60 {
		return fmt.Errorf("GRAM 到账查询间隔需在 2~60 秒之间")
	}
	if len(c.Notify.RetryDelays) == 0 {
		return fmt.Errorf("结果通知重试序列不能为空")
	}
	for _, d := range c.Notify.RetryDelays {
		if d < 0 || d > 3600 {
			return fmt.Errorf("重试间隔需在 0~3600 秒之间")
		}
	}
	if c.Notify.MaxAttempts < 1 || c.Notify.MaxAttempts > 10 {
		return fmt.Errorf("最大重试次数需在 1~10 之间")
	}
	if c.Notify.TimeoutSeconds < 3 || c.Notify.TimeoutSeconds > 60 {
		return fmt.Errorf("回调超时需在 3~60 秒之间")
	}
	return nil
}

func clone(c *config.Config) *config.Config {
	n := *c
	n.Server.NotifyHosts = append([]string(nil), c.Server.NotifyHosts...)
	n.Checkout.Currencies = append([]string(nil), c.Checkout.Currencies...)
	if c.Checkout.BinanceCurrencies != nil {
		n.Checkout.BinanceCurrencies = append([]string{}, c.Checkout.BinanceCurrencies...)
	}
	if c.Checkout.OKXCurrencies != nil {
		n.Checkout.OKXCurrencies = append([]string{}, c.Checkout.OKXCurrencies...)
	}
	if c.Checkout.TONCurrencies != nil {
		n.Checkout.TONCurrencies = append([]string{}, c.Checkout.TONCurrencies...)
	}
	n.Notify.RetryDelays = append([]int(nil), c.Notify.RetryDelays...)
	return &n
}

// UpdatedAt 便于模板展示（读取 settings 表最近更新时间）。
func (m *Manager) UpdatedAt(ctx context.Context) time.Time {
	t, _ := m.st.SettingUpdatedAt(ctx)
	return t
}

// WithPaymentConfig prevents a checkout from locking credentials that are being replaced.
func (m *Manager) WithPaymentConfig(fn func()) {
	m.mu.Lock()
	defer m.mu.Unlock()
	fn()
}
