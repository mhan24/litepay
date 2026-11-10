// Package config 负责加载网关启动配置。
//
// 配置模型：
//  1. config.yaml 只保存服务监听、数据库路径、后台登录和日志级别。
//  2. 支付平台、收款 UID、订单与回调只保存在 SQLite，由后台设置页维护。
//  3. 不读取 .env 或环境变量；YAML 只解析启动配置。
package config

import (
	"fmt"
	"net/url"
	"os"
	"regexp"
	"strings"
	"time"
	"unicode/utf8"

	"gopkg.in/yaml.v3"
)

// -----------------------------------------------------------------------------
// 配置结构体定义
// -----------------------------------------------------------------------------

// Config 网关总配置。
type Config struct {
	Server   ServerConfig   `yaml:"server"`
	Database DatabaseConfig `yaml:"database"`
	Admin    AdminConfig    `yaml:"admin"`
	LogLevel string         `yaml:"log_level"`

	// 以下字段仅属于 SQLite 运行时配置，不参与 YAML 解码。
	BEpusdt  BEpusdtConfig  `yaml:"-"`
	Binance  BinanceConfig  `yaml:"-"`
	OKX      OKXConfig      `yaml:"-"`
	TON      TONConfig      `yaml:"-"`
	Checkout CheckoutConfig `yaml:"-"`
	Order    OrderConfig    `yaml:"-"`
	Notify   NotifyConfig   `yaml:"-"`
}

const (
	ProviderBinance = "binance"
	ProviderOKX     = "okx"
	ProviderTON     = "ton"
)

// SupportedCurrencies 是收银台和到账确认逻辑允许的币种集合。
var SupportedCurrencies = []string{"USDT", "USDC", "GRAM"}

// NormalizeCurrencies 将币种规范为大写、去重，并只保留 USDT/USDC/GRAM。
// 空输入保持为空，表示暂不开放收款币种。
func NormalizeCurrencies(in []string) []string {
	seen := make(map[string]bool, len(in))
	out := make([]string, 0, len(in))
	for _, raw := range in {
		currency := strings.ToUpper(strings.TrimSpace(raw))
		if !IsSupportedCurrency(currency) || seen[currency] {
			continue
		}
		seen[currency] = true
		out = append(out, currency)
	}
	return out
}

// IsSupportedCurrency 判断币种是否受网关支持。
func IsSupportedCurrency(currency string) bool {
	switch strings.ToUpper(strings.TrimSpace(currency)) {
	case "USDT", "USDC", "GRAM":
		return true
	default:
		return false
	}
}

// ServerConfig HTTP 服务配置。
type ServerConfig struct {
	Production     bool     `yaml:"production"`
	NotifyHosts    []string `yaml:"notify_hosts"`    // Exact callback hostnames in production; no wildcards.
	StoreURL       string   `yaml:"store_url"`       // 根地址跳转目标；留空返回 404。
	TrustedProxies []string `yaml:"trusted_proxies"` // Only explicit reverse proxies may supply client IP headers.
	Addr           string   `yaml:"addr"`            // 监听地址，如 0.0.0.0:8080
	BaseURL        string   `yaml:"base_url"`        // 网关对外访问地址
}

// DatabaseConfig SQLite 配置。
type DatabaseConfig struct {
	Path string `yaml:"path"`
}

// BEpusdtConfig 是对独角数卡暴露的 BEpusdt API 配置。
type BEpusdtConfig struct {
	TokenVersion uint   `yaml:"-"`
	APIToken     string `yaml:"api_token"` // 由系统生成和轮换
	Reselect     bool   `yaml:"reselect"`  // 是否允许付款人重选
}

// BinanceConfig 币安只读 API 配置。
type BinanceConfig struct {
	APIKey         string `yaml:"api_key"`
	APISecret      string `yaml:"api_secret"`
	BaseURL        string `yaml:"base_url"`                // 默认 https://api.binance.com
	PollInterval   int    `yaml:"poll_interval"`           // 查询间隔（秒）
	RecvWindow     int    `yaml:"recv_window"`             // recvWindow 毫秒
	Limit          int    `yaml:"limit"`                   // 单次查询条数，最大 100
	LookbackBuffer int    `yaml:"lookback_buffer_minutes"` // 订单时间回看缓冲（分钟）
}

// OKXConfig OKX 只读 API 与站内转账收款配置。
type OKXConfig struct {
	APIKey         string `yaml:"api_key"`
	APISecret      string `yaml:"api_secret"`
	Passphrase     string `yaml:"passphrase"`
	BaseURL        string `yaml:"base_url"`                // 默认 https://www.okx.com
	PollInterval   int    `yaml:"poll_interval"`           // 查询间隔（秒）
	Limit          int    `yaml:"limit"`                   // 单次查询条数，最大 100
	LookbackBuffer int    `yaml:"lookback_buffer_minutes"` // 订单时间回看缓冲（分钟）
}

// TONConfig configures read-only native GRAM collection on TON mainnet.
type TONConfig struct {
	Address        string
	APIKey         string
	BaseURL        string
	PollInterval   int
	LookbackBuffer int
}

// CheckoutConfig 收银台与交易所收款平台的展示配置。
type CheckoutConfig struct {
	BinanceCurrencies []string        `yaml:"-"`
	OKXCurrencies     []string        `yaml:"-"`
	TONCurrencies     []string        `yaml:"-"`
	Currencies        []string        `yaml:"currencies"` // 支持的币种，如 ["USDT","USDC"]
	Note              string          `yaml:"note"`       // 收银台提示文案
	Binance           ProviderDisplay `yaml:"binance"`
	OKX               ProviderDisplay `yaml:"okx"`
}

// ProviderDisplay 收款平台展示信息。UID 同时用于展示、转账和到账归属校验。
type ProviderDisplay struct {
	UID string `yaml:"uid"`
}

// OrderConfig 订单与浮动金额配置。
type OrderConfig struct {
	ExpireMinutes    int    `yaml:"expire_minutes"`     // 订单有效期（分钟）
	AmountStep       string `yaml:"amount_step"`        // 浮动步长，默认 0.01
	MaxOffset        int    `yaml:"max_offset"`         // 最大金额调整次数，默认 99
	MinAmount        string `yaml:"min_amount"`         // 最小应付 USDT
	SlotGraceSeconds int    `yaml:"slot_grace_seconds"` // 过期后继续确认到账的时间
	AmountPrecision  int    `yaml:"amount_precision"`   // 应付金额小数位，2~6
}

// NotifyConfig 回调配置。
type NotifyConfig struct {
	RetryDelays    []int `yaml:"retry_delays"`    // 失败后的退避秒数
	MaxAttempts    int   `yaml:"max_attempts"`    // 最大尝试次数
	TimeoutSeconds int   `yaml:"timeout_seconds"` // 单次请求超时
}

// AdminConfig 后台管理配置。密码留空则不启用后台。
type AdminConfig struct {
	Path         string `yaml:"path"` // 后台入口前缀，修改后重启生效。
	Username     string `yaml:"username"`
	Password     string `yaml:"password"`
	SessionHours int    `yaml:"session_hours"`
}

// -----------------------------------------------------------------------------
// 默认值
// -----------------------------------------------------------------------------

// Default 返回内置默认配置。
func Default() *Config {
	return &Config{
		Server: ServerConfig{
			Addr:       "0.0.0.0:8080",
			Production: true,
		},
		Database: DatabaseConfig{Path: "./data/litepay-dev.db"},
		BEpusdt:  BEpusdtConfig{Reselect: true},
		Binance: BinanceConfig{
			BaseURL:        "https://api.binance.com",
			PollInterval:   4,
			RecvWindow:     5000,
			Limit:          100,
			LookbackBuffer: 2,
		},
		OKX: OKXConfig{
			BaseURL:        "https://www.okx.com",
			PollInterval:   4,
			Limit:          100,
			LookbackBuffer: 2,
		},
		TON:      TONConfig{BaseURL: "https://toncenter.com/api/v2", PollInterval: 5, LookbackBuffer: 2},
		Checkout: CheckoutConfig{Currencies: []string{"USDT", "USDC"}},
		Order: OrderConfig{
			ExpireMinutes:    10,
			AmountStep:       "0.01",
			MaxOffset:        99,
			MinAmount:        "0.01",
			SlotGraceSeconds: 120,
			AmountPrecision:  2,
		},
		Notify: NotifyConfig{
			RetryDelays:    []int{5, 15, 30, 60, 180},
			MaxAttempts:    5,
			TimeoutSeconds: 10,
		},
		Admin: AdminConfig{
			Username:     "admin",
			SessionHours: 12,
		},
		LogLevel: "info",
	}
}

// -----------------------------------------------------------------------------
// 加载
// -----------------------------------------------------------------------------

// Load 从指定路径加载 YAML 启动配置。
func Load(path string) (*Config, error) {
	cfg := Default()
	if data, err := os.ReadFile(path); err == nil {
		if err := yaml.Unmarshal(data, cfg); err != nil {
			return nil, fmt.Errorf("解析配置文件 %s 失败: %w", path, err)
		}
	} else if !os.IsNotExist(err) {
		return nil, fmt.Errorf("读取配置文件 %s 失败: %w", path, err)
	} else {
		fmt.Fprintf(os.Stderr, "[WARN] 未找到配置文件 %s，使用内置默认值启动\n", path)
	}

	if err := cfg.normalize(); err != nil {
		return nil, err
	}
	return cfg, nil
}

// normalize 补齐派生字段并做基本校验。
func (c *Config) normalize() error {
	if err := c.ValidateWebRoutes(); err != nil {
		return err
	}
	c.Server.BaseURL = strings.TrimRight(strings.TrimSpace(c.Server.BaseURL), "/")
	if c.Server.BaseURL == "" {
		c.Server.BaseURL = "http://127.0.0.1" + portSuffix(c.Server.Addr)
	}
	for i, host := range c.Server.NotifyHosts {
		host = strings.ToLower(strings.TrimSpace(host))
		if host == "" || strings.ContainsAny(host, "/:@*?#\\") || strings.HasSuffix(host, ".") {
			return fmt.Errorf("server.notify_hosts 只能填写精确主机名")
		}
		c.Server.NotifyHosts[i] = host
	}
	base, err := url.Parse(c.Server.BaseURL)
	if err != nil || (base.Scheme != "http" && base.Scheme != "https") || base.Host == "" || base.User != nil || base.RawQuery != "" || base.Fragment != "" || (base.Path != "" && base.Path != "/") {
		return fmt.Errorf("server.base_url 必须是无路径、查询参数和用户信息的 HTTP(S) 站点地址")
	}
	if c.Server.Production && (base.Scheme != "https" || len(c.Server.NotifyHosts) == 0) {
		return fmt.Errorf("production 模式要求 HTTPS base_url 和非空 notify_hosts")
	}
	c.Binance.BaseURL = strings.TrimRight(c.Binance.BaseURL, "/")
	if c.Binance.PollInterval < 1 {
		c.Binance.PollInterval = 4
	}
	if c.Binance.Limit <= 0 || c.Binance.Limit > 100 {
		c.Binance.Limit = 100
	}
	if c.Binance.RecvWindow <= 0 {
		c.Binance.RecvWindow = 5000
	}
	if c.Binance.LookbackBuffer <= 0 {
		c.Binance.LookbackBuffer = 2
	}
	c.OKX.BaseURL = strings.TrimRight(strings.TrimSpace(c.OKX.BaseURL), "/")
	if c.OKX.BaseURL == "" {
		c.OKX.BaseURL = "https://www.okx.com"
	}
	if c.OKX.PollInterval < 1 {
		c.OKX.PollInterval = 4
	}
	if c.OKX.Limit <= 0 || c.OKX.Limit > 100 {
		c.OKX.Limit = 100
	}
	if c.OKX.LookbackBuffer <= 0 {
		c.OKX.LookbackBuffer = 2
	}
	c.Checkout.Binance.UID = strings.TrimSpace(c.Checkout.Binance.UID)
	c.Checkout.OKX.UID = strings.TrimSpace(c.Checkout.OKX.UID)
	c.Checkout.Currencies = NormalizeCurrencies(c.Checkout.Currencies)
	if c.Order.ExpireMinutes <= 0 {
		c.Order.ExpireMinutes = 10
	}
	if c.Order.AmountStep == "" {
		c.Order.AmountStep = "0.01"
	}
	if c.Order.MaxOffset <= 0 {
		c.Order.MaxOffset = 99
	}
	if c.Order.MinAmount == "" {
		c.Order.MinAmount = "0.01"
	}
	if c.Order.SlotGraceSeconds < 0 {
		c.Order.SlotGraceSeconds = 120
	}
	if c.Order.AmountPrecision < 2 || c.Order.AmountPrecision > 6 {
		c.Order.AmountPrecision = 2
	}
	if len(c.Notify.RetryDelays) == 0 {
		c.Notify.RetryDelays = []int{5, 15, 30, 60, 180}
	}
	if c.Notify.MaxAttempts <= 0 {
		c.Notify.MaxAttempts = len(c.Notify.RetryDelays)
	}
	if c.Notify.TimeoutSeconds <= 0 {
		c.Notify.TimeoutSeconds = 10
	}
	if c.Admin.Username == "" {
		c.Admin.Username = "admin"
	}
	if c.Admin.SessionHours <= 0 {
		c.Admin.SessionHours = 12
	}
	if utf8.RuneCountInString(c.Admin.Username) > 64 {
		return fmt.Errorf("admin.username 不能超过 64 个字符")
	}
	if c.Admin.SessionHours > 720 {
		return fmt.Errorf("admin.session_hours 需在 1~720 小时之间")
	}
	if c.Admin.Password != "" && utf8.RuneCountInString(c.Admin.Password) < 6 {
		return fmt.Errorf("admin.password 至少需要 6 个字符")
	}
	if c.Admin.Password == "" {
		fmt.Fprintln(os.Stderr, "[WARN] 未在 config.yaml 配置 admin.password，后台管理已禁用")
	}
	return nil
}

// AdminPath returns the configured mount point; existing installations retain /admin.
func (c *Config) AdminPath() string {
	if c.Admin.Path == "" {
		return "/admin"
	}
	return c.Admin.Path
}

// ValidateWebRoutes rejects ambiguous paths and unsafe or looping homepage redirects.
func (c *Config) ValidateWebRoutes() error {
	root := c.AdminPath()
	if !regexp.MustCompile(`^/[A-Za-z0-9][A-Za-z0-9_-]{2,95}$`).MatchString(root) {
		return fmt.Errorf("admin.path 必须为 / 开头、3~96 位字母数字或下划线及连字符的单层路径")
	}
	switch root {
	case "/pay", "/api", "/assets", "/healthz", "/readyz":
		return fmt.Errorf("admin.path 与系统路径冲突")
	}
	if c.Server.StoreURL != "" {
		u, err := url.Parse(c.Server.StoreURL)
		if err != nil || (u.Scheme != "http" && u.Scheme != "https") || u.Host == "" || u.User != nil || strings.ContainsAny(c.Server.StoreURL, "\r\n") {
			return fmt.Errorf("server.store_url 必须是无用户信息的 HTTP(S) 商店地址")
		}
		base, _ := url.Parse(c.Server.BaseURL)
		if base != nil && strings.EqualFold(u.Host, base.Host) {
			return fmt.Errorf("server.store_url 必须指向网关以外的商店站点")
		}
	}
	return nil
}

// BinanceEnabled 是否同时具备只读查询凭据和站内转账收款 UID。
func (c *Config) BinanceEnabled() bool {
	return len(c.ProviderCurrencies(ProviderBinance)) > 0 && strings.TrimSpace(c.Binance.APIKey) != "" &&
		strings.TrimSpace(c.Binance.APISecret) != "" &&
		strings.TrimSpace(c.Checkout.Binance.UID) != ""
}

// OKXEnabled 是否具备 OKX 站内转账到账确认条件。
func (c *Config) OKXEnabled() bool {
	return len(c.ProviderCurrencies(ProviderOKX)) > 0 && strings.TrimSpace(c.OKX.APIKey) != "" &&
		strings.TrimSpace(c.OKX.APISecret) != "" &&
		strings.TrimSpace(c.OKX.Passphrase) != "" &&
		strings.TrimSpace(c.Checkout.OKX.UID) != ""
}

// ProviderEnabled 某收款平台是否已完成只读 API 配置。
func (c *Config) ProviderEnabled(provider string) bool {
	switch strings.ToLower(strings.TrimSpace(provider)) {
	case ProviderBinance:
		return c.BinanceEnabled()
	case ProviderOKX:
		return c.OKXEnabled()
	case ProviderTON:
		return c.TONEnabled()
	default:
		return false
	}
}

// ProviderInterval 返回指定平台的查询间隔。
func (c *Config) ProviderInterval(provider string) int {
	if provider == ProviderTON {
		return c.TON.PollInterval
	}
	if strings.EqualFold(provider, ProviderOKX) {
		return c.OKX.PollInterval
	}
	return c.Binance.PollInterval
}

// ProviderLookback 返回指定通道的订单时间回看缓冲。
func (c *Config) ProviderLookback(provider string) int {
	if provider == ProviderTON {
		return c.TON.LookbackBuffer
	}
	if strings.EqualFold(provider, ProviderOKX) {
		return c.OKX.LookbackBuffer
	}
	return c.Binance.LookbackBuffer
}

// InitialScanLookback bounds only the initial history query. Confirmation tasks have their own
// persistent deadline based on the actual order window, independent of this default.
func (c *Config) InitialScanLookback() time.Duration {
	return time.Duration(c.Order.ExpireMinutes+max(c.Binance.LookbackBuffer, c.OKX.LookbackBuffer, c.TON.LookbackBuffer))*time.Minute +
		time.Duration(c.Order.SlotGraceSeconds)*time.Second
}

// CurrencyEnabled 判断币种是否在支持列表中。
func (c *Config) CurrencyEnabled(cur string) bool {
	cur = strings.ToUpper(strings.TrimSpace(cur))
	for _, v := range c.Checkout.Currencies {
		if strings.EqualFold(v, cur) {
			return true
		}
	}
	return false
}

// AdminEnabled 是否启用后台。
func (c *Config) AdminEnabled() bool { return c.Admin.Password != "" }

func portSuffix(addr string) string {
	if i := strings.LastIndex(addr, ":"); i >= 0 {
		return addr[i:]
	}
	return ":8080"
}

func (c *Config) TONEnabled() bool {
	return strings.TrimSpace(c.TON.Address) != "" && c.ProviderCurrency(ProviderTON, "GRAM")
}

// FilterProviderCurrencies keeps each currency within its supported payment channels.
func FilterProviderCurrencies(provider string, currencies []string) []string {
	out := []string{}
	for _, currency := range NormalizeCurrencies(currencies) {
		if provider == ProviderTON && currency == "GRAM" || (provider == ProviderBinance || provider == ProviderOKX) && (currency == "USDT" || currency == "USDC") {
			out = append(out, currency)
		}
	}
	return out
}

func (c *Config) ProviderCurrencies(provider string) []string {
	var currencies []string
	switch provider {
	case ProviderBinance:
		currencies = c.Checkout.BinanceCurrencies
	case ProviderOKX:
		currencies = c.Checkout.OKXCurrencies
	case ProviderTON:
		currencies = c.Checkout.TONCurrencies
	default:
		return nil
	}
	if currencies == nil {
		currencies = c.Checkout.Currencies
	}
	return FilterProviderCurrencies(provider, currencies)
}

func (c *Config) ProviderCurrency(provider, currency string) bool {
	for _, v := range c.ProviderCurrencies(provider) {
		if v == currency {
			return true
		}
	}
	return false
}

// NormalizePaymentCurrencies resolves legacy shared settings and keeps the public union derived.
func (c *Config) NormalizePaymentCurrencies() {
	c.Checkout.BinanceCurrencies = c.ProviderCurrencies(ProviderBinance)
	c.Checkout.OKXCurrencies = c.ProviderCurrencies(ProviderOKX)
	c.Checkout.TONCurrencies = c.ProviderCurrencies(ProviderTON)
	combined := append(append(append([]string{}, c.Checkout.BinanceCurrencies...), c.Checkout.OKXCurrencies...), c.Checkout.TONCurrencies...)
	c.Checkout.Currencies = NormalizeCurrencies(combined)
}

func (c *Config) AnyProviderEnabled() bool {
	return c.BinanceEnabled() || c.OKXEnabled() || c.TONEnabled()
}

// ValidateNotifyURL enforces explicit production transport and merchant identity.
// Editing base_url cannot disable production policy; HTTPS development also keeps TLS checks.
func (c *Config) ValidateNotifyURL(raw string) error {
	u, err := url.Parse(raw)
	if err != nil || u.Hostname() == "" || u.User != nil || u.Fragment != "" || (u.Scheme != "http" && u.Scheme != "https") {
		return fmt.Errorf("notify_url 必须是有效的商户回调地址")
	}
	if c.Server.Production || strings.HasPrefix(c.Server.BaseURL, "https://") {
		if u.Scheme != "https" || u.Port() != "" && u.Port() != "443" {
			return fmt.Errorf("生产回调必须使用 HTTPS 标准端口")
		}
		for _, host := range c.Server.NotifyHosts {
			if strings.EqualFold(u.Hostname(), host) {
				return nil
			}
		}
		return fmt.Errorf("商户回调主机未列入 server.notify_hosts")
	}
	return nil
}
