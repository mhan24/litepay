// Package config 负责加载网关启动配置。
//
// 配置模型：
//  1. config.yaml 只保存服务监听、数据库路径、后台登录和日志级别。
//  2. 支付平台、收款 UID、订单与回调只保存在 SQLite，由后台设置页维护。
//  3. 不读取 .env 或环境变量；YAML 只解析启动配置。
package config

import (
	"fmt"
	"net"
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
	Bybit    BybitConfig    `yaml:"-"`
	Bitget   BitgetConfig   `yaml:"-"`
	Checkout CheckoutConfig `yaml:"-"`
	Order    OrderConfig    `yaml:"-"`
	Notify   NotifyConfig   `yaml:"-"`
}

const (
	ProviderBinance = "binance"
	ProviderOKX     = "okx"
	ProviderBybit   = "bybit"
	ProviderBitget  = "bitget"
)

// DefaultProviderOrder 定义收银台收款平台的默认展示顺序。
var DefaultProviderOrder = []string{ProviderBinance, ProviderOKX, ProviderBybit, ProviderBitget}

// NormalizeProviderOrder 规范化平台排序：去重、忽略未知项，并自动补全缺失的默认平台。
func NormalizeProviderOrder(in []string) []string {
	valid := map[string]bool{
		ProviderBinance: true,
		ProviderOKX:     true,
		ProviderBybit:   true,
		ProviderBitget:  true,
	}
	seen := make(map[string]bool, len(DefaultProviderOrder))
	out := make([]string, 0, len(DefaultProviderOrder))
	for _, raw := range in {
		p := strings.ToLower(strings.TrimSpace(raw))
		if !valid[p] || seen[p] {
			continue
		}
		seen[p] = true
		out = append(out, p)
	}
	for _, p := range DefaultProviderOrder {
		if !seen[p] {
			out = append(out, p)
		}
	}
	return out
}

// SupportedCurrencies 是收银台和到账确认逻辑允许的币种集合。
var SupportedCurrencies = []string{"USDT", "USDC"}

// NormalizeCurrencies 将币种规范为大写、去重，并只保留 USDT/USDC。
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
	case "USDT", "USDC":
		return true
	default:
		return false
	}
}

// ServerConfig HTTP 服务配置。
type ServerConfig struct {
	CancelSyncRequired  bool     `yaml:"cancel_sync_required"`
	CallbackWorkers     int      `yaml:"callback_workers"`
	MerchantKeyVersions bool     `yaml:"merchant_key_versions"` // Enable only after merchant compatibility acceptance.
	Production          bool     `yaml:"production"`
	NotifyHosts         []string `yaml:"notify_hosts"`    // Exact callback hostnames in production; no wildcards.
	StoreURL            string   `yaml:"store_url"`       // 根地址跳转目标；留空返回 404。
	TrustedProxies      []string `yaml:"trusted_proxies"` // Only explicit reverse proxies may supply client IP headers.
	MasterKeyFile       string   `yaml:"master_key_file"` // 敏感凭据加密主密钥只读文件路径（也可通过 LITEPAY_MASTER_KEY_FILE 指定）
	ReadinessToken      string   `yaml:"readiness_token"` // /readyz 详细状态只读探针令牌
	ProbeToken          string   `yaml:"probe_token"`     // readiness_token 的兼容别名
	Addr                string   `yaml:"addr"`            // 监听地址，如 0.0.0.0:8080
	BaseURL             string   `yaml:"base_url"`        // 网关对外访问地址
}

// ProbeTokenValue 返回配置的只读健康检查探针令牌。
func (s ServerConfig) ProbeTokenValue() string {
	if v := strings.TrimSpace(s.ReadinessToken); v != "" {
		return v
	}
	return strings.TrimSpace(s.ProbeToken)
}

// ValidateProbeToken permits disabling probes, but rejects weak configured tokens, including the legacy alias.
func (s ServerConfig) ValidateProbeToken() error {
	for _, token := range []struct{ name, value string }{{"readiness_token", s.ReadinessToken}, {"probe_token", s.ProbeToken}} {
		if value := strings.TrimSpace(token.value); value != "" && len(value) < 16 {
			return fmt.Errorf("server.%s 至少需要 16 字节随机令牌", token.name)
		}
	}
	return nil
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

// BybitConfig Bybit V5 只读 API 与站内转账收款配置。
type BybitConfig struct {
	APIKey         string `yaml:"api_key"`
	APISecret      string `yaml:"api_secret"`
	BaseURL        string `yaml:"base_url"`                // 默认 https://api.bybit.com
	PollInterval   int    `yaml:"poll_interval"`           // 查询间隔（秒）
	RecvWindow     int    `yaml:"recv_window"`             // recvWindow 毫秒
	Limit          int    `yaml:"limit"`                   // 单次查询条数，最大 50
	LookbackBuffer int    `yaml:"lookback_buffer_minutes"` // 订单时间回看缓冲（分钟）
}

// BitgetConfig Bitget 只读 API 与站内转账收款配置。
type BitgetConfig struct {
	APIKey         string `yaml:"api_key"`
	APISecret      string `yaml:"api_secret"`
	Passphrase     string `yaml:"passphrase"`
	BaseURL        string `yaml:"base_url"`                // 默认 https://api.bitget.com
	PollInterval   int    `yaml:"poll_interval"`           // 查询间隔（秒）
	Limit          int    `yaml:"limit"`                   // 单次查询条数，最大 100
	LookbackBuffer int    `yaml:"lookback_buffer_minutes"` // 订单时间回看缓冲（分钟）
}

// CheckoutConfig 收银台与交易所收款平台的展示配置。
type CheckoutConfig struct {
	BinanceCurrencies []string        `yaml:"-"`
	OKXCurrencies     []string        `yaml:"-"`
	BybitCurrencies   []string        `yaml:"-"`
	BitgetCurrencies  []string        `yaml:"-"`
	ProviderOrder     []string        `yaml:"-"`
	Currencies        []string        `yaml:"currencies"` // 支持的币种，如 ["USDT","USDC"]
	Note              string          `yaml:"note"`       // 收银台提示文案
	Binance           ProviderDisplay `yaml:"binance"`
	OKX               ProviderDisplay `yaml:"okx"`
	Bybit             ProviderDisplay `yaml:"bybit"`
	Bitget            ProviderDisplay `yaml:"bitget"`
}

// ProviderDisplay 收款平台展示信息。UID 同时用于展示、转账和到账归属校验。
type ProviderDisplay struct {
	UID         string `yaml:"uid"`
	RegisterURL string `yaml:"register_url"`
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
		Bybit: BybitConfig{
			BaseURL:        "https://api.bybit.com",
			PollInterval:   4,
			RecvWindow:     5000,
			Limit:          50,
			LookbackBuffer: 2,
		},
		Bitget: BitgetConfig{
			BaseURL:        "https://api.bitget.com",
			PollInterval:   4,
			Limit:          100,
			LookbackBuffer: 2,
		},
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
	if data, err := os.ReadFile(path); err == nil { // #nosec G304 -- The local operator explicitly selects this configuration path via the startup CLI.
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
	c.Admin.Path = strings.TrimSpace(c.Admin.Path)
	c.Server.Addr = strings.TrimSpace(c.Server.Addr)
	if c.Server.Addr == "" {
		c.Server.Addr = "0.0.0.0:8080"
	}
	c.Server.StoreURL = strings.TrimSpace(c.Server.StoreURL)
	c.Server.BaseURL = strings.TrimRight(strings.TrimSpace(c.Server.BaseURL), "/")
	if c.Server.BaseURL == "" {
		c.Server.BaseURL = "http://127.0.0.1" + portSuffix(c.Server.Addr)
	}
	for i, host := range c.Server.NotifyHosts {
		host = strings.ToLower(strings.TrimSpace(host))
		if host == "" || strings.ContainsAny(host, " \t\r\n/:@*?#\\[]") || strings.HasPrefix(host, ".") || strings.HasSuffix(host, ".") {
			return fmt.Errorf("server.notify_hosts 只能填写精确主机名")
		}
		c.Server.NotifyHosts[i] = host
	}
	base, err := url.Parse(c.Server.BaseURL)
	if err != nil || (base.Scheme != "http" && base.Scheme != "https") || base.Hostname() == "" || base.User != nil || base.RawQuery != "" || base.Fragment != "" || (base.Path != "" && base.Path != "/") {
		return fmt.Errorf("server.base_url 必须是无路径、查询参数和用户信息的 HTTP(S) 站点地址")
	}
	c.Server.BaseURL = base.Scheme + "://" + base.Host
	if c.Server.Production && (base.Scheme != "https" || len(c.Server.NotifyHosts) == 0) {
		return fmt.Errorf("production 模式要求 HTTPS base_url 和非空 notify_hosts")
	}
	if err := c.ValidateWebRoutes(); err != nil {
		return err
	}
	c.Binance.BaseURL = strings.TrimRight(strings.TrimSpace(c.Binance.BaseURL), "/")
	if c.Binance.BaseURL == "" {
		c.Binance.BaseURL = "https://api.binance.com"
	}
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
	c.Bybit.BaseURL = strings.TrimRight(strings.TrimSpace(c.Bybit.BaseURL), "/")
	if c.Bybit.BaseURL == "" {
		c.Bybit.BaseURL = "https://api.bybit.com"
	}
	if c.Bybit.PollInterval < 1 {
		c.Bybit.PollInterval = 4
	}
	if c.Bybit.RecvWindow <= 0 {
		c.Bybit.RecvWindow = 5000
	}
	if c.Bybit.Limit <= 0 || c.Bybit.Limit > 50 {
		c.Bybit.Limit = 50
	}
	if c.Bybit.LookbackBuffer <= 0 {
		c.Bybit.LookbackBuffer = 2
	}
	c.Bitget.BaseURL = strings.TrimRight(strings.TrimSpace(c.Bitget.BaseURL), "/")
	if c.Bitget.BaseURL == "" {
		c.Bitget.BaseURL = "https://api.bitget.com"
	}
	if c.Bitget.PollInterval < 1 {
		c.Bitget.PollInterval = 4
	}
	if c.Bitget.Limit <= 0 || c.Bitget.Limit > 100 {
		c.Bitget.Limit = 100
	}
	if c.Bitget.LookbackBuffer <= 0 {
		c.Bitget.LookbackBuffer = 2
	}
	c.Checkout.Binance.UID = strings.TrimSpace(c.Checkout.Binance.UID)
	c.Checkout.Binance.RegisterURL = strings.TrimSpace(c.Checkout.Binance.RegisterURL)
	c.Checkout.OKX.UID = strings.TrimSpace(c.Checkout.OKX.UID)
	c.Checkout.OKX.RegisterURL = strings.TrimSpace(c.Checkout.OKX.RegisterURL)
	c.Checkout.Bybit.UID = strings.TrimSpace(c.Checkout.Bybit.UID)
	c.Checkout.Bybit.RegisterURL = strings.TrimSpace(c.Checkout.Bybit.RegisterURL)
	c.Checkout.Bitget.UID = strings.TrimSpace(c.Checkout.Bitget.UID)
	c.Checkout.Bitget.RegisterURL = strings.TrimSpace(c.Checkout.Bitget.RegisterURL)
	c.Checkout.ProviderOrder = NormalizeProviderOrder(c.Checkout.ProviderOrder)
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
	c.Admin.Username = strings.TrimSpace(c.Admin.Username)
	if c.Admin.Username == "" {
		c.Admin.Username = "admin"
	}
	if c.Admin.SessionHours <= 0 {
		c.Admin.SessionHours = 12
	}
	if utf8.RuneCountInString(c.Admin.Username) > 64 {
		return fmt.Errorf("admin.username 不能超过 64 个字符")
	}
	if c.Server.Production && c.Admin.SessionHours > 72 {
		return fmt.Errorf("production 模式下 admin.session_hours 需在 1~72 小时之间")
	}
	if c.Admin.SessionHours > 720 {
		return fmt.Errorf("admin.session_hours 需在 1~720 小时之间")
	}
	if strings.TrimSpace(c.Admin.Password) == "" {
		c.Admin.Password = ""
	}
	if c.Admin.Password != "" {
		if IsBcryptHash(c.Admin.Password) {
			// bcrypt 哈希密码（$2a$ / $2b$ / $2y$）
		} else if strings.HasPrefix(c.Admin.Password, "$2") {
			return fmt.Errorf("admin.password bcrypt 哈希格式无效")
		} else {
			if c.Server.Production {
				return fmt.Errorf("production 模式要求 admin.password 必须为 bcrypt 哈希密码，禁止明文密码")
			}
			if utf8.RuneCountInString(c.Admin.Password) < 12 {
				return fmt.Errorf("admin.password 明文密码至少需要 12 个字符")
			}
		}
	}
	c.Server.MasterKeyFile = strings.TrimSpace(c.Server.MasterKeyFile)
	c.Server.ReadinessToken = strings.TrimSpace(c.Server.ReadinessToken)
	c.Server.ProbeToken = strings.TrimSpace(c.Server.ProbeToken)
	if c.Server.Production && len(c.Server.TrustedProxies) == 0 {
		fmt.Fprintln(os.Stderr, "[WARN] production 模式下 server.trusted_proxies 为空，若部署在反向代理后客户端 IP 将共用代理地址")
	}
	if c.Admin.Password == "" {
		fmt.Fprintln(os.Stderr, "[WARN] 未在 config.yaml 配置 admin.password，后台管理已禁用")
	}
	return nil
}

var bcryptHashPattern = regexp.MustCompile(`^\$2[aby]\$(?:0[4-9]|[12][0-9]|3[01])\$[./A-Za-z0-9]{53}$`)

// IsBcryptHash 判断密码配置是否为合法的 60 位 bcrypt 哈希（支持 $2a$、$2b$、$2y$ 前缀，cost 04~31）。
func IsBcryptHash(s string) bool {
	return bcryptHashPattern.MatchString(s)
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
	if err := c.Server.ValidateProbeToken(); err != nil {
		return err
	}
	root := c.AdminPath()
	if !regexp.MustCompile(`^/[A-Za-z0-9][A-Za-z0-9_-]{2,95}$`).MatchString(root) {
		return fmt.Errorf("admin.path 必须为 / 开头、3~96 位字母数字或下划线及连字符的单层路径")
	}
	switch strings.ToLower(root) {
	case "/pay", "/api", "/assets", "/healthz", "/readyz":
		return fmt.Errorf("admin.path 与系统路径冲突")
	}
	if c.Server.StoreURL != "" {
		u, err := url.Parse(c.Server.StoreURL)
		if err != nil || (u.Scheme != "http" && u.Scheme != "https") || u.Hostname() == "" || u.User != nil || strings.ContainsAny(c.Server.StoreURL, "\r\n") {
			return fmt.Errorf("server.store_url 必须是无用户信息的 HTTP(S) 商店地址")
		}
		baseRaw := strings.TrimRight(strings.TrimSpace(c.Server.BaseURL), "/")
		if baseRaw == "" {
			baseRaw = "http://127.0.0.1" + portSuffix(c.Server.Addr)
		}
		base, _ := url.Parse(baseRaw)
		if base != nil && sameSiteHost(u, base) {
			return fmt.Errorf("server.store_url 必须指向网关以外的商店站点")
		}
	}
	return nil
}

func sameSiteHost(a, b *url.URL) bool {
	if !strings.EqualFold(strings.TrimSuffix(a.Hostname(), "."), strings.TrimSuffix(b.Hostname(), ".")) {
		return false
	}
	portOf := func(u *url.URL) string {
		if p := u.Port(); p != "" {
			if (u.Scheme == "http" && p == "80") || (u.Scheme == "https" && p == "443") {
				return ""
			}
			return p
		}
		return ""
	}
	return portOf(a) == portOf(b)
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

// BybitEnabled 是否具备 Bybit 站内转账到账确认条件。
func (c *Config) BybitEnabled() bool {
	return len(c.ProviderCurrencies(ProviderBybit)) > 0 && strings.TrimSpace(c.Bybit.APIKey) != "" &&
		strings.TrimSpace(c.Bybit.APISecret) != "" &&
		strings.TrimSpace(c.Checkout.Bybit.UID) != ""
}

// BitgetEnabled 是否具备 Bitget 站内转账到账确认条件。
func (c *Config) BitgetEnabled() bool {
	return len(c.ProviderCurrencies(ProviderBitget)) > 0 && strings.TrimSpace(c.Bitget.APIKey) != "" &&
		strings.TrimSpace(c.Bitget.APISecret) != "" &&
		strings.TrimSpace(c.Bitget.Passphrase) != "" &&
		strings.TrimSpace(c.Checkout.Bitget.UID) != ""
}

// ProviderEnabled 某收款平台是否已完成只读 API 配置。
func (c *Config) ProviderEnabled(provider string) bool {
	switch strings.ToLower(strings.TrimSpace(provider)) {
	case ProviderBinance:
		return c.BinanceEnabled()
	case ProviderOKX:
		return c.OKXEnabled()
	case ProviderBybit:
		return c.BybitEnabled()
	case ProviderBitget:
		return c.BitgetEnabled()
	default:
		return false
	}
}

// ProviderInterval 返回指定平台的查询间隔。
func (c *Config) ProviderInterval(provider string) int {
	provider = strings.ToLower(strings.TrimSpace(provider))
	if provider == ProviderOKX {
		return c.OKX.PollInterval
	}
	if provider == ProviderBybit {
		return c.Bybit.PollInterval
	}
	if provider == ProviderBitget {
		return c.Bitget.PollInterval
	}
	return c.Binance.PollInterval
}

// ProviderLookback 返回指定通道的订单时间回看缓冲。
func (c *Config) ProviderLookback(provider string) int {
	provider = strings.ToLower(strings.TrimSpace(provider))
	if provider == ProviderOKX {
		return c.OKX.LookbackBuffer
	}
	if provider == ProviderBybit {
		return c.Bybit.LookbackBuffer
	}
	if provider == ProviderBitget {
		return c.Bitget.LookbackBuffer
	}
	return c.Binance.LookbackBuffer
}

// InitialScanLookback bounds only the initial history query. Confirmation tasks have their own
// persistent deadline based on the actual order window, independent of this default.
func (c *Config) InitialScanLookback() time.Duration {
	return time.Duration(c.Order.ExpireMinutes+max(c.Binance.LookbackBuffer, c.OKX.LookbackBuffer, c.Bybit.LookbackBuffer, c.Bitget.LookbackBuffer))*time.Minute +
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
	if _, port, err := net.SplitHostPort(strings.TrimSpace(addr)); err == nil && port != "" {
		return ":" + port
	}
	return ":8080"
}

// FilterProviderCurrencies keeps each currency within its supported payment channels.
func FilterProviderCurrencies(provider string, currencies []string) []string {
	provider = strings.ToLower(strings.TrimSpace(provider))
	out := []string{}
	for _, currency := range NormalizeCurrencies(currencies) {
		if (provider == ProviderBinance || provider == ProviderOKX || provider == ProviderBybit || provider == ProviderBitget) && (currency == "USDT" || currency == "USDC") {
			out = append(out, currency)
		}
	}
	return out
}

func (c *Config) ProviderCurrencies(provider string) []string {
	provider = strings.ToLower(strings.TrimSpace(provider))
	var currencies []string
	switch provider {
	case ProviderBinance:
		currencies = c.Checkout.BinanceCurrencies
	case ProviderOKX:
		currencies = c.Checkout.OKXCurrencies
	case ProviderBybit:
		currencies = c.Checkout.BybitCurrencies
	case ProviderBitget:
		currencies = c.Checkout.BitgetCurrencies
	default:
		return nil
	}
	if currencies == nil {
		currencies = c.Checkout.Currencies
	}
	return FilterProviderCurrencies(provider, currencies)
}

func (c *Config) ProviderCurrency(provider, currency string) bool {
	currency = strings.ToUpper(strings.TrimSpace(currency))
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
	c.Checkout.BybitCurrencies = c.ProviderCurrencies(ProviderBybit)
	c.Checkout.BitgetCurrencies = c.ProviderCurrencies(ProviderBitget)
	combined := append(append(append(append([]string{}, c.Checkout.BinanceCurrencies...), c.Checkout.OKXCurrencies...), c.Checkout.BybitCurrencies...), c.Checkout.BitgetCurrencies...)
	c.Checkout.Currencies = NormalizeCurrencies(combined)
}

func (c *Config) AnyProviderEnabled() bool {
	return c.BinanceEnabled() || c.OKXEnabled() || c.BybitEnabled() || c.BitgetEnabled()
}

// OrderedProviders 返回按后台配置排序后的全部平台键列表。
func (c *Config) OrderedProviders() []string {
	return NormalizeProviderOrder(c.Checkout.ProviderOrder)
}

// ValidateNotifyURL enforces explicit production transport and merchant identity.
// Editing base_url cannot disable production policy; HTTPS development also keeps TLS checks.
func (c *Config) ValidateNotifyURL(raw string) error {
	if raw == "" || strings.TrimSpace(raw) != raw {
		return fmt.Errorf("notify_url 必须是有效的商户回调地址")
	}
	u, err := url.Parse(raw)
	if err != nil || u.Hostname() == "" || u.User != nil || u.Fragment != "" || (u.Scheme != "http" && u.Scheme != "https") {
		return fmt.Errorf("notify_url 必须是有效的商户回调地址")
	}
	if c.Server.Production || strings.HasPrefix(strings.ToLower(strings.TrimSpace(c.Server.BaseURL)), "https://") {
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
