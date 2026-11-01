// Package model 定义数据库模型。
package model

import "time"

// 订单状态
const (
	StatusPending  = "PENDING"  // 待支付（尚未选择收款平台）
	StatusReady    = "READY"    // 已选平台与币种，等待收款
	StatusPaid     = "PAID"     // 已支付（已确认到账）
	StatusExpired  = "EXPIRED"  // 已过期
	StatusCanceled = "CANCELED" // 商户已取消，停止继续确认到账
	// StatusSuperseded 表示同一商户订单号已创建了更新的收银台订单。
	// 旧订单停止监听并丢弃未发送回调，避免它关闭商户的新订单。
	StatusSuperseded = "SUPERSEDED"
)

// 回调通知状态
const (
	NotifyPending  = "PENDING"  // 待通知 / 通知失败待重试
	NotifySuccess  = "SUCCESS"  // 商户已确认收到（返回 success）
	NotifyFailed   = "FAILED"   // 达到最大重试次数仍未成功
	NotifyCanceled = "CANCELED" // 商户取消，无需发送过期结果
)

// Order 本地支付订单。
type Order struct {
	ID uint `gorm:"primaryKey" json:"id"`

	Pid        string `gorm:"size:32;index" json:"pid"`            // 商户ID
	TradeNo    string `gorm:"size:64;uniqueIndex" json:"trade_no"` // 网关订单编号
	OutTradeNo string `gorm:"size:128;index" json:"out_trade_no"`  // 商户订单号
	Protocol   string `gorm:"size:16;index" json:"protocol"`       // BEpusdt API 协议
	Name       string `gorm:"size:255" json:"name"`                // 商品名称

	// 金额：origin_money 为发起时的原始 USD 金额字符串（回调必须原样返回），
	// pay_amount 为含浮动尾数的实际应付稳定币数量；修约匹配按该金额的
	// 展示精度处理，尾差只用于区分同金额并发订单。
	OriginMoney     string     `gorm:"size:32" json:"origin_money"`
	OriginCurrency  string     `gorm:"size:16" json:"origin_currency"`
	PayAmount       float64    `gorm:"column:pay_amount" json:"pay_amount"`
	Currency        string     `gorm:"size:16" json:"currency"`
	AmountPrecision int        `gorm:"not null;default:2" json:"amount_precision"`
	SelectedAt      *time.Time `json:"selected_at"`
	SlotOffset      int        `json:"slot_offset"` // 金额调整次数（amount_step * n）

	Status string `gorm:"size:16;index" json:"status"`

	NotifyURL      string     `gorm:"size:512" json:"notify_url"`
	ReturnURL      string     `gorm:"size:512" json:"return_url"`
	NotifyStatus   string     `gorm:"size:16;index" json:"notify_status"`
	NotifyAttempts int        `json:"notify_attempts"`
	NotifyResets   int        `gorm:"not null;default:0" json:"notify_resets"`
	NextNotifyAt   *time.Time `json:"next_notify_at"`
	NotifiedAt     *time.Time `json:"notified_at"`
	LastNotifyResp string     `gorm:"size:512" json:"last_notify_resp"`

	// 收款平台由用户在收银台确认；PayAddress 是收款 UID，
	// ReceiveAddr 是交易所到账记录中的实际收款 UID。
	Collection    string     `gorm:"size:16;index" json:"collection"`
	PayAddress    string     `gorm:"size:256" json:"pay_address"`
	CurrencyLimit string     `gorm:"size:64" json:"currency_limit"`
	Reselect      bool       `gorm:"not null;default:false" json:"reselect"`
	TxID          *string    `gorm:"size:128;index" json:"tx_id"`
	TxTime        *time.Time `gorm:"column:tx_time" json:"tx_time"`
	ReceiveAddr   string     `gorm:"size:256" json:"receive_addr"`
	PaidAt        *time.Time `json:"paid_at"`

	ClientIP  string    `gorm:"size:64" json:"client_ip"`
	ExpiredAt time.Time `gorm:"index" json:"expired_at"`
	// ExpireFinalized 表示支付宽限期已经结束。
	// 自动过期后的宽限期内仍允许晚到记录入账；一旦结束就永久停止确认到账，
	// 并在同一事务中安排 status=3 过期通知。
	ExpireFinalized bool      `gorm:"not null;default:false;index" json:"expire_finalized"`
	CreatedAt       time.Time `json:"created_at"`
	UpdatedAt       time.Time `json:"updated_at"`
}

// TableName 指定表名。
func (Order) TableName() string { return "orders" }

// AmountSlot 用于区分同时到账订单的金额占用记录。
//
// 通过对 (provider, currency, amount) 建唯一索引，币安和 OKX 可各自
// 使用相同的精确金额而不会互相占用。
type AmountSlot struct {
	AmountPrecision int       `gorm:"not null;default:2"`
	ID              uint      `gorm:"primaryKey"`
	Provider        string    `gorm:"size:16;uniqueIndex:uk_slot_provider_currency_amount"`
	Currency        string    `gorm:"size:16;uniqueIndex:uk_slot_provider_currency_amount"`
	Amount          float64   `gorm:"uniqueIndex:uk_slot_provider_currency_amount"`
	OrderID         uint      `gorm:"index"`
	TradeNo         string    `gorm:"size:64;index"`
	ExpiresAt       time.Time `gorm:"index"`
	CreatedAt       time.Time
}

// TableName 指定表名。
func (AmountSlot) TableName() string { return "amount_slots" }

// IncomingTx 已处理过的收款流水（幂等表）。交易所流水号只在各自通道内唯一，
// 因此使用 (provider, transaction_id) 复合主键，避免币安和 OKX 互相碰撞。
type IncomingTx struct {
	Provider        string    `gorm:"primaryKey;size:16"`
	TransactionID   string    `gorm:"primaryKey;size:128"`
	Type            string    `gorm:"size:16"`
	Amount          float64   `gorm:"column:amount"`
	Currency        string    `gorm:"size:16;index"`
	ReceiverID      string    `gorm:"size:128"`
	ReceiverAddr    string    `gorm:"size:256"`
	TransactionTime time.Time `gorm:"index"`
	OrderTradeNo    string    `gorm:"size:64;index"`
	Raw             string    `gorm:"type:text"` // 有界原始 JSON 片段，便于事后审计
	CreatedAt       time.Time
}

// TableName 指定表名。
func (IncomingTx) TableName() string { return "incoming_txs" }

// NotifyLog 回调尝试记录。
type NotifyLog struct {
	ID              uint   `gorm:"primaryKey"`
	OrderID         uint   `gorm:"index"`
	TradeNo         string `gorm:"size:64;index"`
	Attempt         int    `gorm:"column:attempt"`
	URL             string `gorm:"size:512"`
	Success         bool   `gorm:"column:success"`
	StatusCode      int    `gorm:"column:status_code"`
	Response        string `gorm:"type:text"`
	Error           string `gorm:"type:text"`
	Token           string `gorm:"size:64;not null;default:'-'"`  // 审计字段：收款标识
	MerchantOrderID string `gorm:"size:128;not null;default:'-'"` // 审计字段：商户订单号
	OK              bool   `gorm:"column:ok"`
	CreatedAt       time.Time
}

// TableName 指定表名。
func (NotifyLog) TableName() string { return "notify_logs" }

// Setting 后台维护的业务配置（单行，JSON 存储）。
type Setting struct {
	ID        uint   `gorm:"primaryKey"`
	Payload   string `gorm:"type:text"` // settings.Patch 的 JSON
	UpdatedBy string `gorm:"size:64"`
	UpdatedAt time.Time
}

// TableName 指定表名。
func (Setting) TableName() string { return "settings" }

// Credential 只保存由系统生成的敏感值。Token 与普通业务配置分表，
// 这样更新 UID、订单等设置不会覆盖轮换后的 Token。
type Credential struct {
	Name      string `gorm:"primaryKey;size:64"`
	Value     string `gorm:"type:text;not null"`
	UpdatedBy string `gorm:"size:64"`
	UpdatedAt time.Time
}

func (Credential) TableName() string { return "credentials" }
