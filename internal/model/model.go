package model

import (
	"time"

	"litepay/internal/amount"
)

const (
	Pending  = "PENDING"
	Ready    = "READY"
	Paid     = "PAID"
	Expired  = "EXPIRED"
	Canceled = "CANCELED"
)
const (
	Confirmed   = "confirmed"
	Rejected    = "rejected"
	Ambiguous   = "ambiguous"
	Unconfirmed = "pending"
)

type Order struct {
	ID            uint   `gorm:"primaryKey"`
	TradeNo       string `gorm:"uniqueIndex;not null"`
	OutTradeNo    string `gorm:"uniqueIndex;not null"`
	Name          string
	OriginUnits   int64 `gorm:"not null;check:origin_units > 0"`
	PayUnits      int64
	Precision     int
	Provider      string
	Currency      string
	Receiver      string
	CurrencyLimit string
	Reselect      bool
	Status        string `gorm:"index;not null;check:status IN ('PENDING','READY','PAID','EXPIRED','CANCELED')"`
	SelectedAt    *time.Time
	ExpiresAt     time.Time `gorm:"index"`
	PaidAt        *time.Time
	NotifyURL     string
	ReturnURL     string
	CreatedAt     time.Time
	UpdatedAt     time.Time
}

func (o Order) Original() string  { return amount.String(o.OriginUnits) }
func (o Order) PayAmount() string { return amount.Text(o.PayUnits, o.Precision) }

type Slot struct {
	Provider  string    `gorm:"primaryKey"`
	Currency  string    `gorm:"primaryKey"`
	Units     int64     `gorm:"primaryKey"`
	OrderID   *uint     `gorm:"index"`
	ExpiresAt time.Time `gorm:"index"`
}

// Transaction is both the exchange evidence and its unique consumption record.
// Assets holds all asset details for ambiguous events; those events never become claimable.
// RawAmount keeps the exact original amount text of records rejected for amount reasons
// (invalid_amount / conflicting_amount), so manual reconciliation never depends on rounding.
type Transaction struct {
	Provider   string `gorm:"primaryKey"`
	EventID    string `gorm:"primaryKey"`
	Reference  string `gorm:"index"`
	Receiver   string
	Currency   string
	Units      int64
	OccurredAt time.Time `gorm:"index"`
	State      string
	Reason     string
	RawAmount  string
	Assets     string
	OrderID    *uint `gorm:"uniqueIndex"`
	UpdatedAt  time.Time
}

func (t Transaction) Amount() string { return amount.String(t.Units) }

type Callback struct {
	OrderID     uint `gorm:"primaryKey"`
	Attempts    int
	NextAt      time.Time `gorm:"index"`
	DeliveredAt *time.Time
	LastError   string
	UpdatedAt   time.Time
}
type NotifyLog struct {
	ID         uint `gorm:"primaryKey"`
	OrderID    uint `gorm:"index"`
	Attempt    int
	StatusCode int
	Success    bool
	Error      string
	CreatedAt  time.Time
}
type PaymentInquiry struct {
	TradeNo       string `gorm:"primaryKey"`
	Reference     string
	Result        string
	Attempts      int
	LastAttemptAt time.Time
}
type ScanCursor struct {
	Provider     string `gorm:"primaryKey"`
	ScannedUntil time.Time
	LastSuccess  time.Time
	LastError    string
	// 未确认到账的补查是独立的一路：它不推进 ScannedUntil，失败只记录在这里。
	PendingError string
	PendingAt    time.Time
}
type Setting struct {
	ID        uint `gorm:"primaryKey"`
	Payload   string
	UpdatedBy string
	UpdatedAt time.Time
}
type Credential struct {
	Name      string `gorm:"primaryKey"`
	Value     string
	UpdatedBy string
	UpdatedAt time.Time
}
