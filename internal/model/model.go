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
	SigningKeyID  uint   `gorm:"not null;default:0"`
	ID            uint   `gorm:"primaryKey"`
	TradeNo       string `gorm:"uniqueIndex;not null"`
	OutTradeNo    string `gorm:"uniqueIndex;not null"`
	Name          string
	OriginUnits   int64 `gorm:"not null;check:origin_units > 0"`
	RateUSD       string
	RateAt        *time.Time
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
	AttemptID        *uint
	MasterchainSeqno int64
	LogicalTime      string
	Provider         string `gorm:"primaryKey"`
	EventID          string `gorm:"primaryKey"`
	Reference        string `gorm:"index"`
	Receiver         string
	Currency         string
	Units            int64
	OccurredAt       time.Time `gorm:"index"`
	State            string
	Reason           string
	RawAmount        string
	Assets           string
	OrderID          *uint `gorm:"uniqueIndex"`
	UpdatedAt        time.Time
}

func (t Transaction) Amount() string { return amount.String(t.Units) }

type Callback struct {
	SigningKeyID uint   `gorm:"not null;default:0"`
	AttemptBase  int    `gorm:"not null;default:0"`
	AttemptLimit int    `gorm:"not null;default:0"`
	State        string `gorm:"index;not null;default:queued"`
	LeaseUntil   time.Time
	Recoveries   int  `gorm:"not null;default:0"`
	OrderID      uint `gorm:"primaryKey"`
	Attempts     int
	NextAt       time.Time `gorm:"index"`
	DeliveredAt  *time.Time
	LastError    string
	UpdatedAt    time.Time
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

// ConfirmationTask survives restarts and the incremental scan cursor. Deadline never slides.
type ConfirmationTask struct {
	Provider   string `gorm:"primaryKey"`
	EventID    string `gorm:"primaryKey"`
	OrderID    *uint  `gorm:"index"`
	OccurredAt time.Time
	Deadline   time.Time `gorm:"index"`
	State      string    `gorm:"index"` // pending, resolved, manual_review
	UpdatedAt  time.Time
}

// TONScanCheckpoint stores the next inclusive LT/hash boundary for one fixed time range.
type TONScanCheckpoint struct {
	Receiver  string `gorm:"primaryKey"`
	From      time.Time
	To        time.Time
	LT        string
	Hash      string
	OldestAt  time.Time
	Pages     int
	Active    bool
	UpdatedAt time.Time
}

type CallbackRecovery struct {
	ID               uint `gorm:"primaryKey"`
	OrderID          uint `gorm:"index"`
	Operator         string
	Reason           string
	PreviousAttempts int
	CreatedAt        time.Time
}

// PaymentAttempt is an immutable payment instruction, including superseded selections.
type PaymentAttempt struct {
	RecoveredFromEvent string
	ID                 uint   `gorm:"primaryKey"`
	OrderID            uint   `gorm:"index;not null"`
	Provider           string `gorm:"index:attempt_match"`
	Receiver           string `gorm:"index:attempt_match"`
	Currency           string `gorm:"index:attempt_match"`
	Units              int64  `gorm:"index:attempt_match"`
	Precision          int
	SelectedAt         time.Time
	ExpiresAt          time.Time
	SettleUntil        time.Time `gorm:"index"`
	RateUSD            string
	RateAt             *time.Time
	CreatedAt          time.Time
}

type SigningKey struct {
	ID        uint   `gorm:"primaryKey"`
	Token     string `gorm:"not null"`
	RetiredAt *time.Time
	CreatedAt time.Time
}

type PaymentRecovery struct {
	ID        uint `gorm:"primaryKey"`
	OrderID   uint `gorm:"index"`
	AttemptID uint
	Provider  string
	EventID   string
	Operator  string
	Reason    string
	CreatedAt time.Time
}

// HistoricalScan is an audited, resumable administrative request; it never changes a
// closed order's status. Evidence must still pass normal provider and finality validation.
type HistoricalScan struct {
	Automatic bool `gorm:"not null;default:false"`
	ID        uint `gorm:"primaryKey"`
	OrderID   uint `gorm:"index"`
	AttemptID uint
	Provider  string `gorm:"index"`
	Receiver  string
	From      time.Time
	To        time.Time
	Cursor    time.Time
	ChunkTo   time.Time
	LT        string
	Hash      string
	OldestAt  time.Time
	Pages     int
	State     string `gorm:"index"`
	LastError string
	Operator  string
	Reason    string
	CreatedAt time.Time
	UpdatedAt time.Time
}

func (a PaymentAttempt) Amount() string { return amount.Text(a.Units, a.Precision) }
