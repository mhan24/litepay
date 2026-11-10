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
	SelectionChanges  int `gorm:"not null;default:0"`
	LastSelectionAt   *time.Time
	NextExpiryCheckAt time.Time `gorm:"index:idx_order_expiry_due,priority:2"`
	SigningKeyID      uint      `gorm:"not null;default:0"`
	ID                uint      `gorm:"primaryKey"`
	TradeNo           string    `gorm:"uniqueIndex;not null"`
	OutTradeNo        string    `gorm:"uniqueIndex;not null"`
	Name              string
	OriginUnits       int64 `gorm:"not null;check:origin_units > 0"`
	RateUSD           string
	RateAt            *time.Time
	PayUnits          int64
	Precision         int
	Provider          string
	Currency          string
	Receiver          string
	CurrencyLimit     string
	Reselect          bool
	Status            string `gorm:"index;index:idx_order_expiry_due,priority:1;index:idx_pending_expiry,priority:1;not null;check:status IN ('PENDING','READY','PAID','EXPIRED','CANCELED')"`
	SelectedAt        *time.Time
	ExpiresAt         time.Time `gorm:"index;index:idx_pending_expiry,priority:2"`
	PaidAt            *time.Time
	NotifyURL         string
	ReturnURL         string
	CreatedAt         time.Time
	UpdatedAt         time.Time
}

func (o Order) Original() string  { return amount.String(o.OriginUnits) }
func (o Order) PayAmount() string { return amount.Text(o.PayUnits, o.Precision) }

type Slot struct {
	Provider  string    `gorm:"primaryKey"`
	Currency  string    `gorm:"primaryKey"`
	Units     int64     `gorm:"primaryKey"`
	OrderID   *uint     `gorm:"index;index:idx_slot_cleanup,priority:1"`
	ExpiresAt time.Time `gorm:"index;index:idx_slot_cleanup,priority:2"`
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
	HistoricalTurn int    `gorm:"not null;default:0"`
	Provider       string `gorm:"primaryKey"`
	ScannedUntil   time.Time
	LastSuccess    time.Time
	LastError      string
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
	Deadline   time.Time `gorm:"index;index:idx_confirmation_expiry,priority:2"`
	State      string    `gorm:"index;index:idx_confirmation_expiry,priority:1"` // pending, resolved, manual_review
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

type IntegrationHealth struct {
	Generation    string `gorm:"not null;default:''"`
	Missing       int64  `gorm:"not null;default:0"`
	Name          string `gorm:"primaryKey"`
	Source        string
	ReportedAt    int64
	ReceivedAt    time.Time
	LastSuccessAt time.Time
	Healthy       bool
	Pending       int64
	Failures      int64
	Conflicts     int64
}

type GatewayRecovery struct {
	ID           uint   `gorm:"primaryKey"`
	Generation   string `gorm:"not null"`
	Acknowledged bool   `gorm:"not null;default:false"`
	Reason       string
	CreatedAt    time.Time
}

// An authenticated merchant cancellation also fences a delayed future creation.
type CancellationIntent struct {
	OutTradeNo string `gorm:"primaryKey"`
	TradeNo    string `gorm:"uniqueIndex"`
	CreatedAt  time.Time
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
	CoverageAfter time.Time
	RequiredAfter time.Time
	Priority      int   `gorm:"not null;default:0;index:idx_hist_dispatch,priority:3"`
	ChunkMillis   int64 `gorm:"not null;default:3600000"`
	Automatic     bool  `gorm:"not null;default:false;index:idx_final_pass,priority:1;index:idx_hist_auto,priority:1"`
	ID            uint  `gorm:"primaryKey;index:idx_hist_auto,priority:4"`
	OrderID       uint  `gorm:"index"`
	AttemptID     uint
	Provider      string `gorm:"index:idx_final_pass,priority:2;index:idx_hist_dispatch,priority:1;index:idx_hist_pending,priority:1;index:idx_hist_auto,priority:2"`
	Receiver      string `gorm:"index:idx_final_pass,priority:3;index:idx_hist_auto,priority:3"`
	From          time.Time
	To            time.Time
	Cursor        time.Time
	ChunkTo       time.Time
	LT            string
	Hash          string
	OldestAt      time.Time
	Pages         int
	State         string `gorm:"index:idx_final_pass,priority:4;index:idx_hist_dispatch,priority:2;index:idx_hist_pending,priority:2"`
	LastError     string
	Operator      string
	Reason        string
	CreatedAt     time.Time `gorm:"index:idx_final_pass,priority:5"`
	UpdatedAt     time.Time `gorm:"index:idx_hist_pending,priority:3;index:idx_hist_dispatch,priority:4"`
}

// Real, completed API coverage. Reusing it never refreshes ObservedAt.
type HistoryCoverage struct {
	ID         uint      `gorm:"primaryKey"`
	Provider   string    `gorm:"index:idx_coverage,priority:1"`
	Receiver   string    `gorm:"index:idx_coverage,priority:2"`
	From       time.Time `gorm:"index:idx_coverage,priority:3"`
	To         time.Time
	ObservedAt time.Time `gorm:"index"`
	ScanID     uint      `gorm:"index"`
}

func (a PaymentAttempt) Amount() string { return amount.Text(a.Units, a.Precision) }

func (o Order) PaymentPhase(now time.Time) string {
	switch o.Status {
	case Paid:
		return "paid"
	case Canceled, Expired:
		return "closed"
	}
	if !now.Before(o.ExpiresAt) {
		if o.Status == Ready {
			return "settling"
		}
		return "closed"
	}
	return "accepting_payment"
}
