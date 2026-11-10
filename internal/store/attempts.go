package store

import (
	"errors"
	"time"

	"gorm.io/gorm"
	"litepay/internal/model"
)

// This is a conservative reconciliation bound, not a claim about exchange indexing SLAs.
const LateVisibilityWindow = 48 * time.Hour

func createAttempt(db *gorm.DB, o model.Order) error {
	if o.SelectedAt == nil || o.Provider == "" || o.PayUnits <= 0 {
		return nil
	}
	return db.Create(&model.PaymentAttempt{OrderID: o.ID, Provider: o.Provider, Receiver: o.Receiver, Currency: o.Currency, Units: o.PayUnits, Precision: o.Precision, SelectedAt: *o.SelectedAt, ExpiresAt: o.ExpiresAt, SettleUntil: o.ExpiresAt.Add(LateVisibilityWindow), RateUSD: o.RateUSD, RateAt: o.RateAt}).Error
}

func migratePaymentAttempts(db *gorm.DB) error {
	return db.Transaction(func(tx *gorm.DB) error {
		var orders []model.Order
		if err := tx.Where("provider <> '' AND selected_at IS NOT NULL AND pay_units > 0 AND NOT EXISTS (SELECT 1 FROM payment_attempts a WHERE a.order_id=orders.id)").Find(&orders).Error; err != nil {
			return err
		}
		for _, o := range orders {
			if err := createAttempt(tx, o); err != nil {
				return err
			}
			if o.Status == model.Ready {
				// Existing reservations are extended; detached historical slots cannot be guessed.
				if err := tx.Model(&model.Slot{}).Where("order_id = ?", o.ID).Update("expires_at", o.ExpiresAt.Add(LateVisibilityWindow)).Error; err != nil {
					return err
				}
			}
		}
		// A pre-migration confirmation task is authoritative evidence of an older selection.
		// Recover only that proven association; detached slots without an account/window are not guessed.
		var tasks []model.ConfirmationTask
		if err := tx.Where("order_id IS NOT NULL").Find(&tasks).Error; err != nil {
			return err
		}
		for _, q := range tasks {
			var t model.Transaction
			var o model.Order
			if err := tx.First(&t, "provider = ? AND event_id = ?", q.Provider, q.EventID).Error; err != nil {
				if errors.Is(err, gorm.ErrRecordNotFound) {
					continue
				}
				return err
			}
			if err := tx.First(&o, *q.OrderID).Error; err != nil {
				return err
			}
			if o.Status == model.Paid || o.Status == model.Canceled || t.Units <= 0 || t.OccurredAt.After(o.ExpiresAt) {
				continue
			}
			var n int64
			if err := tx.Model(&model.PaymentAttempt{}).Where("order_id = ? AND provider = ? AND receiver = ? AND currency = ? AND units = ? AND selected_at <= ? AND expires_at >= ?", o.ID, t.Provider, t.Receiver, t.Currency, t.Units, t.OccurredAt, t.OccurredAt).Count(&n).Error; err != nil {
				return err
			}
			if n == 0 {
				a := model.PaymentAttempt{OrderID: o.ID, Provider: t.Provider, Receiver: t.Receiver, Currency: t.Currency, Units: t.Units, Precision: 6, SelectedAt: t.OccurredAt, ExpiresAt: o.ExpiresAt, SettleUntil: q.Deadline, RecoveredFromEvent: q.EventID}
				if err := tx.Create(&a).Error; err != nil {
					return err
				}
			}
		}
		return nil
	})
}

func applyAttempt(o *model.Order, a model.PaymentAttempt) {
	o.Provider = a.Provider
	o.Receiver = a.Receiver
	o.Currency = a.Currency
	o.PayUnits = a.Units
	o.Precision = a.Precision
	o.SelectedAt = &a.SelectedAt
	o.RateUSD = a.RateUSD
	o.RateAt = a.RateAt
}

// matchAttempt deduplicates repeated instructions for one order. Historical paid orders
// are retained in the match set so a second deposit becomes additional_payment evidence.
func matchAttempt(db *gorm.DB, t model.Transaction, confirmation bool) (model.PaymentAttempt, model.Order, string, error) {
	var rows []model.PaymentAttempt
	if err := db.Where("provider = ? AND receiver = ? AND currency = ? AND units = ? AND selected_at <= ? AND expires_at >= ?", t.Provider, t.Receiver, t.Currency, t.Units, t.OccurredAt, t.OccurredAt).Order("id").Find(&rows).Error; err != nil {
		return model.PaymentAttempt{}, model.Order{}, "", err
	}
	unique := map[uint]model.PaymentAttempt{}
	for _, a := range rows {
		unique[a.OrderID] = a
	}
	if len(unique) == 0 {
		return model.PaymentAttempt{}, model.Order{}, "no_match", nil
	}
	if len(unique) > 1 {
		return model.PaymentAttempt{}, model.Order{}, "multiple_orders", nil
	}
	var a model.PaymentAttempt
	for _, v := range unique {
		a = v
	}
	var o model.Order
	if err := db.First(&o, a.OrderID).Error; err != nil {
		return a, o, "", err
	}
	if o.Status == model.Paid {
		return a, o, "additional_payment", nil
	}
	if o.Status != model.Ready {
		return a, o, "order_closed", nil
	}
	if !confirmation {
		if !time.Now().Before(a.SettleUntil) {
			return a, o, "late_payment_review_required", nil
		}
		var q model.ConfirmationTask
		if err := db.Where("provider = ? AND event_id = ?", t.Provider, t.EventID).First(&q).Error; err == nil {
			if q.State == "manual_review" || !time.Now().Before(q.Deadline) {
				return a, o, "confirmation_review_required", nil
			}
		} else if !errors.Is(err, gorm.ErrRecordNotFound) {
			return a, o, "", err
		}
	}
	return a, o, "", nil
}
