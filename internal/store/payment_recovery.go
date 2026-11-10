package store

import (
	"context"
	"errors"
	"strings"
	"time"
	"unicode/utf8"

	"gorm.io/gorm"
	"litepay/internal/model"
)

// ReconcilePayment can only consume already-validated, unclaimed evidence matching a stored
// instruction within its original payment window. It never trusts a typed amount or screenshot.
func (s *Store) ReconcilePayment(ctx context.Context, tradeNo, provider, eventID, operator, reason string) error {
	reason = strings.TrimSpace(reason)
	if operator == "" || reason == "" || utf8.RuneCountInString(reason) > 300 {
		return errors.New("请填写核对商城和真实到账后的补单原因（1~300 字）")
	}
	return s.db.WithContext(ctx).Transaction(func(db *gorm.DB) error {
		var o model.Order
		if err := db.Where("trade_no = ?", tradeNo).First(&o).Error; err != nil {
			return err
		}
		if o.Status != model.Expired && o.Status != model.Ready {
			return errors.New("仅自然过期或待确认订单可补单；已支付和主动取消订单禁止补单")
		}
		var t model.Transaction
		if err := db.Where("provider = ? AND event_id = ?", provider, eventID).First(&t).Error; err != nil {
			return err
		}
		if t.State != model.Confirmed || t.OrderID != nil || t.Units <= 0 || t.Provider == "ton" && t.MasterchainSeqno <= 0 {
			return errors.New("必须选择未认领、已确认且满足最终性的到账证据")
		}
		var candidates []model.PaymentAttempt
		if err := db.Where("provider = ? AND receiver = ? AND currency = ? AND units = ? AND selected_at <= ? AND expires_at >= ?", t.Provider, t.Receiver, t.Currency, t.Units, t.OccurredAt, t.OccurredAt).Order("id").Find(&candidates).Error; err != nil {
			return err
		}
		if len(candidates) == 0 {
			return errors.New("到账不匹配已保存付款尝试")
		}
		a := candidates[0]
		for _, candidate := range candidates {
			if candidate.OrderID != o.ID {
				return errors.New("到账归属存在歧义")
			}
			a = candidate
		}
		// Duplicate/conflicting references can never be cleared by manual recovery.
		if t.Reference != "" {
			var n int64
			if err := db.Model(&model.Transaction{}).Where("provider = ? AND reference = ? AND event_id <> ?", t.Provider, t.Reference, t.EventID).Count(&n).Error; err != nil {
				return err
			}
			if n > 0 {
				return errors.New("到账参考号重复，不能补单")
			}
		}
		audit := model.PaymentRecovery{OrderID: o.ID, AttemptID: a.ID, Provider: t.Provider, EventID: t.EventID, Operator: operator, Reason: reason}
		if err := db.Create(&audit).Error; err != nil {
			return err
		}
		applyAttempt(&o, a)
		now := time.Now()
		o.Status = model.Paid
		o.PaidAt = &now
		if err := db.Save(&o).Error; err != nil {
			return err
		}
		if err := db.Model(&t).Updates(map[string]any{"order_id": o.ID, "attempt_id": a.ID, "reason": "manual_reconciled"}).Error; err != nil {
			return err
		}
		if err := db.Model(&model.ConfirmationTask{}).Where("provider = ? AND event_id = ?", t.Provider, t.EventID).Update("state", "resolved").Error; err != nil {
			return err
		}
		if err := db.Model(&model.Slot{}).Where("order_id = ?", o.ID).Update("order_id", nil).Error; err != nil {
			return err
		}
		return db.Create(&model.Callback{OrderID: o.ID, SigningKeyID: o.SigningKeyID, NextAt: now, State: "queued"}).Error
	})
}
