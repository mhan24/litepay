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

// RetryCallback opens a new attempt budget, keeping lifetime attempts and all delivery logs.
// The same trade/order/event identifiers remain the merchant's delivery idempotency key.
func (s *Store) RetryCallback(ctx context.Context, tradeNo, operator, reason string, maxAttempts int) error {
	reason = strings.TrimSpace(reason)
	if reason == "" || utf8.RuneCountInString(reason) > 300 || operator == "" {
		return errors.New("请填写核对商城状态后的重试原因（1~300 字）")
	}
	return s.db.WithContext(ctx).Transaction(func(db *gorm.DB) error {
		var o model.Order
		if err := db.Where("trade_no = ?", tradeNo).First(&o).Error; err != nil {
			return err
		}
		if o.Status != model.Paid {
			return errors.New("仅已支付订单可重新通知")
		}
		var proof int64
		if err := db.Model(&model.Transaction{}).Where("order_id = ? AND state = ?", o.ID, model.Confirmed).Count(&proof).Error; err != nil {
			return err
		}
		if proof != 1 {
			return errors.New("缺少唯一到账证据，不能重新通知")
		}
		var task model.Callback
		if err := db.First(&task, "order_id = ?", o.ID).Error; err != nil {
			return err
		}
		if task.DeliveredAt != nil {
			return errors.New("商城已确认通知，不能重复重新入队")
		}
		if time.Now().Before(task.LeaseUntil) || (task.State == "" && task.Attempts > 0 && time.Now().Before(task.NextAt)) {
			return errors.New("通知仍在发送，请稍后重试")
		}
		if task.State != "merchant_review" && task.State != "exhausted" && task.Attempts < task.AttemptBase+maxAttempts {
			return errors.New("自动通知仍在重试中")
		}
		audit := model.CallbackRecovery{OrderID: o.ID, Operator: operator, Reason: reason, PreviousAttempts: task.Attempts}
		if err := db.Create(&audit).Error; err != nil {
			return err
		}
		return db.Model(&task).Updates(map[string]any{"attempt_base": task.Attempts, "attempt_limit": task.Attempts + maxAttempts, "next_at": time.Now(), "lease_until": time.Time{}, "recoveries": task.Recoveries + 1, "state": "queued"}).Error
	})
}
