package store

import (
	"context"
	"errors"
	"strings"
	"time"
	"unicode/utf8"

	"gorm.io/gorm"
	"gorm.io/gorm/clause"
	"litepay/internal/model"
)

func ensureCancellationReview(db *gorm.DB, o model.Order, t model.Transaction) error {
	if t.State != model.Confirmed || t.EventID == "" || t.Units <= 0 || t.Provider == "ton" && t.MasterchainSeqno <= 0 || t.OrderID != nil && *t.OrderID != o.ID {
		return errors.New("资金证据尚不能用于取消到账核对")
	}
	review := model.CancellationReview{OrderID: o.ID, Provider: t.Provider, EventID: t.EventID, State: "open"}
	if err := db.Clauses(clause.OnConflict{DoNothing: true}).Create(&review).Error; err != nil {
		return err
	}
	var saved model.CancellationReview
	if err := db.First(&saved, "provider=? AND event_id=?", t.Provider, t.EventID).Error; err != nil {
		return err
	}
	if saved.OrderID != o.ID {
		return errors.New("取消到账核对记录归属冲突")
	}
	// Existing callbacks are held per order; a request already on the wire cannot be recalled.
	return db.Model(&model.Callback{}).Where("order_id=? AND delivered_at IS NULL AND state NOT IN ('in_flight','canceled_refunded')", o.ID).Update("state", "merchant_review").Error
}

func (s *Store) RecordCancellationRefund(ctx context.Context, tradeNo, provider, eventID, operator, reference, reason string) error {
	reference = strings.TrimSpace(reference)
	reason = strings.TrimSpace(reason)
	if operator == "" || reference == "" || reason == "" || utf8.RuneCountInString(reference) > 200 || utf8.RuneCountInString(reason) > 300 {
		return errors.New("请填写实际退款凭证和核对说明（凭证不超过 200 字，说明不超过 300 字）")
	}
	return s.db.WithContext(ctx).Transaction(func(db *gorm.DB) error {
		var o model.Order
		if err := db.First(&o, "trade_no=?", tradeNo).Error; err != nil {
			return err
		}
		var review model.CancellationReview
		if err := db.First(&review, "order_id=? AND provider=? AND event_id=?", o.ID, provider, eventID).Error; err != nil {
			return err
		}
		var t model.Transaction
		if err := db.First(&t, "provider=? AND event_id=?", provider, eventID).Error; err != nil {
			return err
		}
		if t.State != model.Confirmed || t.Units <= 0 || t.Provider == "ton" && t.MasterchainSeqno <= 0 || o.Status != model.Canceled && o.Status != model.Paid {
			return errors.New("仅已验证的取消到账可记录退款")
		}
		if t.OrderID != nil && *t.OrderID != o.ID {
			return errors.New("资金归属不一致")
		}
		if t.OrderID == nil {
			var a model.PaymentAttempt
			if t.AttemptID == nil {
				return errors.New("缺少原始付款尝试证据")
			}
			if err := db.First(&a, *t.AttemptID).Error; err != nil {
				return err
			}
			if a.OrderID != o.ID || a.Provider != t.Provider || a.Receiver != t.Receiver || a.Currency != t.Currency || a.Units != t.Units {
				return errors.New("付款尝试不匹配")
			}
		}
		var sending int64
		if err := db.Model(&model.Callback{}).Where("order_id=? AND state='in_flight' AND lease_until>?", o.ID, time.Now()).Count(&sending).Error; err != nil {
			return err
		}
		if sending > 0 {
			return errors.New("通知仍在发送，请结束后核对实际交付再登记退款")
		}
		var r model.GatewayRecovery
		if err := db.First(&r, 1).Error; err != nil {
			return err
		}
		if review.State == "refund_recorded" && review.ReviewGeneration == r.Generation {
			return errors.New("该笔退款已登记，不能重复登记")
		}
		audit := model.CancellationReviewAudit{ReviewID: review.ID, Generation: r.Generation, Operator: operator, Reason: reason, RefundReference: reference}
		if err := db.Create(&audit).Error; err != nil {
			return err
		}
		if err := db.Model(&model.Callback{}).Where("order_id=? AND delivered_at IS NULL", o.ID).Update("state", "canceled_refunded").Error; err != nil {
			return err
		}
		return db.Model(&review).Updates(map[string]any{"state": "refund_recorded", "review_generation": r.Generation, "operator": operator, "reason": reason, "refund_reference": reference, "resolved_at": time.Now()}).Error
	})
}

func (s *Store) CancellationRefundReviewStatus(ctx context.Context, tradeNo, outTradeNo string) (bool, string, error) {
	var result struct {
		Generation string
		Reviewed   bool
	}
	query := s.db.WithContext(ctx).Raw(`SELECT g.generation,EXISTS(SELECT 1 FROM cancellation_reviews r JOIN orders o ON o.id=r.order_id JOIN transactions t ON t.provider=r.provider AND t.event_id=r.event_id WHERE t.state='confirmed' AND t.units>0 AND o.trade_no=? AND o.out_trade_no=? AND r.state='refund_recorded' AND r.review_generation=g.generation AND r.refund_reference<>'' AND EXISTS(SELECT 1 FROM cancellation_review_audits a WHERE a.review_id=r.id AND a.generation=r.review_generation AND a.refund_reference=r.refund_reference) AND NOT EXISTS(SELECT 1 FROM cancellation_reviews u WHERE u.order_id=o.id AND (u.state<>'refund_recorded' OR u.review_generation<>g.generation))) reviewed FROM gateway_recoveries g WHERE g.id=1`, tradeNo, outTradeNo).Scan(&result)
	if query.Error == nil && query.RowsAffected != 1 {
		return false, "", gorm.ErrRecordNotFound
	}
	return result.Reviewed, result.Generation, query.Error
}
func (s *Store) CancellationRefundReviewed(ctx context.Context, tradeNo, outTradeNo string) (bool, error) {
	reviewed, _, err := s.CancellationRefundReviewStatus(ctx, tradeNo, outTradeNo)
	return reviewed, err
}

func (s *Store) CallbackCanSend(ctx context.Context, task model.Callback) (bool, error) {
	var n int64
	err := s.db.WithContext(ctx).Model(&model.Callback{}).Where("order_id=? AND attempts=? AND state='in_flight' AND lease_until=? AND delivered_at IS NULL", task.OrderID, task.Attempts, task.LeaseUntil).Where("NOT EXISTS(SELECT 1 FROM cancellation_reviews r WHERE r.order_id=callbacks.order_id)").Count(&n).Error
	return n == 1, err
}

func backfillCancellationReviews(db *gorm.DB) error {
	return db.Transaction(func(tx *gorm.DB) error {
		var rows []model.Transaction
		if err := tx.Where("state=? AND reason=? AND order_id IS NULL", model.Confirmed, "order_closed").Find(&rows).Error; err != nil {
			return err
		}
		for _, t := range rows {
			a, o, reason, err := matchAttempt(tx, t, false)
			if err != nil {
				return err
			}
			if reason != "order_closed" || o.Status != model.Canceled {
				continue
			}
			if err = tx.Model(&t).Update("attempt_id", a.ID).Error; err != nil {
				return err
			}
			if err = ensureCancellationReview(tx, o, t); err != nil {
				return err
			}
		}
		return nil
	})
}

func (s *Store) CancellationReviewState(ctx context.Context, orderID uint) (bool, bool, error) {
	var n, open int64
	if err := s.db.WithContext(ctx).Model(&model.CancellationReview{}).Where("order_id=?", orderID).Count(&n).Error; err != nil {
		return false, false, err
	}
	if err := s.db.WithContext(ctx).Model(&model.CancellationReview{}).Where("order_id=? AND state<>'refund_recorded'", orderID).Count(&open).Error; err != nil {
		return false, false, err
	}
	return n > 0, n > 0 && open == 0, nil
}
