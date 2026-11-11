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
	return refreshCancellationCallback(db, o.ID)
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
			if a.OrderID != o.ID || a.Provider != t.Provider || a.Receiver != t.Receiver || a.Currency != t.Currency || a.Units != t.Units || t.OccurredAt.Before(a.SelectedAt) || t.OccurredAt.After(a.ExpiresAt) {
				return errors.New("付款尝试不匹配")
			}
			_, matched, _, err := matchAttempt(db, t, false)
			if err != nil {
				return err
			}
			if matched.ID != o.ID {
				return errors.New("额外到账归属尚不唯一，请人工核查")
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
		current, err := cancellationReviewCurrent(db, review.ID)
		if err != nil {
			return err
		}
		if current {
			return errors.New("该笔退款已登记，不能重复登记")
		}
		audit := model.CancellationReviewAudit{ReviewID: review.ID, Generation: r.Generation, Operator: operator, Reason: reason, RefundReference: reference}
		if err := db.Create(&audit).Error; err != nil {
			return err
		}
		if err := db.Model(&review).Updates(map[string]any{"state": "refund_recorded", "review_generation": r.Generation, "operator": operator, "reason": reason, "refund_reference": reference, "resolved_at": time.Now()}).Error; err != nil {
			return err
		}
		if err := db.Model(&model.Callback{}).Where("order_id=? AND delivered_at IS NULL AND state='in_flight' AND lease_until<=?", o.ID, time.Now()).Update("state", "merchant_review").Error; err != nil {
			return err
		}
		return refreshCancellationCallback(db, o.ID)
	})
}

// All consumers use the same per-receipt audit predicate and recovery snapshot.
const currentCancellationReviewSQL = `r.state='refund_recorded' AND r.review_generation=g.generation AND r.refund_reference<>''
 AND EXISTS(SELECT 1 FROM cancellation_review_audits a WHERE a.review_id=r.id AND a.generation=g.generation AND a.refund_reference=r.refund_reference)
 AND EXISTS(SELECT 1 FROM transactions t WHERE t.provider=r.provider AND t.event_id=r.event_id AND t.state='confirmed' AND t.units>0 AND (t.provider<>'ton' OR t.masterchain_seqno>0)
 AND (t.order_id=r.order_id OR (t.order_id IS NULL AND EXISTS(SELECT 1 FROM payment_attempts p WHERE p.id=t.attempt_id AND p.order_id=r.order_id AND p.provider=t.provider AND p.receiver=t.receiver AND p.currency=t.currency AND p.units=t.units AND p.selected_at<=t.occurred_at AND p.expires_at>=t.occurred_at)
 AND NOT EXISTS(SELECT 1 FROM payment_attempts p WHERE p.order_id<>r.order_id AND p.provider=t.provider AND p.receiver=t.receiver AND p.currency=t.currency AND p.units=t.units AND p.selected_at<=t.occurred_at AND p.expires_at>=t.occurred_at))))`

func cancellationReviewCurrent(db *gorm.DB, reviewID uint) (bool, error) {
	var current bool
	err := db.Raw(`SELECT EXISTS(SELECT 1 FROM cancellation_reviews r CROSS JOIN gateway_recoveries g WHERE g.id=1 AND r.id=? AND `+currentCancellationReviewSQL+`)`, reviewID).Scan(&current).Error
	return current, err
}

func (s *Store) CancellationReviewCurrent(ctx context.Context, reviewID uint) (bool, error) {
	return cancellationReviewCurrent(s.db.WithContext(ctx), reviewID)
}

type CancellationReviewSummary struct {
	Generation string
	Total      int64
	Pending    int64
	Stale      int64
	Uncovered  int64
}

func (r CancellationReviewSummary) Present() bool { return r.Total > 0 || r.Uncovered > 0 }
func (r CancellationReviewSummary) Current() bool {
	return r.Total > 0 && r.Pending == 0 && r.Stale == 0 && r.Uncovered == 0
}
func (r CancellationReviewSummary) Status() string {
	switch {
	case !r.Present():
		return "none"
	case r.Pending > 0 || r.Uncovered > 0:
		return "pending"
	case r.Stale > 0:
		return "recheck_required"
	default:
		return "refund_recorded"
	}
}

const uncoveredCancellationReceiptSQL = `t.order_id IS NULL AND t.state IN ('confirmed','ambiguous')
 AND (o.status='CANCELED' OR EXISTS(SELECT 1 FROM cancellation_reviews cr WHERE cr.order_id=o.id))
 AND NOT EXISTS(SELECT 1 FROM cancellation_reviews cr WHERE cr.provider=t.provider AND cr.event_id=t.event_id AND cr.order_id=o.id)`

const cancellationReviewQueueSQL = `SELECT o.trade_no,r.provider,r.event_id FROM cancellation_reviews r JOIN orders o ON o.id=r.order_id CROSS JOIN gateway_recoveries g WHERE g.id=1 AND NOT (` + currentCancellationReviewSQL + `)
 UNION ALL SELECT o.trade_no,t.provider,t.event_id FROM transactions t JOIN payment_attempts p ON p.id=t.attempt_id JOIN orders o ON o.id=p.order_id WHERE ` + uncoveredCancellationReceiptSQL + `
`

type CancellationReviewQueueItem struct {
	TradeNo  string
	Provider string
	EventID  string
}

func (s *Store) CancellationReviewQueue(ctx context.Context) ([]CancellationReviewQueueItem, error) {
	var rows []CancellationReviewQueueItem
	err := s.db.WithContext(ctx).Raw(`SELECT * FROM (` + cancellationReviewQueueSQL + `) ORDER BY trade_no,provider,event_id LIMIT 10`).Scan(&rows).Error
	return rows, err
}

func cancellationReviewSummary(db *gorm.DB, where string, args ...any) (CancellationReviewSummary, error) {
	var result CancellationReviewSummary
	query := db.Raw(`SELECT g.generation,
 (SELECT count(*) FROM cancellation_reviews r WHERE r.order_id=o.id) total,
 (SELECT count(*) FROM cancellation_reviews r WHERE r.order_id=o.id AND r.state<>'refund_recorded') pending,
 (SELECT count(*) FROM cancellation_reviews r WHERE r.order_id=o.id AND r.state='refund_recorded' AND NOT (`+currentCancellationReviewSQL+`)) stale,
 (SELECT count(*) FROM transactions t JOIN payment_attempts p ON p.id=t.attempt_id WHERE p.order_id=o.id AND `+uncoveredCancellationReceiptSQL+`) uncovered
 FROM orders o CROSS JOIN gateway_recoveries g WHERE g.id=1 AND `+where, args...).Scan(&result)
	if query.Error != nil {
		return result, query.Error
	}
	if query.RowsAffected != 1 {
		return result, gorm.ErrRecordNotFound
	}
	return result, nil
}

func (s *Store) CancellationReviewSummary(ctx context.Context, orderID uint) (CancellationReviewSummary, error) {
	return cancellationReviewSummary(s.db.WithContext(ctx), "o.id=?", orderID)
}

func refreshCancellationCallback(db *gorm.DB, orderID uint) error {
	summary, err := cancellationReviewSummary(db, "o.id=?", orderID)
	if err != nil {
		return err
	}
	state := "merchant_review"
	if summary.Current() {
		state = "canceled_refunded"
	}
	return db.Model(&model.Callback{}).Where("order_id=? AND delivered_at IS NULL AND state<>'in_flight'", orderID).Update("state", state).Error
}

func (s *Store) CancellationRefundReviewStatus(ctx context.Context, tradeNo, outTradeNo string) (bool, string, error) {
	summary, err := cancellationReviewSummary(s.db.WithContext(ctx), "o.trade_no=? AND o.out_trade_no=?", tradeNo, outTradeNo)
	if errors.Is(err, gorm.ErrRecordNotFound) {
		// A signed query for a mismatched identity is never evidence of a completed refund.
		r, recoveryErr := s.Recovery(ctx)
		return false, r.Generation, recoveryErr
	}
	return summary.Current(), summary.Generation, err
}
func (s *Store) CancellationRefundReviewed(ctx context.Context, tradeNo, outTradeNo string) (bool, error) {
	reviewed, _, err := s.CancellationRefundReviewStatus(ctx, tradeNo, outTradeNo)
	return reviewed, err
}

// The saved attempt identifies additional receipts discovered before merchant cancellation.
// Recheck the full window and unique order match; uncertain evidence stays unrefunded.
func collectCancellationReviews(db *gorm.DB, o model.Order, claimed model.Transaction) error {
	if err := ensureCancellationReview(db, o, claimed); err != nil {
		return err
	}
	return collectAdditionalCancellationReviews(db, o)
}

func collectAdditionalCancellationReviews(db *gorm.DB, o model.Order) error {
	var rows []model.Transaction
	if err := db.Where("order_id IS NULL AND state=? AND attempt_id IN (SELECT id FROM payment_attempts WHERE order_id=?)", model.Confirmed, o.ID).Find(&rows).Error; err != nil {
		return err
	}
	for _, t := range rows {
		_, matched, reason, err := matchAttempt(db, t, false)
		if err != nil {
			return err
		}
		if matched.ID != o.ID || (reason != "additional_payment" && !(reason == "order_closed" && o.Status == model.Canceled)) {
			continue
		}
		if err := ensureCancellationReview(db, o, t); err != nil {
			return err
		}
	}
	return refreshCancellationCallback(db, o.ID)
}

func backfillAdditionalCancellationReviews(db *gorm.DB) error {
	return db.Transaction(func(tx *gorm.DB) error {
		var orders []model.Order
		if err := tx.Where("status IN ? AND EXISTS(SELECT 1 FROM cancellation_reviews r WHERE r.order_id=orders.id)", []string{model.Paid, model.Canceled}).Find(&orders).Error; err != nil {
			return err
		}
		for _, o := range orders {
			if err := collectAdditionalCancellationReviews(tx, o); err != nil {
				return err
			}
		}
		return nil
	})
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
	summary, err := s.CancellationReviewSummary(ctx, orderID)
	return summary.Present(), summary.Current(), err
}
