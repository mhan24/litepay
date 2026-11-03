package store

import (
	"context"
	"errors"
	"litepay/internal/model"
	"sync"
	"time"
)

type WorkerHealth struct {
	LastOK time.Time
	Failed bool
}
type workerHealth struct {
	mu     sync.RWMutex
	values map[string]WorkerHealth
}

func (s *Store) WorkerResult(name string, success bool) {
	s.workers.mu.Lock()
	defer s.workers.mu.Unlock()
	if s.workers.values == nil {
		s.workers.values = map[string]WorkerHealth{}
	}
	v := s.workers.values[name]
	v.Failed = !success
	if success {
		v.LastOK = time.Now()
	}
	s.workers.values[name] = v
}
func (s *Store) WorkerHealthy(name string, age time.Duration) bool {
	s.workers.mu.RLock()
	defer s.workers.mu.RUnlock()
	v := s.workers.values[name]
	return !v.Failed && !v.LastOK.IsZero() && time.Since(v.LastOK) < age
}

type Operations struct {
	ReconciliationOrders int64
	OverdueOrders        int64
	Unassigned           int64
	ClaimFailures        int64
	NotifyRetrying       int64
	NotifyExhausted      int64
	NotifyRecoverable    int64
	Scans                []model.ScanCursor
}

func (s *Store) Operations(ctx context.Context) (Operations, error) {
	var out Operations
	now := time.Now()
	queries := []struct {
		model any
		where string
		args  []any
		count *int64
	}{
		{&model.Order{}, "reconciliation_reason <> ''", nil, &out.ReconciliationOrders},
		{&model.Order{}, "status IN ? AND expire_finalized = ? AND expired_at < ?", []any{[]string{model.StatusReady, model.StatusExpired}, false, now.Add(-24 * time.Hour)}, &out.OverdueOrders},
		{&model.Order{}, "notify_status = ? AND notify_attempts > 0", []any{model.NotifyPending}, &out.NotifyRetrying},
		{&model.Order{}, "notify_status = ?", []any{model.NotifyFailed}, &out.NotifyExhausted},
		{&model.Order{}, "notify_status = ? AND next_notify_at <= ?", []any{model.NotifySending, now}, &out.NotifyRecoverable},
		{&model.TxObservation{}, "status = ? AND reason = ? AND NOT EXISTS (SELECT 1 FROM incoming_txs WHERE incoming_txs.provider = tx_observations.provider AND incoming_txs.transaction_id = tx_observations.transaction_id)", []any{model.ObservationOpen, model.ObservationReasonClaimFailed}, &out.ClaimFailures},
	}
	for _, q := range queries {
		if err := s.db.WithContext(ctx).Model(q.model).Where(q.where, q.args...).Count(q.count).Error; err != nil {
			return out, err
		}
	}
	var err error
	out.Unassigned, err = s.CountOpenTxObservations(ctx)
	if err != nil {
		return out, err
	}
	err = s.db.WithContext(ctx).Order("provider").Find(&out.Scans).Error
	return out, err
}

// ReconcileObservation only consumes existing exchange evidence via the normal atomic claim.
// Canceled/finalized orders remain rejected; no synthetic MANUAL ID is created.
func (s *Store) ReconcileObservation(ctx context.Context, tradeNo, transactionID string, precision int) (*model.Order, error) {
	o, err := s.GetByTradeNo(ctx, tradeNo)
	if err != nil {
		return nil, err
	}
	if o == nil {
		return nil, errors.New("订单不存在")
	}
	var obs model.TxObservation
	if err := s.db.WithContext(ctx).Where("provider = ? AND transaction_id = ? AND status = ?", o.Collection, transactionID, model.ObservationOpen).First(&obs).Error; err != nil {
		return nil, errors.New("未找到当前通道仍待处理的真实流水")
	}
	if transactionID == "" || obs.Type == "MANUAL" || obs.ReceiverID == "" {
		return nil, errors.New("流水证据不完整，保持人工对账状态")
	}
	ok, err := s.ClaimAndPay(ctx, o, model.IncomingTx{Provider: obs.Provider, TransactionID: obs.TransactionID, Type: obs.Type, Amount: obs.Amount, AmountText: obs.AmountText, Currency: obs.Currency, ReceiverID: obs.ReceiverID, ReceiverAddr: obs.ReceiverID, TransactionTime: obs.TransactionTime, Raw: obs.Raw}, precision)
	if err != nil {
		return nil, err
	}
	if !ok {
		return nil, errors.New("该交易已被认领，未执行补单")
	}
	return s.GetByTradeNo(ctx, tradeNo)
}
