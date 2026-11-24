package worker

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"runtime/debug"
	"strings"
	"sync"
	"time"

	"gorm.io/gorm"

	"litepay/internal/amount"
	"litepay/internal/httpx"
	"litepay/internal/model"
	"litepay/internal/settings"
	"litepay/internal/sign"
	"litepay/internal/store"
)

type Notifier struct {
	cfg  *settings.Manager
	st   *store.Store
	http *httpx.Client
}

func NewNotifier(cfg *settings.Manager, st *store.Store) *Notifier {
	return &Notifier{cfg: cfg, st: st, http: httpx.NewClient(60*time.Second, false)}
}

// UseTestTransport 只用于端到端测试：回调发送到本地模拟商城，不走公网拨号限制。
func (n *Notifier) UseTestTransport() {
	n.http = httpx.NewClient(15*time.Second, true)
}
func retryDelaySeconds(delays []int, attempts int) int {
	if len(delays) == 0 {
		return 5
	}
	idx := min(max(attempts-1, 0), len(delays)-1)
	if delays[idx] <= 0 {
		return 5
	}
	return delays[idx]
}

func (n *Notifier) Deliver(ctx context.Context) (bool, error) {
	did, _, err := n.deliver(ctx)
	return did, err
}

func (n *Notifier) deliver(ctx context.Context) (bool, string, error) {
	cfg := n.cfg.Get()
	if cfg.Server.CancelSyncRequired {
		ready, err := n.st.PaymentOperationsReady(ctx)
		if err != nil || !ready {
			return false, "", err
		}
	}
	maxAttempts := cfg.Notify.MaxAttempts
	if maxAttempts <= 0 {
		maxAttempts = 5
	}
	timeoutSeconds := cfg.Notify.TimeoutSeconds
	if timeoutSeconds <= 0 {
		timeoutSeconds = 10
	}
	task, o, err := n.st.ReserveCallback(ctx, maxAttempts, timeoutSeconds)
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return false, "", nil
	}
	if err != nil {
		return false, "", err
	}
	tradeID := o.TradeNo
	var tx model.Transaction
	if err = n.st.DB().WithContext(ctx).First(&tx, "order_id = ? AND state = ?", o.ID, model.Confirmed).Error; err != nil {
		return false, tradeID, n.releaseCallback(ctx, *task, err)
	}
	payload := map[string]any{"trade_id": o.TradeNo, "order_id": o.OutTradeNo, "amount": json.Number(o.Original()), "actual_amount": json.Number(amount.String(o.PayUnits)), "token": o.Receiver, "block_transaction_id": tx.EventID, "status": 2}
	key, err := n.st.CallbackKey(ctx, *task)
	if err != nil {
		return false, tradeID, n.releaseCallback(ctx, *task, err)
	}
	payload["signature"] = sign.BEpusdtSignGeneric(payload, key)
	body, err := json.Marshal(payload)
	if err != nil {
		return false, tradeID, n.releaseCallback(ctx, *task, err)
	}
	requestCtx, cancel := context.WithTimeout(ctx, time.Duration(timeoutSeconds)*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(requestCtx, http.MethodPost, o.NotifyURL, bytes.NewReader(body))
	status := 0
	ok := false
	message := "callback_request_failed"
	if err == nil {
		err = cfg.ValidateNotifyURL(o.NotifyURL)
		if err != nil {
			message = "callback_policy_rejected"
		}
	}
	if err == nil {
		// The bridge can fail while this task's payload and signing key are loaded.
		if n.cfg.Get().Server.CancelSyncRequired {
			ready, guardErr := n.st.PaymentOperationsReady(ctx)
			if guardErr != nil {
				return false, tradeID, n.releaseCallback(ctx, *task, guardErr)
			}
			if !ready {
				save, done := context.WithTimeout(context.WithoutCancel(ctx), 2*time.Second)
				defer done()
				return false, tradeID, n.st.ReleaseCallback(save, *task)
			}
		}
		allowed, guardErr := n.st.CallbackCanSend(ctx, *task)
		if guardErr != nil {
			return false, tradeID, n.releaseCallback(ctx, *task, guardErr)
		}
		if !allowed {
			save, done := context.WithTimeout(context.WithoutCancel(ctx), 2*time.Second)
			defer done()
			return false, tradeID, n.st.ReleaseCallback(save, *task)
		}
		req.Header.Set("Content-Type", "application/json")
		var response []byte
		status, response, err = n.http.Do(req)
		if err != nil && status == 0 && ctx.Err() != nil && errors.Is(err, ctx.Err()) {
			return false, tradeID, n.releaseCallback(ctx, *task, err)
		}
		if err == nil {
			ack := strings.TrimSpace(string(response))
			ok = status >= 200 && status < 300 && (ack == "success" || ack == "ok")
			message = "merchant_not_acknowledged"
		}
	}
	if ok {
		message = ""
	}
	batchAttempt := task.Attempts - task.AttemptBase
	if batchAttempt <= 0 {
		batchAttempt = task.Attempts
	}
	delay := retryDelaySeconds(cfg.Notify.RetryDelays, batchAttempt)
	save, done := context.WithTimeout(context.WithoutCancel(ctx), 2*time.Second)
	defer done()
	err = n.st.FinishCallback(save, *task, status, ok, message, time.Duration(delay)*time.Second)
	if err == nil && !ok && task.Attempts >= task.AttemptLimit {
		slog.Error("商户通知重试已耗尽，需核对商城后重新通知", "trade_id", o.TradeNo)
	}
	return true, tradeID, err
}

// releaseCallback 归还一次未发出的预约，避免白白消耗商户重试额度；原始 cause 原样返回供上层记录日志。
func (n *Notifier) releaseCallback(ctx context.Context, task model.Callback, cause error) error {
	save, done := context.WithTimeout(context.WithoutCancel(ctx), 2*time.Second)
	defer done()
	if err := n.st.ReleaseCallback(save, task); err != nil {
		return errors.Join(cause, err)
	}
	return cause
}

func (n *Notifier) deliverSafe(ctx context.Context) (did bool, tradeID string, err error) {
	defer func() {
		if r := recover(); r != nil {
			slog.Error("回调任务发生异常", "trade_id", tradeID, "panic", fmt.Sprint(r), "stack", string(debug.Stack()))
			did = false
			err = fmt.Errorf("deliver panic: %v", r)
		}
	}()
	return n.deliver(ctx)
}

func (n *Notifier) Run(ctx context.Context) {
	workers := n.cfg.Get().Server.CallbackWorkers
	if workers <= 0 {
		workers = 8
	}
	workers = min(workers, 32)
	var group sync.WaitGroup
	for range workers {
		group.Go(func() {
			for ctx.Err() == nil {
				did, tradeID, err := n.deliverSafe(ctx)
				if did && err == nil {
					continue
				}
				delay := 250 * time.Millisecond
				if err != nil {
					delay = time.Second
					if ctx.Err() == nil {
						slog.Error("回调任务处理失败", "trade_id", tradeID, "error", err)
					}
				}
				timer := time.NewTimer(delay)
				select {
				case <-ctx.Done():
					timer.Stop()
					return
				case <-timer.C:
				}
			}
		})
	}
	group.Wait()
}
