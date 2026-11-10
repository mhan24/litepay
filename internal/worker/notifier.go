package worker

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"strings"
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
func (n *Notifier) Deliver(ctx context.Context) (bool, error) {
	cfg := n.cfg.Get()
	task, o, err := n.st.ReserveCallback(ctx, cfg.Notify.MaxAttempts, cfg.Notify.TimeoutSeconds)
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	var tx model.Transaction
	if err = n.st.DB().WithContext(ctx).First(&tx, "order_id = ?", o.ID).Error; err != nil {
		return true, err
	}
	payload := map[string]any{"trade_id": o.TradeNo, "order_id": o.OutTradeNo, "amount": json.Number(o.Original()), "actual_amount": json.Number(amount.String(o.PayUnits)), "token": o.Receiver, "block_transaction_id": tx.EventID, "status": 2}
	payload["signature"] = sign.BEpusdtSignGeneric(payload, cfg.BEpusdt.APIToken)
	body, err := json.Marshal(payload)
	if err != nil {
		return true, err
	}
	requestCtx, cancel := context.WithTimeout(ctx, time.Duration(cfg.Notify.TimeoutSeconds)*time.Second)
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
		req.Header.Set("Content-Type", "application/json")
		var response []byte
		status, response, err = n.http.Do(req)
		if err == nil {
			ack := strings.TrimSpace(string(response))
			ok = status >= 200 && status < 300 && (ack == "success" || ack == "ok")
			message = "merchant_not_acknowledged"
		}
	}
	if ok {
		message = ""
	}
	delay := cfg.Notify.RetryDelays[min(task.Attempts-1, len(cfg.Notify.RetryDelays)-1)]
	save, done := context.WithTimeout(context.WithoutCancel(ctx), 2*time.Second)
	defer done()
	err = n.st.FinishCallback(save, *task, status, ok, message, time.Duration(delay)*time.Second)
	if err == nil && !ok && task.Attempts >= task.AttemptLimit {
		slog.Error("商户通知重试已耗尽，需核对商城后重新通知", "trade_id", o.TradeNo)
	}
	return true, err
}
func (n *Notifier) Run(ctx context.Context) {
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if _, err := n.Deliver(ctx); err != nil && ctx.Err() == nil {
				slog.Error("回调任务处理失败")
			}
		}
	}
}
