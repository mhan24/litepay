package worker

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"strconv"
	"strings"
	"time"

	"litepay/internal/amount"
	"litepay/internal/config"
	"litepay/internal/model"
	"litepay/internal/settings"
	"litepay/internal/sign"
	"litepay/internal/store"
)

// notifyLease 是一次回调任务的租约时长。超过这个时间仍未落库结果，说明
// 进程在发送途中崩溃，任务可以被重新领取；正常路径会在租约到期前写回结果。
const notifyLease = 60 * time.Second

// Notifier 负责向商户 notify_url 推送支付成功通知。
type Notifier struct {
	cfg       *settings.Manager
	store     *store.Store
	transport http.RoundTripper
}

// NewNotifier 创建回调通知器。
func NewNotifier(cfg *settings.Manager, st *store.Store) *Notifier {
	return &Notifier{cfg: cfg, store: st, transport: newCallbackTransport()}
}

// Run 启动通知循环。
func (n *Notifier) Run(ctx context.Context) {
	slog.Info("结果通知 Worker 已启动")

	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			slog.Info("结果通知 Worker 已停止")
			return
		case <-ticker.C:
			n.tick(ctx)
		}
	}
}

func (n *Notifier) tick(ctx context.Context) {
	cfg := n.cfg.Get()
	orders, err := n.store.DueNotifications(ctx, cfg.Notify.MaxAttempts, 20)
	if err != nil {
		slog.Error("查询待回调订单失败", "err", err)
		return
	}
	for i := range orders {
		if err := n.store.WithCurrentNotification(ctx, &orders[i], func(current *model.Order) {
			// 先领取任务再发送：领取失败说明另一个 worker 或上一轮仍在处理，
			// 不能重复投递同一笔回调。
			claimed, err := n.store.ClaimNotify(ctx, current, notifyLease)
			if err != nil {
				slog.Error("领取回调任务失败", "trade_no", current.TradeNo, "err", err)
				return
			}
			if !claimed {
				return
			}
			n.notifyOne(ctx, cfg, current)
		}); err != nil {
			slog.Error("检查回调订单失败", "err", err)
		}
	}
}

// BuildNotifyParams 组装 BEpusdt 回调参数（同时用于签名）。
func BuildNotifyParams(o *model.Order) map[string]string {
	status := "2"
	if o.Status == model.StatusExpired {
		status = "3"
	}
	token := strings.TrimSpace(o.PayAddress)
	if token == "" {
		token = strings.TrimSpace(o.ReceiveAddr)
	}
	return map[string]string{
		"trade_id": o.TradeNo,
		"order_id": o.OutTradeNo,
		"amount":   strconv.FormatFloat(parseFloat(o.OriginMoney), 'f', -1, 64),
		// Dujiao decodes actual_amount as a JSON number and re-signs its
		// numeric representation. Round at the configured precision, then
		// remove insignificant trailing zeroes so e.g. 2.5100 -> 2.51.
		"actual_amount":        strconv.FormatFloat(amount.Round(o.PayAmount, o.AmountPrecision), 'f', -1, 64),
		"token":                token,
		"block_transaction_id": stringValue(o.TxID),
		"status":               status,
	}
}

// buildNotifyPayload converts params to the exact JSON value types that are
// sent to the merchant. BEpusdt signs the JSON values, so signing a string
// amount "2.50" while sending number 2.5 would make Dujiao reject the callback.
func buildNotifyPayload(params map[string]string) map[string]interface{} {
	return map[string]interface{}{
		"trade_id":             params["trade_id"],
		"order_id":             params["order_id"],
		"amount":               parseFloat(params["amount"]),
		"actual_amount":        parseFloat(params["actual_amount"]),
		"token":                params["token"],
		"block_transaction_id": params["block_transaction_id"],
		"status":               parseInt(params["status"]),
	}
}

func stringValue(v *string) string {
	if v == nil {
		return ""
	}
	return *v
}

func (n *Notifier) notifyOne(ctx context.Context, cfg *config.Config, o *model.Order) {
	if strings.TrimSpace(o.NotifyURL) == "" {
		_ = n.store.FinishNotify(ctx, o, o.NotifyAttempts, false, "notify_url 为空", 0, "", "empty notify_url", nil)
		slog.Warn("订单未提供 notify_url，跳过回调", "trade_no", o.TradeNo)
		return
	}

	params := BuildNotifyParams(o)
	payload := buildNotifyPayload(params)
	payload["signature"] = sign.BEpusdtSignGeneric(payload, cfg.BEpusdt.APIToken)

	attempt := o.NotifyAttempts + 1
	statusCode, body, reqURL, err := n.postJSON(ctx, cfg.Notify.TimeoutSeconds, o.NotifyURL, payload)

	// status=2 必须同时收到协议约定的正文；status=3 按 BEpusdt 官方
	// 约定只要求 HTTP 200。
	success := err == nil && notifyResponseAccepted(statusCode, body, o.Status)

	var nextAt *time.Time
	if !success && attempt < cfg.Notify.MaxAttempts {
		delays := cfg.Notify.RetryDelays
		idx := attempt - 1
		if idx >= len(delays) {
			idx = len(delays) - 1
		}
		t := time.Now().Add(time.Duration(delays[idx]) * time.Second)
		nextAt = &t
	}

	errMsg := ""
	if err != nil {
		errMsg = err.Error()
	}

	if ferr := n.store.FinishNotify(ctx, o, attempt, success, body, statusCode, reqURL, errMsg, nextAt); ferr != nil {
		slog.Error("保存回调结果失败", "trade_no", o.TradeNo, "err", ferr)
	}

	if success {
		slog.Info("回调成功", "trade_no", o.TradeNo, "attempt", attempt, "status", statusCode)
	} else if nextAt != nil {
		slog.Warn("回调失败，稍后重试",
			"trade_no", o.TradeNo, "attempt", attempt, "status", statusCode,
			"err", errMsg, "next_at", nextAt.Format(time.RFC3339))
	} else {
		slog.Error("回调达到最大重试次数，标记失败", "trade_no", o.TradeNo, "attempt", attempt)
	}
}

// notifyResponseAccepted 判定商户是否确认收到结果通知。
// BEpusdt 官方协议接受 ok；Dujiao Next 的 BEpusdt 适配器返回 success。
// 成功通知必须精确匹配正文。过期通知官方只要求 HTTP 2xx，但部分商户
// （包括当前 Dujiao Next BEpusdt 适配器）会用 200 + fail 明确拒绝 status=3，
// 因此不能把任意 2xx 都误记为送达。
func notifyResponseAccepted(statusCode int, body string, orderStatus string) bool {
	if statusCode < 200 || statusCode >= 300 {
		return false
	}
	bodyText := strings.ToLower(strings.TrimSpace(body))
	if orderStatus == model.StatusExpired {
		switch bodyText {
		case "fail", "failed", "error", "false":
			return false
		default:
			return true
		}
	}
	return bodyText == "success" || bodyText == "ok"
}

func (n *Notifier) postJSON(ctx context.Context, timeoutSec int, rawURL string, payload map[string]interface{}) (int, string, string, error) {
	if timeoutSec <= 0 {
		timeoutSec = 10
	}
	transport := n.transport
	if transport == nil {
		transport = newCallbackTransport()
		defer transport.(*http.Transport).CloseIdleConnections()
	}
	client := &http.Client{Transport: transport, Timeout: time.Duration(timeoutSec) * time.Second,
		CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse },
	}

	bodyJSON, err := json.Marshal(payload)
	if err != nil {
		return 0, "", rawURL, err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, rawURL, bytes.NewReader(bodyJSON))
	if err != nil {
		return 0, "", rawURL, err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("User-Agent", "BEpusdt/LitePay")
	req.Header.Set("Powered-By", "https://github.com/v03413/bepusdt")

	resp, err := client.Do(req)
	if err != nil {
		return 0, "", rawURL, err
	}
	body, readErr := io.ReadAll(io.LimitReader(resp.Body, (64<<10)+1))
	resp.Body.Close()
	if readErr != nil {
		return resp.StatusCode, "", rawURL, readErr
	}
	if len(body) > 64<<10 {
		return resp.StatusCode, "", rawURL, fmt.Errorf("回调响应超过 64 KiB")
	}

	return resp.StatusCode, string(body), rawURL, nil
}

func parseFloat(s string) float64 { v, _ := strconv.ParseFloat(s, 64); return v }

func parseInt(s string) int { v, _ := strconv.Atoi(s); return v }
