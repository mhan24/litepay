package worker

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"litepay/internal/config"
	"litepay/internal/model"
	"litepay/internal/settings"
	"litepay/internal/store"
)

type notifyRecorder struct {
	mu     sync.Mutex
	hits   int32
	bodies []map[string]any
	reply  string
	status int
}

func (n *notifyRecorder) handler(t *testing.T) http.Handler {
	t.Helper()
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		var payload map[string]any
		_ = json.Unmarshal(body, &payload)
		atomic.AddInt32(&n.hits, 1)
		n.mu.Lock()
		n.bodies = append(n.bodies, payload)
		reply, status := n.reply, n.status
		n.mu.Unlock()
		if status == 0 {
			status = http.StatusOK
		}
		if reply == "" {
			reply = "success"
		}
		w.WriteHeader(status)
		_, _ = w.Write([]byte(reply))
	})
}

// paidOrder 构造一张待回调的已支付订单。
func paidOrder(t *testing.T, st *store.Store, tradeNo string) *model.Order {
	t.Helper()
	ctx := context.Background()
	o, err := st.CreateOrder(ctx, store.CreateOrderInput{
		Protocol: "bepusdt", Pid: "bepusdt", TradeNo: tradeNo, OutTradeNo: "M-" + tradeNo,
		OriginMoney: "2.50", OriginCurrency: "USD", ExpiredAt: time.Now().Add(10 * time.Minute),
		NotifyURL: "https://merchant.example/notify",
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := st.DB().Model(&model.Order{}).Where("id = ?", o.ID).Updates(map[string]any{
		"status": model.StatusPaid, "notify_status": model.NotifyPending,
		"next_notify_at": time.Now().Add(-time.Second),
	}).Error; err != nil {
		t.Fatal(err)
	}
	current, err := st.GetByTradeNo(ctx, tradeNo)
	if err != nil {
		t.Fatal(err)
	}
	return current
}

// 进程在发送回调途中崩溃：租约到期后任务必须能被重新领取，
// 不能永久卡在“发送中”。
func TestNotifyTaskRecoversAfterCrash(t *testing.T) {
	ctx := context.Background()
	st, err := store.Open(t.TempDir() + "/notify.db")
	if err != nil {
		t.Fatal(err)
	}
	order := paidOrder(t, st, "LP-NOTIFY-CRASH")

	// 模拟崩溃现场：任务已被领取，租约已过期，结果没有落库。
	if ok, err := st.ClaimNotify(ctx, order, time.Minute); err != nil || !ok {
		t.Fatalf("claim failed: ok=%v err=%v", ok, err)
	}
	inflight, _ := st.GetByTradeNo(ctx, order.TradeNo)
	if inflight.NotifyStatus != model.NotifySending {
		t.Fatalf("status = %q, want SENDING", inflight.NotifyStatus)
	}

	// 租约未过期时不该被重复调度。
	due, err := st.DueNotifications(ctx, 5, 20)
	if err != nil {
		t.Fatal(err)
	}
	for _, o := range due {
		if o.ID == order.ID {
			t.Fatal("in-flight notification was scheduled again before its lease expired")
		}
	}

	// 租约过期后重新到期。
	if err := st.DB().Model(&model.Order{}).Where("id = ?", order.ID).
		Update("next_notify_at", time.Now().Add(-time.Second)).Error; err != nil {
		t.Fatal(err)
	}
	due, err = st.DueNotifications(ctx, 5, 20)
	if err != nil {
		t.Fatal(err)
	}
	if len(due) != 1 || due[0].ID != order.ID {
		t.Fatalf("stale SENDING task was not rescheduled: %+v", due)
	}

	// 重新领取并成功投递。
	rec := &notifyRecorder{}
	server := httptest.NewServer(rec.handler(t))
	defer server.Close()
	// 回调地址写到库里：worker 重新读取的是数据库中的订单，不是内存快照。
	if err := st.DB().Model(&model.Order{}).Where("id = ?", order.ID).
		Update("notify_url", server.URL).Error; err != nil {
		t.Fatal(err)
	}
	due[0].NotifyURL = server.URL

	cfg := config.Default()
	mgr := settings.New(cfg, st)
	n := NewNotifier(mgr, st)
	// 回调客户端默认拒绝回环地址（防 SSRF），测试用真实 transport 访问本地服务。
	n.transport = http.DefaultTransport
	if err := st.WithCurrentNotification(ctx, &due[0], func(current *model.Order) {
		claimed, err := st.ClaimNotify(ctx, current, notifyLease)
		if err != nil || !claimed {
			t.Fatalf("reclaim failed: ok=%v err=%v", claimed, err)
		}
		n.notifyOne(ctx, cfg, current)
	}); err != nil {
		t.Fatal(err)
	}
	if got := atomic.LoadInt32(&rec.hits); got != 1 {
		t.Fatalf("merchant received %d callbacks, want 1", got)
	}
	final, _ := st.GetByTradeNo(ctx, order.TradeNo)
	if final.NotifyStatus != model.NotifySuccess || final.NotifyAttempts != 1 {
		t.Fatalf("final notify state = %s/%d, want SUCCESS/1", final.NotifyStatus, final.NotifyAttempts)
	}
}

// 同一笔回调不能被两个 worker 同时领取。
func TestClaimNotifyIsExclusive(t *testing.T) {
	ctx := context.Background()
	st, err := store.Open(t.TempDir() + "/claim.db")
	if err != nil {
		t.Fatal(err)
	}
	order := paidOrder(t, st, "LP-NOTIFY-CLAIM")

	var claimed int32
	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			ok, err := st.ClaimNotify(ctx, order, time.Minute)
			if err != nil {
				t.Errorf("claim error: %v", err)
				return
			}
			if ok {
				atomic.AddInt32(&claimed, 1)
			}
		}()
	}
	wg.Wait()
	if claimed != 1 {
		t.Fatalf("task claimed %d times, want exactly 1", claimed)
	}
}

// 回调失败后按重试计划重新到期，并记录审计日志。
func TestNotifyFailureSchedulesRetry(t *testing.T) {
	ctx := context.Background()
	st, err := store.Open(t.TempDir() + "/retry.db")
	if err != nil {
		t.Fatal(err)
	}
	order := paidOrder(t, st, "LP-NOTIFY-RETRY")

	rec := &notifyRecorder{reply: "fail", status: http.StatusOK}
	server := httptest.NewServer(rec.handler(t))
	defer server.Close()
	if err := st.DB().Model(&model.Order{}).Where("id = ?", order.ID).
		Update("notify_url", server.URL).Error; err != nil {
		t.Fatal(err)
	}
	current, _ := st.GetByTradeNo(ctx, order.TradeNo)

	cfg := config.Default()
	n := NewNotifier(settings.New(cfg, st), st)
	n.transport = http.DefaultTransport
	claimed, err := st.ClaimNotify(ctx, current, notifyLease)
	if err != nil || !claimed {
		t.Fatalf("claim failed: %v", err)
	}
	n.notifyOne(ctx, cfg, current)

	retried, _ := st.GetByTradeNo(ctx, order.TradeNo)
	if retried.NotifyStatus != model.NotifyPending || retried.NextNotifyAt == nil ||
		retried.NotifyAttempts != 1 {
		t.Fatalf("retry not scheduled: %+v", retried)
	}
	if !retried.NextNotifyAt.After(time.Now()) {
		t.Fatalf("retry scheduled in the past: %v", retried.NextNotifyAt)
	}
	var logs int64
	if err := st.DB().Model(&model.NotifyLog{}).Where("order_id = ?", order.ID).Count(&logs).Error; err != nil {
		t.Fatal(err)
	}
	if logs != 1 {
		t.Fatalf("notify logs = %d, want 1", logs)
	}
}
