package worker

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"litepay/internal/config"
	"litepay/internal/model"
	"litepay/internal/settings"
	"litepay/internal/store"
)

// stubBinance 模拟币安只读接口：账户 UID + 站内转账历史。
type stubBinance struct {
	mu  sync.Mutex
	uid string
	txs []stubBinanceTx
}

type stubBinanceTx struct {
	id        string
	orderType string
	amount    float64 // 正数表示收入
	currency  string
	at        time.Time
	receiver  string
}

func (s *stubBinance) set(txs ...stubBinanceTx) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.txs = txs
}

func (s *stubBinance) handler(t *testing.T) http.Handler {
	t.Helper()
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		s.mu.Lock()
		defer s.mu.Unlock()
		switch r.URL.Path {
		case "/api/v3/account":
			_ = json.NewEncoder(w).Encode(map[string]any{"uid": s.uid})
		case "/sapi/v1/pay/transactions":
			data := make([]map[string]any, 0, len(s.txs))
			for _, tx := range s.txs {
				data = append(data, map[string]any{
					"orderType":       tx.orderType,
					"transactionId":   tx.id,
					"transactionTime": tx.at.UnixMilli(),
					"amount":          tx.amount,
					"currency":        tx.currency,
					"receiverInfo":    map[string]any{"binanceId": tx.receiver},
				})
			}
			_ = json.NewEncoder(w).Encode(map[string]any{
				"code": "000000", "message": "success", "success": true, "data": data,
			})
		default:
			http.NotFound(w, r)
		}
	})
}

type pollFixture struct {
	st     *store.Store
	cfg    *config.Config
	poller *Poller
	stub   *stubBinance
	order  *model.Order
}

func newPollFixture(t *testing.T, tradeNo string) *pollFixture {
	t.Helper()
	const uid = "binance-uid"
	stub := &stubBinance{uid: uid}
	server := httptest.NewServer(stub.handler(t))
	t.Cleanup(server.Close)

	st, err := store.Open(t.TempDir() + "/test.db")
	if err != nil {
		t.Fatal(err)
	}
	cfg := config.Default()
	cfg.Binance.BaseURL = server.URL
	cfg.Binance.APIKey = "key"
	cfg.Binance.APISecret = "secret"
	cfg.Checkout.Binance.UID = uid
	cfg.Checkout.Currencies = []string{"USDT", "USDC"}

	ctx := context.Background()
	o, err := st.CreateOrder(ctx, store.CreateOrderInput{
		Protocol: "bepusdt", Pid: "bepusdt", TradeNo: tradeNo, OutTradeNo: "M-" + tradeNo,
		OriginMoney: "5.00", OriginCurrency: "USD", ExpiredAt: time.Now().Add(10 * time.Minute),
		Reselect: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	o, err = st.SelectPayment(ctx, o.TradeNo, "binance", "USDT", uid,
		5, 0.01, 99, 0.01, 2, 2*time.Minute)
	if err != nil {
		t.Fatal(err)
	}

	return &pollFixture{st: st, cfg: cfg, poller: NewPoller(settings.New(cfg, st), st), stub: stub, order: o}
}

// 交易时间固定在订单支付窗口内。币安接口按毫秒回传，而 created_at 带亚毫秒
// 精度，直接用 time.Now() 会让流水偶尔落在窗口之前。
func (f *pollFixture) txTime() time.Time {
	return f.order.CreatedAt.Add(3 * time.Second)
}

func (f *pollFixture) tick(t *testing.T) {
	t.Helper()
	f.poller.tick(context.Background(), f.cfg, config.ProviderBinance)
}

func (f *pollFixture) observations(t *testing.T) []model.TxObservation {
	t.Helper()
	out, err := f.st.RecentTxObservations(context.Background(), 20)
	if err != nil {
		t.Fatal(err)
	}
	return out
}

func (f *pollFixture) observationRow(t *testing.T, txID string) *model.TxObservation {
	t.Helper()
	var row model.TxObservation
	if err := f.st.DB().Where("provider = ? AND transaction_id = ?", "binance", txID).
		Take(&row).Error; err != nil {
		t.Fatalf("observation %s not found: %v", txID, err)
	}
	return &row
}

// 账本内部只保留优先级最高的原因，已入账则整体撤回。
func TestTxObservationBookKeepsStrongestReason(t *testing.T) {
	book := newTxObservationBook(config.ProviderBinance)
	tx := incomingTransaction{ID: "tx-1", Type: "PAY", Amount: 10.01, Currency: "USDT"}

	book.observe(&tx, model.ObservationReasonClaimFailed, "LP-A")
	book.observe(&tx, model.ObservationReasonNoMatch, "")
	pending := book.pending()
	if len(pending) != 1 || pending[0].Reason != model.ObservationReasonClaimFailed ||
		pending[0].OrderTradeNo != "LP-A" {
		t.Fatalf("weak reason overwrote claim_failed: %+v", pending)
	}

	// 同一原因的重复观察保留最新一次尝试的订单号。
	book.observe(&tx, model.ObservationReasonClaimFailed, "LP-B")
	if pending = book.pending(); pending[0].OrderTradeNo != "LP-B" {
		t.Fatalf("latest attempt not kept: %+v", pending)
	}

	// 未知原因不会被当作高优先级。
	other := incomingTransaction{ID: "tx-2", Type: "PAY", Amount: 1, Currency: "USDT"}
	book.observe(&other, model.ObservationReasonNoMatch, "")
	book.observe(&other, "garbage", "")
	if pending = book.pending(); len(pending) != 2 || pending[1].Reason != model.ObservationReasonNoMatch {
		t.Fatalf("unknown reason replaced a known one: %+v", pending)
	}

	book.retract("tx-1")
	if pending = book.pending(); len(pending) != 1 || pending[0].TransactionID != "tx-2" {
		t.Fatalf("retract failed: %+v", pending)
	}
}

// 核销失败的原因不能被后续扫描的 no_match 覆盖；订单恢复可入账后转为已归属。
func TestPollerKeepsClaimFailedReasonUntilResolved(t *testing.T) {
	f := newPollFixture(t, "LP-CLAIM-FAILED")
	ctx := context.Background()

	// 收款 UID 在订单进行中被改动：轮询器放行（流水收款人是配置的 UID），
	// 但入账校验会拒绝，产生一条 claim_failed 证据。
	if err := f.st.DB().Model(&model.Order{}).Where("id = ?", f.order.ID).
		Update("pay_address", "stale-uid").Error; err != nil {
		t.Fatal(err)
	}
	f.stub.set(stubBinanceTx{
		id: "tx-claim-failed", orderType: "PAY", amount: f.order.PayAmount,
		currency: "USDT", at: f.txTime(), receiver: "binance-uid",
	})
	f.tick(t)

	got := f.observations(t)
	if len(got) != 1 || got[0].Reason != model.ObservationReasonClaimFailed ||
		got[0].OrderTradeNo != f.order.TradeNo {
		t.Fatalf("claim failure not recorded: %+v", got)
	}

	// 第二轮金额对不上，只剩 no_match：不能覆盖真正的核销失败原因。
	f.stub.set(stubBinanceTx{
		id: "tx-claim-failed", orderType: "PAY", amount: f.order.PayAmount + 0.01,
		currency: "USDT", at: f.txTime(), receiver: "binance-uid",
	})
	f.tick(t)
	got = f.observations(t)
	if len(got) != 1 || got[0].Reason != model.ObservationReasonClaimFailed {
		t.Fatalf("no_match overwrote claim_failed: %+v", got)
	}

	// 收款 UID 修好后同一笔流水补入账，账本标记为已归属并退出后台列表。
	if err := f.st.DB().Model(&model.Order{}).Where("id = ?", f.order.ID).
		Update("pay_address", "binance-uid").Error; err != nil {
		t.Fatal(err)
	}
	f.stub.set(stubBinanceTx{
		id: "tx-claim-failed", orderType: "PAY", amount: f.order.PayAmount,
		currency: "USDT", at: f.txTime(), receiver: "binance-uid",
	})
	f.tick(t)

	paid, err := f.st.GetByTradeNo(ctx, f.order.TradeNo)
	if err != nil {
		t.Fatal(err)
	}
	if paid.Status != model.StatusPaid {
		t.Fatalf("order status = %s, want paid", paid.Status)
	}
	if got = f.observations(t); len(got) != 0 {
		t.Fatalf("credited transaction still reported as unassigned: %+v", got)
	}
	row := f.observationRow(t, "tx-claim-failed")
	if row.Status != model.ObservationResolved || row.ResolvedTradeNo != f.order.TradeNo {
		t.Fatalf("observation not resolved: %+v", row)
	}
}

// 已入账流水再次出现在历史窗口（幂等认领返回 false）时，不能记成未归属。
func TestPollerResolvesAlreadyClaimedTransaction(t *testing.T) {
	f := newPollFixture(t, "LP-DUP")
	ctx := context.Background()

	// 先让该流水在别处入账。
	if ok, err := f.st.ClaimAndPay(ctx, f.order, model.IncomingTx{
		TransactionID: "tx-dup", Provider: "binance", Type: "PAY",
		Amount: f.order.PayAmount, Currency: "USDT",
		ReceiverID: "binance-uid", ReceiverAddr: "binance-uid",
		TransactionTime: f.txTime(),
	}, 2); err != nil || !ok {
		t.Fatalf("setup claim failed: ok=%v err=%v", ok, err)
	}

	// 新订单复用同一金额（模拟冷却期结束后的档位复用），并预先留下一条
	// 上一轮扫描写入的未归属记录。
	second, err := f.st.CreateOrder(ctx, store.CreateOrderInput{
		Protocol: "bepusdt", Pid: "bepusdt", TradeNo: "LP-DUP-2", OutTradeNo: "M-LP-DUP-2",
		OriginMoney: "5.00", OriginCurrency: "USD", ExpiredAt: time.Now().Add(10 * time.Minute),
		Reselect: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	var slot model.AmountSlot
	if err := f.st.DB().Where("trade_no = ?", f.order.TradeNo).Take(&slot).Error; err != nil {
		t.Fatal(err)
	}
	if err := f.st.DB().Model(&model.AmountSlot{}).Where("id = ?", slot.ID).
		Updates(map[string]any{
			"order_id": second.ID, "trade_no": second.TradeNo,
			"expires_at": time.Now().Add(5 * time.Minute),
		}).Error; err != nil {
		t.Fatal(err)
	}
	if err := f.st.DB().Model(&model.Order{}).Where("id = ?", second.ID).
		Updates(map[string]any{
			"collection": "binance", "currency": "USDT", "pay_address": "binance-uid",
			"pay_amount": slot.Amount, "amount_precision": 2,
			"status": model.StatusReady, "selected_at": second.CreatedAt,
		}).Error; err != nil {
		t.Fatal(err)
	}
	if err := f.st.RecordTxObservation(ctx, model.TxObservation{
		Provider: "binance", TransactionID: "tx-dup", Type: "PAY",
		Amount: slot.Amount, Currency: "USDT", ReceiverID: "binance-uid",
		TransactionTime: time.Now(), Reason: model.ObservationReasonNoMatch,
	}); err != nil {
		t.Fatal(err)
	}

	f.stub.set(stubBinanceTx{
		id: "tx-dup", orderType: "PAY", amount: slot.Amount,
		currency: "USDT", at: f.txTime(), receiver: "binance-uid",
	})
	f.tick(t)

	if got := f.observations(t); len(got) != 0 {
		t.Fatalf("already credited transaction reported as unassigned: %+v", got)
	}
	row := f.observationRow(t, "tx-dup")
	if row.Status != model.ObservationResolved {
		t.Fatalf("observation status = %q, want resolved", row.Status)
	}
	// 第二张订单不能被同一笔流水二次入账。
	current, err := f.st.GetByTradeNo(ctx, second.TradeNo)
	if err != nil {
		t.Fatal(err)
	}
	if current.Status == model.StatusPaid {
		t.Fatal("duplicate transfer credited a second order")
	}
}
