package binance

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"litepay/internal/model"
)

// 样本按官方 Get Pay Trade History 文档字段构造：orderType / transactionId /
// transactionTime / amount / currency / walletType / walletTypes / fundsDetail
// {currency, amount, walletAssetCost} / payerInfo / receiverInfo {name, type, binanceId}。
// 文档没有商户订单号字段，样本故意带上 orderId 以确认它不会被当成订单号。
const payHistorySample = `{"code":"000000","message":"success","success":true,"data":[` +
	`{"orderType":"PAY","transactionId":"M_P_71505104267788288","orderId":"shop-order-1","transactionTime":100000,"amount":"10.01","currency":"USDT","walletType":1,"walletTypes":[1],"fundsDetail":[{"currency":"USDT","amount":"10.01","walletAssetCost":{"1":"10.01"}}],"payerInfo":{"name":"Payer"},"receiverInfo":{"name":"Shop","type":"MERCHANT","binanceId":"uid"}},` +
	`{"orderType":"C2C","transactionId":"M_C_71505104267788289","transactionTime":100001,"amount":"5.5","currency":"USDC","fundsDetail":[{"currency":"USDC","amount":"5.5"}],"payerInfo":{"name":"Payer"},"receiverInfo":{"name":"Shop","binanceId":"uid"}},` +
	`{"orderType":"PAY","transactionId":"M_P_OUT_1","transactionTime":100002,"amount":"-3","currency":"USDT","fundsDetail":[{"currency":"USDT","amount":"-3"}],"payerInfo":{"binanceId":"uid"},"receiverInfo":{"name":"Other"}},` +
	`{"orderType":"PAY","transactionId":"M_P_8DP_1","transactionTime":100003,"amount":"23.72469206","currency":"BTC","fundsDetail":[{"currency":"BTC","amount":"23.72469206"}],"receiverInfo":{"binanceId":"uid"}},` +
	`{"orderType":"PAY","transactionId":"M_P_MULTI_1","transactionTime":100004,"amount":"0","fundsDetail":[{"currency":"USDT","amount":"10.01"},{"currency":"USDC","amount":"10.02"}],"receiverInfo":{"binanceId":"uid"}}]}`

func TestPayHistoryFieldParsing(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("X-MBX-APIKEY") != "key" || r.URL.Query().Get("signature") == "" {
			t.Error("unsigned request")
		}
		if r.URL.Query().Get("startTime") != "90000" || r.URL.Query().Get("endTime") != "110000" {
			t.Error("wrong window", r.URL.Query())
		}
		fmt.Fprint(w, payHistorySample)
	}))
	defer srv.Close()
	c := New("key", "secret", srv.URL, 5000, 100)
	c.UseTestTransport()
	rows, e := c.History(context.Background(), time.UnixMilli(90000), time.UnixMilli(110000))
	if e != nil || len(rows) != 5 {
		t.Fatal(rows, e)
	}
	// 收款方 PAY：transactionId 既是事件 ID 也是查询编号，binanceId 是收款 UID。
	if r := rows[0]; r.EventID != "M_P_71505104267788288" || r.Reference != r.EventID || r.Receiver != "uid" || r.Currency != "USDT" || r.Units != 10010000 || r.State != model.Confirmed || !r.OccurredAt.Equal(time.UnixMilli(100000)) {
		t.Fatal("pay receiver", r)
	}
	if r := rows[1]; r.State != model.Confirmed || r.Units != 5500000 || r.Currency != "USDC" || r.Reference != r.EventID {
		t.Fatal("c2c receiver", r)
	}
	// 付款方向金额为负，receiverInfo 只有昵称：不是收款，拒绝自动核销。
	if r := rows[2]; r.State != model.Rejected || r.Reason != "not_incoming_payment" || r.Reference != r.EventID {
		t.Fatal("outgoing pay", r)
	}
	// 文档允许 8 位小数，支付域只有 6 位：保留证据但拒绝核销，且不影响整次查询。
	if r := rows[3]; r.State != model.Rejected || r.Reason != "invalid_amount" || r.Units != 0 {
		t.Fatal("8dp amount", r)
	}
	if r := rows[4]; r.State != model.Ambiguous || r.Assets == "" {
		t.Fatal("multiple assets", r)
	}
	found, e := c.Lookup(context.Background(), "M_P_71505104267788288", time.UnixMilli(90000), time.UnixMilli(110000))
	if e != nil || len(found) != 1 || found[0].EventID != "M_P_71505104267788288" {
		t.Fatal("transaction id lookup", found, e)
	}
}

// 空窗口返回 data:null 时按空结果处理，否则游标永远推进不了。
func TestEmptyHistoryAcceptsNullData(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprint(w, `{"code":"000000","message":"success","success":true,"data":null}`)
	}))
	defer srv.Close()
	c := New("k", "s", srv.URL, 5000, 100)
	c.UseTestTransport()
	rows, e := c.History(context.Background(), time.UnixMilli(90000), time.UnixMilli(110000))
	if e != nil || len(rows) != 0 {
		t.Fatal(rows, e)
	}
}

func TestSaturatedMillisecondFails(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprint(w, `{"code":"000000","success":true,"data":[{"transactionId":"a","orderType":"PAY","transactionTime":100000,"amount":"1","currency":"USDT","receiverInfo":{"binanceId":"uid"}}]}`)
	}))
	defer srv.Close()
	c := New("k", "s", srv.URL, 5000, 1)
	c.UseTestTransport()
	if _, e := c.History(context.Background(), time.UnixMilli(100000), time.UnixMilli(100000)); e == nil {
		t.Fatal("incomplete history accepted")
	}
}
