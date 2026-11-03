package okx

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"litepay/internal/model"
)

// 样本按官方 Get deposit history 文档字段构造：depId / fromWdId / ccy / chain / amt /
// from / to / ts / state / txId / actualDepBlkConfirm。响应没有 type 字段，
// 「是否站内转账」由请求参数 type=3 与 fromWdId 是否回填共同确认。
const depositSample = `{"code":"0","msg":"","data":[` +
	`{"actualDepBlkConfirm":"0","amt":"10.000001","areaCodeFrom":"","ccy":"USDC","chain":"","depId":"dep-1","from":"138****1234","fromWdId":"wd-1","state":"2","to":"","ts":"100000","txId":"hash-1"},` +
	`{"actualDepBlkConfirm":"0","amt":"5","ccy":"USDT","depId":"dep-2","from":"139****5678","fromWdId":"wd-2","state":"0","ts":"100001","txId":"hash-2"},` +
	`{"actualDepBlkConfirm":"2","amt":"7","ccy":"USDT","chain":"USDT-TRC20","depId":"dep-3","from":"","fromWdId":"","state":"2","to":"TAddr","ts":"100002","txId":"hash-3"},` +
	`{"actualDepBlkConfirm":"0","amt":"9","ccy":"USDT","depId":"dep-4","from":"137****0000","fromWdId":"wd-4","state":"12","ts":"100003","txId":"hash-4"},` +
	`{"actualDepBlkConfirm":"0","amt":"0.12345678","ccy":"BTC","depId":"dep-5","from":"136****1111","fromWdId":"wd-5","state":"2","ts":"100004","txId":"hash-5"}]}`

func TestDepositFieldParsing(t *testing.T) {
	missing := false
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/api/v5/public/time":
			fmt.Fprint(w, `{"code":"0","data":[{"ts":"100000"}]}`)
		case "/api/v5/account/config":
			fmt.Fprint(w, `{"code":"0","data":[{"uid":"receiver"}]}`)
		default:
			if r.URL.Query().Get("type") != "3" || r.Header.Get("OK-ACCESS-SIGN") == "" {
				t.Error("wrong signed internal-transfer query")
			}
			if missing {
				fmt.Fprint(w, `{"code":"0","data":[{"amt":"1","ccy":"USDT","fromWdId":"wd","state":"2","ts":"100000"}]}`)
				return
			}
			fmt.Fprint(w, depositSample)
		}
	}))
	defer srv.Close()
	c := New("k", "s", "p", srv.URL, 100)
	c.UseTestTransport()
	rows, e := c.History(context.Background(), time.UnixMilli(90000), time.UnixMilli(110000))
	if e != nil || len(rows) != 5 {
		t.Fatal(rows, e)
	}
	// 站内转账完成：depId 是事件 ID，fromWdId 是发起方提币 ID，ts 是记录创建时间。
	if r := rows[0]; r.EventID != "dep-1" || r.Reference != "wd-1" || r.Receiver != "receiver" || r.Currency != "USDC" || r.Units != 10000001 || r.State != model.Confirmed || !r.OccurredAt.Equal(time.UnixMilli(100000)) {
		t.Fatal("completed internal transfer", r)
	}
	// state=0 waiting for confirmation：只作为待确认证据。
	if r := rows[1]; r.State != model.Unconfirmed || r.Reason != "deposit_not_completed" {
		t.Fatal("waiting deposit", r)
	}
	// 没有 fromWdId 的充值（链上）即使 state=2 也不是站内收款：不核销。
	if r := rows[2]; r.State != model.Unconfirmed || r.Reason != "internal_transfer_unverified" {
		t.Fatal("chain deposit", r)
	}
	// state=12 account or deposit is frozen：不核销。
	if r := rows[3]; r.State != model.Unconfirmed {
		t.Fatal("frozen deposit", r)
	}
	// 8 位小数超出支付域精度：保留证据但拒绝核销，且不影响整次查询。
	if r := rows[4]; r.State != model.Rejected || r.Reason != "invalid_amount" || r.Units != 0 {
		t.Fatal("8dp amount", r)
	}
	missing = true
	if _, e = c.History(context.Background(), time.UnixMilli(90000), time.UnixMilli(110000)); e == nil {
		t.Fatal("accepted missing depId")
	}
}
