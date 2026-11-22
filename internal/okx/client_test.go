package okx

import (
	"bytes"
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
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
	// 8 位小数超出支付域精度：保留证据但拒绝核销，且不影响整次查询；原始金额必须保留。
	if r := rows[4]; r.State != model.Rejected || r.Reason != "invalid_amount" || r.Units != 0 || r.RawAmount != "0.12345678" {
		t.Fatal("8dp amount", r)
	}
	missing = true
	if _, e = c.History(context.Background(), time.UnixMilli(90000), time.UnixMilli(110000)); e == nil {
		t.Fatal("accepted missing depId")
	}
	if _, e = c.History(context.Background(), time.UnixMilli(110000), time.UnixMilli(90000)); e == nil {
		t.Fatal("accepted inverted time range")
	}
}

func TestTimeSyncFailureLogsTypeOnceAndContinuesPrivateRequests(t *testing.T) {
	var timeRequests atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/api/v5/public/time" {
			timeRequests.Add(1)
			http.Error(w, "sensitive-time-endpoint-body", http.StatusServiceUnavailable)
			return
		}
		if r.Header.Get("OK-ACCESS-SIGN") == "" {
			t.Error("fallback request is unsigned")
		}
		if _, err := time.Parse("2006-01-02T15:04:05.000Z", r.Header.Get("OK-ACCESS-TIMESTAMP")); err != nil {
			t.Error("invalid fallback timestamp", err)
		}
		fmt.Fprint(w, `{"code":"0","data":[{"uid":"receiver"}]}`)
	}))
	defer srv.Close()
	var logs bytes.Buffer
	previous := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(&logs, &slog.HandlerOptions{Level: slog.LevelDebug})))
	defer slog.SetDefault(previous)
	client := New("private-api-key", "private-api-secret", "private-passphrase", srv.URL, 100)
	client.UseTestTransport()
	for range 2 {
		uid, err := client.AccountUID(context.Background())
		if err != nil || uid != "receiver" {
			t.Fatal("time-sync failure blocked private request", uid, err)
		}
	}
	if timeRequests.Load() != 1 || strings.Count(logs.String(), "OKX 时间同步失败") != 1 || !strings.Contains(logs.String(), "error_type=") || !strings.Contains(logs.String(), "level=DEBUG") {
		t.Fatal("missing or repeated bounded diagnostic", logs.String())
	}
	for _, secret := range []string{"private-api-key", "private-api-secret", "private-passphrase", "sensitive-time-endpoint-body", srv.URL} {
		if strings.Contains(logs.String(), secret) {
			t.Fatal("time sync log leaked credentials or endpoint")
		}
	}
}

func TestHistorySubMillisecondWindowBoundary(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/api/v5/public/time":
			fmt.Fprint(w, `{"code":"0","data":[{"ts":"100000"}]}`)
		case "/api/v5/account/config":
			fmt.Fprint(w, `{"code":"0","data":[{"uid":"receiver"}]}`)
		default:
			fmt.Fprint(w, `{"code":"0","data":[{"depId":"dep-edge","fromWdId":"wd-edge","ccy":"USDT","amt":"12.34","state":"2","ts":"100000"}]}`)
		}
	}))
	defer srv.Close()

	c := New("k", "s", "p", srv.URL, 100)
	c.UseTestTransport()
	start := time.UnixMilli(100000).Add(500 * time.Microsecond)
	end := time.UnixMilli(101000).Add(750 * time.Microsecond)
	rows, err := c.History(context.Background(), start, end)
	if err != nil {
		t.Fatalf("History failed: %v", err)
	}
	if len(rows) != 1 || rows[0].EventID != "dep-edge" {
		t.Fatalf("expected boundary record at start.UnixMilli(), got %+v", rows)
	}
}

func TestTimestampExpiredResyncsAndRetriesOnce(t *testing.T) {
	var timeCalls atomic.Int32
	var accountCalls atomic.Int32
	const syncedTimeMS = int64(1700000500000)

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/api/v5/public/time" {
			n := timeCalls.Add(1)
			if n == 1 {
				http.Error(w, "temporary outage", http.StatusServiceUnavailable)
				return
			}
			fmt.Fprintf(w, `{"code":"0","data":[{"ts":"%d"}]}`, syncedTimeMS)
			return
		}
		if r.URL.Path == "/api/v5/account/config" {
			n := accountCalls.Add(1)
			if n == 1 {
				w.WriteHeader(http.StatusUnauthorized)
				fmt.Fprint(w, `{"code":"50102","msg":"Timestamp request expired"}`)
				return
			}
			ts, err := time.Parse("2006-01-02T15:04:05.000Z", r.Header.Get("OK-ACCESS-TIMESTAMP"))
			if err != nil || ts.UnixMilli() < syncedTimeMS || ts.UnixMilli() > syncedTimeMS+5000 {
				t.Errorf("unexpected retried timestamp: %q (%v)", r.Header.Get("OK-ACCESS-TIMESTAMP"), err)
			}
			fmt.Fprint(w, `{"code":"0","data":[{"uid":"receiver"}]}`)
		}
	}))
	defer srv.Close()

	c := New("k", "s", "p", srv.URL, 100)
	c.UseTestTransport()
	uid, err := c.AccountUID(context.Background())
	if err != nil || uid != "receiver" {
		t.Fatalf("expected resync recovery on 50102, got uid=%q err=%v", uid, err)
	}
	if timeCalls.Load() != 2 || accountCalls.Load() != 2 {
		t.Fatalf("unexpected call counts: time=%d account=%d", timeCalls.Load(), accountCalls.Load())
	}
}
