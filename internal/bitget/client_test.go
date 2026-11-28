package bitget

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"litepay/internal/model"
	"litepay/internal/sign"
)

func TestAccountUIDAndSignature(t *testing.T) {
	const (
		apiKey     = "bg-key"
		apiSecret  = "bg-secret"
		passphrase = "bg-pass"
	)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/api/v3/public/time":
			_ = json.NewEncoder(w).Encode(map[string]any{
				"code": "00000",
				"data": map[string]any{"serverTime": "1700000000123"},
			})
		case "/api/v3/account/info":
			ts := r.Header.Get("ACCESS-TIMESTAMP")
			reqPath := r.URL.RequestURI()
			wantSig := sign.HmacSHA256Base64(ts+"GET"+reqPath, apiSecret)
			if r.Header.Get("ACCESS-KEY") != apiKey || r.Header.Get("ACCESS-PASSPHRASE") != passphrase || r.Header.Get("ACCESS-SIGN") != wantSig {
				w.WriteHeader(http.StatusUnauthorized)
				return
			}
			_ = json.NewEncoder(w).Encode(map[string]any{
				"code": "00000",
				"data": map[string]any{"userId": "55667788", "parentId": ""},
			})
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer srv.Close()

	cli := New(apiKey, apiSecret, passphrase, srv.URL, 100)
	cli.UseTestTransport()
	uid, err := cli.AccountUID(context.Background())
	if err != nil {
		t.Fatalf("AccountUID failed: %v", err)
	}
	if uid != "55667788" {
		t.Fatalf("uid = %q, want 55667788", uid)
	}
}

func TestAccountUIDRejectsSubAccountAndFallsBackToV2(t *testing.T) {
	t.Run("reject_subaccount", func(t *testing.T) {
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.URL.Path == "/api/v3/public/time" {
				_ = json.NewEncoder(w).Encode(map[string]any{"code": "00000", "data": map[string]any{"serverTime": "1700000000000"}})
				return
			}
			_ = json.NewEncoder(w).Encode(map[string]any{"code": "00000", "data": map[string]any{"userId": "222", "parentId": "111"}})
		}))
		defer srv.Close()
		cli := New("k", "s", "p", srv.URL, 100)
		cli.UseTestTransport()
		if _, err := cli.AccountUID(context.Background()); err == nil {
			t.Fatal("expected error for sub-account key")
		}
	})

	t.Run("v2_fallback", func(t *testing.T) {
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			switch r.URL.Path {
			case "/api/v3/public/time":
				w.WriteHeader(http.StatusNotFound)
			case "/api/v2/public/time":
				_ = json.NewEncoder(w).Encode(map[string]any{"code": "00000", "data": map[string]any{"serverTime": "1700000000000"}})
			case "/api/v3/account/info":
				w.WriteHeader(http.StatusNotFound)
			case "/api/v2/spot/account/info":
				_ = json.NewEncoder(w).Encode(map[string]any{"code": "00000", "data": map[string]any{"userId": "778899"}})
			}
		}))
		defer srv.Close()
		cli := New("k", "s", "p", srv.URL, 100)
		cli.UseTestTransport()
		uid, err := cli.AccountUID(context.Background())
		if err != nil || uid != "778899" {
			t.Fatalf("got uid=%q err=%v", uid, err)
		}
	})
}

func TestHistoryPaginationFilteringAndStates(t *testing.T) {
	start := time.UnixMilli(1700000000000)
	end := time.UnixMilli(1700003600000)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/api/v3/public/time":
			_ = json.NewEncoder(w).Encode(map[string]any{"code": "00000", "data": map[string]any{"serverTime": "1700003600000"}})
		case "/api/v3/account/info":
			_ = json.NewEncoder(w).Encode(map[string]any{"code": "00000", "data": map[string]any{"userId": "90001"}})
		case "/api/v3/account/deposit-records":
			cursor := r.URL.Query().Get("cursor")
			if cursor == "" {
				_ = json.NewEncoder(w).Encode(map[string]any{
					"code": "00000",
					"data": map[string]any{
						"cursor": "page-2",
						"list": []map[string]any{
							{"orderId": "204", "tradeId": "tr-204", "coin": "USDT", "amount": "15.25", "status": "success", "dest": "internal_transfer", "ts": "1700000004000"},
							{"orderId": "203", "tradeId": "tr-203", "coin": "USDT", "amount": "99.00", "status": "success", "dest": "on_chain", "ts": "1700000003000"},
						},
					},
				})
				return
			}
			if cursor == "page-2" {
				_ = json.NewEncoder(w).Encode(map[string]any{
					"code": "00000",
					"data": map[string]any{
						"cursor": "",
						"list": []map[string]any{
							{"orderId": "202", "tradeId": "tr-202", "coin": "USDC", "amount": "3.50", "status": "pending", "dest": "internal_transfer", "ts": "1700000002000"},
							{"orderId": "201", "tradeId": "tr-201", "coin": "USDT", "amount": "1.12345678", "status": "success", "dest": "internal_transfer", "ts": "1700000001000"},
						},
					},
				})
				return
			}
			w.WriteHeader(http.StatusBadRequest)
		}
	}))
	defer srv.Close()

	cli := New("k", "s", "p", srv.URL, 2)
	cli.UseTestTransport()
	rows, err := cli.History(context.Background(), start, end)
	if err != nil {
		t.Fatalf("History failed: %v", err)
	}
	if len(rows) != 3 {
		t.Fatalf("got %d rows, want 3 (on_chain skipped): %+v", len(rows), rows)
	}
	if rows[0].Provider != "bitget" || rows[0].Receiver != "90001" || rows[0].EventID != "204" || rows[0].Reference != "tr-204" || rows[0].State != model.Confirmed || rows[0].Units != 15250000 {
		t.Fatalf("row[0] mismatch: %+v", rows[0])
	}
	if rows[1].State != model.Unconfirmed || rows[1].Reason != "deposit_not_completed" {
		t.Fatalf("row[1] mismatch: %+v", rows[1])
	}
	if rows[2].State != model.Rejected || rows[2].Reason != "invalid_amount" || rows[2].RawAmount != "1.12345678" {
		t.Fatalf("row[2] mismatch: %+v", rows[2])
	}
}

func TestAccountUIDFallsBackOn40085AndV3ArrayPaginates(t *testing.T) {
	t.Run("v2_fallback_on_40085", func(t *testing.T) {
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			switch r.URL.Path {
			case "/api/v3/public/time":
				_ = json.NewEncoder(w).Encode(map[string]any{"code": "00000", "data": map[string]any{"serverTime": "1700000000000"}})
			case "/api/v3/account/info":
				w.WriteHeader(http.StatusBadRequest)
				_ = json.NewEncoder(w).Encode(map[string]any{"code": "40085", "msg": "classic account"})
			case "/api/v2/spot/account/info":
				_ = json.NewEncoder(w).Encode(map[string]any{"code": "00000", "data": map[string]any{"userId": "778899"}})
			}
		}))
		defer srv.Close()
		cli := New("k", "s", "p", srv.URL, 100)
		cli.UseTestTransport()
		uid, err := cli.AccountUID(context.Background())
		if err != nil || uid != "778899" {
			t.Fatalf("expected 40085 fallback to succeed, got uid=%q err=%v", uid, err)
		}
	})

	t.Run("v3_array_pagination", func(t *testing.T) {
		start := time.UnixMilli(1700000000000)
		end := time.UnixMilli(1700003600000)
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			switch r.URL.Path {
			case "/api/v3/public/time":
				_ = json.NewEncoder(w).Encode(map[string]any{"code": "00000", "data": map[string]any{"serverTime": "1700003600000"}})
			case "/api/v3/account/info":
				_ = json.NewEncoder(w).Encode(map[string]any{"code": "00000", "data": map[string]any{"userId": "90001"}})
			case "/api/v3/account/deposit-records":
				cursor := r.URL.Query().Get("cursor")
				if cursor == "" {
					_ = json.NewEncoder(w).Encode(map[string]any{
						"code": "00000",
						"data": []map[string]any{
							{"id": "303", "tradeId": "tr-303", "coin": "USDT", "amount": "10", "status": "success", "dest": "internal_transfer", "ts": "1700000003000"},
							{"id": "302", "tradeId": "tr-302", "coin": "USDT", "amount": "11", "status": "success", "dest": "internal_transfer", "ts": "1700000002000"},
						},
					})
					return
				}
				if cursor == "302" {
					_ = json.NewEncoder(w).Encode(map[string]any{
						"code": "00000",
						"data": []map[string]any{
							{"id": "301", "tradeId": "tr-301", "coin": "USDT", "amount": "12", "status": "success", "dest": "internal_transfer", "ts": "1700000001000"},
						},
					})
					return
				}
				w.WriteHeader(http.StatusBadRequest)
			}
		}))
		defer srv.Close()

		cli := New("k", "s", "p", srv.URL, 2)
		cli.UseTestTransport()
		rows, err := cli.History(context.Background(), start, end)
		if err != nil {
			t.Fatalf("History failed: %v", err)
		}
		if len(rows) != 3 {
			t.Fatalf("expected all 3 rows across 2 V3 array pages, got %d", len(rows))
		}
	})
}

func TestTimestampErrorResyncsAndRetriesOnce(t *testing.T) {
	var timeCalls atomic.Int32
	var accountCalls atomic.Int32
	const syncedTimeMS = int64(1700000600000)

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/api/v3/public/time":
			if timeCalls.Add(1) == 1 {
				w.WriteHeader(http.StatusServiceUnavailable)
				return
			}
			_ = json.NewEncoder(w).Encode(map[string]any{
				"code": "00000",
				"data": map[string]any{"serverTime": strconv.FormatInt(syncedTimeMS, 10)},
			})
		case "/api/v2/public/time":
			w.WriteHeader(http.StatusNotFound)
		case "/api/v3/account/info":
			if accountCalls.Add(1) == 1 {
				w.WriteHeader(http.StatusBadRequest)
				_ = json.NewEncoder(w).Encode(map[string]any{"code": "40005", "msg": "timestamp expired"})
				return
			}
			ms, err := strconv.ParseInt(r.Header.Get("ACCESS-TIMESTAMP"), 10, 64)
			if err != nil || ms < syncedTimeMS || ms > syncedTimeMS+5000 {
				t.Errorf("unexpected retried ACCESS-TIMESTAMP: %q (%v)", r.Header.Get("ACCESS-TIMESTAMP"), err)
			}
			_ = json.NewEncoder(w).Encode(map[string]any{"code": "00000", "data": map[string]any{"userId": "55667788"}})
		}
	}))
	defer srv.Close()

	cli := New("k", "s", "p", srv.URL, 100)
	cli.UseTestTransport()
	uid, err := cli.AccountUID(context.Background())
	if err != nil || uid != "55667788" {
		t.Fatalf("expected resync recovery on 40005, got uid=%q err=%v", uid, err)
	}
	if timeCalls.Load() != 2 || accountCalls.Load() != 2 {
		t.Fatalf("unexpected call counts: time=%d account=%d", timeCalls.Load(), accountCalls.Load())
	}
}

func TestContextCancellationAndTerminalFailureStatuses(t *testing.T) {
	t.Run("context_canceled", func(t *testing.T) {
		started := make(chan struct{})
		release := make(chan struct{})
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			close(started)
			<-release
		}))
		defer func() {
			close(release)
			srv.Close()
		}()

		cli := New("secret-key", "secret-val", "secret-pass", srv.URL, 100)
		cli.UseTestTransport()
		cli.syncTime = false
		defer cli.CloseIdleConnections()

		ctx, cancel := context.WithCancel(context.Background())
		errCh := make(chan error, 1)
		go func() {
			_, err := cli.AccountUID(ctx)
			errCh <- err
		}()
		<-started
		cancel()
		err := <-errCh
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("expected context.Canceled to be preserved, got %v", err)
		}
		if strings.Contains(err.Error(), srv.URL) || strings.Contains(err.Error(), "secret-") {
			t.Fatalf("canceled error leaked endpoint or credentials: %v", err)
		}
	})

	t.Run("terminal_failure_statuses", func(t *testing.T) {
		start := time.UnixMilli(1700000000000)
		end := time.UnixMilli(1700003600000)
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			switch r.URL.Path {
			case "/api/v3/public/time":
				_ = json.NewEncoder(w).Encode(map[string]any{"code": "00000", "data": map[string]any{"serverTime": "1700003600000"}})
			case "/api/v3/account/info":
				_ = json.NewEncoder(w).Encode(map[string]any{"code": "00000", "data": map[string]any{"userId": "90001"}})
			case "/api/v3/account/deposit-records":
				_ = json.NewEncoder(w).Encode(map[string]any{
					"code": "00000",
					"data": map[string]any{
						"cursor": "",
						"list": []map[string]any{
							{"orderId": "401", "tradeId": "tr-401", "coin": "USDT", "amount": "5", "status": "pending_fail", "dest": "internal_transfer", "ts": "1700000001000"},
							{"orderId": "402", "tradeId": "tr-402", "coin": "USDT", "amount": "5", "status": "pending_review_fail", "dest": "internal_transfer", "ts": "1700000002000"},
							{"orderId": "403", "tradeId": "tr-403", "coin": "USDT", "amount": "5", "status": "reject", "dest": "internal_transfer", "ts": "1700000003000"},
							{"orderId": "404", "tradeId": "tr-404", "coin": "USDT", "amount": "5", "status": "wallet_fail", "dest": "internal_transfer", "ts": "1700000004000"},
							{"orderId": "405", "tradeId": "tr-405", "coin": "USDT", "amount": "5", "status": "pending_review", "dest": "internal_transfer", "ts": "1700000005000"},
						},
					},
				})
			}
		}))
		defer srv.Close()

		cli := New("k", "s", "p", srv.URL, 100)
		cli.UseTestTransport()
		defer cli.CloseIdleConnections()
		rows, err := cli.History(context.Background(), start, end)
		if err != nil || len(rows) != 5 {
			t.Fatalf("History failed: rows=%+v err=%v", rows, err)
		}
		for i := range 4 {
			if rows[i].State != model.Rejected || rows[i].Reason != "deposit_failed" {
				t.Fatalf("row[%d] want Rejected/deposit_failed, got %+v", i, rows[i])
			}
		}
		if rows[4].State != model.Unconfirmed || rows[4].Reason != "deposit_not_completed" {
			t.Fatalf("row[4] want Unconfirmed/deposit_not_completed, got %+v", rows[4])
		}
	})
}

func TestClassicShortPageWithCursorIsNotFinal(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/api/v3/account/deposit-records" {
			http.NotFound(w, r)
			return
		}
		if r.URL.Query().Get("idLessThan") == "" {
			fmt.Fprint(w, `{"code":"00000","data":{"cursor":"42","list":[]}}`)
			return
		}
		fmt.Fprint(w, `{"code":"00000","data":{"cursor":"","list":[{"id":"41","coin":"USDT","amount":"1","dest":"internal_transfer","status":"success","ts":1500}]}}`)
	}))
	defer srv.Close()
	c := New("key", "secret", "pass", srv.URL, 100)
	c.receiver = "receiver"
	c.syncTime = false
	c.UseTestTransport()
	rows, err := c.History(context.Background(), time.UnixMilli(1000), time.UnixMilli(2000))
	if err != nil || len(rows) != 1 || rows[0].EventID != "41" {
		t.Fatal(rows, err)
	}
}
