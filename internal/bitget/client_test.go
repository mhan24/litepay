package bitget

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
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
		case "/api/v2/spot/wallet/deposit-records":
			less := r.URL.Query().Get("idLessThan")
			if less == "" {
				_ = json.NewEncoder(w).Encode(map[string]any{
					"code": "00000",
					"data": []map[string]any{
						{"orderId": "204", "tradeId": "tr-204", "coin": "USDT", "size": "15.25", "status": "success", "dest": "internal_transfer", "cTime": "1700000004000"},
						{"orderId": "203", "tradeId": "tr-203", "coin": "USDT", "size": "99.00", "status": "success", "dest": "on_chain", "cTime": "1700000003000"},
					},
				})
				return
			}
			if less == "203" {
				_ = json.NewEncoder(w).Encode(map[string]any{
					"code": "00000",
					"data": []map[string]any{
						{"orderId": "202", "tradeId": "tr-202", "coin": "USDC", "size": "3.50", "status": "pending", "dest": "internal_transfer", "cTime": "1700000002000"},
						{"orderId": "201", "tradeId": "tr-201", "coin": "USDT", "size": "1.12345678", "status": "success", "dest": "internal_transfer", "cTime": "1700000001000"},
					},
				})
				return
			}
			if less == "201" {
				_ = json.NewEncoder(w).Encode(map[string]any{"code": "00000", "data": []map[string]any{}})
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
