package bybit

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
		apiKey    = "test-bybit-key"
		apiSecret = "test-bybit-secret"
	)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/v5/market/time":
			_ = json.NewEncoder(w).Encode(map[string]any{
				"retCode": 0,
				"retMsg":  "OK",
				"time":    1700000000123,
				"result":  map[string]any{"timeSecond": "1700000000", "timeNano": "1700000000123000000"},
			})
		case "/v5/user/query-api":
			ts := r.Header.Get("X-BAPI-TIMESTAMP")
			rw := r.Header.Get("X-BAPI-RECV-WINDOW")
			wantSig := sign.HmacSHA256Hex(ts+apiKey+rw+r.URL.RawQuery, apiSecret)
			if r.Header.Get("X-BAPI-API-KEY") != apiKey || r.Header.Get("X-BAPI-SIGN") != wantSig {
				t.Errorf("invalid auth headers: key=%q sig=%q want=%q", r.Header.Get("X-BAPI-API-KEY"), r.Header.Get("X-BAPI-SIGN"), wantSig)
				w.WriteHeader(http.StatusUnauthorized)
				return
			}
			_ = json.NewEncoder(w).Encode(map[string]any{
				"retCode": 0,
				"retMsg":  "OK",
				"result":  map[string]any{"userID": 88990011, "isMaster": true, "readOnly": 1},
			})
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer srv.Close()

	cli := New(apiKey, apiSecret, srv.URL, 5000, 50)
	cli.UseTestTransport()
	uid, err := cli.AccountUID(context.Background())
	if err != nil {
		t.Fatalf("AccountUID failed: %v", err)
	}
	if uid != "88990011" {
		t.Fatalf("uid = %q, want 88990011", uid)
	}
}

func TestAccountUIDRejectsSubAccount(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/v5/market/time" {
			_ = json.NewEncoder(w).Encode(map[string]any{"retCode": 0, "time": time.Now().UnixMilli(), "result": map[string]any{}})
			return
		}
		_ = json.NewEncoder(w).Encode(map[string]any{
			"retCode": 0,
			"retMsg":  "OK",
			"result":  map[string]any{"userID": 999999, "isMaster": false},
		})
	}))
	defer srv.Close()

	cli := New("k", "s", srv.URL, 5000, 50)
	cli.UseTestTransport()
	if _, err := cli.AccountUID(context.Background()); err == nil {
		t.Fatal("expected error for sub-account API key")
	}
}

func TestHistoryPaginationAndStates(t *testing.T) {
	start := time.UnixMilli(1700000000000)
	end := time.UnixMilli(1700003600000)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/v5/market/time":
			_ = json.NewEncoder(w).Encode(map[string]any{"retCode": 0, "time": end.UnixMilli(), "result": map[string]any{}})
		case "/v5/user/query-api":
			_ = json.NewEncoder(w).Encode(map[string]any{"retCode": 0, "result": map[string]any{"userID": 123456, "isMaster": true}})
		case "/v5/asset/deposit/query-internal-record":
			cursor := r.URL.Query().Get("cursor")
			if cursor == "" {
				_ = json.NewEncoder(w).Encode(map[string]any{
					"retCode": 0,
					"result": map[string]any{
						"nextPageCursor": "page-2",
						"rows": []map[string]any{
							{"id": "101", "type": 1, "coin": "USDT", "amount": "12.34", "status": 2, "createdTime": "1700000001000", "txID": "tx-101"},
							{"id": "102", "type": 1, "coin": "USDC", "amount": "5.00", "status": 1, "createdTime": "1700000002000", "txID": "tx-102"},
							{"id": "103", "type": 1, "coin": "USDT", "amount": "9.00", "status": 3, "createdTime": "1700000003000", "txID": "tx-103"},
						},
					},
				})
				return
			}
			if cursor == "page-2" {
				_ = json.NewEncoder(w).Encode(map[string]any{
					"retCode": 0,
					"result": map[string]any{
						"nextPageCursor": "",
						"rows": []map[string]any{
							{"id": "104", "type": 1, "coin": "USDT", "amount": "1.12345678", "status": 2, "createdTime": "1700000004000", "txID": "tx-104"},
							{"id": "105", "type": 1, "coin": "USDT", "amount": "7.50", "status": 2, "createdTime": "1700000005000", "txID": "tx-105", "complianceStatus": 1},
						},
					},
				})
				return
			}
			w.WriteHeader(http.StatusBadRequest)
		}
	}))
	defer srv.Close()

	cli := New("k", "s", srv.URL, 5000, 50)
	cli.UseTestTransport()
	rows, err := cli.History(context.Background(), start, end)
	if err != nil {
		t.Fatalf("History failed: %v", err)
	}
	if len(rows) != 5 {
		t.Fatalf("got %d rows, want 5", len(rows))
	}
	if rows[0].Provider != "bybit" || rows[0].Receiver != "123456" || rows[0].EventID != "101" || rows[0].Reference != "tx-101" || rows[0].State != model.Confirmed || rows[0].Units != 12340000 {
		t.Fatalf("row[0] mismatch: %+v", rows[0])
	}
	if rows[1].State != model.Unconfirmed || rows[1].Reason != "deposit_not_completed" {
		t.Fatalf("row[1] mismatch: %+v", rows[1])
	}
	if rows[2].State != model.Rejected || rows[2].Reason != "deposit_failed" {
		t.Fatalf("row[2] mismatch: %+v", rows[2])
	}
	if rows[3].State != model.Rejected || rows[3].Reason != "invalid_amount" || rows[3].RawAmount != "1.12345678" {
		t.Fatalf("row[3] mismatch: %+v", rows[3])
	}
	if rows[4].State != model.Unconfirmed || rows[4].Reason != "compliance_pending" {
		t.Fatalf("row[4] mismatch: %+v", rows[4])
	}
}

func TestHistoryRejectsNullRowsAndCursorLoop(t *testing.T) {
	start := time.UnixMilli(1700000000000)
	end := time.UnixMilli(1700003600000)

	t.Run("null_rows", func(t *testing.T) {
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			switch r.URL.Path {
			case "/v5/market/time":
				_ = json.NewEncoder(w).Encode(map[string]any{"retCode": 0, "time": end.UnixMilli(), "result": map[string]any{}})
			case "/v5/user/query-api":
				_ = json.NewEncoder(w).Encode(map[string]any{"retCode": 0, "result": map[string]any{"userID": 123456, "isMaster": true}})
			case "/v5/asset/deposit/query-internal-record":
				_, _ = w.Write([]byte(`{"retCode":0,"retMsg":"OK","result":{"rows":null,"nextPageCursor":""}}`))
			}
		}))
		defer srv.Close()
		cli := New("k", "s", srv.URL, 5000, 50)
		cli.UseTestTransport()
		if _, err := cli.History(context.Background(), start, end); err == nil {
			t.Fatal("expected error for null rows")
		}
	})

	t.Run("cursor_loop", func(t *testing.T) {
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			switch r.URL.Path {
			case "/v5/market/time":
				_ = json.NewEncoder(w).Encode(map[string]any{"retCode": 0, "time": end.UnixMilli(), "result": map[string]any{}})
			case "/v5/user/query-api":
				_ = json.NewEncoder(w).Encode(map[string]any{"retCode": 0, "result": map[string]any{"userID": 123456, "isMaster": true}})
			case "/v5/asset/deposit/query-internal-record":
				_ = json.NewEncoder(w).Encode(map[string]any{
					"retCode": 0,
					"result": map[string]any{
						"nextPageCursor": "same-cursor",
						"rows":           []map[string]any{},
					},
				})
			}
		}))
		defer srv.Close()
		cli := New("k", "s", srv.URL, 5000, 50)
		cli.UseTestTransport()
		if _, err := cli.History(context.Background(), start, end); err == nil {
			t.Fatal("expected error for repeated cursor")
		}
	})
}
