package okx

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"litepay/internal/sign"
)

func TestDepositHistoryParsesInternalTransferFields(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.RequestURI() != "/api/v5/asset/deposit-history?ccy=USDT&limit=100&type=3" {
			t.Errorf("request = %q", r.URL.RequestURI())
		}
		_, _ = w.Write([]byte(`{"code":"0","msg":"","data":[{
			"actualDepBlkConfirm":"0",
			"amt":"2.51",
			"ccy":"USDT",
			"chain":"USDT",
			"depId":"dep-1",
			"from":"buyer@example.com",
			"fromWdId":"wd-1",
			"state":"2",
			"to":"",
			"ts":"1674038705000",
			"txId":"tx-1",
			"type":"3"
		}]}`))
	}))
	defer server.Close()

	client := New("key", "secret", "pass", server.URL, 100)
	client.UseTestTransport()
	client.syncTime = false
	rows, err := client.DepositHistorySince(context.Background(), "USDT", time.UnixMilli(0))
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 1 {
		t.Fatalf("rows = %d", len(rows))
	}
	got := rows[0]
	if got.DepositID != "dep-1" || got.FromWithdrawID != "wd-1" ||
		got.TransactionID != "tx-1" || got.Amount != 2.51 ||
		got.Currency != "USDT" || got.State != "2" || got.Type != "3" ||
		got.From != "buyer@example.com" {
		t.Fatalf("unexpected deposit: %+v", got)
	}
	if got.Timestamp.UnixMilli() != 1674038705000 {
		t.Fatalf("timestamp = %d", got.Timestamp.UnixMilli())
	}
	var raw map[string]any
	if err := json.Unmarshal([]byte(got.Raw), &raw); err != nil || raw["fromWdId"] != "wd-1" {
		t.Fatalf("raw deposit invalid: %s", got.Raw)
	}
}

func TestDepositHistoryInfersInternalTransferFromWithdrawalID(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(`{"code":"0","msg":"","data":[{
			"amt":"2.51","ccy":"USDT","depId":"dep-2","fromWdId":"wd-2",
			"state":"2","ts":"1674038705000"
		}]}`))
	}))
	defer server.Close()

	client := New("key", "secret", "pass", server.URL, 100)
	client.UseTestTransport()
	client.syncTime = false
	rows, err := client.DepositHistorySince(context.Background(), "USDT", time.UnixMilli(0))
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 1 || rows[0].FromWithdrawID != "wd-2" || rows[0].Type != "3" {
		t.Fatalf("unexpected internal transfer: %+v", rows)
	}
}

func TestDepositHistorySincePaginatesPastNewestHundred(t *testing.T) {
	const pageSize = 2
	requests := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests++
		switch r.URL.Query().Get("after") {
		case "":
			_, _ = w.Write([]byte(`{"code":"0","msg":"","data":[
				{"depId":"d5","fromWdId":"w5","ccy":"USDT","amt":"5","state":"2","type":"3","ts":"5000"},
				{"depId":"d4","fromWdId":"w4","ccy":"USDT","amt":"4","state":"2","type":"3","ts":"4000"}
			]}`))
		case "4001":
			_, _ = w.Write([]byte(`{"code":"0","msg":"","data":[
				{"depId":"d3","fromWdId":"w3","ccy":"USDT","amt":"3","state":"2","type":"3","ts":"3000"},
				{"depId":"d2","fromWdId":"w2","ccy":"USDT","amt":"2","state":"2","type":"3","ts":"2000"}
			]}`))
		default:
			t.Fatalf("unexpected after cursor %q", r.URL.Query().Get("after"))
		}
	}))
	defer server.Close()

	client := New("key", "secret", "pass", server.URL, pageSize)
	client.UseTestTransport()
	client.syncTime = false
	rows, err := client.DepositHistorySince(context.Background(), "USDT", time.UnixMilli(2500))
	if err != nil {
		t.Fatal(err)
	}
	if requests != 2 {
		t.Fatalf("requests = %d, want 2", requests)
	}
	if len(rows) != 3 || rows[0].DepositID != "d5" || rows[1].DepositID != "d4" ||
		rows[2].DepositID != "d3" {
		t.Fatalf("unexpected rows after filtering since: %+v", rows)
	}
}

func TestAccountUIDSigningAndParsing(t *testing.T) {
	const secret = "secret"
	const apiKey = "api-key"
	const passphrase = "pass"
	fixed := time.Date(2026, 10, 18, 1, 2, 3, 456_000_000, time.UTC)

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		wantPath := "/api/v5/account/config"
		if r.URL.RequestURI() != wantPath {
			t.Errorf("request path = %q, want %q", r.URL.RequestURI(), wantPath)
		}
		if got := r.Header.Get("OK-ACCESS-KEY"); got != apiKey {
			t.Errorf("API key = %q", got)
		}
		if got := r.Header.Get("OK-ACCESS-PASSPHRASE"); got != passphrase {
			t.Errorf("passphrase = %q", got)
		}
		ts := r.Header.Get("OK-ACCESS-TIMESTAMP")
		if ts != "2026-10-18T01:02:03.456Z" {
			t.Errorf("timestamp = %q", ts)
		}
		wantSign := sign.HmacSHA256Base64(ts+"GET"+wantPath, secret)
		if got := r.Header.Get("OK-ACCESS-SIGN"); got != wantSign {
			t.Errorf("signature = %q, want %q", got, wantSign)
		}
		_, _ = w.Write([]byte(`{"code":"0","msg":"","data":[{"uid":"123456789","mainUid":"123456789"}]}`))
	}))
	defer server.Close()

	client := New(apiKey, secret, passphrase, server.URL, 100)
	client.UseTestTransport()
	client.now = func() time.Time { return fixed }
	client.syncTime = false
	uid, err := client.AccountUID(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if uid != "123456789" {
		t.Fatalf("uid = %q", uid)
	}
}

func TestOKXAPIError(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(`{"code":"50102","msg":"Timestamp request expired","data":[]}`))
	}))
	defer server.Close()

	client := New("key", "secret", "pass", server.URL, 100)
	client.UseTestTransport()
	client.syncTime = false
	if _, err := client.DepositHistorySince(context.Background(), "USDT", time.UnixMilli(0)); err == nil {
		t.Fatal("expected API error")
	}
}

func TestOKXServerTimeSync(t *testing.T) {
	fixed := time.Date(2026, 10, 18, 1, 2, 3, 456_000_000, time.UTC)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/api/v5/public/time":
			_, _ = w.Write([]byte(`{"code":"0","msg":"","data":[{"ts":"1760758923456"}]}`))
		case "/api/v5/asset/deposit-history":
			ts := r.Header.Get("OK-ACCESS-TIMESTAMP")
			if ts != "2025-10-18T03:42:03.456Z" {
				t.Errorf("timestamp = %q", ts)
			}
			wantPath := "/api/v5/asset/deposit-history?ccy=USDT&limit=100&type=3"
			wantSign := sign.HmacSHA256Base64(ts+"GET"+wantPath, "secret")
			if got := r.Header.Get("OK-ACCESS-SIGN"); got != wantSign {
				t.Errorf("signature mismatch")
			}
			_, _ = w.Write([]byte(`{"code":"0","msg":"","data":[]}`))
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()

	client := New("key", "secret", "pass", server.URL, 100)
	client.UseTestTransport()
	client.now = func() time.Time { return fixed }
	if _, err := client.DepositHistorySince(context.Background(), "USDT", time.UnixMilli(0)); err != nil {
		t.Fatal(err)
	}
}

func TestDepositHistoryRejectsMissingData(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(`{"code":"0","msg":""}`))
	}))
	defer server.Close()

	client := New("key", "secret", "pass", server.URL, 100)
	client.UseTestTransport()
	client.syncTime = false
	if _, err := client.DepositHistorySince(context.Background(), "USDT", time.UnixMilli(0)); err == nil {
		t.Fatal("expected invalid response error")
	}
}
