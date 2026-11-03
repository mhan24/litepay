package binance

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"

	"litepay/internal/sign"
)

func TestPayTransactionsSignsRequestAndParsesDirections(t *testing.T) {
	const apiKey = "test-api-key"
	const apiSecret = "test-api-secret"
	start := time.UnixMilli(1_760_000_000_000)
	end := start.Add(10 * time.Minute)

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/sapi/v1/pay/transactions" {
			t.Errorf("path = %q", r.URL.Path)
		}
		if got := r.Header.Get("X-MBX-APIKEY"); got != apiKey {
			t.Errorf("API key = %q", got)
		}
		query := r.URL.Query()
		signature := query.Get("signature")
		query.Del("signature")
		if want := sign.HmacSHA256Hex(query.Encode(), apiSecret); signature != want {
			t.Errorf("signature = %q, want %q", signature, want)
		}
		if query.Get("startTime") != "1760000000000" || query.Get("endTime") != "1760000600000" {
			t.Errorf("time range = %s..%s", query.Get("startTime"), query.Get("endTime"))
		}
		_, _ = w.Write([]byte(`{
			"code":"000000",
			"message":"success",
			"success":true,
			"data":[
				{"orderType":"PAY","transactionId":123,"transactionTime":1760000000000,
				 "amount":"2.51","currency":"usdt","receiverInfo":{"binanceId":987654321}},
				{"orderType":"PAYOUT","transactionId":"tx-2","transactionTime":"1760000001000",
				 "amount":-1.23,"currency":"USDC","receiverInfo":{"binanceId":"111111111"}}
			]
		}`))
	}))
	defer server.Close()

	client := New(apiKey, apiSecret, server.URL, 5000, 100)
	client.UseTestTransport()
	rows, err := client.PayTransactions(context.Background(), start, end)
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 2 {
		t.Fatalf("rows = %d, want 2", len(rows))
	}
	if rows[0].TransactionID != "123" || rows[0].Amount != 2.51 ||
		!rows[0].Incoming || rows[0].Currency != "USDT" ||
		rows[0].ReceiverID != "987654321" {
		t.Fatalf("unexpected income: %+v", rows[0])
	}
	if rows[1].Amount != 1.23 || rows[1].Incoming || rows[1].ReceiverID != "111111111" {
		t.Fatalf("unexpected outgoing record: %+v", rows[1])
	}

	for i, row := range rows {
		var raw map[string]any
		if err := json.Unmarshal([]byte(row.Raw), &raw); err != nil {
			t.Fatalf("row %d raw is invalid: %v", i, err)
		}
		if raw["transactionId"] == nil || raw["data"] != nil {
			t.Fatalf("row %d raw must contain only its own record: %s", i, row.Raw)
		}
	}
}

func TestPayTransactionsRejectsBusinessError(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(`{"code":"100001","message":"invalid signature","success":false,"data":[]}`))
	}))
	defer server.Close()

	client := New("key", "secret", server.URL, 5000, 100)
	client.UseTestTransport()
	_, err := client.PayTransactions(context.Background(), time.Now().Add(-time.Minute), time.Now())
	if err == nil || !strings.Contains(err.Error(), "100001") {
		t.Fatalf("business error = %v", err)
	}
}

func TestPayTransactionsUsesOneEncodedQueryForRequestAndSignature(t *testing.T) {
	const secret = "secret"
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		values, err := url.ParseQuery(r.URL.RawQuery)
		if err != nil {
			t.Fatal(err)
		}
		got := values.Get("signature")
		values.Del("signature")
		if want := sign.HmacSHA256Hex(values.Encode(), secret); got != want {
			t.Errorf("signature does not cover encoded query")
		}
		_, _ = w.Write([]byte(`{"code":"000000","message":"success","success":true,"data":[]}`))
	}))
	defer server.Close()

	client := New("key", secret, server.URL, 5000, 100)
	client.UseTestTransport()
	if _, err := client.PayTransactions(context.Background(), time.UnixMilli(1), time.UnixMilli(2)); err != nil {
		t.Fatal(err)
	}
}

func TestAccountUIDUsesSignedAccountEndpoint(t *testing.T) {
	const secret = "secret"
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/api/v3/account" {
			t.Errorf("path = %q", r.URL.Path)
		}
		values, err := url.ParseQuery(r.URL.RawQuery)
		if err != nil {
			t.Fatal(err)
		}
		got := values.Get("signature")
		values.Del("signature")
		if want := sign.HmacSHA256Hex(values.Encode(), secret); got != want {
			t.Errorf("signature mismatch")
		}
		_, _ = w.Write([]byte(`{"uid":34355667,"accountType":"SPOT"}`))
	}))
	defer server.Close()

	client := New("key", secret, server.URL, 5000, 100)
	client.UseTestTransport()
	uid, err := client.AccountUID(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if uid != "34355667" {
		t.Fatalf("uid = %q", uid)
	}
}

func TestPayTransactionsSplitsFundsDetailByCurrency(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(`{"code":"000000","success":true,"data":[{
			"orderType":"PAY","transactionId":"multi","transactionTime":1760000000000,
			"receiverInfo":{"binanceId":34355667},
			"fundsDetail":[
				{"amount":"2.51","currency":"USDT"},
				{"amount":"3.25","currency":"USDC"}
			]
		}]}`))
	}))
	defer server.Close()

	client := New("key", "secret", server.URL, 5000, 100)
	client.UseTestTransport()
	rows, err := client.PayTransactions(context.Background(),
		time.UnixMilli(1_759_999_000_000), time.UnixMilli(1_760_001_000_000))
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 2 || rows[0].Currency != "USDT" || rows[1].Currency != "USDC" ||
		rows[0].Amount != 2.51 || rows[1].Amount != 3.25 ||
		rows[0].TransactionID == rows[1].TransactionID || rows[0].EventID != rows[1].EventID || rows[0].CanonicalID != rows[1].CanonicalID || rows[0].IdentityIssue != "ambiguous" || rows[1].IdentityIssue != "ambiguous" {
		t.Fatalf("funds detail rows = %+v", rows)
	}
}
