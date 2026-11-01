package binance

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"
	"time"
)

func TestAuditPayHistorySplitsTimeWindowsWithoutPageParameter(t *testing.T) {
	requests := 0
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests++
		if r.URL.Query().Has("page") {
			t.Error("Pay API does not support page")
		}
		start, _ := strconv.Atoi(r.URL.Query().Get("startTime"))
		end, _ := strconv.Atoi(r.URL.Query().Get("endTime"))
		fmt.Fprint(w, `{"code":"000000","success":true,"data":[`)
		count := 0
		for _, ts := range []int{1000, 2000, 3000} {
			if ts < start || ts > end || count >= 2 {
				continue
			}
			if count > 0 {
				fmt.Fprint(w, ",")
			}
			fmt.Fprintf(w, `{"transactionId":"tx-%d","transactionTime":%d,"orderType":"PAY","amount":"1","currency":"USDT"}`, ts, ts)
			count++
		}
		fmt.Fprint(w, `]}`)
	}))
	defer srv.Close()
	cli := New("key", "secret", srv.URL, 5000, 2)
	cli.UseTestTransport()
	rows, err := cli.PayTransactions(context.Background(), time.UnixMilli(1000), time.UnixMilli(3000))
	if err != nil || len(rows) != 3 || requests < 3 {
		t.Fatalf("rows=%d requests=%d err=%v", len(rows), requests, err)
	}
}

func TestAuditNegativeFundsDetailNeverBecomesIncome(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprint(w, `{"code":"000000","success":true,"data":[{"transactionId":"tx","transactionTime":2000,"amount":"2","fundsDetail":[{"amount":"-2","currency":"USDT"}]}]}`)
	}))
	defer srv.Close()
	cli := New("key", "secret", srv.URL, 5000, 100)
	cli.UseTestTransport()
	rows, err := cli.PayTransactions(context.Background(), time.UnixMilli(1000), time.UnixMilli(3000))
	if err == nil || len(rows) != 0 {
		t.Fatalf("negative detail accepted: %+v err=%v", rows, err)
	}
}

func TestAuditPositiveFundsDetailWithMissingTopLevelAmountIsIncome(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprint(w, `{"code":"000000","success":true,"data":[
			{"transactionId":"missing-total","transactionTime":2000,"fundsDetail":[{"amount":"2.51","currency":"USDT"}]},
			{"transactionId":"zero-total","transactionTime":3000,"amount":"0","fundsDetail":[{"amount":"3.51","currency":"USDC"}]}
		]}`)
	}))
	defer srv.Close()
	cli := New("key", "secret", srv.URL, 5000, 100)
	cli.UseTestTransport()
	rows, err := cli.PayTransactions(context.Background(), time.UnixMilli(1000), time.UnixMilli(4000))
	if err != nil || len(rows) != 2 || !rows[0].Incoming || !rows[1].Incoming {
		t.Fatalf("positive detail rows = %+v err=%v", rows, err)
	}
}

func TestAuditTopLevelDirectionOverridesConflictingFundsDetail(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprint(w, `{"code":"000000","success":true,"data":[
			{"transactionId":"outgoing","transactionTime":2000,"amount":"-2.51","fundsDetail":[{"amount":"2.51","currency":"USDT"}]},
			{"transactionId":"incoming","transactionTime":3000,"amount":"2.51","fundsDetail":[{"amount":"-2.51","currency":"USDT"}]}
		]}`)
	}))
	defer srv.Close()
	cli := New("key", "secret", srv.URL, 5000, 100)
	cli.UseTestTransport()
	rows, err := cli.PayTransactions(context.Background(), time.UnixMilli(1000), time.UnixMilli(4000))
	if err == nil || len(rows) != 0 {
		t.Fatalf("conflicting detail rows = %+v err=%v", rows, err)
	}
}

func TestAuditFundsDetailMustAccountForTopLevelAmount(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprint(w, `{"code":"000000","success":true,"data":[{
			"transactionId":"incomplete","transactionTime":2000,"amount":"5.00",
			"fundsDetail":[{"amount":"2.51","currency":"USDT"}]
		}]}`)
	}))
	defer srv.Close()
	cli := New("key", "secret", srv.URL, 5000, 100)
	cli.UseTestTransport()
	_, err := cli.PayTransactions(context.Background(), time.UnixMilli(1000), time.UnixMilli(4000))
	if err == nil || !strings.Contains(err.Error(), "明细合计") {
		t.Fatalf("incomplete funds detail accepted: %v", err)
	}
}

func TestAuditRejectsMalformedOrDuplicateTransactions(t *testing.T) {
	tests := []struct {
		name string
		data string
	}{
		{name: "missing time", data: `{"transactionId":"tx","amount":"2.51","currency":"USDT"}`},
		{name: "nan amount", data: `{"transactionId":"tx","transactionTime":2000,"amount":"NaN","currency":"USDT"}`},
		{name: "infinite detail", data: `{"transactionId":"tx","transactionTime":2000,"fundsDetail":[{"amount":"Inf","currency":"USDT"}]}`},
		{name: "non-scalar id", data: `{"transactionId":{"id":"tx"},"transactionTime":2000,"amount":"2.51","currency":"USDT"}`},
		{name: "zero amount", data: `{"transactionId":"tx","transactionTime":2000,"amount":"0","currency":"USDT"}`},
		{name: "duplicate id", data: `{"transactionId":"tx","transactionTime":2000,"amount":"2.51","currency":"USDT"},{"transactionId":"tx","transactionTime":3000,"amount":"3.51","currency":"USDT"}`},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				fmt.Fprintf(w, `{"code":"000000","success":true,"data":[%s]}`, tt.data)
			}))
			defer srv.Close()
			cli := New("key", "secret", srv.URL, 5000, 100)
			cli.UseTestTransport()
			if _, err := cli.PayTransactions(context.Background(), time.UnixMilli(1000), time.UnixMilli(4000)); err == nil {
				t.Fatal("malformed transaction accepted")
			}
		})
	}
}
