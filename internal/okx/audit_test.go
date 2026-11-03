package okx

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func TestAuditDepositPaginationKeepsAdjacentMillisecond(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Query().Get("after") {
		case "":
			fmt.Fprint(w, `{"code":"0","data":[{"depId":"a","amt":"1","ts":"5000"},{"depId":"b","amt":"1","ts":"4000"}]}`)
		case "4001":
			fmt.Fprint(w, `{"code":"0","data":[{"depId":"b","amt":"1","ts":"4000"},{"depId":"c","amt":"1","ts":"3999"}]}`)
		case "4000":
			fmt.Fprint(w, `{"code":"0","data":[{"depId":"c","amt":"1","ts":"3999"}]}`)
		default:
			t.Error("unexpected cursor")
			fmt.Fprint(w, `{"code":"0","data":[]}`)
		}
	}))
	defer srv.Close()
	cli := New("key", "secret", "pass", srv.URL, 2)
	cli.UseTestTransport()
	cli.syncTime = false
	rows, err := cli.DepositHistorySince(context.Background(), "USDT", time.UnixMilli(3000))
	if err != nil || len(rows) != 3 {
		t.Fatalf("lost boundary record: rows=%+v err=%v", rows, err)
	}
}

func TestAuditRejectsMalformedOrDuplicateDeposits(t *testing.T) {
	tests := []struct {
		name string
		data string
	}{
		{name: "missing time", data: `{"depId":"a","amt":"1","ts":"0"}`},
		{name: "nan amount", data: `{"depId":"a","amt":"NaN","ts":"4000"}`},
		{name: "non-scalar id", data: `{"depId":["a"],"amt":"1","ts":"4000"}`},
		{name: "duplicate id", data: `{"depId":"a","amt":"1","ts":"4000"},{"depId":"a","amt":"2","ts":"3000"}`},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				fmt.Fprintf(w, `{"code":"0","data":[%s]}`, tt.data)
			}))
			defer srv.Close()
			cli := New("key", "secret", "pass", srv.URL, 100)
			cli.UseTestTransport()
			cli.syncTime = false
			if _, err := cli.DepositHistorySince(context.Background(), "USDT", time.UnixMilli(1000)); err == nil {
				t.Fatal("malformed deposit accepted")
			}
		})
	}
}

func TestAuditAmountParseErrorIsReported(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprint(w, `{"code":"0","data":[{"depId":"a","amt":"not-a-number","ts":"4000"}]}`)
	}))
	defer srv.Close()
	cli := New("key", "secret", "pass", srv.URL, 100)
	cli.UseTestTransport()
	cli.syncTime = false
	_, err := cli.DepositHistorySince(context.Background(), "USDT", time.UnixMilli(1000))
	if err == nil || !strings.Contains(err.Error(), "金额") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestFullPageAtSinceBoundaryCannotSilentlyTruncate(t *testing.T) {
	calls := 0
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls++
		fmt.Fprint(w, `{"code":"0","data":[{"depId":"a","amt":"1","ts":"4000"},{"depId":"b","amt":"1","ts":"4000"}]}`)
	}))
	defer srv.Close()
	cli := New("key", "secret", "pass", srv.URL, 2)
	cli.UseTestTransport()
	cli.syncTime = false
	if _, err := cli.DepositHistorySince(context.Background(), "USDT", time.UnixMilli(4000)); err == nil {
		t.Fatal("ambiguous saturated timestamp must fail rather than report complete")
	}
	if calls < 2 {
		t.Fatal("full boundary page was not checked")
	}
}

func TestMissingDepositDataIsNotEmptyHistory(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { fmt.Fprint(w, `{"code":"0","data":null}`) }))
	defer srv.Close()
	cli := New("key", "secret", "pass", srv.URL, 100)
	cli.UseTestTransport()
	cli.syncTime = false
	if _, err := cli.DepositHistorySince(context.Background(), "USDT", time.Now().Add(-time.Minute)); err == nil {
		t.Fatal("null response treated as complete scan")
	}
}
