package binance

import (
	"context"
	"errors"
	"fmt"
	"litepay/internal/sign"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"
	"time"
)

func TestTimestampErrorSyncsAndRetriesOnlyOnce(t *testing.T) {
	for _, always := range []bool{false, true} {
		t.Run(fmt.Sprint(always), func(t *testing.T) {
			signed, clocks := 0, 0
			serverNow := time.Now().Add(time.Minute).UnixMilli()
			api := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.URL.Path == "/api/v3/time" {
					clocks++
					fmt.Fprintf(w, `{"serverTime":%d}`, serverNow)
					return
				}
				signed++
				q := r.URL.Query()
				signature := q.Get("signature")
				q.Del("signature")
				if signature != sign.HmacSHA256Hex(q.Encode(), "secret") {
					t.Error("invalid rebuilt signature")
				}
				if signed == 1 || always {
					w.WriteHeader(400)
					fmt.Fprint(w, `{"code":-1021,"msg":"private-account-secret"}`)
					return
				}
				ts, _ := strconv.ParseInt(q.Get("timestamp"), 10, 64)
				if ts < serverNow-1000 || ts > serverNow+1000 {
					t.Errorf("clock not corrected: %d vs %d", ts, serverNow)
				}
				fmt.Fprint(w, `{"uid":"uid"}`)
			}))
			defer api.Close()
			cli := New("key", "secret", api.URL, 5000, 100)
			cli.UseTestTransport()
			uid, err := cli.AccountUID(context.Background())
			if clocks != 1 || signed != 2 {
				t.Fatalf("unbounded retry: clock=%d signed=%d", clocks, signed)
			}
			if always {
				var e *APIError
				if !errors.As(err, &e) || e.Kind != ErrorTimestamp {
					t.Fatal(err)
				}
			} else if err != nil || uid != "uid" {
				t.Fatalf("%s %v", uid, err)
			}
		})
	}
}

func TestErrorClassificationRedactionAndRateBackoff(t *testing.T) {
	for _, tc := range []struct {
		status int
		body   string
		kind   ErrorKind
	}{{401, `{"code":-2015,"msg":"SECRET"}`, ErrorAuth}, {429, `{"code":-1003,"msg":"SECRET"}`, ErrorRate}, {503, `{"msg":"SECRET"}`, ErrorExchange}, {400, `{"code":-1100,"msg":"SECRET"}`, ErrorBusiness}, {200, `{"code":"000000","success":true,"data":`, ErrorParse}, {200, `{"code":"000000","success":true,"data":null}`, ErrorParse}} {
		t.Run(string(tc.kind)+strconv.Itoa(tc.status), func(t *testing.T) {
			calls := 0
			api := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				calls++
				w.WriteHeader(tc.status)
				fmt.Fprint(w, tc.body)
			}))
			defer api.Close()
			cli := New("key", "SECRET", api.URL, 5000, 100)
			cli.UseTestTransport()
			_, err := cli.PayTransactions(context.Background(), time.Now().Add(-time.Minute), time.Now())
			var e *APIError
			if !errors.As(err, &e) || e.Kind != tc.kind {
				t.Fatalf("%T %v", err, err)
			}
			if strings.Contains(err.Error(), "SECRET") || strings.Contains(err.Error(), api.URL) {
				t.Fatal("sensitive error leaked")
			}
			if tc.kind == ErrorRate {
				cli.AccountUID(context.Background())
				if calls != 1 {
					t.Fatal("rate limit bypassed by account/inquiry request")
				}
			}
		})
	}
}

func TestSharedRequestBudgetAndDeadline(t *testing.T) {
	calls := 0
	api := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls++
		if r.URL.Path == "/api/v3/account" {
			fmt.Fprint(w, `{"uid":"uid"}`)
		} else {
			fmt.Fprint(w, `{"code":"000000","success":true,"data":[]}`)
		}
	}))
	defer api.Close()
	cli := New("key", "secret", api.URL, 5000, 100)
	cli.UseTestTransport()
	ctx := WithRequestBudget(context.Background(), 1)
	if _, err := cli.AccountUID(ctx); err != nil {
		t.Fatal(err)
	}
	_, err := cli.PayTransactions(ctx, time.Now().Add(-time.Minute), time.Now())
	var e *APIError
	if !errors.As(err, &e) || e.Kind != ErrorBudget || calls != 1 {
		t.Fatalf("budget bypass: calls=%d %v", calls, err)
	}
	cli.nextRequest = time.Now().Add(time.Hour)
	deadline, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()
	_, err = cli.AccountUID(deadline)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("deadline lost: %v", err)
	}
}

func TestNetworkInterruptionAndTimeoutAreDistinctAndRedacted(t *testing.T) {
	closed := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {}))
	address := closed.URL
	closed.Close()
	cli := New("key", "secret", address, 5000, 100)
	cli.UseTestTransport()
	_, err := cli.AccountUID(context.Background())
	var apiErr *APIError
	if !errors.As(err, &apiErr) || apiErr.Kind != ErrorNetwork || strings.Contains(err.Error(), address) {
		t.Fatalf("network category/redaction: %v", err)
	}
	slow := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { <-r.Context().Done() }))
	defer slow.Close()
	cli = New("key", "secret", slow.URL, 5000, 100)
	cli.UseTestTransport()
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Millisecond)
	defer cancel()
	_, err = cli.AccountUID(ctx)
	if !errors.As(err, &apiErr) || apiErr.Kind != ErrorTimeout || !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("timeout category: %v", err)
	}
}
