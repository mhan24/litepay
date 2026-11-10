package gram

import (
	"context"
	"fmt"
	"math/big"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"litepay/internal/httpx"
)

func TestQuotePrecisionAndStaleness(t *testing.T) {
	s := Snapshot{USD: "1.11", PriceAt: time.Now(), Fresh: true}
	n, err := Quote(10_000_000, s)
	if err != nil || n != 9009010 {
		t.Fatal(n, err)
	}
	limit, err := MaxQuoteUnits(10_000_000, s)
	if err != nil || limit != 9018018 {
		t.Fatal(limit, err)
	}
	// The allocated upper bound, including rounding, never exceeds 0.1%.
	value := new(big.Rat).Mul(new(big.Rat).SetInt64(limit), big.NewRat(111, 100))
	if value.Cmp(big.NewRat(10_010_000, 1)) > 0 {
		t.Fatal("excessive quote delta", value)
	}
	for _, s := range []Snapshot{{USD: "1.11", Fresh: true, PriceAt: time.Now().Add(-16 * time.Second)}, {USD: "1.11", Fresh: true, PriceAt: time.Now().Add(time.Minute)}, {USD: "0", Fresh: true, PriceAt: time.Now()}, {USD: "1.11", Fresh: true, PriceAt: time.Now(), PriceError: "failed"}} {
		if _, err := Quote(1000000, s); err == nil {
			t.Fatal("unsafe quote accepted", s)
		}
	}
}

func TestPriceFailureDisablesCachedQuoteAndRecovery(t *testing.T) {
	body := `{"error":[],"result":{"TONUSD":{"a":["1.1100000"],"b":["1.1080000"]}}}`
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { fmt.Fprint(w, body) }))
	defer srv.Close()
	m := NewMonitor()
	m.http = httpx.NewClient(time.Second, true)
	m.priceURL = srv.URL + "?pair=TONUSD"
	if err := m.CheckPrice(context.Background()); err != nil {
		t.Fatal(err)
	}
	if s := m.Snapshot(); !s.Fresh || s.USD != "1.109000000000000000" {
		t.Fatal(s)
	}
	for _, bad := range []string{`{"error":[],"result":{"TONUSD":{"a":["2"],"b":["1"]}}}`, `{"error":[],"result":{"TONUSD":{"a":["1"],"b":["2"]}}}`, `{"error":[],"result":{}}`, `{"error":["failure"]}`, `{}`} {
		body = bad
		if err := m.CheckPrice(context.Background()); err == nil {
			t.Fatal("bad ticker accepted", body)
		}
		if s := m.Snapshot(); s.Fresh || s.USD == "" {
			t.Fatal("failed ticker lost diagnostic value or enabled quote", s)
		}
	}
	body = `{"error":[],"result":{"TONUSD":{"a":["1.11"],"b":["1.11"]}}}`
	if err := m.CheckPrice(context.Background()); err != nil || !m.Snapshot().Fresh {
		t.Fatal(err)
	}
}

func TestStaleBalanceIsNotPresentedAsCurrent(t *testing.T) {
	m := NewMonitor()
	m.snapshot = Snapshot{Balance: "0.299946011", BalanceAt: time.Now(), USD: "1.11", PriceAt: time.Now()}
	if s := m.Snapshot(); !s.Fresh || !s.BalanceFresh || s.BalanceUSD != "0.33294007" {
		t.Fatal(s)
	}
	m.snapshot.BalanceAt = time.Now().Add(-2*BalanceInterval - time.Second)
	if s := m.Snapshot(); !s.Fresh || s.BalanceFresh || s.BalanceUSD != "" {
		t.Fatal("stale balance presented as current", s)
	}
}
