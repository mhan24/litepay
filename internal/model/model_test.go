package model

import (
	"testing"
	"time"

	"litepay/internal/amount"
)

func TestOrderAndAttemptAmountFallbackAndPaymentPhase(t *testing.T) {
	if got := (Order{PayUnits: 0, Precision: 0}).PayAmount(); got != "0" {
		t.Fatalf("expected 0 for zero units, got %q", got)
	}
	if got := (Order{PayUnits: 10*amount.Scale + 10000, Precision: 2}).PayAmount(); got != "10.01" {
		t.Fatalf("expected 10.01 for precision 2, got %q", got)
	}
	if got := (Order{PayUnits: 10*amount.Scale + 1, Precision: 0}).PayAmount(); got != "10.000001" {
		t.Fatalf("expected full decimal fallback when precision is unset, got %q", got)
	}
	if got := (PaymentAttempt{Units: 10*amount.Scale + 1, Precision: 0}).Amount(); got != "10.000001" {
		t.Fatalf("expected full decimal fallback on PaymentAttempt, got %q", got)
	}

	now := time.Now()
	future := now.Add(time.Minute)
	past := now.Add(-time.Minute)
	cases := []struct {
		name  string
		order Order
		want  string
	}{
		{"paid", Order{Status: Paid, ExpiresAt: past}, "paid"},
		{"canceled", Order{Status: Canceled, ExpiresAt: future}, "closed"},
		{"expired", Order{Status: Expired, ExpiresAt: future}, "closed"},
		{"pending_open", Order{Status: Pending, ExpiresAt: future}, "accepting_payment"},
		{"pending_past", Order{Status: Pending, ExpiresAt: past}, "closed"},
		{"ready_open", Order{Status: Ready, ExpiresAt: future}, "accepting_payment"},
		{"ready_settling", Order{Status: Ready, ExpiresAt: past}, "settling"},
		{"unknown_status", Order{Status: "", ExpiresAt: future}, "closed"},
	}
	for _, tc := range cases {
		if got := tc.order.PaymentPhase(now); got != tc.want {
			t.Fatalf("%s: got phase %q, want %q", tc.name, got, tc.want)
		}
	}
}

func TestAmountDoesNotTruncateSubPrecisionDigits(t *testing.T) {
	if got := (Order{PayUnits: 10*amount.Scale + 5000, Precision: 2}).PayAmount(); got != "10.005" {
		t.Fatalf("Order.PayAmount truncated sub-precision digits: got %q, want 10.005", got)
	}
	if got := (Order{PayUnits: 10*amount.Scale + 1, Precision: 9}).PayAmount(); got != "10.000001" {
		t.Fatalf("Order.PayAmount failed out-of-range precision fallback: got %q, want 10.000001", got)
	}
	if got := (PaymentAttempt{Units: 10*amount.Scale + 5000, Precision: 2}).Amount(); got != "10.005" {
		t.Fatalf("PaymentAttempt.Amount truncated sub-precision digits: got %q, want 10.005", got)
	}
	if got := (PaymentAttempt{Units: 10*amount.Scale + 1, Precision: 7}).Amount(); got != "10.000001" {
		t.Fatalf("PaymentAttempt.Amount failed out-of-range precision fallback: got %q, want 10.000001", got)
	}
}
