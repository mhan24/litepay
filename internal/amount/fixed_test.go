package amount

import "testing"

func TestDecimalHalfBoundaries(t *testing.T) {
	if got := FormatPrecision(1.005, 2); got != "1.01" {
		t.Fatalf("binary rounding changed decimal: %s", got)
	}
}

func TestFixedAmountBoundariesAndInputs(t *testing.T) {
	tests := []struct {
		raw  string
		p    int
		want string
	}{
		{"1.005", 2, "1.01"}, {"0.0000005", 6, "0.000001"}, {"9.9999995", 6, "10.000000"},
		{"9000000000", 6, "9000000000.000000"}, {"2.5100", 4, "2.5100"},
	}
	for _, tt := range tests {
		u, err := Units(tt.raw, tt.p)
		if err != nil || Text(u, tt.p) != tt.want {
			t.Fatalf("%s -> %d %v", tt.raw, u, err)
		}
	}
	for _, bad := range []string{"NaN", "Inf", "1e99", "1/3", "9000000001", "--1", ".", "1.2.3"} {
		if _, err := Units(bad, 6); err == nil {
			t.Errorf("accepted %q", bad)
		}
	}
	// Existing BEpusdt business rule: round the actual amount at the order precision.
	if !MatchDecimal("99.50", "99.498") || MatchDecimal("99.50", "99.494") {
		t.Fatal("unexpected rounding rule")
	}
}

func TestActualDecimalDoesNotCrossHalfBoundaryThroughFloat(t *testing.T) {
	if MatchDecimal("1.000001", "1.000000499999999999") {
		t.Fatal("underpayment rounded up through binary conversion")
	}
	if !MatchDecimal("1.000001", "1.000000500000000001") {
		t.Fatal("decimal half boundary lost")
	}
}
