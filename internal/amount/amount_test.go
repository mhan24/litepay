package amount

import "testing"

func TestFormatPrecision(t *testing.T) {
	cases := []struct {
		in   float64
		want string
	}{
		{10, "10.00"},
		{10.005, "10.01"},
		{10.014, "10.01"},
		{0.1 + 0.2, "0.30"},
		{9.999, "10.00"},
	}
	for _, c := range cases {
		if got := FormatPrecision(c.in, 2); got != c.want {
			t.Errorf("FormatPrecision(%v, 2) = %s, want %s", c.in, got, c.want)
		}
	}
}

func TestFormatDecimalKeepsSignificantPrecision(t *testing.T) {
	tests := []struct {
		in        float64
		precision int
		want      string
	}{
		{2.51, 4, "2.51"},
		{2.5, 4, "2.5"},
		{7, 4, "7"},
		{2.5101, 4, "2.5101"},
	}
	for _, tt := range tests {
		if got := FormatDecimal(tt.in, tt.precision); got != tt.want {
			t.Errorf("FormatDecimal(%v, %d) = %q, want %q",
				tt.in, tt.precision, got, tt.want)
		}
	}
}

func TestMatchRoundedUsesOrderAmountDecimalPlaces(t *testing.T) {
	tests := []struct {
		order  string
		actual float64
		want   bool
	}{
		{"100.00", 100.004, true},
		{"100.00", 100.005, false},
		{"99.50", 99.498, true},
		{"99.50", 99.494, false},
		{"88.5", 88.51, true},
		{"88.5", 88.49, true},
		{"88.5", 88.45, true},
		{"88.5", 88.44, false},
		{"123.456", 123.4564, true},
		{"2.5100", 2.5102, false},
		{"7", 7.4, true},
	}
	for _, tt := range tests {
		if got := MatchRounded(tt.order, tt.actual); got != tt.want {
			t.Errorf("MatchRounded(%s, %v) = %v, want %v",
				tt.order, tt.actual, got, tt.want)
		}
	}
}

func TestDecimalPlacesCountsDisplayedPrecision(t *testing.T) {
	tests := map[string]int{
		"7":         0,
		"7.0":       1,
		"7.30":      2,
		"2.5100":    4,
		"123.456":   3,
		"0.000001":  6,
		"0.0000001": 6,
	}
	for raw, want := range tests {
		if got := DecimalPlaces(raw); got != want {
			t.Errorf("DecimalPlaces(%q) = %d, want %d", raw, got, want)
		}
	}
}
