package amount

import "testing"

func TestExactAmounts(t *testing.T) {
	for raw, want := range map[string]int64{"0": 0, "10.01": 10010000, "10.009999": 10009999, "0.000001": 1, "1.00000000": Scale} {
		got, e := Parse(raw)
		if e != nil || got != want {
			t.Fatalf("%s: %d %v", raw, got, e)
		}
	}
	for _, raw := range []string{"10.0099999", "NaN", "1e2", "-1", "1.2.3"} {
		if _, e := Parse(raw); e == nil {
			t.Fatal("accepted", raw)
		}
	}
	if String(10*Scale) != "10" || Text(10010000, 2) != "10.01" {
		t.Fatal("format")
	}
}
