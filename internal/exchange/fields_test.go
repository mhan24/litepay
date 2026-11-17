package exchange

import (
	"encoding/json"
	"testing"
)

func TestTextUnmarshalAndParsing(t *testing.T) {
	var row struct {
		A Text `json:"a"`
		B Text `json:"b"`
		C Text `json:"c"`
	}
	if err := json.Unmarshal([]byte(`{"a":" 10.5 ","b":123456,"c":null}`), &row); err != nil {
		t.Fatal(err)
	}
	if n, err := row.A.SignedUnits(); err != nil || n != 10_500_000 {
		t.Fatalf("A SignedUnits = %d, %v", n, err)
	}
	if ms, err := row.B.Millis(); err != nil || ms != 123456 {
		t.Fatalf("B Millis = %d, %v", ms, err)
	}
	if row.C != "" {
		t.Fatalf("C = %q, want empty", row.C)
	}
	if n, err := Text(" -2.5 ").SignedUnits(); err != nil || n != -2_500_000 {
		t.Fatalf("negative SignedUnits = %d, %v", n, err)
	}
	for _, bad := range []Text{"- 1", "--1", "-", "+1", "1.0000001"} {
		if _, err := bad.SignedUnits(); err == nil {
			t.Fatalf("accepted invalid SignedUnits %q", bad)
		}
	}
	for _, bad := range []Text{"0", "-100", "abc", ""} {
		if _, err := bad.Millis(); err == nil {
			t.Fatalf("accepted invalid Millis %q", bad)
		}
	}
}
