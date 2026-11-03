// Package exchange contains only shared wire decoding, not payment policy.
package exchange

import (
	"encoding/json"
	"errors"
	"strconv"
	"strings"

	"litepay/internal/amount"
)

type Text string

func (t *Text) UnmarshalJSON(b []byte) error {
	if string(b) == "null" {
		*t = ""
		return nil
	}
	if len(b) > 0 && b[0] == '"' {
		var s string
		if err := json.Unmarshal(b, &s); err != nil {
			return err
		}
		*t = Text(strings.TrimSpace(s))
		return nil
	}
	var n json.Number
	if err := json.Unmarshal(b, &n); err != nil {
		return errors.New("invalid exchange scalar")
	}
	*t = Text(n.String())
	return nil
}
func (t Text) Millis() (int64, error) {
	n, e := strconv.ParseInt(string(t), 10, 64)
	if e != nil || n <= 0 {
		return 0, errors.New("invalid exchange timestamp")
	}
	return n, nil
}
func (t Text) SignedUnits() (int64, error) {
	s := string(t)
	negative := strings.HasPrefix(s, "-")
	n, e := amount.Parse(strings.TrimPrefix(s, "-"))
	if negative {
		n = -n
	}
	return n, e
}
