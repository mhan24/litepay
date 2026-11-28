// Package exchange contains shared exchange API decoding and errors.
package exchange

import (
	"encoding/json"
	"errors"
	"strconv"
	"strings"

	"litepay/internal/amount"
)

// ErrHistoryLimit means only a shorter time window can prove complete coverage.
var ErrHistoryLimit = errors.New("history pagination limit reached")

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
	n, e := strconv.ParseInt(strings.TrimSpace(string(t)), 10, 64)
	if e != nil || n <= 0 {
		return 0, errors.New("invalid exchange timestamp")
	}
	return n, nil
}
func (t Text) SignedUnits() (int64, error) {
	s := strings.TrimSpace(string(t))
	negative := strings.HasPrefix(s, "-")
	if negative {
		s = strings.TrimPrefix(s, "-")
		if s == "" || s[0] < '0' || s[0] > '9' {
			return 0, errors.New("金额格式无效")
		}
	}
	n, e := amount.Parse(s)
	if negative {
		n = -n
	}
	return n, e
}
