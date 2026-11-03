// Package amount uses exact integer micro-units throughout the payment domain.
package amount

import (
	"errors"
	"strconv"
	"strings"
)

const Scale int64 = 1_000_000
const Max int64 = 9_000_000_000_000_000

// Parse rejects any nonzero precision beyond micro-units. It never rounds a payment.
func Parse(raw string) (int64, error) {
	raw = strings.TrimSpace(raw)
	if len(raw) == 0 || len(raw) > 64 {
		return 0, errors.New("金额长度无效")
	}
	whole, frac, _ := strings.Cut(raw, ".")
	if whole == "" {
		return 0, errors.New("金额格式无效")
	}
	for _, r := range whole + frac {
		if r < '0' || r > '9' {
			return 0, errors.New("金额必须为非负十进制数")
		}
	}
	if strings.Contains(raw, ".") && frac == "" {
		return 0, errors.New("金额格式无效")
	}
	if len(frac) > 6 {
		if strings.Trim(frac[6:], "0") != "" {
			return 0, errors.New("金额超过六位精度，不能修约核销")
		}
		frac = frac[:6]
	}
	n, err := strconv.ParseInt(whole, 10, 64)
	if err != nil || n > Max/Scale {
		return 0, errors.New("金额超出范围")
	}
	frac += strings.Repeat("0", 6-len(frac))
	f, err := strconv.ParseInt(frac, 10, 64)
	if err != nil || n*Scale+f > Max {
		return 0, errors.New("金额超出范围")
	}
	return n*Scale + f, nil
}
func Text(units int64, precision int) string {
	if units < 0 {
		return "-" + Text(-units, precision)
	}
	if precision < 0 || precision > 6 {
		precision = 6
	}
	whole := strconv.FormatInt(units/Scale, 10)
	if precision == 0 {
		return whole
	}
	frac := strconv.FormatInt(Scale+units%Scale, 10)[1:]
	return whole + "." + frac[:precision]
}
func String(units int64) string {
	s := strings.TrimRight(strings.TrimRight(Text(units, 6), "0"), ".")
	if s == "" {
		return "0"
	}
	return s
}
func Quantum(precision int) int64 {
	q := Scale
	for range precision {
		q /= 10
	}
	return q
}
