package amount

import (
	"errors"
	"math/big"
	"strconv"
	"strings"
)

const Scale int64 = 1000000

// MaxUnits keeps integer arithmetic bounded and JSON interoperability below 2^53.
const MaxUnits int64 = 9000000000000000

// Decimal parses bounded plain decimal text without ever passing through float64.
func Decimal(raw string) (*big.Rat, error) {
	raw = strings.TrimSpace(raw)
	if len(raw) == 0 || len(raw) > 64 {
		return nil, errors.New("金额长度无效")
	}
	digits, dots := 0, 0
	for i, c := range raw {
		if c == '-' && i == 0 {
			continue
		}
		if c == '.' {
			dots++
			if dots > 1 {
				return nil, errors.New("金额格式无效")
			}
			continue
		}
		if c < '0' || c > '9' {
			return nil, errors.New("金额必须为十进制数")
		}
		digits++
	}
	if digits == 0 || strings.HasSuffix(raw, ".") {
		return nil, errors.New("金额格式无效")
	}
	r, ok := new(big.Rat).SetString(raw)
	if !ok {
		return nil, errors.New("金额格式无效")
	}
	limit := new(big.Rat).SetInt64(MaxUnits / Scale)
	if new(big.Rat).Abs(r).Cmp(limit) > 0 {
		return nil, errors.New("金额超出安全范围")
	}
	return r, nil
}

// Units rounds half away from zero at precision, then returns micro-units.
func Units(raw string, precision int) (int64, error) {
	if precision < 0 || precision > 6 {
		return 0, errors.New("金额精度无效")
	}
	r, err := Decimal(raw)
	if err != nil {
		return 0, err
	}
	neg := r.Sign() < 0
	r.Abs(r)
	factor := int64(1)
	for i := 0; i < precision; i++ {
		factor *= 10
	}
	r.Mul(r, new(big.Rat).SetInt64(factor))
	q, rem := new(big.Int), new(big.Int)
	q.QuoRem(r.Num(), r.Denom(), rem)
	if rem.Mul(rem, big.NewInt(2)).Cmp(r.Denom()) >= 0 {
		q.Add(q, big.NewInt(1))
	}
	q.Mul(q, big.NewInt(Scale/factor))
	if !q.IsInt64() || q.Int64() > MaxUnits {
		return 0, errors.New("金额溢出")
	}
	u := q.Int64()
	if neg {
		u = -u
	}
	return u, nil
}
func FloatUnits(v float64, p int) (int64, error) {
	return Units(strconv.FormatFloat(v, 'f', -1, 64), p)
}
func Text(u int64, p int) string {
	sign := ""
	if u < 0 {
		sign = "-"
		u = -u
	}
	raw := strconv.FormatInt(u/Scale, 10)
	if p == 0 {
		return sign + raw
	}
	frac := strconv.FormatInt(Scale+u%Scale, 10)[1:]
	return sign + raw + "." + frac[:p]
}
func Float(u int64) float64 { return float64(u) / float64(Scale) }
func MatchDecimal(expected, actual string) bool {
	p := DecimalPlaces(expected)
	e, err := Units(expected, p)
	if err != nil || e <= 0 {
		return false
	}
	a, err := Units(actual, p)
	return err == nil && a > 0 && e == a
}
