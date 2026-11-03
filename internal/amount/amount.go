// Package amount 提供金额精度处理工具。
//
// 说明：到账确认采用 BEpusdt 的修约匹配（四舍五入），按订单展示金额的
// 小数位修约实际到账金额后比较，同时保留 2~6 位精度支持。
package amount

import (
	"math"
	"math/big"
	"strconv"
	"strings"
)

// Round 四舍五入到指定小数位（2~6）。
func Round(v float64, precision int) float64 {
	u, err := FloatUnits(v, normalizePrecision(precision))
	if err != nil {
		return math.NaN()
	}
	return Float(u)
}

// FormatPrecision 按指定小数位格式化，不做多余尾零截断。
func FormatPrecision(v float64, precision int) string {
	precision = normalizePrecision(precision)
	return strconv.FormatFloat(Round(v, precision), 'f', precision, 64)
}

// FormatDecimal 仅用于非支付展示，不得用于支付金额匹配。保留实际需要的
// 小数位。例如 2.51 在精度 4 下仍输出 2.51，避免无意义的尾零改变修约规则。
func FormatDecimal(v float64, precision int) string {
	precision = normalizePrecision(precision)
	raw := strconv.FormatFloat(v, 'f', precision, 64)
	raw = strings.TrimRight(raw, "0")
	raw = strings.TrimRight(raw, ".")
	if raw == "" || raw == "-0" {
		return "0"
	}
	return raw
}

// MatchRounded 使用 BEpusdt 修约匹配（四舍五入）比较订单金额和实际到账金额。
// 精度由订单金额字符串的小数位决定，实际到账金额修约后再比较。全程按
// 十进制字符串处理，避免浮点容差把 100.005 错误地当成 100.00。
func MatchRounded(orderAmount string, actualAmount float64) bool {
	expected := parseAmount(orderAmount)
	if expected <= 0 || actualAmount <= 0 || math.IsNaN(expected) || math.IsNaN(actualAmount) || math.IsInf(expected, 0) || math.IsInf(actualAmount, 0) {
		return false
	}
	precision := DecimalPlaces(orderAmount)
	return roundedUnits(expected, precision) ==
		roundedUnits(actualAmount, precision)
}

// DecimalPlaces 返回金额字符串的小数位（0~6），尾随零同样计入精度。
func DecimalPlaces(raw string) int {
	raw = strings.TrimSpace(raw)
	_, frac, ok := strings.Cut(raw, ".")
	if !ok || frac == "" {
		return 0
	}
	if len(frac) > 6 {
		return 6
	}
	return len(frac)
}

func normalizePrecision(p int) int {
	if p < 2 {
		return 2
	}
	if p > 6 {
		return 6
	}
	return p
}

// roundedUnits 把金额转成“修约后最小单位的整数”，全程按十进制字符串
// 处理，不依赖二进制浮点的四舍五入行为。
func roundedUnits(v float64, precision int) string {
	raw := strconv.FormatFloat(math.Abs(v), 'f', -1, 64)
	whole, frac, _ := strings.Cut(raw, ".")
	if frac == "" {
		frac = "0"
	}
	if len(frac) <= precision {
		frac += strings.Repeat("0", precision-len(frac))
	}
	kept := frac[:precision]
	roundUp := len(frac) > precision && frac[precision] >= '5'
	n, ok := new(big.Int).SetString(whole+kept, 10)
	if !ok {
		return whole + kept
	}
	if roundUp {
		n.Add(n, big.NewInt(1))
	}
	return n.String()
}

func parseAmount(raw string) float64 {
	v, err := strconv.ParseFloat(strings.TrimSpace(raw), 64)
	if err != nil {
		return 0
	}
	return v
}
