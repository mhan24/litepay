// Package sign 提供 BEpusdt 与交易所 API 签名工具。
package sign

import (
	"crypto/hmac"
	"crypto/md5"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"sort"
	"strconv"
	"strings"
)

// BEpusdtSignGeneric 按 JSON 原始值签名。官方 BEpusdt 对布尔值使用
// true / false 小写字符串，数值使用其自然 JSON 表示，不能经过 fmt.Sprint
// 后再签名，否则 reselect / timeout 等可选字段会被错误改写。
func BEpusdtSignGeneric(params map[string]interface{}, token string) string {
	keys := make([]string, 0, len(params))
	for k, v := range params {
		if k == "signature" || v == nil {
			continue
		}
		text := genericValue(v)
		if text != "" {
			keys = append(keys, k)
		}
	}
	sort.Strings(keys)
	var b strings.Builder
	for i, k := range keys {
		if i > 0 {
			b.WriteByte('&')
		}
		b.WriteString(k)
		b.WriteByte('=')
		b.WriteString(genericValue(params[k]))
	}
	b.WriteString(token)
	sum := md5.Sum([]byte(b.String()))
	return hex.EncodeToString(sum[:])
}

func BEpusdtVerifyGeneric(params map[string]interface{}, token string) bool {
	got, _ := params["signature"].(string)
	return compareSign(got, BEpusdtSignGeneric(params, token))
}

func genericValue(v interface{}) string {
	switch value := v.(type) {
	case string:
		return value
	case bool:
		if value {
			return "true"
		}
		return "false"
	case float64:
		return strconv.FormatFloat(value, 'f', -1, 64)
	case json.Number:
		return value.String()
	case map[string]interface{}, []interface{}:
		return fmt.Sprint(value)
	default:
		return fmt.Sprint(value)
	}
}

func compareSign(got, want string) bool {
	got = strings.ToLower(strings.TrimSpace(got))
	if got == "" {
		return false
	}
	return subtle.ConstantTimeCompare([]byte(got), []byte(want)) == 1
}

// HmacSHA256Hex 计算 HMAC-SHA256 十六进制签名（币安 API 用）。
// payload 必须是实际发送的 query string（不含 signature 参数本身）。
func HmacSHA256Hex(payload, secret string) string {
	mac := hmac.New(sha256.New, []byte(secret))
	mac.Write([]byte(payload))
	return hex.EncodeToString(mac.Sum(nil))
}

// HmacSHA256Base64 计算 HMAC-SHA256 的 Base64 签名（OKX API v5 用）。
func HmacSHA256Base64(payload, secret string) string {
	mac := hmac.New(sha256.New, []byte(secret))
	mac.Write([]byte(payload))
	return base64.StdEncoding.EncodeToString(mac.Sum(nil))
}
