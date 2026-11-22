package bitget

import "fmt"

// APIError exposes documented codes, never raw responses or credentials.
type APIError struct {
	HTTPStatus int
	Code       string
}

func boundedCode(raw string) string {
	if len(raw) != 5 {
		return ""
	}
	for _, r := range raw {
		if r < '0' || r > '9' {
			return ""
		}
	}
	return raw
}

func (e *APIError) Error() string {
	code := boundedCode(e.Code)
	if code == "" {
		return fmt.Sprintf("Bitget API 返回 HTTP %d（未提供可识别的错误码）", e.HTTPStatus)
	}
	reason := map[string]string{
		"40001": "缺少 ACCESS-KEY，请填写 API Key",
		"40002": "缺少 ACCESS-SIGN，签名生成失败",
		"40003": "缺少 ACCESS-TIMESTAMP，请检查时间配置",
		"40004": "请求时间格式无效，请检查服务器时钟",
		"40005": "请求时间已过期，请检查服务器与 Bitget 的时间同步",
		"40006": "API Key 无效，请核对 API Key",
		"40007": "请求参数或 Content-Type 无效",
		"40008": "请求时间超出允许窗口，请检查服务器时间同步",
		"40009": "签名无效，请核对 API Secret 与 Passphrase 是否与 API Key 匹配",
		"40014": "API Key 无权访问该接口，请检查只读权限配置",
		"40015": "系统暂时不可用，请稍后重试",
		"40017": "参数校验失败，请检查查询配置",
		"40037": "API Key 已过期或不存在，请重新生成",
		"40099": "服务器出口 IP 不在 API Key 的 IP 白名单中",
		"42999": "请求频率超过限制，请稍后重试",
	}[code]
	if reason == "" {
		reason = "请求被 Bitget 拒绝，请根据错误码核对 API 配置"
	}
	return fmt.Sprintf("Bitget API 请求失败（HTTP %d，错误码 %s）：%s", e.HTTPStatus, code, reason)
}
