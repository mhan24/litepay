package bybit

import "fmt"

// APIError exposes documented codes, never raw responses or credentials.
type APIError struct {
	HTTPStatus int
	Code       string
}

func boundedCode(raw string) string {
	if len(raw) < 4 || len(raw) > 6 {
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
		return fmt.Sprintf("Bybit API 返回 HTTP %d（未提供可识别的错误码）", e.HTTPStatus)
	}
	reason := map[string]string{
		"10001": "请求参数无效，请检查查询配置",
		"10002": "请求时间超出接收窗口，请检查服务器与 Bybit 的时间同步",
		"10003": "API Key 无效，请核对 API Key 是否与实盘环境匹配",
		"10004": "签名无效，请核对 API Secret 是否与 API Key 属于同一组",
		"10005": "API Key 权限不足，请检查只读资产/账户查询权限",
		"10006": "请求频率超过限制，请稍后重试",
		"10007": "API 认证失败，请核对 API 凭据",
		"10008": "账号已被限制访问，请检查 Bybit 账户状态",
		"10009": "服务器出口 IP 不在 API Key 的 IP 白名单中",
		"10010": "服务器出口 IP 未绑定或不在 API Key 的 IP 白名单中",
		"10016": "Bybit 服务暂时不可用，请稍后重试",
		"10024": "当前账户受区域合规限制，无法访问该接口",
		"33004": "API Key 已过期，请在 Bybit 重新生成",
	}[code]
	if reason == "" {
		reason = "请求被 Bybit 拒绝，请根据错误码核对 API 配置"
	}
	return fmt.Sprintf("Bybit API 请求失败（HTTP %d，错误码 %s）：%s", e.HTTPStatus, code, reason)
}
