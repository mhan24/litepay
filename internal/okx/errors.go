package okx

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
		return fmt.Sprintf("OKX API 返回 HTTP %d（未提供可识别的错误码）", e.HTTPStatus)
	}
	reason := map[string]string{
		"50101": "API Key 与实盘环境不匹配，请使用实盘账户的 API Key",
		"50102": "请求时间已过期，请检查服务器与 OKX 的时间同步",
		"50103": "API Key 为空，请填写 API Key",
		"50104": "API Passphrase 为空，请填写创建 API Key 时设置的口令",
		"50105": "API Passphrase 不正确，请核对创建该 API Key 时设置的口令",
		"50110": "服务器出口 IP 不在 API Key 的 IP 白名单中",
		"50111": "API Key 无效，请核对 API Key",
		"50112": "请求时间格式无效，请检查时间同步",
		"50113": "签名无效，请核对 API Secret 是否与 API Key 属于同一组",
		"50114": "API 授权无效，请检查 API 权限",
		"50119": "API Key 不存在或账户区域与 API 域名不匹配",
		"50120": "API Key 无权访问该接口，请检查读取权限",
		"50030": "API Key 无权访问该接口，请检查读取权限",
		"50121": "OKX 限制了当前服务器出口 IP 的访问",
		"50011": "请求频率超过限制，请稍后重试",
	}[code]
	if reason == "" {
		reason = "请求被 OKX 拒绝，请根据错误码核对 API 配置"
	}
	return fmt.Sprintf("OKX API 请求失败（HTTP %d，错误码 %s）：%s", e.HTTPStatus, code, reason)
}
