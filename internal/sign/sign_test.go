package sign

import (
	"encoding/json"
	"strings"
	"testing"
)

func TestBEpusdtSignOfficialExample(t *testing.T) {
	params := map[string]interface{}{
		"order_id": "20220201030210321", "amount": "42",
		"notify_url":   "http://example.com/notify",
		"redirect_url": "http://example.com/redirect",
	}
	const token = "epusdt_password_xasddawqe"
	const want = "1cd4b52df5587cfb1968b0c0c6e156cd"
	if got := BEpusdtSignGeneric(params, token); got != want {
		t.Fatalf("BEpusdtSignGeneric() = %s, want %s", got, want)
	}
	params["signature"] = want
	if !BEpusdtVerifyGeneric(params, token) {
		t.Fatal("BEpusdtVerifyGeneric() rejected official example")
	}
}

func TestBEpusdtGenericSignaturesPreserveJSONTypes(t *testing.T) {
	const token = "0123456789abcdef0123456789abcdef"

	withTrue := map[string]interface{}{
		"order_id": "A-1",
		"amount":   float64(2.51),
		"reselect": true,
	}
	wantTrue := BEpusdtSignGeneric(map[string]interface{}{
		"order_id": "A-1", "amount": "2.51", "reselect": "true",
	}, token)
	if got := BEpusdtSignGeneric(withTrue, token); got != wantTrue {
		t.Fatalf("generic bool sign = %s, want %s", got, wantTrue)
	}

	withFalse := map[string]interface{}{"reselect": false}
	wantFalse := BEpusdtSignGeneric(map[string]interface{}{"reselect": "false"}, token)
	if got := BEpusdtSignGeneric(withFalse, token); got != wantFalse {
		t.Fatalf("generic false sign = %s, want %s", got, wantFalse)
	}

	withNumber := map[string]interface{}{"timeout": float64(1200)}
	wantNumber := BEpusdtSignGeneric(map[string]interface{}{"timeout": "1200"}, token)
	if got := BEpusdtSignGeneric(withNumber, token); got != wantNumber {
		t.Fatalf("generic number sign = %s, want %s", got, wantNumber)
	}

	var precise map[string]interface{}
	decoder := json.NewDecoder(strings.NewReader(`{"amount":2.5100,"timeout":1200}`))
	decoder.UseNumber()
	if err := decoder.Decode(&precise); err != nil {
		t.Fatal(err)
	}
	wantPrecise := BEpusdtSignGeneric(map[string]interface{}{
		"amount": "2.5100", "timeout": "1200",
	}, token)
	if got := BEpusdtSignGeneric(precise, token); got != wantPrecise {
		t.Fatalf("json.Number sign = %s, want %s", got, wantPrecise)
	}
}

func TestHmacSHA256Hex(t *testing.T) {
	// 固定向量：HMAC-SHA256("timestamp=1&recvWindow=5000", "secret")
	got := HmacSHA256Hex("timestamp=1&recvWindow=5000", "secret")
	if len(got) != 64 {
		t.Fatalf("HMAC 十六进制长度应为 64，实际 %d", len(got))
	}
	if got != HmacSHA256Hex("timestamp=1&recvWindow=5000", "secret") {
		t.Fatal("HMAC 结果不稳定")
	}
	if got == HmacSHA256Hex("timestamp=1&recvWindow=5000", "secret2") {
		t.Fatal("不同密钥应产生不同签名")
	}
}
