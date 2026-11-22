package bybit

import (
	"strings"
	"testing"
)

func TestAPIErrorFormatting(t *testing.T) {
	err := (&APIError{HTTPStatus: 200, Code: "10004"}).Error()
	if !strings.Contains(err, "10004") || !strings.Contains(err, "签名无效") {
		t.Fatalf("unexpected error: %s", err)
	}
	unknown := (&APIError{HTTPStatus: 500, Code: "not-a-code"}).Error()
	if !strings.Contains(unknown, "HTTP 500") || strings.Contains(unknown, "not-a-code") {
		t.Fatalf("unexpected error for unbounded code: %s", unknown)
	}
}
