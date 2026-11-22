package bitget

import (
	"strings"
	"testing"
)

func TestAPIErrorFormatting(t *testing.T) {
	err := (&APIError{HTTPStatus: 400, Code: "40009"}).Error()
	if !strings.Contains(err, "40009") || !strings.Contains(err, "签名无效") {
		t.Fatalf("unexpected error: %s", err)
	}
	unknown := (&APIError{HTTPStatus: 500, Code: "bad"}).Error()
	if !strings.Contains(unknown, "HTTP 500") || strings.Contains(unknown, "bad") {
		t.Fatalf("unexpected error for unbounded code: %s", unknown)
	}
}
