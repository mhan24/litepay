package okx

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestAccountErrorsRetainDocumentedCodeWithoutRawResponse(t *testing.T) {
	for _, tc := range []struct {
		code, reason string
		status       int
	}{
		{"50105", "Passphrase 不正确", 401},
		{"50110", "IP 白名单", 401},
		{"50111", "API Key 无效", 401},
		{"50113", "签名无效", 401},
		{"50102", "请求时间已过期", 401},
		{"50119", "账户区域", 200},
		{"50120", "读取权限", 200},
	} {
		t.Run(tc.code, func(t *testing.T) {
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.WriteHeader(tc.status)
				fmt.Fprintf(w, `{"code":%q,"msg":"secret-value raw-account-data","data":[]}`, tc.code)
			}))
			defer srv.Close()
			c := New("key", "secret", "phrase", srv.URL, 100)
			c.UseTestTransport()
			c.syncTime = false
			_, err := c.AccountUID(context.Background())
			var apiErr *APIError
			if !errors.As(err, &apiErr) || apiErr.Code != tc.code || apiErr.HTTPStatus != tc.status || !strings.Contains(err.Error(), tc.reason) {
				t.Fatal(err)
			}
			if strings.Contains(err.Error(), "secret-value") || strings.Contains(err.Error(), "raw-account-data") {
				t.Fatal("upstream response leaked", err)
			}
		})
	}
}

func TestHTTPErrorBodiesRemainBounded(t *testing.T) {
	for _, body := range []string{`<html>secret-value</html>`, `{"code":"secret-value","msg":"secret-value"}`, `{"code":0,"msg":"secret-value"}`, `{"code":"50105extra","msg":"secret-value"}`} {
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(401); fmt.Fprint(w, body) }))
		c := New("key", "secret", "phrase", srv.URL, 100)
		c.UseTestTransport()
		c.syncTime = false
		_, err := c.AccountUID(context.Background())
		srv.Close()
		if err == nil || !strings.Contains(err.Error(), "HTTP 401") || strings.Contains(err.Error(), "secret-value") || strings.Contains(err.Error(), "50105extra") {
			t.Fatal(err)
		}
	}
}
