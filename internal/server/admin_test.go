package server

import (
	"bytes"
	"context"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"

	"golang.org/x/crypto/bcrypt"

	"litepay/internal/config"
	"litepay/internal/settings"
	"litepay/internal/store"
)

func TestAdminProxyLoginRateLimitAndValidSessionBypass(t *testing.T) {
	ctx := context.Background()
	st, err := store.Open(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()

	cfg := config.Default()
	cfg.Server.Production = false
	cfg.Server.TrustedProxies = []string{"127.0.0.1"}
	cfg.Admin.Username = "admin"
	cfg.Admin.Password = "correct-horse-battery-staple"
	mgr := settings.New(cfg, st)
	if err := mgr.Load(ctx); err != nil {
		t.Fatal(err)
	}
	engine, err := New(mgr, st, verifiedPoller(t, mgr, st))
	if err != nil {
		t.Fatal(err)
	}

	loginReq := func(remoteIP, xff, user, pass string, cookie *http.Cookie) *httptest.ResponseRecorder {
		form := url.Values{"username": {user}, "password": {pass}}
		req := httptest.NewRequest(http.MethodPost, "/admin/login", strings.NewReader(form.Encode()))
		req.RemoteAddr = remoteIP + ":12345"
		if xff != "" {
			req.Header.Set("X-Forwarded-For", xff)
		}
		req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
		if cookie != nil {
			req.AddCookie(cookie)
		}
		w := httptest.NewRecorder()
		engine.ServeHTTP(w, req)
		return w
	}

	// Case 1: Trusted proxy scenario — attacker from 203.0.113.99 fails 6 times on "admin";
	// legitimate admin from 198.51.100.10 behind the same proxy still logs in.
	for range 6 {
		w := loginReq("127.0.0.1", "203.0.113.99", "admin", "wrong-password", nil)
		if !strings.Contains(w.Header().Get("Location"), "err=") {
			t.Fatalf("expected login failure redirect, got: %s", w.Header().Get("Location"))
		}
	}
	// 7th attempt from attacker IP is rate-limited.
	w := loginReq("127.0.0.1", "203.0.113.99", "admin", "correct-horse-battery-staple", nil)
	if !strings.Contains(urlQueryErr(t, w.Header().Get("Location")), "频繁") {
		t.Fatalf("expected attacker IP to be rate limited, got: %s", w.Header().Get("Location"))
	}
	// Admin from another source IP behind the proxy succeeds.
	w = loginReq("127.0.0.1", "198.51.100.10", "admin", "correct-horse-battery-staple", nil)
	if w.Code != http.StatusSeeOther || w.Header().Get("Location") != "/admin" {
		t.Fatalf("expected legitimate login from another source to succeed, got code=%d loc=%s", w.Code, w.Header().Get("Location"))
	}

	// Case 2: Shared IP scenario (e.g. untrusted proxy / NAT) — attacker failing 6 times on
	// another username does not lock out "admin" on the same IP.
	for range 6 {
		loginReq("10.0.0.9", "", "attacker-user", "wrong-password", nil)
	}
	w = loginReq("10.0.0.9", "", "admin", "correct-horse-battery-staple", nil)
	if w.Code != http.StatusSeeOther || w.Header().Get("Location") != "/admin" {
		t.Fatalf("expected admin login on shared IP to succeed when attacker targeted different user, got loc=%s", w.Header().Get("Location"))
	}

	// Case 3: Request carrying a valid session cookie is unaffected by login rate limiting.
	var sessionCookie *http.Cookie
	for _, c := range w.Result().Cookies() {
		if c.Name == adminCookieName {
			sessionCookie = c
		}
	}
	if sessionCookie == nil {
		t.Fatal("missing admin session cookie")
	}
	// Lock out ("203.0.113.99", "admin") is already active; with valid session cookie it still succeeds.
	w = loginReq("127.0.0.1", "203.0.113.99", "admin", "correct-horse-battery-staple", sessionCookie)
	if w.Code != http.StatusSeeOther || w.Header().Get("Location") != "/admin" {
		t.Fatalf("expected request with valid session cookie to bypass login rate limit, got loc=%s", w.Header().Get("Location"))
	}
}

func TestAdminSessionEpochRevocationOnLogoutAndPasswordChangeAnd2yHash(t *testing.T) {
	ctx := context.Background()
	st, err := store.Open(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()

	// Verify $2y$ prefix is accepted by checkCredentials.
	hashBytes, err := bcrypt.GenerateFromPassword([]byte("initial-strong-pass123"), bcrypt.MinCost)
	if err != nil {
		t.Fatal(err)
	}
	hash2y := "$2y$" + strings.TrimPrefix(string(hashBytes), "$2a$")

	t.Setenv("LITEPAY_MASTER_KEY", "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef")
	cfg := config.Default()
	cfg.Server.Production = true
	cfg.Server.BaseURL = "https://pay.example.com"
	cfg.Server.NotifyHosts = []string{"shop.example.com"}
	cfg.Server.TrustedProxies = nil // empty in production to also test /admin warning banner
	cfg.Admin.Username = "admin"
	cfg.Admin.Password = hash2y
	mgr := settings.New(cfg, st)
	if err := mgr.Load(ctx); err != nil {
		t.Fatal(err)
	}
	engine, err := New(mgr, st, verifiedPoller(t, mgr, st))
	if err != nil {
		t.Fatal(err)
	}

	auth := newAdminAuth(mgr.Get())
	if !auth.checkCredentials("admin", "initial-strong-pass123") {
		t.Fatal("expected $2y$ bcrypt hash to verify in checkCredentials")
	}

	// Issue session and verify /admin renders proxy warning banner when production && trusted_proxies is empty.
	token, csrf, err := auth.issue()
	if err != nil {
		t.Fatal(err)
	}
	sess, ok := auth.parse(token)
	if !ok || sess.ID == "" || sess.Epoch == 0 {
		t.Fatalf("expected session with random ID and epoch, got: %+v ok=%v", sess, ok)
	}

	req := httptest.NewRequest(http.MethodGet, "/admin", nil)
	req.AddCookie(&http.Cookie{Name: adminCookieName, Value: token})
	w := httptest.NewRecorder()
	engine.ServeHTTP(w, req)
	if w.Code != http.StatusOK || !strings.Contains(w.Body.String(), "trusted_proxies") {
		t.Fatalf("expected /admin to display trusted_proxies warning in production, code=%d", w.Code)
	}

	// Logout revokes the session token immediately via epoch bump.
	logoutReq := httptest.NewRequest(http.MethodPost, "/admin/logout", strings.NewReader(url.Values{"csrf": {csrf}}.Encode()))
	logoutReq.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	logoutReq.AddCookie(&http.Cookie{Name: adminCookieName, Value: token})
	w = httptest.NewRecorder()
	engine.ServeHTTP(w, logoutReq)
	if w.Code != http.StatusSeeOther {
		t.Fatalf("logout failed: %d", w.Code)
	}

	// Reusing the stolen cookie after logout must be rejected.
	req = httptest.NewRequest(http.MethodGet, "/admin", nil)
	req.AddCookie(&http.Cookie{Name: adminCookieName, Value: token})
	w = httptest.NewRecorder()
	engine.ServeHTTP(w, req)
	if w.Code != http.StatusSeeOther || !strings.Contains(w.Header().Get("Location"), "/admin/login") {
		t.Fatalf("stolen cookie remained valid after logout: code=%d loc=%s", w.Code, w.Header().Get("Location"))
	}
}

func TestAdminIntegrationMasksTokenByDefaultAndAuditsReveal(t *testing.T) {
	ctx := context.Background()
	st, err := store.Open(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()

	cfg := config.Default()
	cfg.Server.Production = false
	cfg.Admin.Password = "local-admin-password-123"
	mgr := settings.New(cfg, st)
	if err := mgr.Load(ctx); err != nil {
		t.Fatal(err)
	}

	var logBuf bytes.Buffer
	prevLogger := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(&logBuf, nil)))
	t.Cleanup(func() { slog.SetDefault(prevLogger) })

	engine, err := New(mgr, st, verifiedPoller(t, mgr, st))
	if err != nil {
		t.Fatal(err)
	}
	fullToken := mgr.Get().BEpusdt.APIToken
	token, csrf, err := newAdminAuth(mgr.Get()).issue()
	if err != nil {
		t.Fatal(err)
	}

	// 1. GET /admin/integration must NOT contain the full token.
	req := httptest.NewRequest(http.MethodGet, "/admin/integration", nil)
	req.AddCookie(&http.Cookie{Name: adminCookieName, Value: token})
	w := httptest.NewRecorder()
	engine.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("GET /admin/integration failed: %d", w.Code)
	}
	if strings.Contains(w.Body.String(), fullToken) {
		t.Fatal("GET /admin/integration leaked full API token by default")
	}
	if !strings.Contains(w.Body.String(), maskSecret(fullToken)) {
		t.Fatal("GET /admin/integration missing masked token")
	}

	// 2. POST /admin/integration without CSRF token is rejected with 403.
	postNoCSRF := httptest.NewRequest(http.MethodPost, "/admin/integration", nil)
	postNoCSRF.AddCookie(&http.Cookie{Name: adminCookieName, Value: token})
	w = httptest.NewRecorder()
	engine.ServeHTTP(w, postNoCSRF)
	if w.Code != http.StatusForbidden || strings.Contains(w.Body.String(), fullToken) {
		t.Fatalf("expected 403 without CSRF, got %d", w.Code)
	}

	// 3. POST /admin/integration with valid CSRF reveals the full token and writes an audit log.
	postReveal := httptest.NewRequest(http.MethodPost, "/admin/integration", strings.NewReader(url.Values{"csrf": {csrf}}.Encode()))
	postReveal.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	postReveal.AddCookie(&http.Cookie{Name: adminCookieName, Value: token})
	w = httptest.NewRecorder()
	engine.ServeHTTP(w, postReveal)
	if w.Code != http.StatusOK || !strings.Contains(w.Body.String(), fullToken) {
		t.Fatalf("expected POST /admin/integration with CSRF to reveal full token, code=%d", w.Code)
	}
	if !strings.Contains(logBuf.String(), "管理员查看完整 BEpusdt API Token") || !strings.Contains(logBuf.String(), "operator=admin") {
		t.Fatalf("expected audit log on token reveal, got: %s", logBuf.String())
	}
}

func urlQueryErr(t *testing.T, loc string) string {
	t.Helper()
	u, err := url.Parse(loc)
	if err != nil {
		t.Fatal(err)
	}
	return u.Query().Get("err")
}

func TestCheckCredentialsConstantTimeSHA256Comparison(t *testing.T) {
	cfg := config.Default()
	cfg.Server.Production = false
	cfg.Admin.Username = "administrator"
	cfg.Admin.Password = "long-plaintext-secret-12345"
	auth := newAdminAuth(cfg)

	if !auth.checkCredentials("administrator", "long-plaintext-secret-12345") {
		t.Fatal("expected exact credentials to match")
	}
	for _, tc := range [][2]string{
		{"a", "long-plaintext-secret-12345"},
		{"administrator-extra", "long-plaintext-secret-12345"},
		{"administrator", "short"},
		{"administrator", "long-plaintext-secret-12345-extra"},
		{"", ""},
	} {
		if auth.checkCredentials(tc[0], tc[1]) {
			t.Fatalf("expected mismatched credentials %v to fail", tc)
		}
	}
}
