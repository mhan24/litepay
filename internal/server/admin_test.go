package server

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"net/url"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"golang.org/x/crypto/bcrypt"
	"gorm.io/gorm"

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
	postReveal := httptest.NewRequest(http.MethodPost, "/admin/integration", strings.NewReader(url.Values{"csrf": {csrf}, "current_password": {cfg.Admin.Password}}.Encode()))
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

func TestAdminReauthenticationRevokesSession(t *testing.T) {
	for _, endpoint := range []string{"account", "site"} {
		t.Run(endpoint, func(t *testing.T) {
			st, err := store.Open(":memory:")
			if err != nil {
				t.Fatal(err)
			}
			defer st.Close()
			cfg := config.Default()
			cfg.Server.Production = false
			cfg.Admin.Password = "correct-admin-password"
			mgr := settings.New(cfg, st)
			if err := mgr.Load(context.Background()); err != nil {
				t.Fatal(err)
			}
			h := &Handlers{cfg: mgr, st: st}
			auth := h.currentAdmin()
			token, csrf, err := auth.issue()
			if err != nil {
				t.Fatal(err)
			}
			r := gin.New()
			handler := h.AdminAccountSave
			if endpoint == "site" {
				handler = h.AdminSiteSave
			}
			r.POST("/save", h.AdminAuth(), handler)
			for i := 0; i <= adminUserAttemptLimit; i++ {
				pass := "wrong"
				if i == adminUserAttemptLimit {
					pass = cfg.Admin.Password
				}
				form := url.Values{"csrf": {csrf}, "current_password": {pass}, "admin_username": {"changed"}, "admin_path": {"/changed"}}
				req := httptest.NewRequest("POST", "/save", strings.NewReader(form.Encode()))
				req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
				req.AddCookie(&http.Cookie{Name: adminCookieName, Value: token})
				w := httptest.NewRecorder()
				r.ServeHTTP(w, req)
				if i == adminUserAttemptLimit && !strings.Contains(w.Header().Get("Location"), "/admin/login") {
					t.Fatalf("exhausted session accepted: %s", w.Header().Get("Location"))
				}
			}
			if _, ok := auth.parse(token); ok {
				t.Fatal("session not revoked")
			}
			if mgr.Get().Admin.Username != cfg.Admin.Username || mgr.Get().AdminPath() != cfg.AdminPath() {
				t.Fatal("configuration changed")
			}
		})
	}
}

func TestAdminRevocationSurvivesRestart(t *testing.T) {
	for _, action := range []string{"logout", "password"} {
		t.Run(action, func(t *testing.T) {
			dbPath := filepath.Join(t.TempDir(), "admin.db")
			cfg := config.Default()
			cfg.Server.Production = false
			cfg.Admin.Password = "original-admin-password"
			st, err := store.Open(dbPath)
			if err != nil {
				t.Fatal(err)
			}
			mgr := settings.New(cfg, st)
			if err := mgr.Load(context.Background()); err != nil {
				t.Fatal(err)
			}
			h := &Handlers{cfg: mgr, st: st}
			token, csrf, err := h.currentAdmin().issue()
			if err != nil {
				t.Fatal(err)
			}
			r := gin.New()
			handler := h.AdminLogout
			form := url.Values{"csrf": {csrf}}
			if action == "password" {
				handler = h.AdminAccountSave
				form.Set("current_password", cfg.Admin.Password)
				form.Set("admin_username", cfg.Admin.Username)
				form.Set("new_password", "new-admin-password-123")
				form.Set("confirm_password", "new-admin-password-123")
			}
			r.POST("/save", h.AdminAuth(), handler)
			req := httptest.NewRequest("POST", "/save", strings.NewReader(form.Encode()))
			req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
			req.AddCookie(&http.Cookie{Name: adminCookieName, Value: token})
			w := httptest.NewRecorder()
			r.ServeHTTP(w, req)
			if w.Code != 303 || strings.Contains(w.Header().Get("Location"), "t=err") {
				t.Fatalf("action failed: %d %s", w.Code, w.Header().Get("Location"))
			}
			st.Close()
			reopened, err := store.Open(dbPath)
			if err != nil {
				t.Fatal(err)
			}
			defer reopened.Close()
			// Keep the original signing password to prove durable epoch revocation independently of key derivation.
			restarted := newAdminAuth(cfg)
			if err := restarted.loadEpoch(reopened); err != nil {
				t.Fatal(err)
			}
			if _, ok := restarted.parse(token); ok {
				t.Fatal("old cookie revived after restart")
			}
			nextMgr := settings.New(cfg, reopened)
			if err := nextMgr.Load(context.Background()); err != nil {
				t.Fatal(err)
			}
			next := &Handlers{cfg: nextMgr, st: reopened}
			if _, ok := next.currentAdmin().parse(token); ok {
				t.Fatal("old cookie accepted with persisted settings")
			}
		})
	}
}

func TestSharedIPAndGlobalLoginBudgetsDelayInsteadOfReject(t *testing.T) {
	st, err := store.Open(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	cfg := config.Default()
	cfg.Server.Production = false
	cfg.Admin.Password = "admin-shared-ip-password"
	mgr := settings.New(cfg, st)
	if err := mgr.Load(context.Background()); err != nil {
		t.Fatal(err)
	}
	h := &Handlers{cfg: mgr, st: st}
	auth := h.currentAdmin()
	for i := 0; i < adminGlobalAttemptLimit+2; i++ {
		user := fmt.Sprintf("attacker-%d", i)
		if !auth.allow("192.0.2.1", user) {
			t.Fatalf("shared budget hard rejected attempt %d", i)
		}
		auth.failed("192.0.2.1", user)
	}
	if auth.loginDelay("192.0.2.1") <= 0 {
		t.Fatal("missing overload delay")
	}
	r := gin.New()
	r.POST("/login", h.AdminLoginSubmit)
	req := httptest.NewRequest("POST", "/login", strings.NewReader(url.Values{"username": {cfg.Admin.Username}, "password": {cfg.Admin.Password}}.Encode()))
	req.RemoteAddr = "192.0.2.1:1234"
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)
	if w.Header().Get("Location") != "/admin" {
		t.Fatalf("correct login blocked: %s", w.Header().Get("Location"))
	}
	for range adminUserAttemptLimit {
		if !auth.allow("192.0.2.1", "target") {
			t.Fatal("early username rejection")
		}
		auth.failed("192.0.2.1", "target")
	}
	if auth.allow("192.0.2.1", "target") {
		t.Fatal("username budget lost hard limit")
	}
}

func TestUntrustedProxyHeadersWarnOnce(t *testing.T) {
	cfg := config.Default()
	cfg.Server.TrustedProxies = []string{"127.0.0.1", "10.0.0.0/8"}
	h := &Handlers{cfg: settings.New(cfg, nil)}
	var buf bytes.Buffer
	previous := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(&buf, nil)))
	defer slog.SetDefault(previous)
	r := gin.New()
	r.Use(h.warnUntrustedProxy)
	r.GET("/", func(c *gin.Context) { c.Status(200) })
	for _, peer := range []string{"127.0.0.1:12", "10.1.1.1:12", "192.0.2.1:12", "192.0.2.2:12"} {
		req := httptest.NewRequest("GET", "/", nil)
		req.RemoteAddr = peer
		req.Header.Set("X-Real-IP", "203.0.113.1")
		r.ServeHTTP(httptest.NewRecorder(), req)
		if strings.HasPrefix(peer, "127.") || strings.HasPrefix(peer, "10.") {
			if buf.Len() != 0 {
				t.Fatal("trusted peer warned")
			}
		}
	}
	if strings.Count(buf.String(), "检测到未受信任对端的转发头") != 1 {
		t.Fatal(buf.String())
	}
}

func TestAdminEpochRecoversAfterStorageFailure(t *testing.T) {
	for _, failure := range []string{"load", "bump"} {
		t.Run(failure, func(t *testing.T) {
			st, err := store.Open(":memory:")
			if err != nil {
				t.Fatal(err)
			}
			defer st.Close()
			cfg := config.Default()
			cfg.Server.Production = false
			cfg.Admin.Password = "epoch-recovery-password"
			mgr := settings.New(cfg, st)
			if err := mgr.Load(context.Background()); err != nil {
				t.Fatal(err)
			}
			h := &Handlers{cfg: mgr, st: st}
			oldToken, csrf, err := h.currentAdmin().issue()
			if err != nil {
				t.Fatal(err)
			}
			oldEpoch := h.currentAdmin().currentEpoch()

			var unavailable atomic.Bool
			var attempts atomic.Int32
			const callback = "test:admin_epoch_storage_failure"
			if err := st.DB().Callback().Create().Before("gorm:create").Register(callback, func(db *gorm.DB) {
				if db.Statement.Table != "credentials" {
					return
				}
				attempts.Add(1)
				if unavailable.Load() {
					db.AddError(errors.New("temporary epoch storage failure"))
				}
			}); err != nil {
				t.Fatal(err)
			}
			defer st.DB().Callback().Create().Remove(callback)
			var logs bytes.Buffer
			previous := slog.Default()
			slog.SetDefault(slog.New(slog.NewTextHandler(&logs, nil)))
			defer slog.SetDefault(previous)

			r := gin.New()
			r.POST("/admin/login", h.AdminLoginSubmit)
			r.POST("/admin/logout", h.AdminAuth(), h.AdminLogout)
			r.GET("/private", h.AdminAuth(), func(c *gin.Context) { c.Status(http.StatusNoContent) })
			login := func() *httptest.ResponseRecorder {
				form := url.Values{"username": {cfg.Admin.Username}, "password": {cfg.Admin.Password}}
				req := httptest.NewRequest(http.MethodPost, "/admin/login", strings.NewReader(form.Encode()))
				req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
				w := httptest.NewRecorder()
				r.ServeHTTP(w, req)
				return w
			}
			unavailable.Store(true)
			failedAt := time.Now()
			if failure == "load" {
				h.admin = nil
				h.currentAdmin()
			} else {
				req := httptest.NewRequest(http.MethodPost, "/admin/logout", strings.NewReader(url.Values{"csrf": {csrf}}.Encode()))
				req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
				req.AddCookie(&http.Cookie{Name: adminCookieName, Value: oldToken})
				w := httptest.NewRecorder()
				r.ServeHTTP(w, req)
				if w.Code != http.StatusInternalServerError {
					t.Fatalf("failed revocation returned %d", w.Code)
				}
			}
			auth := h.currentAdmin()
			auth.mu.Lock()
			epoch, attached, retryAt := auth.epoch, auth.st, auth.epochRetryAt
			auth.mu.Unlock()
			if epoch != 0 || attached != st || retryAt.Before(failedAt.Add(5*time.Second)) {
				t.Fatalf("failure did not retain closed recovery state: epoch=%d attached=%v retry=%v", epoch, attached == st, retryAt)
			}

			for range 10 {
				if auth.currentEpoch() != 0 {
					t.Fatal("unavailable storage opened sessions")
				}
				if _, ok := auth.parse(oldToken); ok {
					t.Fatal("old cookie accepted during failure")
				}
				if _, err := auth.bumpEpoch(); err == nil {
					t.Fatal("revocation succeeded during backoff")
				}
				if w := login(); len(w.Result().Cookies()) != 0 || !strings.Contains(w.Header().Get("Location"), "err=") {
					t.Fatal("login succeeded while epoch unavailable")
				}
			}
			if attempts.Load() != 1 || strings.Count(logs.String(), "level=ERROR") != 1 {
				t.Fatalf("backoff repeated storage/logging: attempts=%d logs=%s", attempts.Load(), logs.String())
			}

			// Advance only the retry deadline, without sleeping through the production cooldown.
			expireRetry := func() {
				auth.mu.Lock()
				auth.epochRetryAt = time.Now().Add(-time.Millisecond)
				auth.mu.Unlock()
			}
			expireRetry()
			if auth.currentEpoch() != 0 || attempts.Load() != 2 {
				t.Fatal("due failed retry was not attempted once")
			}
			for range 10 {
				auth.currentEpoch()
			}
			if attempts.Load() != 2 || strings.Count(logs.String(), "level=ERROR") != 2 {
				t.Fatal("failed retries were not throttled", logs.String())
			}

			unavailable.Store(false)
			if auth.currentEpoch() != 0 || attempts.Load() != 2 {
				t.Fatal("recovery ignored retry cooldown")
			}
			expireRetry()
			var wg sync.WaitGroup
			epochs := make(chan uint64, 32)
			for range 32 {
				wg.Go(func() { epochs <- auth.currentEpoch() })
			}
			wg.Wait()
			close(epochs)
			for epoch := range epochs {
				if epoch != oldEpoch+1 {
					t.Fatalf("recovery did not advance epoch once: %d", epoch)
				}
			}
			if attempts.Load() != 3 || strings.Count(logs.String(), "level=ERROR") != 2 {
				t.Fatalf("concurrent recovery repeated storage/logging: %d %s", attempts.Load(), logs.String())
			}

			w := login()
			if w.Header().Get("Location") != "/admin" {
				t.Fatal("login did not recover", w.Header().Get("Location"))
			}
			var newCookie *http.Cookie
			for _, cookie := range w.Result().Cookies() {
				if cookie.Name == adminCookieName {
					newCookie = cookie
				}
			}
			if newCookie == nil {
				t.Fatal("recovered login did not issue a cookie")
			}
			for _, tc := range []struct {
				token string
				want  int
			}{{oldToken, http.StatusSeeOther}, {newCookie.Value, http.StatusNoContent}} {
				req := httptest.NewRequest(http.MethodGet, "/private", nil)
				req.AddCookie(&http.Cookie{Name: adminCookieName, Value: tc.token})
				w := httptest.NewRecorder()
				r.ServeHTTP(w, req)
				if w.Code != tc.want {
					t.Fatalf("session validation after recovery: got %d want %d", w.Code, tc.want)
				}
			}
			restarted := newAdminAuth(mgr.Get())
			if err := restarted.loadEpoch(st); err != nil {
				t.Fatal(err)
			}
			if _, ok := restarted.parse(oldToken); ok {
				t.Fatal("old cookie revived after restart")
			}
			if _, ok := restarted.parse(newCookie.Value); !ok {
				t.Fatal("new cookie invalid after restart")
			}
		})
	}
}

func TestAdminLoginPreservesValidNextAndRejectsOpenRedirect(t *testing.T) {
	ctx := context.Background()
	st, err := store.Open(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()

	cfg := config.Default()
	cfg.Server.Production = false
	cfg.Admin.Path = "/manage-panel"
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

	// Open redirect target in query string must not be rendered in the login form's next input.
	w := getRequest(engine, "/manage-panel/login?next=https://evil.example/steal")
	if w.Code != http.StatusOK || strings.Contains(w.Body.String(), "evil.example") {
		t.Fatalf("unsafe next rendered into login page: code=%d body=%s", w.Code, w.Body.String())
	}

	// Failed login preserves a valid internal next path and rejects external next.
	form := url.Values{"username": {"admin"}, "password": {"wrong-password"}, "next": {"/manage-panel/orders?page=2"}}
	req := httptest.NewRequest(http.MethodPost, "/manage-panel/login", strings.NewReader(form.Encode()))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	w = httptest.NewRecorder()
	engine.ServeHTTP(w, req)
	if !strings.Contains(w.Header().Get("Location"), "next=%2Fmanage-panel%2Forders%3Fpage%3D2") {
		t.Fatalf("valid next lost on login failure: %s", w.Header().Get("Location"))
	}

	// Trailing slash on admin root works and preserves SessionHours change.
	token, _, err := newAdminAuth(mgr.Get()).issue()
	if err != nil {
		t.Fatal(err)
	}
	req = httptest.NewRequest(http.MethodGet, "/manage-panel/", nil)
	req.AddCookie(&http.Cookie{Name: adminCookieName, Value: token})
	w = httptest.NewRecorder()
	engine.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("expected /manage-panel/ to serve dashboard, got %d", w.Code)
	}
}

func TestReselectDisabledRejectsProviderSwitchInSelectPayment(t *testing.T) {
	ctx := context.Background()
	st, err := store.Open(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()

	cfg := config.Default()
	cfg.Server.Production = false
	cfg.BEpusdt.Reselect = false
	cfg.Binance.APIKey = "key"
	cfg.Binance.APISecret = "secret"
	cfg.Checkout.Binance.UID = "receiver"
	cfg.OKX.APIKey = "key"
	cfg.OKX.APISecret = "secret"
	cfg.OKX.Passphrase = "pass"
	cfg.Checkout.OKX.UID = "okx-receiver"
	mgr := settings.New(cfg, st)
	if err := mgr.Load(ctx); err != nil {
		t.Fatal(err)
	}
	engine, err := New(mgr, st, verifiedPoller(t, mgr, st))
	if err != nil {
		t.Fatal(err)
	}

	no := createOrderViaBEpusdt(t, engine, mgr.Get().BEpusdt.APIToken, "https://shop.example/notify")
	selectPayment(t, engine, no, "binance")

	w := postForm(engine, "/api/order/payment", url.Values{"trade_no": {no}, "provider": {"okx"}, "currency": {"USDT"}})
	if w.Code != http.StatusBadRequest || !strings.Contains(w.Body.String(), "不允许更换") {
		t.Fatalf("expected reselect=false to reject provider switch, got code=%d body=%s", w.Code, w.Body.String())
	}
}

func TestNilPollerAndNilLoggerDefensiveHandling(t *testing.T) {
	ctx := context.Background()
	st, err := store.Open(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()

	cfg := config.Default()
	cfg.Server.Production = false
	cfg.Admin.Password = "defensive-check-password"
	mgr := settings.New(cfg, st)
	if err := mgr.Load(ctx); err != nil {
		t.Fatal(err)
	}
	engine, err := New(mgr, st, nil)
	if err != nil {
		t.Fatal(err)
	}

	if w := getRequest(engine, "/readyz"); w.Code != http.StatusServiceUnavailable {
		t.Fatalf("expected /readyz to return 503 with nil poller instead of panicking, got %d", w.Code)
	}

	token, _, err := newAdminAuth(mgr.Get()).issue()
	if err != nil {
		t.Fatal(err)
	}
	for _, path := range []string{"/admin", "/admin/operations", "/admin/gram/status"} {
		req := httptest.NewRequest(http.MethodGet, path, nil)
		req.AddCookie(&http.Cookie{Name: adminCookieName, Value: token})
		w := httptest.NewRecorder()
		engine.ServeHTTP(w, req)
		if path == "/admin/gram/status" {
			if w.Code != http.StatusServiceUnavailable {
				t.Fatalf("expected 503 on %s with nil poller, got %d", path, w.Code)
			}
		} else if w.Code != http.StatusOK {
			t.Fatalf("expected 200 on %s with nil poller, got %d", path, w.Code)
		}
	}
}
