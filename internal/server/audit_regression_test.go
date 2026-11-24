package server

import (
	"bytes"
	"context"
	"crypto/tls"
	"encoding/json"
	"errors"
	"log/slog"
	"mime/multipart"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"gorm.io/gorm"

	"litepay/internal/config"
	"litepay/internal/model"
	"litepay/internal/settings"
	"litepay/internal/sign"
	"litepay/internal/store"
)

func TestReadyFailsClosedOnIntegrationHealthDBErrorWhenCancelSyncOptional(t *testing.T) {
	ctx := context.Background()
	st, err := store.Open(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()

	cfg := config.Default()
	cfg.Server.Production = false
	cfg.Server.CancelSyncRequired = false
	cfg.Server.ReadinessToken = "test-probe-token-long"
	cfg.Binance.APIKey = "key"
	cfg.Binance.APISecret = "secret"
	cfg.Checkout.Binance.UID = "receiver"
	mgr := settings.New(cfg, st)
	if err := mgr.Load(ctx); err != nil {
		t.Fatal(err)
	}
	p := verifiedPoller(t, mgr, st)
	engine, err := New(mgr, st, p)
	if err != nil {
		t.Fatal(err)
	}
	primeScanners(t, p, mgr)
	if w := getRequest(engine, "/readyz"); w.Code != http.StatusOK {
		t.Fatalf("expected healthy /readyz before DB failure, got %d %s", w.Code, w.Body.String())
	}

	const cbName = "test:readyz_fail_integration_health"
	if err := st.DB().Callback().Query().Before("gorm:query").Register(cbName, func(db *gorm.DB) {
		if db.Statement.Table == "integration_healths" {
			db.AddError(errors.New("sqlite database disk image is malformed"))
		}
	}); err != nil {
		t.Fatal(err)
	}
	defer st.DB().Callback().Query().Remove(cbName)

	w := getRequest(engine, "/readyz")
	if w.Code != http.StatusServiceUnavailable || strings.TrimSpace(w.Body.String()) != `{"ready":false}` {
		t.Fatalf("expected unauthenticated /readyz 503 on DB error, got %d %s", w.Code, w.Body.String())
	}
	w = getProbeRequest(engine, "/readyz", "test-probe-token-long")
	if w.Code != http.StatusServiceUnavailable || !strings.Contains(w.Body.String(), "取消同步健康状态暂不可用") {
		t.Fatalf("expected detailed /readyz 503 reason on DB error, got %d %s", w.Code, w.Body.String())
	}
}

func TestCurrencyOptionsSelectedScopedToCurrentProvider(t *testing.T) {
	cfg := config.Default()
	cfg.Server.Production = false
	cfg.Binance.APIKey = "k"
	cfg.Binance.APISecret = "s"
	cfg.Checkout.Binance.UID = "b-uid"
	cfg.OKX.APIKey = "k"
	cfg.OKX.APISecret = "s"
	cfg.OKX.Passphrase = "p"
	cfg.Checkout.OKX.UID = "o-uid"
	cfg.Checkout.Currencies = []string{"USDT", "USDC"}
	cfg.NormalizePaymentCurrencies()

	o := &model.Order{Provider: config.ProviderBinance, Currency: "USDT"}
	binanceOpts := currencyOptionsFor(cfg, o, config.ProviderBinance)
	okxOpts := currencyOptionsFor(cfg, o, config.ProviderOKX)

	foundBinanceSelected := false
	for _, opt := range binanceOpts {
		if opt.Currency == "USDT" && opt.Selected {
			foundBinanceSelected = true
		}
	}
	if !foundBinanceSelected {
		t.Fatalf("expected USDT selected for current provider binance: %+v", binanceOpts)
	}
	for _, opt := range okxOpts {
		if opt.Selected {
			t.Fatalf("unexpected selected currency on non-current provider okx: %+v", okxOpts)
		}
	}
}

func TestLimitPaymentSelectionMultipartSizeLimitAndEmptyTradeNo(t *testing.T) {
	h := &Handlers{selectionLimit: newSelectionLimiter()}
	calls := 0
	r := gin.New()
	r.POST("/api/order/payment", h.LimitPaymentSelection, func(c *gin.Context) {
		calls++
		c.JSON(http.StatusOK, gin.H{"code": 0})
	})

	// 1. Oversized multipart/form-data body (>64KB) must return 413 and not reach handler.
	var mpBuf bytes.Buffer
	mw := multipart.NewWriter(&mpBuf)
	_ = mw.WriteField("trade_no", "order-1")
	_ = mw.WriteField("padding", strings.Repeat("A", 70<<10))
	_ = mw.Close()
	req := httptest.NewRequest(http.MethodPost, "/api/order/payment", &mpBuf)
	req.Header.Set("Content-Type", mw.FormDataContentType())
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)
	if w.Code != http.StatusRequestEntityTooLarge || calls != 0 {
		t.Fatalf("expected 413 for oversized multipart body, got code=%d calls=%d body=%s", w.Code, calls, w.Body.String())
	}

	// 2. Malformed multipart/form-data must return 400 and not reach handler.
	req = httptest.NewRequest(http.MethodPost, "/api/order/payment", strings.NewReader("not-valid-multipart"))
	req.Header.Set("Content-Type", "multipart/form-data; boundary=missing")
	w = httptest.NewRecorder()
	r.ServeHTTP(w, req)
	if w.Code != http.StatusBadRequest || calls != 0 {
		t.Fatalf("expected 400 for malformed multipart body, got code=%d calls=%d", w.Code, calls)
	}

	// 3. Oversized urlencoded body (>64KB) must return 413.
	bigForm := "trade_no=order-1&pad=" + strings.Repeat("x", 70<<10)
	req = httptest.NewRequest(http.MethodPost, "/api/order/payment", strings.NewReader(bigForm))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	w = httptest.NewRecorder()
	r.ServeHTTP(w, req)
	if w.Code != http.StatusRequestEntityTooLarge || calls != 0 {
		t.Fatalf("expected 413 for oversized urlencoded body, got code=%d calls=%d", w.Code, calls)
	}

	// 4. Empty trade_no must return 400 and not pollute per-order limiter or invoke handler.
	req = httptest.NewRequest(http.MethodPost, "/api/order/payment", strings.NewReader("trade_no=%20%20&provider=binance&currency=USDT"))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	w = httptest.NewRecorder()
	r.ServeHTTP(w, req)
	if w.Code != http.StatusBadRequest || calls != 0 {
		t.Fatalf("expected 400 for empty trade_no, got code=%d calls=%d", w.Code, calls)
	}
	h.selectionLimit.mu.Lock()
	_, polluted := h.selectionLimit.entries["order:"]
	h.selectionLimit.mu.Unlock()
	if polluted {
		t.Fatal("empty trade_no polluted order: rate-limit bucket")
	}
}

func TestBEpusdtDoesNotLeakInternalSQLiteErrors(t *testing.T) {
	ctx := context.Background()
	st, err := store.Open(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()

	cfg := config.Default()
	cfg.Server.Production = false
	cfg.Binance.APIKey = "key"
	cfg.Binance.APISecret = "secret"
	cfg.Checkout.Binance.UID = "receiver"
	mgr := settings.New(cfg, st)
	if err := mgr.Load(ctx); err != nil {
		t.Fatal(err)
	}
	p := verifiedPoller(t, mgr, st)
	engine, err := New(mgr, st, p)
	if err != nil {
		t.Fatal(err)
	}
	primeScanners(t, p, mgr)
	token := mgr.Get().BEpusdt.APIToken

	const cbName = "test:bepusdt_sqlite_error"
	if err := st.DB().Callback().Query().Before("gorm:query").Register(cbName, func(db *gorm.DB) {
		if db.Statement.Table == "orders" {
			db.AddError(errors.New("sqlite3: database is locked (internal table schema)"))
		}
	}); err != nil {
		t.Fatal(err)
	}
	defer st.DB().Callback().Query().Remove(cbName)

	createPayload := map[string]any{
		"order_id":     "err-leak-create",
		"amount":       json.Number("10"),
		"notify_url":   "https://shop.example/notify",
		"redirect_url": "https://shop.example/return",
	}
	createPayload["signature"] = sign.BEpusdtSignGeneric(createPayload, token)
	raw, _ := json.Marshal(createPayload)
	w := httptest.NewRecorder()
	engine.ServeHTTP(w, httptest.NewRequest(http.MethodPost, "/api/v1/order/create-order", bytes.NewReader(raw)))
	if strings.Contains(strings.ToLower(w.Body.String()), "sqlite") || !strings.Contains(w.Body.String(), "database temporarily unavailable") {
		t.Fatalf("create-order leaked internal SQLite error: %s", w.Body.String())
	}

	cancelPayload := map[string]any{"order_id": "err-leak-cancel"}
	cancelPayload["signature"] = sign.BEpusdtSignGeneric(cancelPayload, token)
	raw, _ = json.Marshal(cancelPayload)
	w = httptest.NewRecorder()
	engine.ServeHTTP(w, httptest.NewRequest(http.MethodPost, "/api/v1/order/cancel-transaction", bytes.NewReader(raw)))
	if strings.Contains(strings.ToLower(w.Body.String()), "sqlite") || !strings.Contains(w.Body.String(), "database temporarily unavailable") {
		t.Fatalf("cancel-transaction leaked internal SQLite error: %s", w.Body.String())
	}
}

func TestAdminErrorStatusCodesLogRedactionBodyLimitAndHTTPSCookie(t *testing.T) {
	ctx := context.Background()
	st, err := store.Open(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()

	cfg := config.Default()
	cfg.Server.Production = false
	cfg.Server.BaseURL = "HTTPS://pay.example.com"
	cfg.Admin.Username = "admin"
	cfg.Admin.Password = "correct-admin-password-123"
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

	// 1. Login over uppercase HTTPS BaseURL sets Secure cookie and HSTS header.
	loginForm := url.Values{"username": {"admin"}, "password": {cfg.Admin.Password}}
	req := httptest.NewRequest(http.MethodPost, "/admin/login", strings.NewReader(loginForm.Encode()))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	req.TLS = &tls.ConnectionState{}
	w := httptest.NewRecorder()
	engine.ServeHTTP(w, req)
	if w.Code != http.StatusSeeOther || w.Header().Get("Strict-Transport-Security") == "" {
		t.Fatalf("expected HSTS header on HTTPS login, code=%d hsts=%q", w.Code, w.Header().Get("Strict-Transport-Security"))
	}
	var adminCookie *http.Cookie
	for _, ck := range w.Result().Cookies() {
		if ck.Name == adminCookieName {
			adminCookie = ck
		}
	}
	if adminCookie == nil || !adminCookie.Secure || !adminCookie.HttpOnly || adminCookie.SameSite != http.SameSiteStrictMode {
		t.Fatalf("insecure admin cookie attributes: %+v", adminCookie)
	}

	// 2. Missing order detail returns 404 (not 200).
	req = httptest.NewRequest(http.MethodGet, "/admin/orders/nonexistent-trade-no", nil)
	req.AddCookie(adminCookie)
	w = httptest.NewRecorder()
	engine.ServeHTTP(w, req)
	if w.Code != http.StatusNotFound {
		t.Fatalf("expected 404 for missing admin order detail, got %d", w.Code)
	}

	// 3. Oversized (>1MB) POST with valid X-CSRF-Token returns 413 (cannot bypass body limit).
	auth := newAdminAuth(mgr.Get())
	token, csrf, err := auth.issue()
	if err != nil {
		t.Fatal(err)
	}
	hugeBody := "settings_scope=checkout&checkout_note=" + strings.Repeat("Z", (1<<20)+1024)
	req = httptest.NewRequest(http.MethodPost, "/admin/settings", strings.NewReader(hugeBody))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	req.Header.Set("X-CSRF-Token", csrf)
	req.AddCookie(&http.Cookie{Name: adminCookieName, Value: token})
	w = httptest.NewRecorder()
	engine.ServeHTTP(w, req)
	if w.Code != http.StatusRequestEntityTooLarge {
		t.Fatalf("expected 413 on >1MB admin POST with X-CSRF-Token, got %d", w.Code)
	}

	// 4. Oversized login body returns 413; huge username (<1MB) is truncated in logs.
	req = httptest.NewRequest(http.MethodPost, "/admin/login", strings.NewReader("username="+strings.Repeat("U", (1<<20)+1024)+"&password=x"))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	w = httptest.NewRecorder()
	engine.ServeHTTP(w, req)
	if w.Code != http.StatusRequestEntityTooLarge {
		t.Fatalf("expected 413 on >1MB login POST, got %d", w.Code)
	}
	logBuf.Reset()
	longUser := strings.Repeat("X", 2000)
	req = httptest.NewRequest(http.MethodPost, "/admin/login", strings.NewReader(url.Values{"username": {longUser}, "password": {"wrong"}}.Encode()))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	w = httptest.NewRecorder()
	engine.ServeHTTP(w, req)
	if strings.Contains(logBuf.String(), longUser) || !strings.Contains(logBuf.String(), "...(truncated)") {
		t.Fatalf("expected long failed login username to be truncated in logs, len=%d", logBuf.Len())
	}

	// 5. AdminSiteSave does not log secret new admin path in plaintext.
	logBuf.Reset()
	siteForm := url.Values{
		"csrf":             {csrf},
		"current_password": {cfg.Admin.Password},
		"admin_path":       {"/ultra-secret-panel-999"},
		"store_url":        {"https://shop.example"},
	}
	req = httptest.NewRequest(http.MethodPost, "/admin/settings/site", strings.NewReader(siteForm.Encode()))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	req.AddCookie(&http.Cookie{Name: adminCookieName, Value: token})
	w = httptest.NewRecorder()
	engine.ServeHTTP(w, req)
	if w.Code != http.StatusSeeOther || strings.Contains(w.Header().Get("Location"), "t=err") {
		t.Fatalf("AdminSiteSave failed: %d %s", w.Code, w.Header().Get("Location"))
	}
	if strings.Contains(logBuf.String(), "/ultra-secret-panel-999") {
		t.Fatalf("AdminSiteSave leaked secret admin_path to logs: %s", logBuf.String())
	}

	// 6. parseSettingsForm rejects empty-comma or oversized notify_retry_delays.
	for _, rawDelays := range []string{",,,", strings.TrimSuffix(strings.Repeat("5,", 25), ",")} {
		c, _ := gin.CreateTestContext(httptest.NewRecorder())
		c.Request = httptest.NewRequest(http.MethodPost, "/", strings.NewReader(url.Values{"settings_scope": {"merchant"}, "notify_retry_delays": {rawDelays}}.Encode()))
		c.Request.Header.Set("Content-Type", "application/x-www-form-urlencoded")
		_, errs := parseSettingsForm(c)
		if len(errs) == 0 {
			t.Fatalf("expected validation error for notify_retry_delays=%q", rawDelays)
		}
	}
}

func TestAdminOrdersPaginationDoesNotDoubleEscapeQuery(t *testing.T) {
	ctx := context.Background()
	st, err := store.Open(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()

	cfg := config.Default()
	cfg.Server.Production = false
	cfg.Admin.Password = "orders-pagination-password"
	mgr := settings.New(cfg, st)
	if err := mgr.Load(ctx); err != nil {
		t.Fatal(err)
	}
	engine, err := New(mgr, st, verifiedPoller(t, mgr, st))
	if err != nil {
		t.Fatal(err)
	}
	token, _, err := newAdminAuth(mgr.Get()).issue()
	if err != nil {
		t.Fatal(err)
	}

	req := httptest.NewRequest(http.MethodGet, "/admin/orders?page=2&q="+url.QueryEscape("ORD#123+test"), nil)
	req.AddCookie(&http.Cookie{Name: adminCookieName, Value: token})
	w := httptest.NewRecorder()
	engine.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", w.Code)
	}
	body := w.Body.String()
	if strings.Contains(body, "%2523") || strings.Contains(body, "%252B") || strings.Contains(body, "%252b") {
		t.Fatalf("pagination link double-escaped query parameter: %s", body)
	}
	wantHref := `href="?page=3&q=ORD%23123%2btest"`
	if !strings.Contains(body, wantHref) {
		t.Fatalf("pagination link missing properly single-escaped query %s: %s", wantHref, body)
	}
	u, err := url.Parse("?page=3&q=ORD%23123%2btest")
	if err != nil || u.Query().Get("q") != "ORD#123+test" {
		t.Fatalf("escaped query failed round-trip: %v %q", err, u.Query().Get("q"))
	}
}

func TestCashierAndStatusPollTemplateFixesAndCSP(t *testing.T) {
	ctx := context.Background()
	st, err := store.Open(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()

	cfg := config.Default()
	cfg.Server.Production = false
	cfg.Binance.APIKey = "key"
	cfg.Binance.APISecret = "secret"
	cfg.Checkout.Binance.UID = "receiver"
	mgr := settings.New(cfg, st)
	if err := mgr.Load(ctx); err != nil {
		t.Fatal(err)
	}
	engine, err := New(mgr, st, verifiedPoller(t, mgr, st))
	if err != nil {
		t.Fatal(err)
	}

	if _, err := st.Create(ctx, model.Order{
		TradeNo:     "tpl-check-1",
		OutTradeNo:  "tpl-check-1",
		OriginUnits: 10_000_000,
		ExpiresAt:   time.Now().Add(time.Hour),
		ReturnURL:   "https://shop.example/done",
	}); err != nil {
		t.Fatal(err)
	}
	selectPayment(t, engine, "tpl-check-1", "binance")

	w := getRequest(engine, "/pay/tpl-check-1")
	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", w.Code)
	}
	assertPageCSP(t, w, map[string]bool{})

	body := w.Body.String()
	for _, needle := range []string{
		`closedby="none"`,
		`overlay.addEventListener('cancel'`,
		`clearInterval(timerId)`,
		`copyTimers`,
		`btn.dataset.label`,
		`enabledButtons`,
		`onCanceled(d)`,
		`isSafeExternalURL`,
		`removeEventListener('visibilitychange'`,
		`retryUntil`,
		`AbortSignal.any`,
	} {
		if !strings.Contains(body, needle) {
			t.Fatalf("cashier page missing expected fix marker %q", needle)
		}
	}
}

func TestAdminRateLimitWindowRolloverPreservesInflightAndChecksExpiry(t *testing.T) {
	auth := &adminAuth{
		user:       "admin",
		hardLimits: true,
		attempts:   make(map[string]*loginAttempt),
		ipAttempts: make(map[string]*loginAttempt),
	}
	ip, user := "203.0.113.5", "admin"
	ipKey, userKey := adminAttemptKeys(ip, user)

	// 1. Request A enters and stays in flight while the 10-minute window expires.
	if !auth.allow(ip, user) {
		t.Fatal("request A unexpectedly blocked")
	}
	past := time.Now().Add(-time.Second)
	auth.mu.Lock()
	auth.attempts[userKey].count = 4 // simulate earlier failures in the old window
	auth.attempts[userKey].until = past
	auth.ipAttempts[ipKey].count = 10
	auth.ipAttempts[ipKey].until = past
	auth.globalCount = 20
	auth.globalUntil = past
	auth.mu.Unlock()

	// Request B arrives in the new window while A is still in flight.
	if !auth.allow(ip, user) {
		t.Fatal("request B unexpectedly blocked after rollover")
	}
	auth.mu.Lock()
	uAt, ipAt := auth.attempts[userKey], auth.ipAttempts[ipKey]
	if uAt == nil || uAt.inflight != 2 || uAt.count != 2 || ipAt == nil || ipAt.inflight != 2 || ipAt.count != 2 || auth.globalInflight != 2 || auth.globalCount != 2 {
		auth.mu.Unlock()
		t.Fatalf("rollover lost inflight accounting: uAt=%+v ipAt=%+v global=(%d,%d)", uAt, ipAt, auth.globalCount, auth.globalInflight)
	}
	auth.mu.Unlock()

	// Request A completes with succeeded(): request B's inflight reservation must remain intact.
	auth.succeeded(ip, user)
	auth.mu.Lock()
	uAt, ipAt = auth.attempts[userKey], auth.ipAttempts[ipKey]
	if uAt == nil || uAt.inflight != 1 || uAt.count != 1 || ipAt == nil || ipAt.inflight != 1 || ipAt.count != 1 || auth.globalInflight != 1 || auth.globalCount != 1 {
		auth.mu.Unlock()
		t.Fatalf("completing old request A corrupted active request B: uAt=%+v ipAt=%+v global=(%d,%d)", uAt, ipAt, auth.globalCount, auth.globalInflight)
	}
	auth.mu.Unlock()

	// Request B completes with failed().
	auth.failed(ip, user)
	auth.mu.Lock()
	if auth.attempts[userKey].inflight != 0 || auth.attempts[userKey].count != 1 || auth.globalInflight != 0 {
		auth.mu.Unlock()
		t.Fatalf("unexpected post-B state: uAt=%+v global=(%d,%d)", auth.attempts[userKey], auth.globalCount, auth.globalInflight)
	}
	auth.mu.Unlock()

	// 2. When adminUserAttemptLimit requests are all in flight across window rollover, a 7th is still blocked.
	auth2 := &adminAuth{
		user:       "admin",
		hardLimits: true,
		attempts:   make(map[string]*loginAttempt),
		ipAttempts: make(map[string]*loginAttempt),
	}
	for i := range adminUserAttemptLimit {
		if !auth2.allow(ip, user) {
			t.Fatalf("concurrent inflight request %d unexpectedly blocked", i)
		}
	}
	auth2.mu.Lock()
	auth2.attempts[userKey].until = past
	auth2.ipAttempts[ipKey].until = past
	auth2.globalUntil = past
	auth2.mu.Unlock()
	if auth2.allow(ip, user) {
		t.Fatal("window rollover allowed bypassing concurrent inflight limit")
	}

	// 3. Expired uncleaned entries must not trigger isExhausted() or loginDelay().
	auth3 := &adminAuth{
		user:        "admin",
		hardLimits:  true,
		attempts:    map[string]*loginAttempt{userKey: {count: adminUserAttemptLimit, until: past}},
		ipAttempts:  map[string]*loginAttempt{ipKey: {count: adminIPAttemptLimit + 10, until: past}},
		globalCount: adminGlobalAttemptLimit + 10,
		globalUntil: past,
	}
	if auth3.isExhausted(ip, user) {
		t.Fatal("expired uncleaned attempt triggered isExhausted")
	}
	if d := auth3.loginDelay(ip); d != 0 {
		t.Fatalf("expired uncleaned attempt triggered loginDelay=%v", d)
	}
}
