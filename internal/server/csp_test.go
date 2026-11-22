package server

import (
	"context"
	"encoding/base64"
	"net/http"
	"net/http/httptest"
	"net/url"
	"regexp"
	"strings"
	"testing"
	"time"

	"golang.org/x/net/html"
	"litepay/internal/config"
	"litepay/internal/model"
	"litepay/internal/settings"
	"litepay/internal/store"
)

func TestCSPNoncesCoverCashierAdminAndErrorPages(t *testing.T) {
	ctx := context.Background()
	st, err := store.Open(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	cfg := config.Default()
	cfg.Server.Production = false
	cfg.Admin.Password = "csp-test-admin-password"
	cfg.Binance.APIKey = "k"
	cfg.Binance.APISecret = "s"
	cfg.Checkout.Binance.UID = "binance-uid"
	cfg.Checkout.Currencies = []string{"USDT", "USDC"}
	mgr := settings.New(cfg, st)
	if err := mgr.Load(ctx); err != nil {
		t.Fatal(err)
	}
	poller := verifiedPoller(t, mgr, st)
	engine, err := New(mgr, st, poller)
	if err != nil {
		t.Fatal(err)
	}
	for _, state := range []string{"ready", "pending", "paid", "expired", "canceled"} {
		no := "csp-" + state
		if _, err := st.Create(ctx, model.Order{TradeNo: no, OutTradeNo: no, OriginUnits: 10000000, Reselect: true, ExpiresAt: time.Now().Add(time.Minute)}); err != nil {
			t.Fatal(err)
		}
		if state != "pending" {
			if _, err := st.Select(ctx, no, "binance", "USDT", "binance-uid", 2, 10000, 10000, 99, 120); err != nil {
				t.Fatal(err)
			}
		}
		if state != "pending" && state != "ready" {
			if err := st.DB().Model(&model.Order{}).Where("trade_no = ?", no).Update("status", strings.ToUpper(state)).Error; err != nil {
				t.Fatal(err)
			}
		}
	}
	token, csrf, err := newAdminAuth(mgr.Get()).issue()
	if err != nil {
		t.Fatal(err)
	}
	paths := []string{"/admin/login", "/admin", "/admin/orders", "/admin/orders/csp-ready", "/admin/settings", "/admin/integration", "/admin/orders/missing", "/pay/csp-ready", "/pay/csp-pending", "/pay/csp-paid", "/pay/csp-expired", "/pay/csp-canceled", "/pay/missing"}
	seen := map[string]bool{}
	for _, path := range paths {
		t.Run(path, func(t *testing.T) {
			req := httptest.NewRequest(http.MethodGet, path, nil)
			if path != "/admin/login" {
				req.AddCookie(&http.Cookie{Name: adminCookieName, Value: token})
			}
			w := httptest.NewRecorder()
			engine.ServeHTTP(w, req)
			assertPageCSP(t, w, seen)
		})
	}
	req := httptest.NewRequest(http.MethodPost, "/admin/integration", strings.NewReader(url.Values{"csrf": {csrf}, "current_password": {cfg.Admin.Password}}.Encode()))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	req.AddCookie(&http.Cookie{Name: adminCookieName, Value: token})
	w := httptest.NewRecorder()
	engine.ServeHTTP(w, req)
	assertPageCSP(t, w, seen)
}

func assertPageCSP(t *testing.T, w *httptest.ResponseRecorder, seen map[string]bool) {
	t.Helper()
	policy := w.Header().Get("Content-Security-Policy")
	match := regexp.MustCompile(`script-src 'nonce-([^']+)'`).FindStringSubmatch(policy)
	if len(match) != 2 {
		t.Fatal("missing CSP nonce", policy)
	}
	nonce := match[1]
	if raw, err := base64.RawStdEncoding.DecodeString(nonce); err != nil || len(raw) < 16 {
		t.Fatal("weak nonce", err)
	}
	if seen[nonce] {
		t.Fatal("nonce reused across responses")
	}
	seen[nonce] = true
	for _, directive := range []string{"default-src 'none'", "style-src 'nonce-" + nonce + "'", "img-src 'self' data:", "connect-src 'self'", "form-action 'self'", "base-uri 'none'", "frame-ancestors 'none'", "object-src 'none'"} {
		if !strings.Contains(policy, directive) {
			t.Fatal("missing directive", directive, policy)
		}
	}
	if strings.Contains(policy, "unsafe-inline") || strings.Contains(policy, "unsafe-eval") {
		t.Fatal("CSP was weakened")
	}
	if !strings.Contains(w.Header().Get("Content-Type"), "text/html") {
		t.Fatalf("expected rendered HTML, got %d %s", w.Code, w.Body.String())
	}
	doc, err := html.Parse(strings.NewReader(w.Body.String()))
	if err != nil {
		t.Fatal(err)
	}
	scripts, styles := 0, 0
	var visit func(*html.Node)
	visit = func(n *html.Node) {
		if n.Type == html.ElementNode {
			attrs := map[string]string{}
			for _, a := range n.Attr {
				attrs[a.Key] = a.Val
				if a.Key == "style" || strings.HasPrefix(a.Key, "on") {
					t.Errorf("CSP-incompatible attribute %s on %s", a.Key, n.Data)
				}
			}
			if n.Data == "script" || n.Data == "style" {
				if attrs["nonce"] != nonce {
					t.Errorf("%s nonce mismatch: %q", n.Data, attrs["nonce"])
				}
				if n.Data == "script" {
					scripts++
				} else {
					styles++
				}
			}
		}
		for child := n.FirstChild; child != nil; child = child.NextSibling {
			visit(child)
		}
	}
	visit(doc)
	if scripts == 0 || styles == 0 {
		t.Fatal("page did not exercise inline scripts and styles")
	}
}
