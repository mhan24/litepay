package server

import (
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"

	"litepay/internal/config"
)

func TestStoreHomepageAndHiddenAdminLifecycle(t *testing.T) {
	const root = "/workspace-test-42"
	engine, mgr, _ := newServerTestEnv(t, func(c *config.Config) {
		c.Server.StoreURL = "https://shop.example.test/catalog"
		c.Admin.Path = root
	})
	send := func(method, target string, values url.Values, cookie *http.Cookie) *httptest.ResponseRecorder {
		req := httptest.NewRequest(method, target, strings.NewReader(values.Encode()))
		req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
		if cookie != nil {
			req.AddCookie(cookie)
		}
		rec := httptest.NewRecorder()
		engine.ServeHTTP(rec, req)
		return rec
	}
	index := send("GET", "/?next=/admin", nil, nil)
	if index.Code != http.StatusFound || index.Header().Get("Location") != mgr.Get().Server.StoreURL || strings.Contains(index.Body.String(), root) {
		t.Fatalf("homepage exposed gateway or failed redirect: %d %s", index.Code, index.Body.String())
	}
	for _, target := range []string{"/admin", "/admin/login", "/admin/settings", "/admin/", "/Admin/login"} {
		for _, method := range []string{"GET", "POST"} {
			rec := send(method, target, nil, nil)
			if rec.Code != 404 || rec.Header().Get("Location") != "" || strings.Contains(rec.Body.String(), root) {
				t.Fatalf("old admin route revealed entry: %s %s %d", method, target, rec.Code)
			}
		}
	}
	unauth := send("GET", root+"/orders", nil, nil)
	if unauth.Code != 303 || !strings.HasPrefix(unauth.Header().Get("Location"), root+"/login?") {
		t.Fatal("hidden route bypassed authentication")
	}
	login := send("GET", root+"/login", nil, nil)
	if login.Code != 200 || !strings.Contains(login.Body.String(), `action="`+root+`/login"`) {
		t.Fatal("incorrect login form")
	}
	values := url.Values{"username": {mgr.Get().Admin.Username}, "password": {mgr.Get().Admin.Password}, "next": {root + "-evil"}}
	loggedIn := send("POST", root+"/login", values, nil)
	if loggedIn.Code != 303 || loggedIn.Header().Get("Location") != root {
		t.Fatalf("login redirect: %d %s", loggedIn.Code, loggedIn.Header().Get("Location"))
	}
	cookies := loggedIn.Result().Cookies()
	if len(cookies) != 1 || cookies[0].Path != root || !cookies[0].HttpOnly || !cookies[0].Secure {
		t.Fatal("incorrect session cookie scope")
	}
	cookie := cookies[0]
	for _, suffix := range []string{"", "/orders", "/settings", "/integration"} {
		rec := send("GET", root+suffix, nil, cookie)
		if rec.Code != 200 || strings.Contains(rec.Body.String(), `"/admin`) {
			t.Fatalf("hidden admin navigation failed: %s %d", suffix, rec.Code)
		}
	}
	denied := send("POST", root+"/settings", nil, cookie)
	if denied.Code != 403 {
		t.Fatal("hidden path bypassed CSRF")
	}
	logout := send("POST", root+"/logout", nil, cookie)
	if logout.Header().Get("Location") != root+"/login" || logout.Result().Cookies()[0].Path != root {
		t.Fatal("incorrect logout destination/cookie deletion")
	}
}

func TestHomepageUnconfiguredDoesNotExposeGateway(t *testing.T) {
	engine, _, _ := newServerTestEnv(t, nil)
	rec := httptest.NewRecorder()
	engine.ServeHTTP(rec, httptest.NewRequest("GET", "/", nil))
	if rec.Code != 404 || strings.Contains(rec.Body.String(), "LitePay") || strings.Contains(rec.Body.String(), "/admin") {
		t.Fatal("unconfigured homepage exposed gateway")
	}
}

func TestAdminNextMustStayInsideMount(t *testing.T) {
	for _, raw := range []string{"//evil.test", "https://evil.test", "/secret-evil", "/secret/../pay", "/secret/%2e%2e/pay", "/secret/%5cevil", "/secret//orders", "/admin", "/secret/%0d%0aLocation:evil"} {
		if safeAdminNext(raw, "/secret") {
			t.Errorf("unsafe next accepted: %q", raw)
		}
	}
	for _, raw := range []string{"/secret", "/secret/orders?page=2"} {
		if !safeAdminNext(raw, "/secret") {
			t.Errorf("valid next rejected: %q", raw)
		}
	}
}
