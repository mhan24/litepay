package server

import (
	"context"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"

	"litepay/internal/config"
	"litepay/internal/settings"
	"litepay/internal/worker"
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
	if logout.Header().Get("Location") != root+"/login" || len(logout.Result().Cookies()) != 2 || logout.Result().Cookies()[0].Path != "/" || logout.Result().Cookies()[1].Path != root {
		t.Fatal("incorrect logout destination/cookie deletion")
	}
}

func TestAdminChangesEntryLiveAndPersistsAcrossRestart(t *testing.T) {
	const oldRoot, newRoot = "/office-old-42", "/office-new-57"
	engine, mgr, st := newServerTestEnv(t, func(c *config.Config) {
		c.Admin.Path = oldRoot
		c.Server.StoreURL = "https://shop.example.test"
	})
	initial := mgr.Get()
	auth := newAdminAuth(initial)
	token, csrf, err := auth.issue()
	if err != nil {
		t.Fatal(err)
	}
	values := url.Values{"csrf": {csrf}, "admin_path": {newRoot}, "store_url": {"https://store.example.test/catalog"}, "current_password": {"wrong"}}
	send := func() *httptest.ResponseRecorder {
		req := httptest.NewRequest("POST", oldRoot+"/settings/site", strings.NewReader(values.Encode()))
		req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
		req.AddCookie(&http.Cookie{Name: adminCookieName, Value: token})
		rec := httptest.NewRecorder()
		engine.ServeHTTP(rec, req)
		return rec
	}
	send()
	if mgr.Get().AdminPath() != oldRoot {
		t.Fatal("wrong password changed entry")
	}
	values.Set("current_password", initial.Admin.Password)
	values.Set("csrf", "wrong")
	if send().Code != 403 {
		t.Fatal("missing CSRF allowed entry change")
	}
	values.Set("csrf", csrf)
	for _, bad := range []string{"/api", "/pay", "//evil", "/nested/path", ""} {
		values.Set("admin_path", bad)
		send()
		if mgr.Get().AdminPath() != oldRoot {
			t.Fatalf("invalid path %s persisted", bad)
		}
	}
	values.Set("admin_path", newRoot)
	values.Set("store_url", "https://pay.example.test/")
	send()
	if mgr.Get().AdminPath() != oldRoot {
		t.Fatal("invalid URL partially applied entry change")
	}
	values.Set("store_url", "https://store.example.test/catalog")
	rec := send()
	if rec.Code != 303 || rec.Header().Get("Location") != newRoot+"/login" {
		t.Fatalf("new entry redirect: %d %s", rec.Code, rec.Header().Get("Location"))
	}
	clearedOld := false
	for _, cookie := range rec.Result().Cookies() {
		if cookie.Path == oldRoot && cookie.MaxAge < 0 {
			clearedOld = true
		}
	}
	if !clearedOld {
		t.Fatal("old path session cookie retained")
	}
	for _, path := range []string{oldRoot, oldRoot + "/", oldRoot + "/login", oldRoot + "/settings/site"} {
		w := httptest.NewRecorder()
		engine.ServeHTTP(w, httptest.NewRequest("GET", path, nil))
		if w.Code != 404 || strings.Contains(w.Body.String(), newRoot) {
			t.Fatalf("retired entry exposed: %s %d", path, w.Code)
		}
	}
	w := requestAsAdmin(t, engine, mgr.Get(), newRoot+"/settings")
	if w.Code != 200 || !strings.Contains(w.Body.String(), `action="`+newRoot+`/settings/site"`) {
		t.Fatal("live switch lost form/navigation")
	}
	reloaded := settings.New(initial, st)
	if err := reloaded.Load(context.Background()); err != nil {
		t.Fatal(err)
	}
	if reloaded.Get().AdminPath() != newRoot || reloaded.Get().Server.StoreURL != values.Get("store_url") {
		t.Fatal("entry/redirect lost on restart")
	}
	restarted, err := New(reloaded, st, worker.NewPoller(reloaded, st))
	if err != nil {
		t.Fatal(err)
	}
	w = httptest.NewRecorder()
	restarted.ServeHTTP(w, httptest.NewRequest("GET", newRoot+"/login", nil))
	if w.Code != 200 {
		t.Fatal("persisted entry unavailable after restart")
	}
	w = httptest.NewRecorder()
	restarted.ServeHTTP(w, httptest.NewRequest("GET", "/", nil))
	if w.Header().Get("Location") != values.Get("store_url") {
		t.Fatal("persisted homepage redirect incorrect")
	}
	// Unrelated payment configuration updates must retain both website fields.
	if err := reloaded.Update(context.Background(), settings.Patch{}, "test"); err != nil {
		t.Fatal(err)
	}
	if reloaded.Get().AdminPath() != newRoot {
		t.Fatal("payment save reset entry")
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
