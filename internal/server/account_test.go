package server

import (
	"context"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"

	"litepay/internal/settings"
)

func TestAdminAccountChangePersistsAndRevokesSession(t *testing.T) {
	engine, mgr, st := newServerTestEnv(t, nil)
	old := mgr.Get()
	auth := newAdminAuth(old)
	token, csrf, err := auth.issue()
	if err != nil {
		t.Fatal(err)
	}
	values := url.Values{"csrf": {csrf}, "admin_username": {"new-admin"}, "current_password": {old.Admin.Password}, "new_password": {"abc123"}, "confirm_password": {"abc123"}}
	send := func() *httptest.ResponseRecorder {
		req := httptest.NewRequest(http.MethodPost, "/admin/settings/account", strings.NewReader(values.Encode()))
		req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
		req.AddCookie(&http.Cookie{Name: adminCookieName, Value: token})
		rec := httptest.NewRecorder()
		engine.ServeHTTP(rec, req)
		return rec
	}
	values.Set("current_password", "wrong")
	send()
	if mgr.Get().Admin.Username != old.Admin.Username {
		t.Fatal("changed account with incorrect current password")
	}
	values.Set("current_password", old.Admin.Password)
	if rec := send(); rec.Header().Get("Location") != "/admin/login" {
		t.Fatalf("unexpected redirect: %s", rec.Header().Get("Location"))
	}
	fresh := newAdminAuth(mgr.Get())
	if !fresh.checkCredentials("new-admin", "abc123") {
		t.Fatal("new credentials rejected")
	}
	if _, ok := fresh.parse(token); ok {
		t.Fatal("old session accepted")
	}
	if err := mgr.Update(context.Background(), settings.Patch{}, "test"); err != nil {
		t.Fatal(err)
	}
	reloaded := settings.New(old, st)
	if err := reloaded.Load(context.Background()); err != nil {
		t.Fatal(err)
	}
	if !newAdminAuth(reloaded.Get()).checkCredentials("new-admin", "abc123") {
		t.Fatal("credentials lost after restart")
	}
	payload, _ := st.LoadSettingPayload(context.Background())
	if strings.Contains(payload, "abc123") {
		t.Fatal("plaintext password persisted")
	}
	req := httptest.NewRequest(http.MethodGet, "/admin/settings", nil)
	req.AddCookie(&http.Cookie{Name: adminCookieName, Value: token})
	rec := httptest.NewRecorder()
	engine.ServeHTTP(rec, req)
	if rec.Code != http.StatusSeeOther {
		t.Fatal("old session still authorized")
	}
}
