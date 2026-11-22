package server

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"

	"litepay/internal/config"
	"litepay/internal/model"
	"litepay/internal/settings"
	"litepay/internal/store"
)

func TestSensitiveAdminActionsRequirePasswordAndRevokeOnLimit(t *testing.T) {
	cases := []struct{ name, path, scope, field string }{
		{name: "reveal", path: "/admin/integration"}, {name: "rotate", path: "/admin/settings/token"},
	}
	for _, field := range []struct{ scope, field string }{{"binance", "binance_api_key"}, {"binance", "binance_api_secret"}, {"okx", "okx_api_key"}, {"okx", "okx_api_secret"}, {"okx", "okx_passphrase"}, {"bybit", "bybit_api_key"}, {"bybit", "bybit_api_secret"}, {"bitget", "bitget_api_key"}, {"bitget", "bitget_api_secret"}, {"bitget", "bitget_passphrase"}} {
		cases = append(cases, struct{ name, path, scope, field string }{field.field, "/admin/settings", field.scope, field.field})
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			st, err := store.Open(":memory:")
			if err != nil {
				t.Fatal(err)
			}
			defer st.Close()
			cfg := config.Default()
			cfg.Server.Production = false
			cfg.Admin.Password = "sensitive-operation-password"
			mgr := settings.New(cfg, st)
			if err := mgr.Load(context.Background()); err != nil {
				t.Fatal(err)
			}
			engine, err := New(mgr, st, verifiedPoller(t, mgr, st))
			if err != nil {
				t.Fatal(err)
			}
			token, csrf, err := newAdminAuth(mgr.Get()).issue()
			if err != nil {
				t.Fatal(err)
			}
			post := func(password string) *httptest.ResponseRecorder {
				values := url.Values{"csrf": {csrf}}
				if password != "" {
					values.Set("current_password", password)
				}
				if tc.scope != "" {
					values.Set("settings_scope", tc.scope)
					values.Set(tc.field, "changed-secret")
				}
				req := httptest.NewRequest(http.MethodPost, tc.path, strings.NewReader(values.Encode()))
				req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
				req.AddCookie(&http.Cookie{Name: adminCookieName, Value: token})
				w := httptest.NewRecorder()
				engine.ServeHTTP(w, req)
				return w
			}
			originalToken := mgr.Get().BEpusdt.APIToken
			before, err := st.LoadSettingPayload(context.Background())
			if err != nil {
				t.Fatal(err)
			}
			for _, password := range []string{"", "wrong"} {
				w := post(password)
				if tc.scope == "" && w.Code != http.StatusForbidden {
					t.Fatalf("sensitive action accepted: %d", w.Code)
				}
				if tc.scope != "" && !strings.Contains(w.Header().Get("Location"), "t=err") {
					t.Fatal("secret update accepted")
				}
				if strings.Contains(w.Body.String(), originalToken) {
					t.Fatal("rejected request leaked token")
				}
				after, err := st.LoadSettingPayload(context.Background())
				if err != nil || after != before || mgr.Get().BEpusdt.APIToken != originalToken {
					t.Fatal("rejected request changed credentials", err)
				}
				var keys int64
				if err := st.DB().Model(&model.SigningKey{}).Count(&keys).Error; err != nil || keys != 1 {
					t.Fatal("rejected request rotated signing key", keys, err)
				}
			}
			w := post(cfg.Admin.Password)
			if tc.name == "reveal" {
				if w.Code != http.StatusOK || !strings.Contains(w.Body.String(), originalToken) {
					t.Fatal("password did not authorize reveal", w.Code)
				}
			} else if w.Code != http.StatusSeeOther || strings.Contains(w.Header().Get("Location"), "t=err") {
				t.Fatal("password did not authorize operation", w.Code, w.Header().Get("Location"))
			}
			if tc.name == "rotate" && mgr.Get().BEpusdt.APIToken == originalToken {
				t.Fatal("token was not rotated")
			}
			if tc.field != "" {
				raw, err := json.Marshal(settings.Snapshot(mgr.Get()))
				if err != nil {
					t.Fatal(err)
				}
				var snapshot map[string]any
				if err := json.Unmarshal(raw, &snapshot); err != nil || snapshot[tc.field] != "changed-secret" {
					t.Fatal("secret not updated", err)
				}
			}
			saved := mgr.Get()
			for range adminUserAttemptLimit {
				post("wrong")
			}
			w = post(cfg.Admin.Password)
			if w.Code != http.StatusSeeOther || !strings.Contains(w.Header().Get("Location"), "/admin/login") {
				t.Fatal("exhausted session accepted", w.Code, w.Header().Get("Location"))
			}
			if mgr.Get() != saved {
				t.Fatal("revoked request changed configuration")
			}
		})
	}
}

func TestDisplaySettingsDoNotRequirePasswordAndCannotEditDeployment(t *testing.T) {
	st, err := store.Open(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	cfg := config.Default()
	cfg.Server.Production = false
	cfg.Admin.Password = "display-settings-password"
	mgr := settings.New(cfg, st)
	if err := mgr.Load(context.Background()); err != nil {
		t.Fatal(err)
	}
	engine, err := New(mgr, st, verifiedPoller(t, mgr, st))
	if err != nil {
		t.Fatal(err)
	}
	token, csrf, err := newAdminAuth(mgr.Get()).issue()
	if err != nil {
		t.Fatal(err)
	}
	form := url.Values{"csrf": {csrf}, "settings_scope": {"checkout"}, "checkout_note": {"updated note"}, "production": {"true"}, "base_url": {"https://attacker.example"}, "notify_hosts": {"attacker.example"}}
	req := httptest.NewRequest("POST", "/admin/settings", strings.NewReader(form.Encode()))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	req.AddCookie(&http.Cookie{Name: adminCookieName, Value: token})
	w := httptest.NewRecorder()
	engine.ServeHTTP(w, req)
	if w.Code != 303 || strings.Contains(w.Header().Get("Location"), "t=err") || mgr.Get().Checkout.Note != "updated note" {
		t.Fatal("display update required password", w.Header().Get("Location"))
	}
	if mgr.Get().Server.Production != cfg.Server.Production || mgr.Get().Server.BaseURL != cfg.Server.BaseURL || strings.Join(mgr.Get().Server.NotifyHosts, ",") != strings.Join(cfg.Server.NotifyHosts, ",") {
		t.Fatal("startup-only controls became editable")
	}
	for _, page := range []string{"/admin/settings", "/admin/integration"} {
		req := httptest.NewRequest("GET", page, nil)
		req.AddCookie(&http.Cookie{Name: adminCookieName, Value: token})
		w := httptest.NewRecorder()
		engine.ServeHTTP(w, req)
		if w.Code != 200 || !strings.Contains(w.Body.String(), `name="current_password"`) || !strings.Contains(w.Body.String(), `autocomplete="current-password"`) {
			t.Fatal("missing reauthentication form", page, w.Code)
		}
	}
}
