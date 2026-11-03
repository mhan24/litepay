package server

import (
	"litepay/internal/config"
	"litepay/internal/settings"
	"litepay/internal/store"
	"litepay/internal/worker"
	"net/http/httptest"
	"testing"
)

func TestHealthyDatabaseDoesNotImplyWorkerReadiness(t *testing.T) {
	st, err := store.Open(t.TempDir() + "/ready.db")
	if err != nil {
		t.Fatal(err)
	}
	cfg := config.Default()
	mgr := settings.New(cfg, st)
	poll := worker.NewPoller(mgr, st)
	engine, err := New(mgr, st, poll)
	if err != nil {
		t.Fatal(err)
	}
	w := httptest.NewRecorder()
	engine.ServeHTTP(w, httptest.NewRequest("GET", "/healthz", nil))
	if w.Code != 200 {
		t.Fatal("health failed")
	}
	w = httptest.NewRecorder()
	engine.ServeHTTP(w, httptest.NewRequest("GET", "/readyz", nil))
	if w.Code != 503 {
		t.Fatalf("unstarted workers returned %d", w.Code)
	}
}
