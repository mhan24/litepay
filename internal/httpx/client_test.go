package httpx

import (
	"bytes"
	"net/http"
	"net/http/httptest"
	"net/netip"
	"testing"
	"time"
)

func TestPublicIPRejectsInternalAndReservedRanges(t *testing.T) {
	for _, raw := range []string{
		"127.0.0.1", "10.0.0.1", "172.16.0.1", "192.168.1.1", "169.254.1.1",
		"100.64.0.1", "192.0.2.1", "198.51.100.1", "203.0.113.1", "::1", "fc00::1", "fe80::1",
		"2001:db8::1%lo0", "64:ff9b::1%eth0", "2606:4700:4700::1111%lo0", "100::1", "2001::1", "2001:2::1", "::127.0.0.1", "::ffff:127.0.0.1", "192.88.99.1", "fec0::1",
	} {
		if publicIP(netip.MustParseAddr(raw)) {
			t.Fatalf("accepted internal address %s", raw)
		}
	}
	for _, raw := range []string{"1.1.1.1", "8.8.8.8", "2606:4700:4700::1111"} {
		if !publicIP(netip.MustParseAddr(raw)) {
			t.Fatalf("rejected public address %s", raw)
		}
	}
}

func TestClientRejectsOversizedResponseAndDoesNotFollowRedirects(t *testing.T) {
	var redirected bool
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/redirect":
			http.Redirect(w, r, "/target", http.StatusFound)
		case "/target":
			redirected = true
			w.WriteHeader(http.StatusOK)
		case "/oversized":
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write(bytes.Repeat([]byte("a"), maxResponseBytes+1))
		}
	}))
	defer srv.Close()

	cli := NewClient(5*time.Second, true)
	defer cli.CloseIdleConnections()

	req, err := http.NewRequest(http.MethodGet, srv.URL+"/redirect", nil)
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Authorization", "Bearer secret-token")
	status, _, err := cli.Do(req)
	if err != nil || status != http.StatusFound || redirected {
		t.Fatalf("expected 302 without following redirect, got status=%d redirected=%v err=%v", status, redirected, err)
	}

	bigReq, err := http.NewRequest(http.MethodGet, srv.URL+"/oversized", nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := cli.Do(bigReq); err == nil {
		t.Fatal("expected oversized response > 4 MiB to be rejected")
	}

	var nilClient *Client
	nilClient.CloseIdleConnections()
}
