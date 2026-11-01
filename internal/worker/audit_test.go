package worker

import (
	"context"
	"net/http"
	"net/http/httptest"
	"net/netip"
	"testing"

	"litepay/internal/model"
)

func TestAuditRoundedMatchingKeepsTrailingZeros(t *testing.T) {
	for _, precision := range []int{2, 4, 6} {
		o := &model.Order{PayAmount: 10, AmountPrecision: precision}
		if orderMatchesTransaction(o, &incomingTransaction{Amount: 9.6}, precision) {
			t.Fatal("underpayment matched integer order")
		}
	}
	o := &model.Order{PayAmount: 2.5100, AmountPrecision: 4}
	if orderMatchesTransaction(o, &incomingTransaction{Amount: 2.5102}, 2) {
		t.Fatal("live config replaced locked precision")
	}
	if BuildNotifyParams(o)["actual_amount"] != "2.51" {
		t.Fatal("wrong locked callback amount")
	}
}

func TestAuditCallbacksRejectInternalNetworksAndRedirects(t *testing.T) {
	for _, raw := range []string{"127.0.0.1", "10.0.0.1", "169.254.169.254", "100.100.100.200", "::1", "::ffff:127.0.0.1", "fc00::1", "64:ff9b::7f00:1"} {
		if publicCallbackIP(netip.MustParseAddr(raw)) {
			t.Fatalf("allowed %s", raw)
		}
	}
	if !publicCallbackIP(netip.MustParseAddr("8.8.8.8")) {
		t.Fatal("public address rejected")
	}
	hits := 0
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hits++
		w.Header().Set("Location", "/target")
		w.WriteHeader(http.StatusTemporaryRedirect)
	}))
	defer srv.Close()
	n := &Notifier{}
	if _, _, _, err := n.postJSON(context.Background(), 3, srv.URL, map[string]interface{}{}); err == nil {
		t.Fatal("callback reached loopback")
	}
	if hits != 0 {
		t.Fatal("internal endpoint was contacted")
	}
	n.transport = http.DefaultTransport
	status, _, _, err := n.postJSON(context.Background(), 3, srv.URL, map[string]interface{}{})
	if err != nil || status != 307 || hits != 1 {
		t.Fatalf("followed redirect: status=%d hits=%d err=%v", status, hits, err)
	}
}
