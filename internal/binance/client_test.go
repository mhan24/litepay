package binance

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"litepay/internal/model"
)

func TestNormalizeEventAndAppReference(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("X-MBX-APIKEY") != "key" || r.URL.Query().Get("signature") == "" {
			t.Error("unsigned request")
		}
		fmt.Fprint(w, `{"code":"000000","success":true,"data":[{"transactionId":"parent","orderId":"app-order","orderType":"PAY","transactionTime":100000,"amount":"10.01","currency":"USDT","receiverInfo":{"binanceId":"uid"}},{"transactionId":"multi","orderType":"PAY","transactionTime":100001,"amount":"0","fundsDetail":[{"currency":"USDT","amount":"10.01"},{"currency":"USDC","amount":"10.02"}],"receiverInfo":{"binanceId":"uid"}},{"transactionId":"refund","orderType":"REFUND","transactionTime":100002,"amount":"10.01","currency":"USDT","receiverInfo":{"binanceId":"uid"}}]}`)
	}))
	defer srv.Close()
	c := New("key", "secret", srv.URL, 5000, 100)
	c.UseTestTransport()
	rows, e := c.History(context.Background(), time.UnixMilli(90000), time.UnixMilli(110000))
	if e != nil || len(rows) != 3 {
		t.Fatal(rows, e)
	}
	if rows[0].EventID != "parent" || rows[0].Reference != "app-order" || rows[0].Units != 10010000 || rows[0].State != model.Confirmed {
		t.Fatal(rows[0])
	}
	if rows[1].State != model.Ambiguous || rows[1].Assets == "" || rows[2].State != model.Rejected {
		t.Fatal("unsafe normalization", rows)
	}
	found, e := c.Lookup(context.Background(), "app-order", time.UnixMilli(90000), time.UnixMilli(110000))
	if e != nil || len(found) != 1 || found[0].EventID != "parent" {
		t.Fatal("App reference lost", found, e)
	}
}
func TestSaturatedMillisecondFails(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprint(w, `{"code":"000000","success":true,"data":[{"transactionId":"a","orderType":"PAY","transactionTime":100000,"amount":"1","currency":"USDT","receiverInfo":{"binanceId":"uid"}}]}`)
	}))
	defer srv.Close()
	c := New("k", "s", srv.URL, 5000, 1)
	c.UseTestTransport()
	if _, e := c.History(context.Background(), time.UnixMilli(100000), time.UnixMilli(100000)); e == nil {
		t.Fatal("incomplete history accepted")
	}
}
