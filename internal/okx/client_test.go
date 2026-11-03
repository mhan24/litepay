package okx

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"litepay/internal/model"
)

func TestDepositIdentityAndMissingID(t *testing.T) {
	missing := false
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/api/v5/public/time":
			fmt.Fprint(w, `{"code":"0","data":[{"ts":"100000"}]}`)
		case "/api/v5/account/config":
			fmt.Fprint(w, `{"code":"0","data":[{"uid":"receiver"}]}`)
		default:
			if r.URL.Query().Get("type") != "3" || r.Header.Get("OK-ACCESS-SIGN") == "" {
				t.Error("wrong signed internal-transfer query")
			}
			id := "dep-1"
			if missing {
				id = ""
			}
			fmt.Fprintf(w, `{"code":"0","data":[{"depId":%q,"fromWdId":"sender-reference","ccy":"USDC","amt":"10.000001","ts":"100000","state":"2"},{"depId":"chain","fromWdId":"sender-other","type":"4","ccy":"USDT","amt":"10","ts":"100001","state":"2"}]}`, id)
		}
	}))
	defer srv.Close()
	c := New("k", "s", "p", srv.URL, 100)
	c.UseTestTransport()
	rows, e := c.History(context.Background(), time.UnixMilli(90000), time.UnixMilli(110000))
	if e != nil || len(rows) != 2 {
		t.Fatal(rows, e)
	}
	if rows[0].EventID != "dep-1" || rows[0].Reference != "sender-reference" || rows[0].Receiver != "receiver" || rows[0].Units != 10000001 || rows[1].State != model.Rejected {
		t.Fatal(rows)
	}
	missing = true
	if _, e = c.History(context.Background(), time.UnixMilli(90000), time.UnixMilli(110000)); e == nil {
		t.Fatal("accepted missing depId")
	}
}
