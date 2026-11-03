package okx

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func TestFundingBillsPaginationAndSafety(t *testing.T) {
	for _, scenario := range []string{"complete", "stuck", "conflict", "null", "rate", "negative"} {
		t.Run(scenario, func(t *testing.T) {
			calls := 0
			api := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.URL.Path == "/api/v5/public/time" {
					fmt.Fprintf(w, `{"code":"0","data":[{"ts":"%d"}]}`, time.Now().UnixMilli())
					return
				}
				calls++
				if r.URL.Query().Get("ccy") != "USDT" || r.URL.Query().Get("type") != "72" {
					t.Error("missing filters")
				}
				if scenario == "rate" {
					w.WriteHeader(429)
					return
				}
				if scenario == "null" {
					fmt.Fprint(w, `{"code":"0","data":null}`)
					return
				}
				if calls > 1 && scenario != "stuck" {
					if r.URL.Query().Get("after") != "2001" {
						t.Error("after must be documented timestamp, not billId")
					}
					if scenario == "conflict" {
						fmt.Fprint(w, `{"code":"0","data":[{"billId":"900","ccy":"USDT","type":"72","balChg":"99","ts":"3000"}]}`)
					} else {
						fmt.Fprint(w, `{"code":"0","data":[]}`)
					}
					return
				}
				value := "2.510000"
				if scenario == "negative" {
					value = "-2.51"
				}
				fmt.Fprintf(w, `{"code":"0","data":[{"billId":"900","ccy":"USDT","type":"72","balChg":"%s","ts":"3000","notes":"private-email","bal":"secret"},{"billId":"800","ccy":"USDT","type":"72","balChg":"1.00","ts":"2000"}]}`, value)
			}))
			defer api.Close()
			cli := New("key", "secret", "pass", api.URL, 2)
			cli.UseTestTransport()
			rows, err := cli.FundingBillsSince(context.Background(), "USDT", time.UnixMilli(1000))
			if scenario == "complete" || scenario == "negative" {
				if err != nil || len(rows) != 2 {
					t.Fatalf("%+v %v", rows, err)
				}
				if rows[0].Incoming != (scenario != "negative") {
					t.Fatal("direction lost")
				}
			} else if err == nil {
				t.Fatal("incomplete/failed query reported complete")
			}
			if scenario == "conflict" && !errors.Is(err, ErrBillsInconsistent) {
				t.Fatalf("expected conflict: %v", err)
			}
		})
	}
}
