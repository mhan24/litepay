package gram

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"litepay/internal/model"
)

func TestFinalityRequiresExactIdentityAndCommittedMasterchainHeight(t *testing.T) {
	raw := sample(t)[0]
	for _, test := range []string{"valid", "missing", "unconfirmed", "future_mc", "wrong_account", "wrong_lt", "wrong_time", "wrong_hash", "not_masterchain", "http_error"} {
		t.Run(test, func(t *testing.T) {
			indexed := map[string]any{"account": DefaultAddress, "hash": raw.ID.Hash, "lt": raw.ID.LT, "now": raw.Utime, "mc_block_seqno": 99, "block_ref": map[string]any{"workchain": 0, "shard": "8000000000000000", "seqno": 1}}
			switch test {
			case "unconfirmed":
				indexed["mc_block_seqno"] = 0
			case "future_mc":
				indexed["mc_block_seqno"] = 101
			case "wrong_account":
				indexed["account"] = "0:0000000000000000000000000000000000000000000000000000000000000000"
			case "wrong_lt":
				indexed["lt"] = "1"
			case "wrong_time":
				indexed["now"] = 1
			case "wrong_hash":
				indexed["hash"] = "invalid"
			}
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if test == "http_error" {
					http.Error(w, "unavailable", 503)
					return
				}
				if r.URL.Path == "/api/v3/masterchainInfo" {
					wc := -1
					if test == "not_masterchain" {
						wc = 0
					}
					json.NewEncoder(w).Encode(map[string]any{"last": map[string]any{"workchain": wc, "seqno": 100}})
					return
				}
				if r.URL.Query().Get("start_lt") != raw.ID.LT || r.URL.Query().Get("account") != DefaultAddress {
					t.Error("incorrect finality window")
				}
				items := []any{indexed}
				if test == "missing" {
					items = []any{}
				}
				json.NewEncoder(w).Encode(map[string]any{"transactions": items})
			}))
			defer srv.Close()
			c := New(srv.URL, "", DefaultAddress)
			c.UseTestTransport()
			tx, _, err := c.decode(raw)
			if err != nil {
				t.Fatal(err)
			}
			rows := []model.Transaction{tx}
			err = c.verifyFinality(context.Background(), rows)
			if test == "valid" {
				if err != nil || rows[0].MasterchainSeqno != 99 {
					t.Fatal(rows, err)
				}
			} else if err == nil {
				t.Fatal("unproven finality accepted", test)
			}
		})
	}
}
