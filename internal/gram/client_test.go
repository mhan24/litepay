package gram

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"math/big"
	"net/http"
	"net/http/httptest"
	"os"
	"strconv"
	"testing"
	"time"

	"github.com/xssnick/tonutils-go/tlb"
	"github.com/xssnick/tonutils-go/tvm/cell"
	"litepay/internal/model"
)

func sample(t *testing.T) []rawTransaction {
	t.Helper()
	b, err := os.ReadFile("testdata/transactions.json")
	if err != nil {
		t.Fatal(err)
	}
	var v struct {
		Result []rawTransaction `json:"result"`
	}
	if err = json.Unmarshal(b, &v); err != nil {
		t.Fatal(err)
	}
	return v.Result
}

func changed(t *testing.T, raw rawTransaction, change func(*tlb.Transaction)) rawTransaction {
	t.Helper()
	b, err := base64.StdEncoding.DecodeString(raw.Data)
	if err != nil {
		t.Fatal(err)
	}
	c, err := cell.FromBOC(b)
	if err != nil {
		t.Fatal(err)
	}
	var tx tlb.Transaction
	if err = tlb.Parse(&tx, c); err != nil {
		t.Fatal(err)
	}
	change(&tx)
	c, err = tx.ToCell()
	if err != nil {
		t.Fatal(err)
	}
	raw.Data = base64.StdEncoding.EncodeToString(c.ToBOC())
	raw.ID.Hash = base64.StdEncoding.EncodeToString(c.Hash())
	raw.ID.LT = strconv.FormatUint(tx.LT, 10)
	raw.Utime = int64(tx.Now)
	return raw
}

func TestActualToncenterTransactions(t *testing.T) {
	c := New("", "", DefaultAddress)
	rows := sample(t)
	if len(rows) != 5 {
		t.Fatal(len(rows))
	}
	for i, r := range rows {
		tx, _, err := c.decode(r)
		if err != nil {
			t.Fatal(i, err)
		}
		if i < 3 && (tx.State != model.Confirmed || tx.Units != 100000 || tx.Currency != "GRAM") {
			t.Fatal(i, tx)
		}
		if i >= 3 && tx.State != model.Rejected {
			t.Fatal("contract/aborted payment accepted", i, tx)
		}
	}
	if tx, _, err := c.decode(rows[0]); err != nil || tx.EventID != "bcc0cf62bf04f0a72370a4e92265bfc94cdac92103cfcb4eed379229f9a209bc" {
		t.Fatal(tx, err)
	}
}

func TestRejectUnsafePaymentsAndMalformedIdentity(t *testing.T) {
	c := New("", "", DefaultAddress)
	base := sample(t)[0]
	cases := map[string]func(*tlb.Transaction){
		"bounced": func(tx *tlb.Transaction) { tx.IO.In.AsInternal().Bounced = true },
		"aborted": func(tx *tlb.Transaction) {
			d := tx.Description.(tlb.TransactionDescriptionOrdinary)
			d.Aborted = true
			tx.Description = d
		},
		"failed_compute": func(tx *tlb.Transaction) {
			d := tx.Description.(tlb.TransactionDescriptionOrdinary)
			v := d.ComputePhase.Phase.(tlb.ComputePhaseVM)
			v.Success = false
			d.ComputePhase.Phase = v
			tx.Description = d
		},
		"nanogram_tail": func(tx *tlb.Transaction) { tx.IO.In.AsInternal().Amount = tlb.MustFromNano(big.NewInt(100000001), 9) },
		"contract_call": func(tx *tlb.Transaction) {
			tx.IO.In.AsInternal().Body = cell.BeginCell().MustStoreUInt(0x7362d09c, 32).EndCell()
		},
	}
	for name, change := range cases {
		t.Run(name, func(t *testing.T) {
			r := changed(t, base, change)
			tx, _, err := c.decode(r)
			if err != nil || tx.State != model.Rejected {
				t.Fatal(tx, err)
			}
		})
	}
	for _, field := range []string{"hash", "lt", "time", "boc"} {
		t.Run(field, func(t *testing.T) {
			r := base
			switch field {
			case "hash":
				r.ID.Hash = base64.StdEncoding.EncodeToString(make([]byte, 32))
			case "lt":
				r.ID.LT = "1"
			case "time":
				r.Utime++
			case "boc":
				r.Data = "bad"
			}
			if _, _, err := c.decode(r); err == nil {
				t.Fatal("invalid transaction accepted")
			}
		})
	}
}

func TestHistoryPaginationAndFailure(t *testing.T) {
	base := sample(t)[0]
	rows := make([]rawTransaction, 101)
	for i := range rows {
		rows[i] = changed(t, base, func(tx *tlb.Transaction) { tx.LT = uint64(1000 - i); tx.Now = 2000 })
	}
	calls := 0
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls++
		if r.URL.Query().Get("address") != DefaultAddress || r.URL.Query().Get("archival") != "true" {
			t.Error("wrong history query")
		}
		page := rows[:100]
		if calls > 1 {
			if r.URL.Query().Get("lt") != rows[99].ID.LT || r.URL.Query().Get("hash") != rows[99].ID.Hash {
				t.Error("missing pagination boundary")
			}
			page = rows[99:]
		}
		json.NewEncoder(w).Encode(map[string]any{"ok": true, "result": page})
	}))
	defer srv.Close()
	c := New(srv.URL, "", DefaultAddress)
	c.UseTestTransport()
	got, err := c.History(context.Background(), time.Unix(1999, 0), time.Unix(2001, 0))
	if err != nil || len(got) != 101 || calls != 2 {
		t.Fatal(len(got), calls, err)
	}
	for _, body := range []string{`{"ok":true}`, `{"ok":true,"result":null}`, `{"ok":false,"result":[]}`, `{"ok":true,"result":{}}`} {
		s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { fmt.Fprint(w, body) }))
		c := New(s.URL, "", DefaultAddress)
		c.UseTestTransport()
		_, err := c.History(context.Background(), time.Unix(1, 0), time.Now())
		s.Close()
		if err == nil {
			t.Fatal("incomplete history accepted", body)
		}
	}
}

func TestBalanceExactPrecision(t *testing.T) {
	rows := sample(t)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/getAddressBalance" {
			fmt.Fprint(w, `{"ok":true,"result":"299946011"}`)
			return
		}
		json.NewEncoder(w).Encode(map[string]any{"ok": true, "result": rows})
	}))
	defer srv.Close()
	c := New(srv.URL, "", DefaultAddress)
	c.UseTestTransport()
	b, err := c.Balance(context.Background())
	if err != nil || b != "0.299946011" {
		t.Fatal(b, err)
	}

}
