package gram

import (
	"context"
	"encoding/json"
	"fmt"
	"math/big"
	"net/http"
	"net/http/httptest"
	"strconv"
	"testing"
	"time"

	"github.com/xssnick/tonutils-go/tlb"
	"litepay/internal/model"
)

func TestComputeSkippedCreditSafety(t *testing.T) {
	// This public chain transaction funded the receiver before wallet initialization.
	base := sample(t)[4]
	c := New("", "", DefaultAddress)
	for name, tc := range map[string]struct {
		change func(*tlb.Transaction)
		want   bool
	}{
		"real_uninitialized_non_bounceable": {func(*tlb.Transaction) {}, true},
		"existing_uninitialized":            {func(tx *tlb.Transaction) { tx.OrigStatus = tlb.AccountStatusUninit }, true},
		"bounceable":                        {func(tx *tlb.Transaction) { tx.IO.In.AsInternal().Bounce = true }, false},
		"active_account":                    {func(tx *tlb.Transaction) { tx.OrigStatus = tlb.AccountStatusActive }, false},
		"frozen_account":                    {func(tx *tlb.Transaction) { tx.EndStatus = tlb.AccountStatusFrozen }, false},
		"no_credit": {func(tx *tlb.Transaction) {
			d := tx.Description.(tlb.TransactionDescriptionOrdinary)
			d.CreditPhase = nil
			tx.Description = d
		}, false},
		"partial_credit": {func(tx *tlb.Transaction) {
			d := tx.Description.(tlb.TransactionDescriptionOrdinary)
			d.CreditPhase.Credit.Coins = tlb.MustFromNano(big.NewInt(1), 9)
			tx.Description = d
		}, false},
		"no_gas": {func(tx *tlb.Transaction) {
			d := tx.Description.(tlb.TransactionDescriptionOrdinary)
			d.ComputePhase.Phase = tlb.ComputePhaseSkipped{Reason: tlb.ComputeSkipReason{Type: tlb.ComputeSkipReasonNoGas}}
			tx.Description = d
		}, false},
		"fees_consume_credit": {func(tx *tlb.Transaction) { tx.TotalFees.Coins = tx.IO.In.AsInternal().Amount }, false},
		"bounce_phase": {func(tx *tlb.Transaction) {
			d := tx.Description.(tlb.TransactionDescriptionOrdinary)
			d.BouncePhase = &tlb.BouncePhase{Phase: tlb.BouncePhaseNegFunds{}}
			tx.Description = d
		}, false},
	} {
		t.Run(name, func(t *testing.T) {
			row := changed(t, base, tc.change)
			got, _, err := c.decode(row)
			if err != nil || (got.State == model.Confirmed) != tc.want {
				t.Fatal(got, err)
			}
		})
	}
}

func TestHistoryPageMoreThanHundredPagesWithInterruption(t *testing.T) {
	base := sample(t)[0]
	rows := make([]rawTransaction, 10101)
	for i := range rows {
		rows[i] = changed(t, base, func(tx *tlb.Transaction) { tx.LT = uint64(20000 - i); tx.Now = 2000 })
	}
	calls := 0
	fail := false
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls++
		if fail {
			http.Error(w, "temporary", 503)
			return
		}
		start := 0
		if lt := r.URL.Query().Get("lt"); lt != "" {
			n, _ := strconv.Atoi(lt)
			start = 20000 - n
		}
		json.NewEncoder(w).Encode(map[string]any{"ok": true, "result": rows[start:min(start+100, len(rows))]})
	}))
	defer srv.Close()
	c := New(srv.URL, "", DefaultAddress)
	c.UseTestTransport()
	cp := model.TONScanCheckpoint{Receiver: DefaultAddress, From: time.Unix(1999, 0), To: time.Unix(2001, 0), Active: true}
	seen := map[string]bool{}
	for cp.Active {
		if cp.Pages == 30 {
			fail = true
			_, unchanged, err := c.HistoryPage(context.Background(), cp)
			if err == nil || unchanged.LT != cp.LT {
				t.Fatal("failure lost boundary")
			}
			fail = false
			// Reconstruct the client and serialized durable checkpoint as after a restart.
			c = New(srv.URL, "", DefaultAddress)
			c.UseTestTransport()
			raw, _ := json.Marshal(cp)
			json.Unmarshal(raw, &cp)
		}
		page, next, err := c.HistoryPage(context.Background(), cp)
		if err != nil {
			t.Fatal(err, cp.Pages)
		}
		for _, tx := range page {
			if seen[tx.EventID] {
				t.Fatal("duplicate", tx.EventID)
			}
			seen[tx.EventID] = true
		}
		cp = next
		if calls > 120 {
			t.Fatal("pagination stuck")
		}
	}
	if len(seen) != len(rows) || cp.Pages < 100 {
		t.Fatal(fmt.Sprint(len(seen), cp.Pages, calls))
	}
}

func TestHistoryPageInclusiveAnchorAtEndOfHistory(t *testing.T) {
	raw := sample(t)[0]
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		json.NewEncoder(w).Encode(map[string]any{"ok": true, "result": []rawTransaction{raw}})
	}))
	defer srv.Close()
	c := New(srv.URL, "", DefaultAddress)
	c.UseTestTransport()
	cp := model.TONScanCheckpoint{Receiver: DefaultAddress, From: time.Unix(1, 0), To: time.Now(), LT: raw.ID.LT, Hash: raw.ID.Hash, Pages: 1, OldestAt: time.Unix(raw.Utime, 0), Active: true}
	rows, next, err := c.HistoryPage(context.Background(), cp)
	if err != nil || len(rows) != 0 || next.Active {
		t.Fatal(rows, next, err)
	}
}
