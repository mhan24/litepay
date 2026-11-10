package worker

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"math/big"
	"net/http"
	"net/http/httptest"
	"os"
	"strconv"
	"testing"
	"time"

	"github.com/xssnick/tonutils-go/tlb"
	"github.com/xssnick/tonutils-go/tvm/cell"
	"litepay/internal/config"
	"litepay/internal/gram"
	"litepay/internal/model"
	"litepay/internal/settings"
	"litepay/internal/sign"
	"litepay/internal/store"
)

func TestGRAMScanLocksQuoteAndDeliversOnce(t *testing.T) {
	ctx := context.Background()
	st, err := store.Open(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	const address = "UQAVr5gouSRmaPaNPNmNFqO7VicqWr56h-qyn735bfrOYisp"
	var response struct {
		Result []struct {
			Data  string            `json:"data"`
			Utime int64             `json:"utime"`
			ID    map[string]string `json:"transaction_id"`
		} `json:"result"`
	}
	b, err := os.ReadFile("../gram/testdata/transactions.json")
	if err != nil {
		t.Fatal(err)
	}
	if err = json.Unmarshal(b, &response); err != nil {
		t.Fatal(err)
	}
	raw := response.Result[0]
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/api/v3/masterchainInfo" {
			json.NewEncoder(w).Encode(map[string]any{"last": map[string]any{"workchain": -1, "seqno": 100}})
			return
		}
		if r.URL.Path == "/api/v3/transactions" {
			json.NewEncoder(w).Encode(map[string]any{"transactions": []any{map[string]any{"account": address, "hash": raw.ID["hash"], "lt": raw.ID["lt"], "now": raw.Utime, "mc_block_seqno": 99, "block_ref": map[string]any{"workchain": 0, "shard": "8000000000000000", "seqno": 1}}}})
			return
		}
		json.NewEncoder(w).Encode(map[string]any{"ok": true, "result": []any{raw}})
	}))
	defer srv.Close()
	cfg := config.Default()
	cfg.Server.Production = false
	cfg.TON.Address = address
	cfg.TON.BaseURL = srv.URL
	cfg.Checkout.Currencies = []string{"GRAM"}
	mgr := settings.New(cfg, st)
	if err = mgr.Load(ctx); err != nil {
		t.Fatal(err)
	}
	var callbacks int
	merchant := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		callbacks++
		var p map[string]any
		d := json.NewDecoder(r.Body)
		d.UseNumber()
		if d.Decode(&p) != nil || !sign.BEpusdtVerifyGeneric(p, mgr.Get().BEpusdt.APIToken) {
			t.Error("invalid callback signature")
		}
		if p["amount"] != json.Number("10") || p["actual_amount"] != json.Number("9.00901") || p["token"] != address {
			t.Error("USD/GRAM callback amounts conflated", p)
		}
		w.Write([]byte("success"))
	}))
	defer merchant.Close()
	quote := gram.Snapshot{Fresh: true, USD: "1.11", PriceAt: time.Now()}
	for _, no := range []string{"gram-a", "gram-b"} {
		_, err = st.Create(ctx, model.Order{TradeNo: no, OutTradeNo: no, OriginUnits: 10_000_000, Reselect: true, ExpiresAt: time.Now().Add(time.Minute), NotifyURL: merchant.URL})
		if err != nil {
			t.Fatal(err)
		}
		if _, err = st.SelectGRAM(ctx, no, address, quote, 120); err != nil {
			t.Fatal(err)
		}
	}
	a, _ := st.GetByTradeNo(ctx, "gram-a")
	bOrder, _ := st.GetByTradeNo(ctx, "gram-b")
	if a.PayUnits != 9009010 || bOrder.PayUnits != 9009011 || a.RateUSD != "1.11" || a.OriginUnits != 10_000_000 {
		t.Fatal(a, bOrder)
	}
	// A repeated selection keeps the locked price even if the market has moved or is unavailable.
	locked, err := st.SelectGRAM(ctx, "gram-a", address, gram.Snapshot{}, 120)
	if err != nil || locked.PayUnits != a.PayUnits || locked.RateUSD != a.RateUSD {
		t.Fatal(locked, err)
	}
	_, err = st.Create(ctx, model.Order{TradeNo: "stale", OutTradeNo: "stale", OriginUnits: 10_000_000, ExpiresAt: time.Now().Add(time.Minute)})
	if err != nil {
		t.Fatal(err)
	}
	if _, err = st.SelectGRAM(ctx, "stale", address, gram.Snapshot{}, 120); err == nil {
		t.Fatal("new quote accepted without a current price")
	}
	var slots int64
	st.DB().Model(&model.Slot{}).Count(&slots)
	if slots != 2 {
		t.Fatal("failed quote reserved a slot", slots)
	}
	// Rebuild an actual serialized incoming transaction for the locked payment amount.
	encoded, _ := base64.StdEncoding.DecodeString(raw.Data)
	root, err := cell.FromBOC(encoded)
	if err != nil {
		t.Fatal(err)
	}
	var tx tlb.Transaction
	if err = tlb.Parse(&tx, root); err != nil {
		t.Fatal(err)
	}
	tx.Now = uint32(time.Now().Unix())
	tx.IO.In.AsInternal().Amount = tlb.MustFromNano(big.NewInt(a.PayUnits*1000), 9)
	root, err = tx.ToCell()
	if err != nil {
		t.Fatal(err)
	}
	raw.Data = base64.StdEncoding.EncodeToString(root.ToBOC())
	raw.Utime = int64(tx.Now)
	raw.ID["hash"] = base64.StdEncoding.EncodeToString(root.Hash())
	raw.ID["lt"] = strconv.FormatUint(tx.LT, 10)
	st.DB().Model(&model.Order{}).Where("provider = ?", "ton").Update("selected_at", time.Now().Add(-time.Second))
	syncAttemptFixture(t, st)
	p := NewPoller(mgr, st)
	p.UseTestTransports()
	for range 2 {
		if err = p.Scan(ctx, "ton"); err != nil {
			t.Fatal(err)
		}
	}
	a, _ = st.GetByTradeNo(ctx, "gram-a")
	bOrder, _ = st.GetByTradeNo(ctx, "gram-b")
	if a.Status != model.Paid || bOrder.Status != model.Ready {
		t.Fatal("wrong order claimed", a.Status, bOrder.Status)
	}
	n := NewNotifier(mgr, st)
	n.UseTestTransport()
	if did, err := n.Deliver(ctx); err != nil || !did {
		t.Fatal(did, err)
	}
	if did, err := n.Deliver(ctx); err != nil || did || callbacks != 1 {
		t.Fatal(did, err, callbacks)
	}
}

func TestGRAMOffsetNeverExceedsQuoteTolerance(t *testing.T) {
	ctx := context.Background()
	st, err := store.Open(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	s := gram.Snapshot{Fresh: true, USD: "1", PriceAt: time.Now()}
	for i := range 3 {
		no := strconv.Itoa(i)
		_, err = st.Create(ctx, model.Order{TradeNo: no, OutTradeNo: no, OriginUnits: 1000, ExpiresAt: time.Now().Add(time.Minute)})
		if err != nil {
			t.Fatal(err)
		}
		o, err := st.SelectGRAM(ctx, no, "receiver", s, 120)
		if i < 2 && (err != nil || o.PayUnits > 1001) {
			t.Fatal(o, err)
		}
		if i == 2 && err == nil {
			t.Fatal("offset exceeded 0.1%")
		}
	}
}
