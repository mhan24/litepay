package worker

import (
	"context"
	"fmt"
	"litepay/internal/config"
	"litepay/internal/model"
	"litepay/internal/settings"
	"litepay/internal/store"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"testing"
	"time"
)

func TestAuxiliaryEvidenceNeverPaysEvenWhenIDsAndAmountsCoincide(t *testing.T) {
	api := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/api/v5/public/time":
			fmt.Fprintf(w, `{"code":"0","data":[{"ts":"%d"}]}`, time.Now().UnixMilli())
		case "/api/v5/account/config":
			fmt.Fprint(w, `{"code":"0","data":[{"uid":"uid"}]}`)
		case "/api/v5/asset/bills":
			fmt.Fprintf(w, `{"code":"0","data":[{"billId":"same-id","ccy":"USDT","type":"72","balChg":"1.01","ts":"%d","notes":"private"}]}`, time.Now().UnixMilli())
		default:
			fmt.Fprintf(w, `{"code":"0","data":[{"depId":"same-id","fromWdId":"sender","ccy":"USDT","amt":"1.01","state":"2","type":"3","ts":"%d"}]}`, time.Now().UnixMilli())
		}
	}))
	defer api.Close()
	cfg := config.Default()
	cfg.Database.Path = filepath.Join(t.TempDir(), "aux.db")
	cfg.Checkout.Currencies = []string{"USDT"}
	cfg.OKX.APIKey = "key"
	cfg.OKX.APISecret = "secret"
	cfg.OKX.Passphrase = "pass"
	cfg.OKX.BaseURL = api.URL
	cfg.Checkout.OKX.UID = "uid"
	st, err := store.Open(cfg.Database.Path)
	if err != nil {
		t.Fatal(err)
	}
	o, err := st.CreateOrder(context.Background(), store.CreateOrderInput{Pid: "bepusdt", TradeNo: "AUX", OutTradeNo: "AUX", OriginMoney: "1.00", OriginCurrency: "USD", ExpiredAt: time.Now().Add(time.Hour)})
	if err != nil {
		t.Fatal(err)
	}
	o, err = st.SelectPayment(context.Background(), o.TradeNo, "okx", "USDT", "uid", 1, .01, 99, .01, 2, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	p := NewPoller(settings.New(cfg, st), st)
	if err := p.ReconcileOKXBills(context.Background()); err != nil {
		t.Fatal(err)
	}
	run, rows, err := st.AuxiliaryEvidence(context.Background())
	if err != nil || run == nil || len(rows) != 1 || rows[0].Result != "candidate_only" {
		t.Fatalf("%+v %+v %v", run, rows, err)
	}
	current, _ := st.GetByTradeNo(context.Background(), o.TradeNo)
	if current.Status != model.StatusReady {
		t.Fatal("auxiliary evidence paid order")
	}
	var count int64
	st.DB().Model(&model.IncomingTx{}).Count(&count)
	if count != 0 {
		t.Fatal("auxiliary created claim")
	}
	cursor, _ := st.ScanCursor(context.Background(), "okx")
	if cursor != nil && !cursor.ScannedUntil.IsZero() {
		t.Fatal("auxiliary advanced cursor")
	}
	if err := p.ReconcileOKXBills(context.Background()); err == nil {
		t.Fatal("auxiliary cooldown bypassed")
	}
}
