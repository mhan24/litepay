package worker

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"litepay/internal/config"
	"litepay/internal/model"
	"litepay/internal/settings"
	"litepay/internal/store"
)

func TestIsIncomingTypeRejectsRefunds(t *testing.T) {
	if !isIncomingType("pay") || !isIncomingType("C2C") {
		t.Fatal("expected PAY and C2C to be accepted")
	}
	for _, orderType := range []string{"PAY_REFUND", "PAYOUT", "C2C_REFUND", ""} {
		if isIncomingType(orderType) {
			t.Fatalf("%q must never be accepted", orderType)
		}
	}
}

func TestRejectTransactionFiltersBinanceIncomeStrictly(t *testing.T) {
	cfg := config.Default()
	cfg.Checkout.Currencies = []string{"USDT", "USDC"}
	cfg.Checkout.Binance.UID = "987654321"

	base := incomingTransaction{
		Type: "PAY", Currency: "USDT", Amount: 2.51,
		ReceiverID: "987654321", Incoming: true,
	}
	tests := []struct {
		name string
		tx   incomingTransaction
		want transactionRejectReason
	}{
		{name: "PAY income", tx: base, want: transactionAccepted},
		{name: "C2C income", tx: func() incomingTransaction {
			tx := base
			tx.Type = "C2C"
			tx.Currency = "USDC"
			return tx
		}(), want: transactionAccepted},
		{name: "refund", tx: func() incomingTransaction {
			tx := base
			tx.Type = "PAY_REFUND"
			return tx
		}(), want: transactionType},
		{name: "payout", tx: func() incomingTransaction {
			tx := base
			tx.Type = "PAYOUT"
			return tx
		}(), want: transactionType},
		{name: "negative amount", tx: func() incomingTransaction {
			tx := base
			tx.Incoming = false
			return tx
		}(), want: transactionOutgoing},
		{name: "wrong receiver", tx: func() incomingTransaction {
			tx := base
			tx.ReceiverID = "111111111"
			return tx
		}(), want: transactionReceiver},
		{name: "missing receiver", tx: func() incomingTransaction {
			tx := base
			tx.ReceiverID = ""
			return tx
		}(), want: transactionReceiver},
		{name: "unsupported currency", tx: func() incomingTransaction {
			tx := base
			tx.Currency = "BTC"
			return tx
		}(), want: transactionCurrency},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := rejectTransaction(config.ProviderBinance, cfg, &tt.tx); got != tt.want {
				t.Fatalf("reject reason = %d, want %d", got, tt.want)
			}
		})
	}
}

func TestRejectTransactionOnlyAcceptsOKXInternalTransfers(t *testing.T) {
	cfg := config.Default()
	cfg.Checkout.Currencies = []string{"USDT", "USDC"}
	base := incomingTransaction{
		Type: "3", Currency: "USDT", Amount: 2.51,
		ReceiverID: "okx-uid", Incoming: true,
	}
	tests := []struct {
		name string
		tx   incomingTransaction
		want transactionRejectReason
	}{
		{name: "internal", tx: base, want: transactionAccepted},
		{name: "chain deposit", tx: func() incomingTransaction {
			tx := base
			tx.Type = "4"
			return tx
		}(), want: transactionType},
		{name: "missing type", tx: func() incomingTransaction {
			tx := base
			tx.Type = ""
			return tx
		}(), want: transactionType},
		{name: "outgoing", tx: func() incomingTransaction {
			tx := base
			tx.Incoming = false
			return tx
		}(), want: transactionOutgoing},
		{name: "unsupported currency", tx: func() incomingTransaction {
			tx := base
			tx.Currency = "ETH"
			return tx
		}(), want: transactionCurrency},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := rejectTransaction(config.ProviderOKX, cfg, &tt.tx); got != tt.want {
				t.Fatalf("reject reason = %d, want %d", got, tt.want)
			}
		})
	}
}

func TestHasOrdersForProviderIgnoresPendingAndOtherProvider(t *testing.T) {
	orders := []model.Order{
		{TradeNo: "pending", Status: model.StatusPending},
		{TradeNo: "okx", Status: model.StatusReady, Collection: "okx"},
	}
	if hasOrdersForProvider(orders, "binance") {
		t.Fatal("Binance must not poll when only an OKX order exists")
	}
	if !hasOrdersForProvider(orders, "okx") {
		t.Fatal("OKX order must enable its own polling")
	}
}

func TestOKXPollerAcceptsOnlyInternalTransfers(t *testing.T) {
	const uid = "123456789"
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/api/v5/account/config":
			_, _ = w.Write([]byte(`{"code":"0","msg":"","data":[{"uid":"` + uid + `"}]}`))
		case "/api/v5/asset/deposit-history":
			_, _ = w.Write([]byte(`{"code":"0","msg":"","data":[
				{"depId":"internal-usdt","fromWdId":"wd-1","txId":"tx-1","ccy":"USDT","amt":"2.51","state":"2","type":"3","ts":"1760000000000"},
				{"depId":"chain-usdc","fromWdId":"conflicting-withdraw-id","txId":"0xchain","ccy":"USDC","amt":"1.23","state":"2","type":"4","ts":"1760000001000"},
				{"depId":"pending","fromWdId":"wd-2","txId":"tx-2","ccy":"USDT","amt":"3.51","state":"1","type":"3","ts":"1760000002000"},
				{"depId":"missing-type","fromWdId":"wd-3","txId":"tx-3","ccy":"USDT","amt":"4.51","state":"2","ts":"1760000003000"},
				{"depId":"missing-withdraw","txId":"tx-4","ccy":"USDC","amt":"5.51","state":"2","type":"3","ts":"1760000004000"}
			]}`))
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()

	st, err := store.Open(t.TempDir() + "/test.db")
	if err != nil {
		t.Fatal(err)
	}
	cfg := config.Default()
	cfg.OKX.BaseURL = server.URL
	cfg.OKX.APIKey = "key"
	cfg.OKX.APISecret = "secret"
	cfg.OKX.Passphrase = "pass"
	cfg.Checkout.OKX.UID = uid
	cfg.Checkout.Currencies = []string{"USDT", "USDC"}
	mgr := settings.New(cfg, st)
	p := NewPoller(mgr, st)

	txs, err := p.okxTransactions(context.Background(), cfg, time.UnixMilli(0))
	if err != nil {
		t.Fatal(err)
	}
	if len(txs) != 2 {
		t.Fatalf("transactions = %d, want both completed internal transfers: %+v", len(txs), txs)
	}
	if txs[0].ID != "okx:internal-usdt" || txs[0].Type != "3" ||
		txs[0].Amount != 2.51 || txs[0].Currency != "USDT" {
		t.Fatalf("unexpected internal transfer: %+v", txs[0])
	}
	if txs[1].ID != "okx:missing-type" || txs[1].Type != "3" || txs[1].Amount != 4.51 {
		t.Fatalf("missing-type internal transfer lost: %+v", txs[1])
	}
	if txs[0].ReceiverAddr != uid {
		t.Fatalf("receiver uid = %q", txs[0].ReceiverAddr)
	}
	var raw map[string]any
	if err := json.Unmarshal([]byte(txs[0].Raw), &raw); err != nil || raw["fromWdId"] != "wd-1" {
		t.Fatalf("raw internal transfer invalid: %s", txs[0].Raw)
	}
}

func TestOKXPollerRejectsConfiguredUIDMismatch(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(`{"code":"0","msg":"","data":[{"uid":"999999999"}]}`))
	}))
	defer server.Close()

	st, err := store.Open(t.TempDir() + "/test.db")
	if err != nil {
		t.Fatal(err)
	}
	cfg := config.Default()
	cfg.OKX.BaseURL = server.URL
	cfg.OKX.APIKey = "key"
	cfg.OKX.APISecret = "secret"
	cfg.OKX.Passphrase = "pass"
	cfg.Checkout.OKX.UID = "123456789"
	p := NewPoller(settings.New(cfg, st), st)
	if _, err := p.okxTransactions(context.Background(), cfg, time.UnixMilli(0)); err == nil {
		t.Fatal("accepted API account UID different from configured receiver UID")
	}
}

func TestBinancePollerRejectsConfiguredUIDMismatch(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/api/v3/account":
			_, _ = w.Write([]byte(`{"uid":999999999}`))
		case "/sapi/v1/pay/transactions":
			t.Fatal("must not query payment history before account UID validation")
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()

	st, err := store.Open(t.TempDir() + "/test.db")
	if err != nil {
		t.Fatal(err)
	}
	cfg := config.Default()
	cfg.Binance.BaseURL = server.URL
	cfg.Binance.APIKey = "key"
	cfg.Binance.APISecret = "secret"
	cfg.Checkout.Binance.UID = "123456789"
	p := NewPoller(settings.New(cfg, st), st)
	if _, err := p.binanceTransactions(context.Background(), cfg,
		time.Now().Add(-time.Minute), time.Now()); err == nil {
		t.Fatal("accepted API account UID different from configured receiver UID")
	}
}

func TestOKXInternalTransferRejectsWrongUIDAmountCurrencyAndDuplicate(t *testing.T) {
	st, err := store.Open(t.TempDir() + "/test.db")
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	o, err := st.CreateOrder(ctx, store.CreateOrderInput{
		Protocol: "bepusdt", Pid: "bepusdt", TradeNo: "LP-OKX-INTERNAL",
		OutTradeNo: "MERCHANT-OKX-INTERNAL", OriginMoney: "7.30", OriginCurrency: "USD",
		ExpiredAt: time.Now().Add(10 * time.Minute), Reselect: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	o, err = st.SelectPayment(ctx, o.TradeNo, "okx", "USDT", "okx-uid",
		1, 0.01, 99, 0.01, 2, 2*time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	base := model.IncomingTx{
		TransactionID: "okx:internal-1", Provider: "okx", Type: "3",
		Amount: o.PayAmount, Currency: "USDT",
		ReceiverID: "okx-uid", ReceiverAddr: "okx-uid",
		TransactionTime: time.Now(),
	}
	wrongUID := base
	wrongUID.ReceiverAddr = "other-uid"
	if _, err := st.ClaimAndPay(ctx, o, wrongUID, 2); err == nil {
		t.Fatal("accepted wrong receiver UID")
	}
	wrongAmount := base
	wrongAmount.Amount += 0.01
	if _, err := st.ClaimAndPay(ctx, o, wrongAmount, 2); err == nil {
		t.Fatal("accepted wrong amount")
	}
	wrongCurrency := base
	wrongCurrency.Currency = "USDC"
	if _, err := st.ClaimAndPay(ctx, o, wrongCurrency, 2); err == nil {
		t.Fatal("accepted wrong currency")
	}
	ok, err := st.ClaimAndPay(ctx, o, base, 2)
	if err != nil || !ok {
		t.Fatalf("valid internal transfer failed: ok=%v err=%v", ok, err)
	}
	paid, _ := st.GetByTradeNo(ctx, o.TradeNo)
	if paid.Status != model.StatusPaid {
		t.Fatalf("status = %s", paid.Status)
	}
	dup, err := st.ClaimAndPay(ctx, o, base, 2)
	if dup {
		t.Fatalf("duplicate transfer was claimed again")
	}
	if err == nil {
		t.Fatal("duplicate transfer should be rejected because the order is already paid")
	}
}
