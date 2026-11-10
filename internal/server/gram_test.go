package server

import (
	"bytes"
	"context"
	"encoding/base64"
	"image/png"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"

	"golang.org/x/net/html"

	"litepay/internal/config"
	"litepay/internal/gram"
	"litepay/internal/model"
	"litepay/internal/settings"
	"litepay/internal/store"
	"litepay/internal/worker"
)

func TestGRAMCashierAndSettings(t *testing.T) {
	ctx := context.Background()
	st, err := store.Open(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	cfg := config.Default()
	cfg.Admin.Password = "local-test-password"
	cfg.TON.Address = gram.DefaultAddress
	cfg.Checkout.Currencies = []string{"USDT", "USDC", "GRAM"}
	mgr := settings.New(cfg, st)
	if err = mgr.Load(ctx); err != nil {
		t.Fatal(err)
	}
	p := worker.NewPoller(mgr, st)
	engine, err := New(mgr, st, p)
	if err != nil {
		t.Fatal(err)
	}
	_, err = st.Create(ctx, model.Order{TradeNo: "gram-ui", OutTradeNo: "gram-ui", OriginUnits: 10_000_000, Reselect: true, ExpiresAt: time.Now().Add(time.Minute)})
	if err != nil {
		t.Fatal(err)
	}
	// A missing current quote and invalid provider/currency combinations never reserve money.
	for _, pair := range [][2]string{{"ton", "GRAM"}, {"ton", "USDT"}, {"binance", "GRAM"}} {
		w := postForm(engine, "/api/order/payment", url.Values{"trade_no": {"gram-ui"}, "provider": {pair[0]}, "currency": {pair[1]}})
		if w.Code != 400 {
			t.Fatal("invalid payment choice accepted", pair, w.Body.String())
		}
	}
	_, err = st.SelectGRAM(ctx, "gram-ui", gram.DefaultAddress, gram.Snapshot{Fresh: true, USD: "1.11", PriceAt: time.Now()}, 120)
	if err != nil {
		t.Fatal(err)
	}
	w := httptest.NewRecorder()
	engine.ServeHTTP(w, httptest.NewRequest("GET", "/pay/gram-ui", nil))
	for _, text := range []string{"9.009010", "收款地址", "1 GRAM = 1.11 USD", "ton://transfer/" + gram.DefaultAddress + "?amount=9009010000", "手续费另付", "右上角「扫一扫」", "Telegram 钱包扫码付款"} {
		if !strings.Contains(w.Body.String(), text) {
			t.Fatal("missing GRAM payment information", text, w.Body.String())
		}
	}
	doc, err := html.Parse(strings.NewReader(w.Body.String()))
	if err != nil {
		t.Fatal(err)
	}
	var qrSource string
	var findQR func(*html.Node)
	findQR = func(n *html.Node) {
		if n.Type == html.ElementNode && n.Data == "img" {
			attrs := map[string]string{}
			for _, a := range n.Attr {
				attrs[a.Key] = a.Val
			}
			if attrs["id"] == "gram-payment-qr" {
				qrSource = attrs["src"]
			}
		}
		for child := n.FirstChild; child != nil; child = child.NextSibling {
			findQR(child)
		}
	}
	findQR(doc)
	if !strings.HasPrefix(qrSource, "data:image/png;base64,") {
		t.Fatal("missing local payment QR image")
	}
	imageBytes, err := base64.StdEncoding.DecodeString(strings.TrimPrefix(qrSource, "data:image/png;base64,"))
	if err != nil {
		t.Fatal(err)
	}
	image, err := png.Decode(bytes.NewReader(imageBytes))
	if err != nil || image.Bounds().Dx() != 480 || image.Bounds().Dy() != 480 {
		t.Fatal("invalid payment QR PNG", err)
	}
	for _, path := range []string{"/pay/gram-ui?reselect=1", "/pay/gram-ui?reselect=1&provider=ton"} {
		w := httptest.NewRecorder()
		engine.ServeHTTP(w, httptest.NewRequest("GET", path, nil))
		if strings.Contains(w.Body.String(), `id="gram-payment-qr"`) {
			t.Fatal("old QR exposed during payment reselection", path)
		}
	}
	for _, text := range []string{"ZgotmplZ", "1 GRAM = 1 USD", "复制 UID", "向此 UID"} {
		if strings.Contains(w.Body.String(), text) {
			t.Fatal("incorrect GRAM information", text)
		}
	}
	methods := bepusdtPaymentMethods(cfg, &model.Order{OriginUnits: 10_000_000}, gram.Snapshot{Fresh: true, USD: "1.11", PriceAt: time.Now()})
	if len(methods) != 1 || methods[0]["network"] != "TON" || methods[0]["actual_amount"] != "9.00901" {
		t.Fatal(methods)
	}
	auth := newAdminAuth(mgr.Get())
	token, _, err := auth.issue()
	if err != nil {
		t.Fatal(err)
	}
	for _, path := range []string{"/admin/settings", "/admin/gram/status"} {
		w := httptest.NewRecorder()
		req := httptest.NewRequest("GET", path, nil)
		req.AddCookie(&http.Cookie{Name: adminCookieName, Value: token})
		engine.ServeHTTP(w, req)
		if w.Code != 200 || strings.Contains(w.Body.String(), "<no value>") {
			t.Fatal(path, w.Code, w.Body.String())
		}
	}
	// The monitoring endpoint uses the existing admin authentication.
	w = httptest.NewRecorder()
	engine.ServeHTTP(w, httptest.NewRequest("GET", "/admin/gram/status", nil))
	if w.Code == 200 {
		t.Fatal("private balance exposed")
	}
	// Native collection settings survive the same SQLite patch path as exchange settings.
	interval := 7
	if err = mgr.Update(ctx, settings.Patch{TONPollInterval: &interval}, "test"); err != nil {
		t.Fatal(err)
	}
	reloaded := settings.New(config.Default(), st)
	if err = reloaded.Load(ctx); err != nil {
		t.Fatal(err)
	}
	if reloaded.Get().TON.Address != cfg.TON.Address || reloaded.Get().TON.PollInterval != 7 {
		t.Fatal("GRAM settings not persisted")
	}
}
