package server

import (
	"context"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"

	"litepay/internal/model"
	"litepay/internal/store"
)

func TestInquiryEligibilityAndRequestProtection(t *testing.T) {
	engine, _, st := newServerTestEnv(t, nil)
	o, err := st.CreateOrder(context.Background(), store.CreateOrderInput{Pid: "bepusdt", TradeNo: "LP-SELF", OutTradeNo: "SHOP-SELF", OriginMoney: "2.50", OriginCurrency: "USD", ExpiredAt: time.Now().Add(10 * time.Minute)})
	if err != nil {
		t.Fatal(err)
	}
	o, err = st.SelectPaymentDecimal(context.Background(), o.TradeNo, "binance", "USDT", "987654321", "2.50", .01, 99, .01, 2, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	page := httptest.NewRecorder()
	engine.ServeHTTP(page, httptest.NewRequest("GET", "/pay/"+o.TradeNo, nil))
	if !strings.Contains(page.Body.String(), `id="payment-inquiry" hidden`) {
		t.Fatal("early inquiry was not hidden")
	}
	values := url.Values{"trade_no": {o.TradeNo}, "reference": {"reference-1"}}
	send := func(site, origin string, body string) *httptest.ResponseRecorder {
		r := httptest.NewRequest("POST", "/api/order/inquiry", strings.NewReader(body))
		r.Header.Set("Content-Type", "application/x-www-form-urlencoded")
		if site != "" {
			r.Header.Set("Sec-Fetch-Site", site)
		}
		if origin != "" {
			r.Header.Set("Origin", origin)
		}
		w := httptest.NewRecorder()
		engine.ServeHTTP(w, r)
		return w
	}
	if send("same-origin", "", values.Encode()).Code != 409 {
		t.Fatal("server permitted lookup before 1 minute")
	}
	if send("cross-site", "", values.Encode()).Code != 403 || send("", "https://evil.test", values.Encode()).Code != 403 {
		t.Fatal("cross-site inquiry permitted")
	}
	if send("", "", strings.Repeat("x", 3000)).Code != 400 {
		t.Fatal("oversized request accepted")
	}
	values.Set("reference", "<script>")
	if send("", "", values.Encode()).Code != 400 {
		t.Fatal("invalid reference accepted")
	}
	st.DB().Model(o).Update("selected_at", time.Now().Add(-61*time.Second))
	page = httptest.NewRecorder()
	engine.ServeHTTP(page, httptest.NewRequest("GET", "/pay/"+o.TradeNo, nil))
	if strings.Contains(page.Body.String(), `id="payment-inquiry" hidden`) {
		t.Fatal("inquiry remained hidden past 1 minute")
	}
	if err := st.CancelOrder(context.Background(), o.TradeNo); err != nil {
		t.Fatal(err)
	}
	values.Set("reference", "reference-1")
	if send("", "", values.Encode()).Code != 409 {
		t.Fatal("canceled order accepted inquiry")
	}
	page = httptest.NewRecorder()
	engine.ServeHTTP(page, httptest.NewRequest("GET", "/pay/"+o.TradeNo, nil))
	if strings.Contains(page.Body.String(), `id="payment-inquiry"`) {
		t.Fatal("closed order showed automatic inquiry form")
	}
	row, _ := st.GetByTradeNo(context.Background(), o.TradeNo)
	if row.Status != model.StatusCanceled {
		t.Fatal("inquiry changed closed order")
	}
}
