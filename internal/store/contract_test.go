package store

import (
	"context"
	"litepay/internal/model"
	"testing"
	"time"
)

func TestChangedMerchantRequestCannotDestroyDisplayedPayment(t *testing.T) {
	st := openTestStore(t)
	ctx := context.Background()
	in := CreateOrderInput{Protocol: "bepusdt", Pid: "bepusdt", TradeNo: "CONTRACT-A", OutTradeNo: "CONTRACT", OriginMoney: "1.00", OriginCurrency: "USD", ExpiredAt: time.Now().Add(time.Hour), ReplaceReady: true}
	a, err := st.CreateOrReuseCashierOrder(ctx, in)
	if err != nil {
		t.Fatal(err)
	}
	a, err = st.SelectPayment(ctx, a.TradeNo, "okx", "USDT", "uid", 1, .01, 99, .01, 2, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	in.TradeNo = "CONTRACT-B"
	in.OriginMoney = "2.00"
	if _, err := st.CreateOrReuseCashierOrder(ctx, in); err == nil {
		t.Fatal("silently replaced displayed payment")
	}
	got, _ := st.GetByTradeNo(ctx, a.TradeNo)
	if got.Status != model.StatusReady || got.PayAmount != a.PayAmount {
		t.Fatal("payment contract mutated")
	}
}
