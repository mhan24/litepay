package worker

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"litepay/internal/model"
	"litepay/internal/sign"
)

func TestBuildNotifyParamsUsesAddressAndConfiguredPrecision(t *testing.T) {
	txID := "tx-1"
	txTime := time.Date(2026, 10, 18, 12, 0, 0, 0, time.Local)
	order := &model.Order{
		TradeNo:     "LP-1",
		OutTradeNo:  "M-1",
		OriginMoney: "2.50",
		PayAmount:   2.51,
		Currency:    "USDT",
		PayAddress:  "TPayAddress",
		TxID:        &txID,
		TxTime:      &txTime,
		Status:      model.StatusPaid,
	}
	params := BuildNotifyParams(order)
	if params["actual_amount"] != "2.51" {
		t.Fatalf("actual_amount = %q, want 2.51", params["actual_amount"])
	}
	if params["token"] != "TPayAddress" {
		t.Fatalf("token = %q, want wallet address", params["token"])
	}
	if params["status"] != "2" {
		t.Fatalf("status = %q, want 2", params["status"])
	}
}

func TestBuildNotifyPayloadSignsActualAmountAsJSONNumber(t *testing.T) {
	o := &model.Order{
		TradeNo: "LP-EXPIRED", OutTradeNo: "MERCHANT-EXPIRED", OriginMoney: "2.50",
		PayAmount: 2.5100, Currency: "USDT", PayAddress: "TPay",
		Status: model.StatusExpired,
	}
	payload := buildNotifyPayload(BuildNotifyParams(o))
	if payload["actual_amount"] != float64(2.51) {
		t.Fatalf("actual_amount = %#v, want JSON number 2.51", payload["actual_amount"])
	}
	if payload["status"] != 3 {
		t.Fatalf("status = %#v, want 3", payload["status"])
	}
}

func TestNotifyResponseAccepted(t *testing.T) {
	tests := []struct {
		statusCode  int
		body        string
		orderStatus string
		want        bool
	}{
		{200, "success", model.StatusPaid, true},
		{200, " OK ", model.StatusPaid, true},
		{204, " Success\n", model.StatusPaid, true},
		{200, "not success", model.StatusPaid, false},
		{200, "\"success\"", model.StatusPaid, false},
		{200, "{\"message\":\"success\"}", model.StatusPaid, false},
		{200, "fail", model.StatusPaid, false},
		{500, "success", model.StatusPaid, false},
		{302, "success", model.StatusPaid, false},
		{200, "", model.StatusExpired, true},
		{204, "anything", model.StatusExpired, true},
		{200, "fail", model.StatusExpired, false},
		{200, " FAIL\n", model.StatusExpired, false},
		{500, "", model.StatusExpired, false},
		{302, "", model.StatusExpired, false},
	}
	for _, tt := range tests {
		if got := notifyResponseAccepted(tt.statusCode, tt.body, tt.orderStatus); got != tt.want {
			t.Errorf("notifyResponseAccepted(%d, %q, %s) = %v, want %v",
				tt.statusCode, tt.body, tt.orderStatus, got, tt.want)
		}
	}
}

func TestNotifierSignsExactJSONPayloadSent(t *testing.T) {
	const token = "0123456789abcdef0123456789abcdef"
	txID := "tx-1"
	o := &model.Order{
		TradeNo: "LP-NOTIFY", OutTradeNo: "MERCHANT-NOTIFY", OriginMoney: "2.50",
		PayAmount: 2.5100, Currency: "USDC", PayAddress: "receiver-uid",
		TxID: &txID, Status: model.StatusPaid,
	}
	wantPayload := buildNotifyPayload(BuildNotifyParams(o))
	wantSignature := sign.BEpusdtSignGeneric(wantPayload, token)

	var got map[string]interface{}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if err := json.NewDecoder(r.Body).Decode(&got); err != nil {
			t.Errorf("decode notify body: %v", err)
		}
		_, _ = w.Write([]byte("success"))
	}))
	defer server.Close()

	n := &Notifier{transport: http.DefaultTransport}
	payload := buildNotifyPayload(BuildNotifyParams(o))
	payload["signature"] = wantSignature
	status, body, _, err := n.postJSON(context.Background(), 3, server.URL, payload)
	if err != nil || status != http.StatusOK || body != "success" {
		t.Fatalf("postJSON: status=%d body=%q err=%v", status, body, err)
	}
	if got["amount"] != float64(2.5) || got["actual_amount"] != float64(2.51) ||
		got["status"] != float64(2) {
		t.Fatalf("unexpected JSON value types: %#v", got)
	}
	signature, _ := got["signature"].(string)
	delete(got, "signature")
	if gotSignature := sign.BEpusdtSignGeneric(got, token); gotSignature != signature {
		t.Fatalf("signature does not cover exact sent JSON: got=%s want=%s", signature, gotSignature)
	}
}
