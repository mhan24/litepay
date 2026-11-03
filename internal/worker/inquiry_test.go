package worker

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"litepay/internal/config"
	"litepay/internal/model"
	"litepay/internal/settings"
	"litepay/internal/store"
)

func TestCustomerInquiryRejectsUnsafeEvidence(t *testing.T) {
	for _, provider := range []string{"binance", "okx"} {
		for _, scenario := range []string{"valid", "order_reference", "missing_order_reference", "early", "missing", "wrong_amount", "wrong_currency", "wrong_type", "wrong_uid", "old_time", "future_time", "pending", "closed", "api_error", "different_reference", "consumed", "ambiguous", "no_depid"} {
			t.Run(provider+"/"+scenario, func(t *testing.T) {
				if provider == "okx" && (scenario == "order_reference" || scenario == "missing_order_reference") {
					t.Skip("Binance optional orderId field")
				}
				ctx := context.Background()
				now := time.Now()
				var calls atomic.Int32
				api := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					calls.Add(1)
					if r.URL.Path == "/api/v3/account" {
						uid := "receiver"
						if scenario == "wrong_uid" {
							uid = "other"
						}
						json.NewEncoder(w).Encode(map[string]any{"uid": uid})
						return
					}
					if r.URL.Path == "/api/v5/account/config" {
						uid := "receiver"
						if scenario == "wrong_uid" {
							uid = "other"
						}
						json.NewEncoder(w).Encode(map[string]any{"code": "0", "data": []any{map[string]string{"uid": uid}}})
						return
					}
					if r.URL.Path == "/api/v5/public/time" {
						json.NewEncoder(w).Encode(map[string]any{"code": "0", "data": []any{map[string]string{"ts": "1"}}})
						return
					}
					if scenario == "api_error" {
						w.WriteHeader(503)
						return
					}
					amount, currency, kind, id, stamp := "2.51", "USDT", "PAY", "reference-1", now.UnixMilli()
					if scenario == "wrong_amount" {
						amount = "9.99"
					}
					if scenario == "wrong_currency" {
						currency = "USDC"
					}
					if scenario == "wrong_type" {
						kind = "PAY_REFUND"
					}
					if scenario == "old_time" {
						stamp = now.Add(-2 * time.Minute).UnixMilli()
					}
					if scenario == "future_time" {
						stamp = now.Add(time.Hour).UnixMilli()
					}
					if scenario == "different_reference" {
						id = "another"
					}
					rows := []any{}
					if scenario != "missing" {
						if provider == "binance" {
							rows = append(rows, map[string]any{"orderId": "app-order-77", "transactionId": id, "transactionTime": stamp, "orderType": kind, "currency": currency, "amount": amount, "receiverInfo": map[string]string{"binanceId": "receiver"}})
						} else {
							if r.URL.Query().Get("fromWdId") != "reference-1" || r.URL.Query().Get("type") != "3" || r.URL.Query().Get("ccy") != "USDT" {
								t.Error("missing targeted OKX filters")
							}
							kind = "3"
							if scenario == "wrong_type" {
								kind = "4"
							}
							state := "2"
							if scenario == "pending" {
								state = "1"
							}
							dep := "deposit-1"
							if scenario == "no_depid" {
								dep = ""
							}
							row := map[string]any{"depId": dep, "fromWdId": id, "ccy": currency, "amt": amount, "type": kind, "state": state, "ts": stamp}
							rows = append(rows, row)
							if scenario == "ambiguous" {
								rows = append(rows, map[string]any{"depId": "deposit-2", "fromWdId": id, "ccy": currency, "amt": amount, "type": kind, "state": state, "ts": stamp})
							}
						}
					}
					code := "0"
					if provider == "binance" {
						code = "000000"
					}
					json.NewEncoder(w).Encode(map[string]any{"code": code, "success": true, "data": rows})
				}))
				defer api.Close()
				cfg := config.Default()
				cfg.Server.BaseURL = "https://pay.example.test"
				cfg.BEpusdt.APIToken = "inquiry-test-token"
				cfg.Database.Path = filepath.Join(t.TempDir(), "test.db")
				cfg.Checkout.Binance.UID = "receiver"
				cfg.Checkout.OKX.UID = "receiver"
				cfg.Binance.APIKey = "key"
				cfg.Binance.APISecret = "secret"
				cfg.Binance.BaseURL = api.URL
				cfg.OKX.APIKey = "key"
				cfg.OKX.APISecret = "secret"
				cfg.OKX.Passphrase = "pass"
				cfg.OKX.BaseURL = api.URL
				st, err := store.Open(cfg.Database.Path)
				if err != nil {
					t.Fatal(err)
				}
				o, err := st.CreateOrder(ctx, store.CreateOrderInput{Pid: "bepusdt", TradeNo: "LP-INQUIRY", OutTradeNo: "ORDER", OriginMoney: "2.50", OriginCurrency: "USD", ExpiredAt: now.Add(10 * time.Minute)})
				if err != nil {
					t.Fatal(err)
				}
				o, err = st.SelectPaymentDecimal(ctx, o.TradeNo, provider, "USDT", "receiver", "2.50", .01, 99, .01, 2, time.Minute)
				if err != nil {
					t.Fatal(err)
				}
				if scenario != "early" {
					st.DB().Model(o).Updates(map[string]any{"created_at": now.Add(-2 * time.Minute), "selected_at": now.Add(-61 * time.Second)})
				}
				if scenario == "closed" {
					if err := st.CancelOrder(ctx, o.TradeNo); err != nil {
						t.Fatal(err)
					}
				}
				if scenario == "consumed" {
					id := "reference-1"
					if provider == "okx" {
						id = "okx:deposit-1"
					}
					if err := st.DB().Create(&model.IncomingTx{Provider: provider, TransactionID: id, OrderTradeNo: "OTHER", Type: "PAY", Currency: "USDT", AmountText: "2.51"}).Error; err != nil {
						t.Fatal(err)
					}
				}
				p := NewPoller(settings.New(cfg, st), st)
				reference := "reference-1"
				if provider == "binance" && scenario == "order_reference" {
					reference = "app-order-77"
				}
				if provider == "binance" && scenario == "missing_order_reference" {
					reference = "app-order-not-returned"
				}
				result, err := p.CheckPayment(ctx, o.TradeNo, reference)
				wantPaid := scenario == "valid" || (provider == "binance" && (scenario == "order_reference" || scenario == "pending" || scenario == "ambiguous" || scenario == "no_depid"))
				if wantPaid {
					if err != nil || result != "paid" {
						t.Fatalf("expected paid: %s %v", result, err)
					}
					again, err := p.CheckPayment(ctx, o.TradeNo, "reference-1")
					if again != "paid" || err != nil {
						t.Fatal("paid retry not idempotent")
					}
				} else {
					current, _ := st.GetByTradeNo(ctx, o.TradeNo)
					if current.Status == model.StatusPaid {
						t.Fatalf("unsafe evidence paid: %s %v", result, err)
					}
				}
				if scenario == "early" && !errors.Is(err, ErrInquiryEarly) {
					t.Fatalf("early: %v", err)
				}
				if (scenario == "early" || scenario == "closed") && calls.Load() != 0 {
					t.Fatal("ineligible order reached exchange")
				}
				if scenario == "missing" {
					if result != "not_found" || err != nil {
						t.Fatalf("not found: %s %v", result, err)
					}
					fresh := NewPoller(settings.New(cfg, st), st)
					_, err = fresh.CheckPayment(ctx, o.TradeNo, "reference-1")
					if !errors.Is(err, store.ErrInquiryLimited) {
						t.Fatalf("restart bypassed rate limit: %v", err)
					}
				}
				cursor, err := st.ScanCursor(ctx, provider)
				if err != nil {
					t.Fatal(err)
				}
				if cursor != nil && !cursor.ScannedUntil.IsZero() {
					t.Fatal("targeted inquiry advanced scan cursor")
				}
			})
		}
	}
}
