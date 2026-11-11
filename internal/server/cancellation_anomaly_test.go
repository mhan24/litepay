package server

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os/exec"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/glebarez/sqlite"
	"gorm.io/gorm"
	"litepay/internal/config"
	"litepay/internal/model"
	"litepay/internal/settings"
	"litepay/internal/sign"
	"litepay/internal/store"
	"litepay/internal/worker"
)

// A real gateway notification writes the merchant's closed-order success shape,
// then the shipped bridge reconciles it over signed HTTP through manual closure.
func TestCanceledMerchantLateSuccessCallbackCreatesAuditedFundsConflict(t *testing.T) {
	python, err := exec.LookPath("python3")
	if err != nil {
		t.Skip("python3 unavailable")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	root := t.TempDir()
	sourcePath := filepath.Join(root, "merchant.db")
	commerce, err := gorm.Open(sqlite.Open(sourcePath), &gorm.Config{})
	if err != nil {
		t.Fatal(err)
	}
	sourceSQL, _ := commerce.DB()
	defer sourceSQL.Close()
	for _, sql := range []string{
		"CREATE TABLE orders(id INTEGER PRIMARY KEY,status,paid_at,canceled_at,deleted_at)",
		"CREATE TABLE payments(id INTEGER PRIMARY KEY,order_id,channel_id,provider_type,status,paid_at,deleted_at,provider_ref,gateway_order_no,pay_url,exception_code,updated_at)",
		"CREATE TABLE payment_channels(id INTEGER PRIMARY KEY,provider_type,config_json)",
		"CREATE INDEX payments_updates ON payments(updated_at)",
		"INSERT INTO orders VALUES(1,'pending_payment',NULL,NULL,NULL)",
	} {
		if err = commerce.Exec(sql).Error; err != nil {
			t.Fatal(err)
		}
	}
	st, err := store.Open(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	cfg := config.Default()
	cfg.Server.Production = false
	mgr := settings.New(cfg, st)
	if err = mgr.Load(ctx); err != nil {
		t.Fatal(err)
	}
	var callbacks atomic.Int32
	merchant := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var body map[string]any
		decoder := json.NewDecoder(r.Body)
		decoder.UseNumber()
		if decoder.Decode(&body) != nil || !sign.BEpusdtVerifyGeneric(body, mgr.Get().BEpusdt.APIToken) {
			http.Error(w, "invalid", 403)
			return
		}
		// Dujiao's verified callback keeps the canceled order closed and records this exception.
		if err := commerce.Exec("UPDATE payments SET status='success',paid_at=?,exception_code='closed_order_payment_succeeded',updated_at=? WHERE id=1", time.Now(), time.Now()).Error; err != nil {
			http.Error(w, "database", 500)
			return
		}
		callbacks.Add(1)
		w.Write([]byte("success"))
	}))
	defer merchant.Close()
	engine, err := New(mgr, st, verifiedPoller(t, mgr, st))
	if err != nil {
		t.Fatal(err)
	}
	gateway := httptest.NewServer(engine)
	defer gateway.Close()
	_, err = st.Create(ctx, model.Order{TradeNo: "late-callback", OutTradeNo: "closed-merchant-payment", OriginUnits: 10000000, ExpiresAt: time.Now().Add(time.Hour), NotifyURL: merchant.URL})
	if err != nil {
		t.Fatal(err)
	}
	order, err := st.Select(ctx, "late-callback", "binance", "USDT", "receiver", 2, 10000, 10000, 99, 120)
	if err != nil {
		t.Fatal(err)
	}
	channel, _ := json.Marshal(map[string]string{"gateway_url": gateway.URL, "auth_token": mgr.Get().BEpusdt.APIToken})
	if err = commerce.Exec("INSERT INTO payment_channels VALUES(1,'bepusdt',?)", string(channel)).Error; err != nil {
		t.Fatal(err)
	}
	if err = commerce.Exec("INSERT INTO payments VALUES(1,1,1,'bepusdt','expired',NULL,NULL,?,?,?,'',?)", order.TradeNo, order.OutTradeNo, gateway.URL+"/pay/"+order.TradeNo, time.Now()).Error; err != nil {
		t.Fatal(err)
	}
	if err = commerce.Exec("UPDATE orders SET status='canceled',canceled_at=? WHERE id=1", time.Now()).Error; err != nil {
		t.Fatal(err)
	}
	funds := model.Transaction{Provider: "binance", EventID: "late-real-funds", Receiver: "receiver", Currency: "USDT", Units: order.PayUnits, OccurredAt: time.Now(), State: model.Confirmed}
	if result, e := st.Process(ctx, funds, ""); e != nil || result != "paid" {
		t.Fatal(result, e)
	}
	notifier := worker.NewNotifier(mgr, st)
	notifier.UseTestTransport()
	if delivered, e := notifier.Deliver(ctx); e != nil || !delivered || callbacks.Load() != 1 {
		t.Fatal(delivered, e, callbacks.Load())
	}
	var merchantState struct {
		Status string
		PaidAt *time.Time
	}
	if err = commerce.Table("orders").First(&merchantState).Error; err != nil || merchantState.Status != "canceled" || merchantState.PaidAt != nil {
		t.Fatal(merchantState, err)
	}
	script, _ := filepath.Abs("../../scripts/dujiao_cancel_sync.py")
	run := func(action string) {
		t.Helper()
		cmd := exec.CommandContext(ctx, python, "-c", `
import importlib.util,json,sys,time,pathlib
spec=importlib.util.spec_from_file_location('sync',sys.argv[1]);s=importlib.util.module_from_spec(spec);spec.loader.exec_module(s)
gateway=sys.argv[2];root=pathlib.Path(sys.argv[3]);action=sys.argv[4];state=s.journal(root/'journal.db')
s.check_generation(root/'merchant.db',state,gateway)
if action=='scan':
 s.sync_once(root/'merchant.db',state,gateway)
 assert state.execute('SELECT count(*) FROM cancellations').fetchone()[0]==0
 assert state.execute('SELECT outcome FROM payment_anomalies').fetchone()[0]=='paid_conflict'
 assert s.queue_status(state,gateway)['conflicts']==1
 s.report_health(root/'merchant.db',state,gateway,True)
elif action=='unproved':
 try:s.review_conflict(root/'merchant.db',state,gateway,'late-callback','no actual refund evidence yet')
 except ValueError:pass
 else:raise AssertionError('unproved refund closed conflict')
 assert s.queue_status(state,gateway)['conflicts']==1
else:
 s.review_conflict(root/'merchant.db',state,gateway,'late-callback','verified original funds, canceled fulfillment, and actual refund proof')
 assert state.execute('SELECT outcome FROM payment_anomalies').fetchone()[0]=='reviewed_conflict'
 assert state.execute('SELECT count(*) FROM conflict_reviews').fetchone()[0]==1
 assert s.sync_once(root/'merchant.db',state,gateway)==0
 assert s.queue_status(state,gateway)['replay_complete']
 original=time.time;s.time.time=lambda:original()+2
 s.report_health(root/'merchant.db',state,gateway,True)
`, script, gateway.URL, root, action)
		if output, e := cmd.CombinedOutput(); e != nil {
			t.Fatalf("late callback bridge %s: %v: %s", action, e, output)
		}
	}
	run("scan")
	run("unproved")
	var reviews int64
	st.DB().Model(&model.CancellationReview{}).Where("order_id=?", order.ID).Count(&reviews)
	if reviews != 1 {
		t.Fatal("late callback funds absent from refund queue", reviews)
	}
	health, err := st.IntegrationHealth(ctx)
	if err != nil || health.Healthy || health.Conflicts != 1 {
		t.Fatal(health, err)
	}
	if err = st.RecordCancellationRefund(ctx, order.TradeNo, "binance", funds.EventID, "operator", "external-refund-fixture", "verified canceled delivery and actual external refund"); err != nil {
		t.Fatal(err)
	}
	run("review")
	health, err = st.IntegrationHealth(ctx)
	if err != nil || !health.Healthy || health.Conflicts != 0 {
		t.Fatal(health, err)
	}
	paid, err := st.GetByTradeNo(ctx, order.TradeNo)
	if err != nil || paid.Status != model.Paid || callbacks.Load() != 1 {
		t.Fatal(paid, callbacks.Load(), err)
	}
	var stored model.Transaction
	if err = st.DB().First(&stored, "provider=? AND event_id=?", funds.Provider, funds.EventID).Error; err != nil || stored.OrderID == nil || *stored.OrderID != order.ID {
		t.Fatal("original unique claim changed", stored, err)
	}
	if w := getRequest(engine, "/pay/"+order.TradeNo); !strings.Contains(w.Body.String(), "退款凭证已登记") {
		t.Fatal(w.Body.String())
	}
}
