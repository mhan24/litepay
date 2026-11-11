package server

import (
	"context"
	"encoding/json"
	"net/http/httptest"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"litepay/internal/config"
	"litepay/internal/model"
	"litepay/internal/settings"
	"litepay/internal/store"
)

// Exercise the shipped Python bridge against Gin and real payment invariants.
func TestPythonCancellationBridgeAgainstGateway(t *testing.T) {
	python, err := exec.LookPath("python3")
	if err != nil {
		t.Skip("python3 unavailable")
	}
	ctx := context.Background()
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
	engine, err := New(mgr, st, verifiedPoller(t, mgr, st))
	if err != nil {
		t.Fatal(err)
	}
	gateway := httptest.NewServer(engine)
	defer gateway.Close()
	var payments []model.Transaction
	for _, no := range []string{"bridge-cancel", "bridge-paid"} {
		_, err = st.Create(ctx, model.Order{TradeNo: no, OutTradeNo: no, OriginUnits: 10000000, ExpiresAt: time.Now().Add(time.Minute)})
		if err != nil {
			t.Fatal(err)
		}
		o, e := st.Select(ctx, no, "binance", "USDT", "receiver", 2, 10000, 10000, 99, 120)
		if e != nil {
			t.Fatal(e)
		}
		payments = append(payments, model.Transaction{Provider: "binance", EventID: no, Receiver: "receiver", Currency: "USDT", Units: o.PayUnits, OccurredAt: time.Now(), State: model.Confirmed})
	}
	if _, err = st.Process(ctx, payments[1], ""); err != nil {
		t.Fatal(err)
	}
	script, err := filepath.Abs("../../scripts/dujiao_cancel_sync.py")
	if err != nil {
		t.Fatal(err)
	}
	// The token is passed through stdin, never a process argument or test output.
	cmd := exec.CommandContext(ctx, python, "-c", `
import importlib.util,json,sqlite3,sys,pathlib
spec=importlib.util.spec_from_file_location('sync',sys.argv[1]);s=importlib.util.module_from_spec(spec);spec.loader.exec_module(s)
gateway=sys.argv[2];token=json.load(sys.stdin);path=pathlib.Path(sys.argv[3])/'commerce.db';db=sqlite3.connect(path)
db.executescript('CREATE TABLE orders(id,status,paid_at,canceled_at,deleted_at); CREATE TABLE payments(id,order_id,channel_id,provider_type,status,paid_at,deleted_at,provider_ref,gateway_order_no,pay_url); CREATE TABLE payment_channels(id,provider_type,config_json);')
db.execute('INSERT INTO payment_channels VALUES(1,?,?)',('bepusdt',json.dumps({'gateway_url':gateway,'auth_token':token})))
for i,no in enumerate(['bridge-cancel','bridge-paid'],1):
 db.execute('INSERT INTO orders VALUES(?,?,NULL,?,NULL)',(i,'canceled','2026-11-10'))
 db.execute('INSERT INTO payments VALUES(?,?,1,?,?,NULL,NULL,?,?,?)',(i,i,'bepusdt','expired',no,no,gateway+'/pay/'+no))
db.commit();state=s.journal(str(path.parent/'journal.db'))
s.check_generation(path,state,gateway)
assert s.sync_once(path,state,gateway)==2
assert dict(state.execute('SELECT trade_id,outcome FROM cancellations'))=={'bridge-cancel':'closed','bridge-paid':'paid_conflict'}
assert s.sync_once(path,state,gateway)==0
s.report_health(path,state,gateway,True)
`, script, gateway.URL, t.TempDir())
	encoded, _ := json.Marshal(mgr.Get().BEpusdt.APIToken)
	cmd.Stdin = strings.NewReader(string(encoded))
	if output, e := cmd.CombinedOutput(); e != nil {
		t.Fatalf("bridge integration: %v: %s", e, output)
	}
	if result, e := st.Process(ctx, payments[0], ""); e != nil || result != "order_closed" {
		t.Fatal(result, e)
	}
	health, e := st.IntegrationHealth(ctx)
	if e != nil || health.Healthy || health.Source != "sqlite" || health.Conflicts != 1 {
		t.Fatal(health, e)
	}
	canceled, _ := st.GetByTradeNo(ctx, "bridge-cancel")
	if canceled.Status != model.Canceled {
		t.Fatal(canceled.Status)
	}
	var count int64
	st.DB().Model(&model.Callback{}).Where("order_id = ?", canceled.ID).Count(&count)
	if count != 0 {
		t.Fatal("canceled order got callback")
	}
}
