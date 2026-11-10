package server

import (
	"context"
	"encoding/json"
	"github.com/gin-gonic/gin"
	"litepay/internal/config"
	"litepay/internal/model"
	"litepay/internal/settings"
	"litepay/internal/store"
	"litepay/internal/worker"
	"net/http"
	"net/http/httptest"
	"os/exec"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func TestGatewayRestoreReplaysIndependentClosedJournalBeforeAcceptingPayment(t *testing.T) {
	python, err := exec.LookPath("python3")
	if err != nil {
		t.Skip("python3 unavailable")
	}
	ctx := context.Background()
	root := t.TempDir()
	path := filepath.Join(root, "live.db")
	st, err := store.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { st.Close() }()
	cfg := config.Default()
	cfg.Server.Production = false
	cfg.Server.CancelSyncRequired = true
	cfg.Binance.APIKey = "key"
	cfg.Binance.APISecret = "secret"
	cfg.Checkout.Binance.UID = "receiver"
	build := func() (*gin.Engine, *settings.Manager, *worker.Poller) {
		mgr := settings.New(cfg, st)
		if err := mgr.Load(ctx); err != nil {
			t.Fatal(err)
		}
		p := worker.NewPoller(mgr, st)
		engine, err := New(mgr, st, p)
		if err != nil {
			t.Fatal(err)
		}
		return engine, mgr, p
	}
	engine, mgr, poller := build()
	var handler atomic.Pointer[gin.Engine]
	handler.Store(engine)
	gateway := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { handler.Load().ServeHTTP(w, r) }))
	defer gateway.Close()
	_, err = st.Create(ctx, model.Order{TradeNo: "restore-trade", OutTradeNo: "restore-mall", OriginUnits: 1000000, ExpiresAt: time.Now().Add(time.Hour)})
	if err != nil {
		t.Fatal(err)
	}
	if _, err = st.Select(ctx, "restore-trade", "binance", "USDT", "receiver", 2, 10000, 10000, 99, 120); err != nil {
		t.Fatal(err)
	}
	st.ScanResult(ctx, "binance", time.Now(), nil)
	dbtool, _ := filepath.Abs("../../scripts/payment_db.py")
	bridge, _ := filepath.Abs("../../scripts/dujiao_cancel_sync.py")
	call := func(code string) {
		cmd := exec.Command(python, "-c", code, dbtool, bridge, root, gateway.URL)
		secret, _ := json.Marshal(mgr.Get().BEpusdt.APIToken)
		cmd.Stdin = strings.NewReader(string(secret))
		if out, e := cmd.CombinedOutput(); e != nil {
			t.Fatalf("restore integration: %v %s", e, out)
		}
	}
	common := `import importlib.util,sys,pathlib,json,sqlite3
spec=importlib.util.spec_from_file_location('dbtool',sys.argv[1]);d=importlib.util.module_from_spec(spec);spec.loader.exec_module(d)
spec=importlib.util.spec_from_file_location('bridge',sys.argv[2]);b=importlib.util.module_from_spec(spec);spec.loader.exec_module(b)
r=pathlib.Path(sys.argv[3]);gateway=sys.argv[4];token=json.load(sys.stdin)
`
	call(common + `d.snapshot(r/'live.db',r/'before.db')
c=sqlite3.connect(r/'merchant.db');c.executescript('CREATE TABLE orders(id INTEGER PRIMARY KEY,status,paid_at,canceled_at,deleted_at); CREATE TABLE payments(id INTEGER PRIMARY KEY,order_id,channel_id,provider_type,status,paid_at,deleted_at,provider_ref,gateway_order_no,pay_url);CREATE TABLE payment_channels(id,provider_type,config_json);')
c.execute('INSERT INTO payment_channels VALUES(1,?,?)',('bepusdt',json.dumps({'gateway_url':gateway,'auth_token':token})))
c.execute("INSERT INTO orders VALUES(1,'canceled',NULL,'2026-11-10',NULL)")
c.execute("INSERT INTO payments VALUES(1,1,1,'bepusdt','expired',NULL,NULL,'restore-trade','restore-mall',?)",(gateway+'/pay/restore-trade',));c.commit();c.close()
s=b.journal(r/'bridge.db');assert b.run_cycle(r/'merchant.db',s,gateway,True)
assert s.execute('SELECT outcome FROM cancellations').fetchone()[0]=='closed'
`)
	canceled, _ := st.GetByTradeNo(ctx, "restore-trade")
	if canceled.Status != model.Canceled {
		t.Fatal(canceled)
	}
	original, _ := st.Recovery(ctx)
	if !original.Acknowledged {
		t.Fatal("initial reconciliation never acknowledged")
	}
	st.Close()
	call(common + `d.restore(r/'before.db',r/'restored.db')`)
	st, err = store.Open(filepath.Join(root, "restored.db"))
	if err != nil {
		t.Fatal(err)
	}
	engine, mgr, poller = build()
	handler.Store(engine)
	restored, _ := st.Recovery(ctx)
	if restored.Generation == original.Generation || restored.Acknowledged {
		t.Fatal(restored)
	}
	rolledBack, _ := st.GetByTradeNo(ctx, "restore-trade")
	if rolledBack.Status != model.Ready {
		t.Fatal("fixture failed to restore cancellation", rolledBack)
	}
	if w := postForm(engine, "/api/v1/order/create-order", nil); !strings.Contains(w.Body.String(), "暂停接单") {
		t.Fatal("restored gateway accepted creation", w.Body.String())
	}
	if w := getRequest(engine, "/readyz"); w.Code != 503 {
		t.Fatal(w.Code)
	}
	if w := getRequest(engine, "/pay/restore-trade"); w.Code != 503 || strings.Contains(w.Body.String(), "复制金额") {
		t.Fatal("payment instructions exposed during recovery")
	}
	if err = poller.Scan(ctx, "binance"); err != nil {
		t.Fatal("paused scanner should not contact exchange", err)
	}
	n := worker.NewNotifier(mgr, st)
	if did, e := n.Deliver(ctx); did || e != nil {
		t.Fatal("notifications escaped recovery hold", did, e)
	}
	call(common + `s=b.journal(r/'bridge.db')
assert s.execute('SELECT outcome FROM cancellations').fetchone()[0]=='closed'
assert b.run_cycle(r/'merchant.db',s,gateway,True)
assert s.execute('SELECT count(*) FROM epoch_receipts').fetchone()[0]==2
`)
	canceled, _ = st.GetByTradeNo(ctx, "restore-trade")
	if canceled.Status != model.Canceled {
		t.Fatal("old journal success suppressed recovery replay", canceled)
	}
	if w := getRequest(engine, "/readyz"); w.Code != 200 {
		t.Fatal(w.Code, w.Body.String())
	}
	var callbacks int64
	st.DB().Model(&model.Callback{}).Count(&callbacks)
	if callbacks != 0 {
		t.Fatal("canceled order notified")
	}
}
