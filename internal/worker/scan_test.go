package worker

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"litepay/internal/config"
	"litepay/internal/model"
	"litepay/internal/settings"
	"litepay/internal/store"
)

func TestScanPlanCoversRealtimeWindowAndBacklog(t *testing.T) {
	now := time.Now()
	order := model.Order{CreatedAt: now.Add(-5 * time.Minute)}
	lookback := 2 * time.Minute

	// 没有游标：只扫待确认订单的回看窗口。
	plan := newScanPlan([]model.Order{order}, nil, lookback, now)
	if plan.backlog != nil {
		t.Fatalf("unexpected backlog without cursor: %+v", plan.backlog)
	}
	wantStart := now.Add(-7 * time.Minute)
	if !plan.recent.start.Equal(wantStart) || !plan.recent.end.Equal(now) {
		t.Fatalf("recent window = %v..%v, want %v..%v", plan.recent.start, plan.recent.end, wantStart, now)
	}

	// 游标只落后几分钟：并进实时窗口，不留空洞（这里没有待确认订单，
	// 实时窗口只覆盖回看时长，因此起点由游标决定）。
	cursor := &model.ScanCursor{ScannedUntil: now.Add(-4 * time.Minute)}
	plan = newScanPlan(nil, cursor, lookback, now)
	if plan.backlog != nil {
		t.Fatalf("short lag should not create a backlog: %+v", plan.backlog)
	}
	if want := now.Add(-6 * time.Minute); !plan.recent.start.Equal(want) {
		t.Fatalf("recent start = %v, want %v (cursor minus overlap)", plan.recent.start, want)
	}

	// 游标落后 20 小时：分块补扫，本轮只推进到积压窗口末尾。
	cursor = &model.ScanCursor{ScannedUntil: now.Add(-20 * time.Hour)}
	plan = newScanPlan([]model.Order{order}, cursor, lookback, now)
	if plan.backlog == nil {
		t.Fatal("long lag must create a backlog window")
	}
	if got := plan.advance(); !got.Equal(plan.backlog.end) {
		t.Fatalf("advance = %v, want backlog end %v", got, plan.backlog.end)
	}
	if span := plan.backlog.end.Sub(plan.backlog.start); span > maxScanWindow+time.Second {
		t.Fatalf("backlog span = %v, want <= %v", span, maxScanWindow)
	}
	if plan.backlog.end.After(now) {
		t.Fatalf("backlog must stay in the past: %v", plan.backlog.end)
	}
	if windows := plan.windows(); len(windows) != 2 {
		t.Fatalf("windows = %d, want realtime + backlog", len(windows))
	}

	// 游标损坏成很久以前：回溯被 maxScanBackfill 截断。
	cursor = &model.ScanCursor{ScannedUntil: now.Add(-90 * 24 * time.Hour)}
	plan = newScanPlan([]model.Order{order}, cursor, lookback, now)
	if got := now.Sub(plan.backlog.start); got > maxScanBackfill+time.Second {
		t.Fatalf("backfill = %v, want <= %v", got, maxScanBackfill)
	}
}

func TestScanPlanWithoutOrdersStillCoversCursorGap(t *testing.T) {
	now := time.Now()
	cursor := &model.ScanCursor{ScannedUntil: now.Add(-30 * time.Minute)}
	plan := newScanPlan(nil, cursor, 2*time.Minute, now)
	if plan.backlog != nil {
		t.Fatalf("30m lag should fold into the realtime window: %+v", plan.backlog)
	}
	if want := now.Add(-32 * time.Minute); !plan.recent.start.Equal(want) {
		t.Fatalf("recent start = %v, want %v", plan.recent.start, want)
	}
}

// 扫描窗口来自持久化游标：服务停机期间的到账在重启后仍会被扫到并留痕。
func TestPollerResumesScanFromPersistedCursor(t *testing.T) {
	f := newPollFixture(t, "LP-CURSOR")
	ctx := context.Background()

	// 模拟停机：游标停在 40 分钟前，期间到账的流水没有被扫过。
	stoppedAt := time.Now().Add(-40 * time.Minute)
	if err := f.st.SaveScanCursor(ctx, config.ProviderBinance, stoppedAt); err != nil {
		t.Fatal(err)
	}
	// 停机期间到账的一笔钱，金额与任何订单都不匹配（订单已过期终结）。
	paidAt := time.Now().Add(-30 * time.Minute)
	f.stub.set(stubBinanceTx{
		id: "tx-outage", orderType: "PAY", amount: 77.77,
		currency: "USDT", at: paidAt, receiver: "binance-uid",
	})
	f.tick(t)

	got := f.observations(t)
	if len(got) != 1 || got[0].TransactionID != "tx-outage" {
		t.Fatalf("late payment during downtime was not observed: %+v", got)
	}
	cursor, err := f.st.ScanCursor(ctx, config.ProviderBinance)
	if err != nil {
		t.Fatal(err)
	}
	if cursor == nil || !cursor.ScannedUntil.After(stoppedAt) {
		t.Fatalf("cursor did not advance: %+v", cursor)
	}
}

// 扫描失败时游标必须原地不动，否则那段未扫到的时间会被永久跳过。
func TestPollerKeepsCursorWhenExchangeFails(t *testing.T) {
	f := newPollFixture(t, "LP-FAIL")
	ctx := context.Background()

	stoppedAt := time.Now().Add(-30 * time.Minute)
	if err := f.st.SaveScanCursor(ctx, config.ProviderBinance, stoppedAt); err != nil {
		t.Fatal(err)
	}

	// 让交易所接口返回业务错误。
	failing := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/api/v3/account" {
			_, _ = w.Write([]byte(`{"uid":"binance-uid"}`))
			return
		}
		_, _ = w.Write([]byte(`{"code":"10001","message":"busy","success":false,"data":[]}`))
	}))
	defer failing.Close()
	f.cfg.Binance.BaseURL = failing.URL
	f.poller = NewPoller(settings.New(f.cfg, f.st), f.st)

	f.tick(t)

	cursor, err := f.st.ScanCursor(ctx, config.ProviderBinance)
	if err != nil {
		t.Fatal(err)
	}
	if cursor == nil || !cursor.ScannedUntil.Equal(stoppedAt) {
		t.Fatalf("cursor advanced after a failed scan: %+v", cursor)
	}
	if cursor.LastError == "" {
		t.Fatal("scan error was not recorded")
	}
	if stats := f.poller.StatsFor(config.ProviderBinance); stats.LastError == "" {
		t.Fatal("poll stats did not record the error")
	}
}

func TestScanCursorStoreRoundTrip(t *testing.T) {
	st, err := store.Open(t.TempDir() + "/cursor.db")
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()

	if cursor, err := st.ScanCursor(ctx, "binance"); err != nil || cursor != nil {
		t.Fatalf("fresh cursor = %+v err = %v, want nil", cursor, err)
	}
	until := time.Now().Add(-time.Minute).Truncate(time.Millisecond)
	if err := st.SaveScanCursor(ctx, "BINANCE", until); err != nil {
		t.Fatal(err)
	}
	cursor, err := st.ScanCursor(ctx, "binance")
	if err != nil || cursor == nil {
		t.Fatalf("cursor not saved: %+v err = %v", cursor, err)
	}
	if !cursor.ScannedUntil.Equal(until) || cursor.LastError != "" {
		t.Fatalf("cursor = %+v", cursor)
	}

	// 失败不改游标，只记原因。
	if err := st.SaveScanError(ctx, "binance", "boom"); err != nil {
		t.Fatal(err)
	}
	cursor, _ = st.ScanCursor(ctx, "binance")
	if !cursor.ScannedUntil.Equal(until) || cursor.LastError != "boom" {
		t.Fatalf("error update touched the cursor: %+v", cursor)
	}
	// 成功后清空错误。
	if err := st.SaveScanCursor(ctx, "binance", until.Add(time.Minute)); err != nil {
		t.Fatal(err)
	}
	cursor, _ = st.ScanCursor(ctx, "binance")
	if cursor.LastError != "" || !cursor.ScannedUntil.Equal(until.Add(time.Minute)) {
		t.Fatalf("success did not clear the error: %+v", cursor)
	}
}

func TestObservationFailureKeepsScanCursorForRetry(t *testing.T) {
	f := newPollFixture(t, "LP-LEDGER-FAIL")
	ctx := context.Background()
	until := time.Now().Add(-time.Minute)
	if err := f.st.SaveScanCursor(ctx, "binance", until); err != nil {
		t.Fatal(err)
	}
	if err := f.st.DB().Exec(`CREATE TRIGGER fail_observation BEFORE INSERT ON tx_observations BEGIN SELECT RAISE(FAIL, 'disk failure'); END`).Error; err != nil {
		t.Fatal(err)
	}
	f.stub.set(stubBinanceTx{id: "retry-evidence", orderType: "PAY", amount: 77, currency: "USDT", at: f.txTime(), receiver: "binance-uid"})
	f.tick(t)
	cursor, err := f.st.ScanCursor(ctx, "binance")
	if err != nil || !cursor.ScannedUntil.Equal(until) || cursor.LastError == "" {
		t.Fatalf("failed evidence write advanced cursor: %+v, %v", cursor, err)
	}
	if err := f.st.DB().Exec(`DROP TRIGGER fail_observation`).Error; err != nil {
		t.Fatal(err)
	}
	f.tick(t)
	if got := f.observations(t); len(got) != 1 || got[0].TransactionID != "retry-evidence" {
		t.Fatalf("evidence not recovered: %+v", got)
	}
}
