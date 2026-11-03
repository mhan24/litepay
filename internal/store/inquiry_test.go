package store

import (
	"context"
	"errors"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"litepay/internal/model"
)

func TestInquiryReservationIsAtomicAndDurable(t *testing.T) {
	path := filepath.Join(t.TempDir(), "inquiry.db")
	st, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	var count atomic.Int32
	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if st.ReserveInquiry(context.Background(), "order", "reference") == nil {
				count.Add(1)
			}
		}()
	}
	wg.Wait()
	if count.Load() != 1 {
		t.Fatalf("reserved %d times", count.Load())
	}
	if err := st.FinishInquiry(context.Background(), "order", "not_found"); err != nil {
		t.Fatal(err)
	}
	db, _ := st.DB().DB()
	db.Close()
	st, err = Open(path)
	if err != nil {
		t.Fatal(err)
	}
	if !errors.Is(st.ReserveInquiry(context.Background(), "order", "reference-2"), ErrInquiryLimited) {
		t.Fatal("restart bypassed interval")
	}
	row, err := st.PaymentInquiry(context.Background(), "order")
	if err != nil || row.Reference != "reference" || row.Result != "not_found" || row.Attempts != 1 {
		t.Fatalf("bad persisted inquiry %+v %v", row, err)
	}
	for attempt := 2; attempt <= 10; attempt++ {
		st.DB().Model(&model.PaymentInquiry{}).Where("trade_no = ?", "order").Update("last_attempt_at", time.Now().Add(-2*time.Minute))
		if err := st.ReserveInquiry(context.Background(), "order", "reference"); err != nil {
			t.Fatal(err)
		}
	}
	st.DB().Model(&model.PaymentInquiry{}).Where("trade_no = ?", "order").Update("last_attempt_at", time.Now().Add(-2*time.Minute))
	if !errors.Is(st.ReserveInquiry(context.Background(), "order", "reference"), ErrInquiryLimited) {
		t.Fatal("maximum attempts bypassed")
	}
}
