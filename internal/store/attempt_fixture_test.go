package store

import (
	"litepay/internal/model"
	"testing"
)

// Legacy tests move their clocks by editing order fixtures. Mirror only the latest
// instruction for those fixtures; business paths never mutate a saved attempt.
func syncAttemptFixture(t *testing.T, s *Store) {
	t.Helper()
	var orders []model.Order
	if err := s.DB().Where("selected_at IS NOT NULL").Find(&orders).Error; err != nil {
		t.Fatal(err)
	}
	for _, o := range orders {
		var a model.PaymentAttempt
		if err := s.DB().Where("order_id = ?", o.ID).Order("id DESC").First(&a).Error; err != nil {
			t.Fatal(err)
		}
		if err := s.DB().Model(&a).Updates(map[string]any{"selected_at": o.SelectedAt, "expires_at": o.ExpiresAt, "units": o.PayUnits}).Error; err != nil {
			t.Fatal(err)
		}
	}
}

func completedSettlementFixture(t *testing.T, s *Store, orderID uint) {
	t.Helper()
	var a model.PaymentAttempt
	if err := s.DB().Where("order_id = ?", orderID).First(&a).Error; err != nil {
		t.Fatal(err)
	}
	if err := s.DB().Create(&model.HistoricalScan{Automatic: true, OrderID: orderID, AttemptID: a.ID, Provider: a.Provider, Receiver: a.Receiver, From: a.SelectedAt, To: a.ExpiresAt, Cursor: a.ExpiresAt, State: "completed", Operator: "system", Reason: "fixture final settlement coverage"}).Error; err != nil {
		t.Fatal(err)
	}
}
