package server

import (
	"litepay/internal/model"
	"litepay/internal/store"
	"testing"
)

// Legacy tests move their clocks by editing order fixtures. Mirror only the latest
// instruction for those fixtures; business paths never mutate a saved attempt.
func syncAttemptFixture(t *testing.T, s *store.Store) {
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
