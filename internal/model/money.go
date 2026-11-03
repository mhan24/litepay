package model

import (
	"gorm.io/gorm"
	"litepay/internal/amount"
)

// Hooks keep legacy internal/test constructors consistent with integer slot keys.
func (s *AmountSlot) BeforeSave(tx *gorm.DB) error {
	if s.AmountUnits == 0 && s.Amount != 0 {
		u, err := amount.FloatUnits(s.Amount, s.AmountPrecision)
		if err != nil {
			return err
		}
		s.AmountUnits = u
	}
	return nil
}
