package store

import (
	"fmt"
	"gorm.io/gorm"
	"litepay/internal/amount"
	"litepay/internal/model"
)

func migrateFixedAmounts(db *gorm.DB) error {
	return db.Transaction(func(tx *gorm.DB) error {
		if tx.Migrator().HasIndex(&model.AmountSlot{}, "uk_slot_provider_currency_amount") {
			if err := tx.Migrator().DropIndex(&model.AmountSlot{}, "uk_slot_provider_currency_amount"); err != nil {
				return err
			}
		}
		var slots []model.AmountSlot
		if err := tx.Where("amount_units = 0 AND amount <> 0").Find(&slots).Error; err != nil {
			return err
		}
		for _, s := range slots {
			u, err := amount.FloatUnits(s.Amount, s.AmountPrecision)
			if err != nil {
				return fmt.Errorf("档位金额迁移失败: %w", err)
			}
			if err := tx.Model(&s).Update("amount_units", u).Error; err != nil {
				return err
			}
		}
		var orders []model.Order
		if err := tx.Where("pay_units = 0 AND pay_amount <> 0").Find(&orders).Error; err != nil {
			return err
		}
		for _, o := range orders {
			u, err := amount.FloatUnits(o.PayAmount, o.AmountPrecision)
			if err != nil {
				return err
			}
			if err := tx.Model(&o).Update("pay_units", u).Error; err != nil {
				return err
			}
		}
		return tx.Exec(`CREATE UNIQUE INDEX IF NOT EXISTS uk_live_merchant_order ON orders(pid, out_trade_no) WHERE status IN ('PENDING','READY','EXPIRED') AND expire_finalized = 0`).Error
	})
}
