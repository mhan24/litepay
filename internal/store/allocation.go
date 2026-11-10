package store

import (
	"errors"
	"time"

	"gorm.io/gorm"
	"litepay/internal/amount"
	"litepay/internal/model"
)

const MaxPaymentAttempts = 6
const MaxSelectionChanges = 12
const SelectionInterval = 2 * time.Second

var ErrSelectionThrottled = errors.New("切换过于频繁，请等待 2 秒后重试")
var ErrSelectionLimit = errors.New("本订单付款方式切换次数已达上限，请使用当前付款信息或联系商户")

// Allocation first uses the requested coarse spacing. If full, it uses micro-unit gaps
// inside the SAME price envelope. Old instructions and their 48-hour reservations stay intact.
func allocateSlot(db *gorm.DB, order model.Order, provider, currency string, base, ceiling, step int64, preferredPrecision int, grace int, now time.Time) (int64, int, error) {
	if base < amount.MinMall {
		base = amount.MinMall
	}
	ceiling = min(ceiling, amount.MaxMall-1)
	if ceiling < base {
		return 0, 0, errors.New("应付金额超出可核销范围")
	}
	var occupied []int64
	if err := db.Model(&model.Slot{}).Where("provider = ? AND currency = ? AND units BETWEEN ? AND ?", provider, currency, base, ceiling).Where("order_id IS NOT NULL OR expires_at >= ?", now).Order("units").Pluck("units", &occupied).Error; err != nil {
		return 0, 0, err
	}
	used := make(map[int64]bool, len(occupied))
	for _, units := range occupied {
		used[units] = true
	}
	chosen := int64(-1)
	precision := preferredPrecision
	for candidate := base; candidate <= ceiling; candidate += step {
		if !used[candidate] {
			chosen = candidate
			break
		}
	}
	if chosen < 0 {
		candidate := base
		for _, units := range occupied {
			if units < candidate {
				continue
			}
			if units == candidate {
				candidate++
			} else {
				break
			}
		}
		if candidate > ceiling {
			return 0, 0, errors.New("允许的精确金额区间已满，请稍后重试或选择其他付款方式")
		}
		chosen = candidate
		precision = 6
	}
	// Only remove the one expired, detached row being reused. Large cleanup runs in bounded batches.
	if err := db.Where("provider = ? AND currency = ? AND units = ? AND order_id IS NULL AND expires_at < ?", provider, currency, chosen, now).Delete(&model.Slot{}).Error; err != nil {
		return 0, 0, err
	}
	slot := model.Slot{Provider: provider, Currency: currency, Units: chosen, OrderID: &order.ID, ExpiresAt: order.ExpiresAt.Add(max(time.Duration(grace)*time.Second, LateVisibilityWindow))}
	if err := db.Create(&slot).Error; err != nil {
		return 0, 0, err
	}
	return chosen, precision, nil
}
