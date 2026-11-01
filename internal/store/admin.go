package store

import (
	"context"
	"errors"
	"strings"
	"time"

	"gorm.io/gorm"

	"litepay/internal/model"
)

// OrderFilter 后台订单查询条件。
type OrderFilter struct {
	Status   string // PENDING / PAID / EXPIRED
	Keyword  string // 订单号 / 商户订单号 / 渠道流水号
	Page     int
	PageSize int
}

const (
	maxOrderPage     = 100000
	maxOrderPageSize = 100
)

// ListOrders 分页查询订单。
func (s *Store) ListOrders(ctx context.Context, f OrderFilter) ([]model.Order, int64, error) {
	q := s.db.WithContext(ctx).Model(&model.Order{})

	if st := strings.TrimSpace(f.Status); st != "" {
		q = q.Where("status = ?", st)
	}
	if kw := strings.TrimSpace(f.Keyword); kw != "" {
		like := "%" + kw + "%"
		q = q.Where("trade_no LIKE ? OR out_trade_no LIKE ? OR tx_id LIKE ?", like, like, like)
	}

	var total int64
	if err := q.Count(&total).Error; err != nil {
		return nil, 0, err
	}

	if f.PageSize <= 0 {
		f.PageSize = 20
	}
	if f.PageSize > maxOrderPageSize {
		f.PageSize = maxOrderPageSize
	}
	if f.Page < 1 {
		f.Page = 1
	}
	if f.Page > maxOrderPage {
		f.Page = maxOrderPage
	}

	var out []model.Order
	err := q.Order("id DESC").
		Offset((f.Page - 1) * f.PageSize).
		Limit(f.PageSize).
		Find(&out).Error
	return out, total, err
}

// RecentOrders 最近订单。
func (s *Store) RecentOrders(ctx context.Context, limit int) ([]model.Order, error) {
	var out []model.Order
	err := s.db.WithContext(ctx).Order("id DESC").Limit(limit).Find(&out).Error
	return out, err
}

// CountByStatus 各状态订单数。
func (s *Store) CountByStatus(ctx context.Context) (map[string]int64, error) {
	type row struct {
		Status string
		Cnt    int64
	}
	var rows []row
	if err := s.db.WithContext(ctx).Model(&model.Order{}).
		Select("status, count(*) as cnt").Group("status").Scan(&rows).Error; err != nil {
		return nil, err
	}
	out := map[string]int64{
		model.StatusPending:    0,
		model.StatusReady:      0,
		model.StatusPaid:       0,
		model.StatusExpired:    0,
		model.StatusCanceled:   0,
		model.StatusSuperseded: 0,
	}
	for _, r := range rows {
		out[r.Status] = r.Cnt
	}
	return out, nil
}

// PeriodStats 某段时间的统计。
type PeriodStats struct {
	Orders  int64   `json:"orders"`
	Paid    int64   `json:"paid"`
	Volume  float64 `json:"volume"`
	Pending int64   `json:"pending"`
}

// StatsSince 统计 since 之后的订单情况。
func (s *Store) StatsSince(ctx context.Context, since time.Time) (PeriodStats, error) {
	var st PeriodStats

	type agg struct {
		Orders int64
		Paid   int64
		Volume float64
	}
	var a agg
	err := s.db.WithContext(ctx).Model(&model.Order{}).
		Select(`count(*) as orders,
		        sum(case when status = ? then 1 else 0 end) as paid,
		        coalesce(sum(case when status = ? then pay_amount else 0 end), 0) as volume`,
			model.StatusPaid, model.StatusPaid).
		Where("created_at >= ?", since).
		Scan(&a).Error
	if err != nil {
		return st, err
	}
	st.Orders, st.Paid, st.Volume = a.Orders, a.Paid, a.Volume

	// 当前待支付数量（不受时间限制）
	s.db.WithContext(ctx).Model(&model.Order{}).
		Where("status IN ?", []string{model.StatusPending, model.StatusReady}).Count(&st.Pending)
	return st, nil
}

// VolumeByCurrency 按币种统计某段时间的成交额。
func (s *Store) VolumeByCurrency(ctx context.Context, since time.Time) (map[string]float64, error) {
	type row struct {
		Currency string
		Volume   float64
	}
	var rows []row
	err := s.db.WithContext(ctx).Model(&model.Order{}).
		Select("currency, coalesce(sum(pay_amount), 0) as volume").
		Where("status = ? AND created_at >= ?", model.StatusPaid, since).
		Group("currency").Scan(&rows).Error
	if err != nil {
		return nil, err
	}
	out := make(map[string]float64, len(rows))
	for _, r := range rows {
		out[r.Currency] = r.Volume
	}
	return out, nil
}

// NotifyFailureCount 回调失败/待重试的订单数。
func (s *Store) NotifyFailureCount(ctx context.Context) (int64, error) {
	var n int64
	err := s.db.WithContext(ctx).Model(&model.Order{}).
		Where("status = ? AND notify_status = ?", model.StatusPaid, model.NotifyFailed).
		Count(&n).Error
	return n, err
}

// ExpiredNotifyFailureCount 返回已终结且过期回调失败的订单数。
func (s *Store) ExpiredNotifyFailureCount(ctx context.Context) (int64, error) {
	var n int64
	err := s.db.WithContext(ctx).Model(&model.Order{}).
		Where("status = ? AND expire_finalized = ? AND notify_status = ?",
			model.StatusExpired, true, model.NotifyFailed).
		Count(&n).Error
	return n, err
}

// NotifyLogs 查询某订单的结果通知记录。
func (s *Store) NotifyLogs(ctx context.Context, orderID uint, limit int) ([]model.NotifyLog, error) {
	if limit <= 0 {
		limit = 50
	}
	var out []model.NotifyLog
	err := s.db.WithContext(ctx).Where("order_id = ?", orderID).
		Order("id DESC").Limit(limit).Find(&out).Error
	return out, err
}

// ResetNotify 重置结果通知状态，让通知 Worker 立即重试。
func (s *Store) ResetNotify(ctx context.Context, tradeNo string) error {
	return s.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		var o model.Order
		if err := tx.Where("trade_no = ?", tradeNo).First(&o).Error; err != nil {
			if errors.Is(err, gorm.ErrRecordNotFound) {
				return errors.New("订单不存在")
			}
			return err
		}
		if o.Status != model.StatusPaid &&
			!(o.Status == model.StatusExpired && o.ExpireFinalized) {
			return errors.New("仅已支付或已终结的过期订单可以重发通知")
		}
		if o.NotifyStatus != model.NotifyPending && o.NotifyStatus != model.NotifyFailed {
			return errors.New("仅等待中或发送失败的通知可以重发")
		}
		if o.NotifyResets >= 2 {
			return errors.New("该订单已人工重发两次，请检查商户通知地址")
		}
		res := tx.Model(&model.Order{}).
			Where("id = ? AND status = ? AND notify_status = ? AND notify_resets = ?",
				o.ID, o.Status, o.NotifyStatus, o.NotifyResets).
			Updates(map[string]any{
				"notify_status":   model.NotifyPending,
				"notify_attempts": 0,
				"notify_resets":   o.NotifyResets + 1,
				"next_notify_at":  time.Now(),
				"updated_at":      time.Now(),
			})
		if res.Error != nil {
			return res.Error
		}
		if res.RowsAffected != 1 {
			return errors.New("订单通知状态已变化，请刷新后重试")
		}
		return nil
	})
}

// ForceExpire 手动结束收银台订单。只关闭付款页面，不改变实际付款
// 截止时间，也不立即释放金额；宽限期结束后才终结订单并发送过期通知。
func (s *Store) ForceExpire(ctx context.Context, tradeNo string) error {
	return s.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		var o model.Order
		if err := tx.Where("trade_no = ?", tradeNo).First(&o).Error; err != nil {
			if errors.Is(err, gorm.ErrRecordNotFound) {
				return errors.New("订单不存在")
			}
			return err
		}
		if o.Status != model.StatusPending && o.Status != model.StatusReady {
			return errors.New("仅待支付订单可以强制过期")
		}
		now := time.Now()
		res := tx.Model(&model.Order{}).
			Where("id = ? AND status = ?", o.ID, o.Status).
			Where("notify_status = '' AND next_notify_at IS NULL").
			Updates(map[string]any{
				"status":           model.StatusExpired,
				"expire_finalized": false,
				"notify_status":    "",
				"notify_attempts":  0,
				"next_notify_at":   nil,
				"updated_at":       now,
			})
		if res.Error != nil {
			return res.Error
		}
		if res.RowsAffected != 1 {
			return errors.New("订单状态已变化，请刷新后重试")
		}
		return nil
	})
}

// ManualPay 后台补单：将订单标记为已支付并触发回调。
func (s *Store) ManualPay(ctx context.Context, tradeNo string, amountPrecision int,
	slotGrace time.Duration) (*model.Order, error) {
	o, err := s.GetByTradeNo(ctx, tradeNo)
	if err != nil {
		return nil, err
	}
	if o == nil {
		return nil, errors.New("订单不存在")
	}
	if o.Status == model.StatusPaid {
		return o, nil
	}
	if o.PayAmount <= 0 {
		return nil, errors.New("订单尚未选择收款方式")
	}
	if o.ExpireFinalized {
		return nil, errors.New("订单支付窗口已终结")
	}
	if o.Status == model.StatusReady {
		// READY 订单超过有效期与配置宽限后应等待 Reaper 标为 EXPIRED，
		// 不再允许后台绕过状态直接补单。
		if time.Now().After(o.ExpiredAt.Add(slotGrace)) {
			return nil, errors.New("订单已超过支付宽限期")
		}
	} else if o.Status == model.StatusExpired {
		if time.Now().After(o.ExpiredAt.Add(slotGrace)) {
			return nil, errors.New("订单已超过支付宽限期")
		}
	} else {
		return nil, errors.New("当前订单状态不允许补单")
	}

	tx := model.IncomingTx{
		TransactionID:   "MANUAL-" + tradeNo,
		Provider:        o.Collection,
		Type:            "MANUAL",
		Amount:          o.PayAmount,
		Currency:        o.Currency,
		ReceiverAddr:    o.PayAddress,
		TransactionTime: time.Now(),
		Raw:             `{"source":"admin-manual"}`,
	}
	if _, err := s.ClaimAndPay(ctx, o, tx, amountPrecision); err != nil {
		return nil, err
	}
	return s.GetByTradeNo(ctx, tradeNo)
}

// DatabaseSize 返回数据库文件大小（字节）。
func (s *Store) DatabaseSize() int64 {
	var pageCount, pageSize int64
	if err := s.db.Raw("PRAGMA page_count").Scan(&pageCount).Error; err != nil {
		return 0
	}
	if err := s.db.Raw("PRAGMA page_size").Scan(&pageSize).Error; err != nil {
		return 0
	}
	return pageCount * pageSize
}
