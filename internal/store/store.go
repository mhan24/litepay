// Package store 封装 SQLite 数据访问（Gorm）。
package store

import (
	"context"
	"errors"
	"fmt"
	"log"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/glebarez/sqlite"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
	"gorm.io/gorm/logger"

	"litepay/internal/amount"
	"litepay/internal/model"
)

// ErrNoSlotAvailable 当前没有可用的浮动金额档位（并发订单过多）。
var ErrNoSlotAvailable = errors.New("当前支付通道繁忙，请稍后重试")

// Store 数据访问对象。
type Store struct {
	db *gorm.DB

	// orderLocks serializes callbacks and replacement for one merchant order
	// without blocking unrelated orders. The reference count lets idle entries
	// be removed, so this map cannot grow with every order ever processed.
	orderLocksMu sync.Mutex
	orderLocks   map[string]*orderLock
}

type orderLock struct {
	mu   sync.Mutex
	refs int
}

// Open 打开（并初始化）SQLite 数据库。
// 使用纯 Go 驱动 glebarez/sqlite，无需 cgo，方便交叉编译与部署。
func Open(path string) (*Store, error) {
	if dir := filepath.Dir(path); dir != "" && dir != "." {
		if err := os.MkdirAll(dir, 0o700); err != nil {
			return nil, fmt.Errorf("创建数据目录失败: %w", err)
		}
	}

	// SQLite stores exchange credentials. Create the file before opening WAL
	// so both the database and sidecars inherit restrictive permissions.
	if path != ":memory:" {
		f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0o600)
		if err != nil {
			return nil, err
		}
		if err := f.Chmod(0o600); err != nil {
			f.Close()
			return nil, err
		}
		if err := f.Close(); err != nil {
			return nil, err
		}
	}
	// WAL 模式提升读写并发；busy_timeout 避免瞬时锁冲突直接报错
	dbPath := path
	if path != ":memory:" {
		absolute, err := filepath.Abs(path)
		if err != nil {
			return nil, err
		}
		dbPath = (&url.URL{Scheme: "file", Path: absolute}).String()
	}
	dsn := dbPath + "?_pragma=busy_timeout(5000)&_pragma=journal_mode(WAL)&_pragma=synchronous(NORMAL)"
	db, err := gorm.Open(sqlite.Open(dsn), &gorm.Config{
		Logger:  logger.New(log.New(os.Stderr, "", log.LstdFlags), logger.Config{LogLevel: logger.Warn, ParameterizedQueries: true}),
		NowFunc: func() time.Time { return time.Now().Local() },
	})
	if err != nil {
		return nil, fmt.Errorf("打开数据库失败: %w", err)
	}

	sqlDB, err := db.DB()
	if err != nil {
		return nil, err
	}
	// 单写者模型：SQLite 只保留一个连接，彻底避免 SQLITE_BUSY
	sqlDB.SetMaxOpenConns(1)
	sqlDB.SetMaxIdleConns(1)

	if err := db.AutoMigrate(&model.Order{}, &model.AmountSlot{}, &model.IncomingTx{},
		&model.NotifyLog{}, &model.Setting{}, &model.Credential{}); err != nil {
		return nil, fmt.Errorf("数据库迁移失败: %w", err)
	}
	return &Store{db: db, orderLocks: make(map[string]*orderLock)}, nil
}

// DB 暴露底层连接（测试/调试用）。
func (s *Store) DB() *gorm.DB { return s.db }

// Ping 健康检查。
func (s *Store) Ping(ctx context.Context) error {
	sqlDB, err := s.db.DB()
	if err != nil {
		return err
	}
	return sqlDB.PingContext(ctx)
}

// -----------------------------------------------------------------------------
// 订单
// -----------------------------------------------------------------------------

// CreateOrderInput 创建订单入参。
type CreateOrderInput struct {
	Protocol       string
	Pid            string
	TradeNo        string
	OutTradeNo     string
	Name           string
	OriginMoney    string
	OriginCurrency string
	NotifyURL      string
	ReturnURL      string
	ClientIP       string
	ExpiredAt      time.Time
	CurrencyLimit  string
	Reselect       bool
	ReplaceReady   bool
}

// CreateOrder 直接创建一张订单，仅供测试与内部构造使用。
func (s *Store) CreateOrder(ctx context.Context, in CreateOrderInput) (*model.Order, error) {
	order := newOrder(in)
	if err := s.db.WithContext(ctx).Create(order).Error; err != nil {
		return nil, err
	}
	return order, nil
}

// CreateOrReuseCashierOrder 原子创建收银台订单。
//
// 同一商户订单号只允许存在一张可继续支付的订单：
//   - 已支付订单原样返回；
//   - 未选择付款方式的未过期订单原样返回，且不延长原有效期；
//   - 收银台订单已选择付款方式但尚未收款时，商户重建会静默退役旧单；
//   - 已过期、已取消或已替代的旧订单静默退役后创建新订单。
//
// 静默退役会清除旧订单的待发送回调，因此旧 status=3 不会关闭商户后来
// 重新创建的同号订单。
func (s *Store) CreateOrReuseCashierOrder(ctx context.Context, in CreateOrderInput) (*model.Order, error) {
	unlock := s.lockOrder(in.Pid, in.OutTradeNo)
	defer unlock()
	var result *model.Order
	err := s.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		var existing model.Order
		err := tx.Where("pid = ? AND out_trade_no = ?", in.Pid, in.OutTradeNo).
			Order("id DESC").First(&existing).Error
		if err == nil {
			if existing.Status == model.StatusPaid {
				result = &existing
				return nil
			}
			// 官方 BEpusdt 会以最新参数重建可替换订单。未选择付款方式时
			// 没有金额档位，重复建单继续幂等；一旦用户已选择平台和币种，
			// 商户显式重建就要使用新金额与有效期，不能沿用旧支付信息。
			if in.ReplaceReady && existing.Status == model.StatusReady &&
				!existing.ExpireFinalized {
				if err := retireOrder(tx, &existing); err != nil {
					return err
				}
			} else if (existing.Status == model.StatusPending || existing.Status == model.StatusReady) &&
				time.Now().Before(existing.ExpiredAt) {
				result = &existing
				return nil
			} else {
				if err := retireOrder(tx, &existing); err != nil {
					return err
				}
			}
		} else if !errors.Is(err, gorm.ErrRecordNotFound) {
			return err
		}

		order := newOrder(in)
		if err := tx.Create(order).Error; err != nil {
			return err
		}
		result = order
		return nil
	})
	if err != nil {
		return nil, err
	}
	return result, nil
}

func newOrder(in CreateOrderInput) *model.Order {
	return &model.Order{
		Protocol:       in.Protocol,
		Pid:            in.Pid,
		TradeNo:        in.TradeNo,
		OutTradeNo:     in.OutTradeNo,
		Name:           in.Name,
		OriginMoney:    in.OriginMoney,
		OriginCurrency: in.OriginCurrency,
		CurrencyLimit:  normalizeCurrencyLimit(in.CurrencyLimit),
		Reselect:       in.Reselect,
		Status:         model.StatusPending,
		NotifyURL:      in.NotifyURL,
		ReturnURL:      in.ReturnURL,
		ClientIP:       in.ClientIP,
		ExpiredAt:      in.ExpiredAt,
	}
}

// retireOrder 静默结束不再允许支付的旧订单。
func retireOrder(tx *gorm.DB, o *model.Order) error {
	now := time.Now()
	if err := tx.Model(&model.Order{}).Where("id = ?", o.ID).Updates(map[string]any{
		"status":           model.StatusSuperseded,
		"expire_finalized": true,
		"notify_status":    "",
		"notify_attempts":  0,
		"next_notify_at":   nil,
		"updated_at":       now,
	}).Error; err != nil {
		return err
	}
	return tx.Model(&model.AmountSlot{}).Where("order_id = ?", o.ID).Update("order_id", 0).Error
}

// GetByTradeNo 按网关流水号查询订单。
func (s *Store) GetByTradeNo(ctx context.Context, tradeNo string) (*model.Order, error) {
	var o model.Order
	err := s.db.WithContext(ctx).Where("trade_no = ?", tradeNo).First(&o).Error
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return &o, nil
}

// MatchableOrders 返回待确认到账的订单。
//
// collection 为空表示“允许在任意已配置通道选择”；这类订单由收银台
// 的具体选择接口写入平台和币种。金额档位记录原支付窗口加宽限期，
// 因此订单被取消、替换或重选后，旧档位也不会被新订单复用。
func (s *Store) MatchableOrders(ctx context.Context, provider string, grace time.Duration) ([]model.Order, error) {
	var out []model.Order
	now := time.Now()
	err := s.db.WithContext(ctx).Model(&model.Order{}).
		Joins("JOIN amount_slots ON amount_slots.order_id = orders.id").
		Where("orders.collection = ? AND amount_slots.provider = ?", provider, provider).
		Where("amount_slots.currency = orders.currency AND amount_slots.amount = orders.pay_amount").
		Where("orders.origin_currency = ?", "USD").
		Where("amount_slots.expires_at > ?", now).
		Distinct("orders.*").
		// EXPIRED 在宽限期内仍可入账，覆盖付款已发出但交易所记录稍晚的情况。
		// CANCELED/SUPERSEDED 永不进入这里。
		Where("(orders.status = ? OR (orders.status = ? AND orders.expire_finalized = ? AND orders.expired_at > ?))",
			model.StatusReady, model.StatusExpired, false, now.Add(-grace)).
		Order("orders.created_at ASC").
		Find(&out).Error
	return out, err
}

// SelectPayment 原子切换订单收款平台与币种，并重新分配唯一应付金额。
// 同一订单可多次切换（reselect），切换过程中保持原订单号和有效期。
func (s *Store) SelectPayment(ctx context.Context, tradeNo, provider, currency string,
	payAddress string, baseAmount, step float64, maxOffset int, minAmount float64,
	amountPrecision int, slotGrace time.Duration) (*model.Order, error) {
	var updated model.Order
	err := s.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		var order model.Order
		if err := tx.Where("trade_no = ?", tradeNo).First(&order).Error; err != nil {
			return err
		}
		if order.Status == model.StatusReady && !order.Reselect {
			return errors.New("当前订单不允许更换收款方式")
		}
		if order.Status != model.StatusPending && order.Status != model.StatusReady {
			return errors.New("当前订单不允许更换收款方式")
		}
		if time.Now().After(order.ExpiredAt) {
			return errors.New("当前订单不允许更换收款方式")
		}
		if order.Collection == provider && order.Currency == currency &&
			payAddress == order.PayAddress {
			updated = order
			return nil
		}

		if order.Collection == provider && order.Currency == currency && order.Status == model.StatusReady {
			return errors.New("订单已锁定收款 UID，请重新创建订单")
		}

		if err := tx.Where("expires_at < ?", time.Now()).Delete(&model.AmountSlot{}).Error; err != nil {
			return err
		}

		if amountPrecision < 2 || amountPrecision > 6 {
			amountPrecision = 2
		}
		// Different rounding grids can overlap even when exact amounts differ.
		// Only the currently bound slot is replaced. Detached slots belong
		// to canceled/superseded/finalized orders and stay reserved until
		// their original payment window ends, preventing late transfers from
		// being credited to another order.
		var incompatible int64
		if err := tx.Model(&model.AmountSlot{}).
			Where("amount_slots.provider = ? AND amount_slots.currency = ? AND amount_slots.expires_at > ? AND amount_slots.amount_precision <> ?", provider, currency, time.Now(), amountPrecision).
			Count(&incompatible).Error; err != nil {
			return err
		}
		if incompatible > 0 {
			return errors.New("该通道仍有其他精度的金额占用，请等待其结束后重试")
		}
		step = amount.Round(step, amountPrecision)
		if step <= 0 {
			return errors.New("金额步长必须大于 0")
		}
		base := amount.Round(baseAmount, amountPrecision)
		if base < amount.Round(minAmount, amountPrecision) {
			return fmt.Errorf("订单应付金额低于最低金额 %.2f %s", minAmount, currency)
		}
		for i := 1; i <= maxOffset; i++ {
			candidate := amount.Round(base+float64(i)*step, amountPrecision)
			slot := model.AmountSlot{
				Provider: provider, Currency: currency, Amount: candidate, TradeNo: order.TradeNo, AmountPrecision: amountPrecision,
				OrderID: order.ID, ExpiresAt: order.ExpiredAt.Add(slotGrace),
			}
			res := tx.Clauses(clause.OnConflict{DoNothing: true}).Create(&slot)
			if res.Error != nil {
				return res.Error
			}
			if res.RowsAffected == 1 {
				selectedAt := time.Now().Truncate(time.Millisecond)
				if err := tx.Where("order_id = ? AND id <> ?", order.ID, slot.ID).
					Delete(&model.AmountSlot{}).Error; err != nil {
					return err
				}
				if err := tx.Model(&order).Updates(map[string]any{
					"currency":   currency,
					"collection": provider, "pay_address": payAddress,
					"pay_amount": candidate, "slot_offset": i,
					"amount_precision": amountPrecision, "selected_at": selectedAt,
					"status": model.StatusReady, "updated_at": time.Now(),
				}).Error; err != nil {
					return err
				}
				order.Currency, order.Collection = currency, provider
				order.PayAddress = payAddress
				order.PayAmount, order.SlotOffset = candidate, i
				order.AmountPrecision, order.SelectedAt = amountPrecision, &selectedAt
				order.Status = model.StatusReady
				updated = order
				return nil
			}
		}
		return ErrNoSlotAvailable
	})
	if err != nil {
		return nil, err
	}
	return &updated, nil
}

// CancelOrder 停止确认到账并释放待支付订单的金额占用。
//
// 商户主动取消表示独角数卡已经自行关闭订单，因此不能再用 status=3
// 回调干预商户状态；这里将通知状态写成 CANCELED，而不是空字符串，
// 让“无需通知”和“尚未发送”成为两种明确状态。
func (s *Store) CancelOrder(ctx context.Context, tradeNo string) error {
	return s.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		var order model.Order
		if err := tx.Where("trade_no = ?", tradeNo).First(&order).Error; err != nil {
			if errors.Is(err, gorm.ErrRecordNotFound) {
				return errors.New("订单不存在")
			}
			return err
		}
		// 重复取消按幂等成功处理；Dujiao may retry the cancellation request.
		if order.Status == model.StatusCanceled {
			return nil
		}
		if order.Status == model.StatusPaid {
			return errors.New("订单已支付，不能取消")
		}
		if order.Status != model.StatusPending && order.Status != model.StatusReady &&
			(order.Status != model.StatusExpired || order.ExpireFinalized) {
			return errors.New("当前订单状态不允许取消")
		}
		if order.Status == model.StatusExpired {
			// 已经安排过期回调时，取消请求到达得太晚；保持原过期通知，
			// 避免同一商户订单同时收到取消和超时两种终态。
			if order.NotifyStatus == model.NotifyPending || order.NotifyStatus == model.NotifyFailed ||
				order.NextNotifyAt != nil {
				return errors.New("订单已安排过期回调，不能再取消")
			}
		}
		now := time.Now()
		res := tx.Model(&model.Order{}).
			Where("id = ? AND status = ?", order.ID, order.Status).
			Where("notify_status NOT IN ? AND next_notify_at IS NULL",
				[]string{model.NotifySuccess, model.NotifyCanceled}).
			Updates(map[string]any{
				"status": model.StatusCanceled, "expired_at": now,
				"expire_finalized": true,
				"notify_status":    model.NotifyCanceled, "notify_attempts": 0,
				"next_notify_at": nil, "updated_at": now,
			})
		if res.Error != nil {
			return res.Error
		}
		if res.RowsAffected != 1 {
			var latest model.Order
			if err := tx.Where("id = ?", order.ID).First(&latest).Error; err != nil {
				return err
			}
			if latest.Status == model.StatusCanceled {
				return nil
			}
			return errors.New("订单状态已变化，请刷新后重试")
		}
		return tx.Model(&model.AmountSlot{}).Where("order_id = ?", order.ID).Update("order_id", 0).Error
	})
}

// ClaimAndPay 幂等地将一笔收款流水入账到指定订单。
//
// 返回 (true, nil) 表示本次真正完成了入账；(false, nil) 表示该流水或该订单
// 已被处理过，调用方应直接跳过。
func (s *Store) ClaimAndPay(ctx context.Context, order *model.Order, tx model.IncomingTx,
	amountPrecision int) (bool, error) {
	claimed := false
	tx.Provider = strings.ToLower(strings.TrimSpace(tx.Provider))
	tx.Currency = strings.ToUpper(strings.TrimSpace(tx.Currency))
	tx.Type = strings.ToUpper(strings.TrimSpace(tx.Type))
	tx.ReceiverAddr = strings.TrimSpace(tx.ReceiverAddr)
	tx.Raw = truncate(tx.Raw, 16<<10)
	if strings.TrimSpace(tx.TransactionID) == "" {
		return false, errors.New("收款流水号不能为空")
	}
	if tx.Type != "MANUAL" && !((tx.Provider == "binance" && (tx.Type == "PAY" || tx.Type == "C2C")) || (tx.Provider == "okx" && tx.Type == "3")) {
		return false, errors.New("不支持的收款流水类型")
	}

	err := s.db.WithContext(ctx).Transaction(func(d *gorm.DB) error {
		tx.OrderTradeNo = order.TradeNo
		// 在幂等闸门前先重新读取订单。调用方传入的是查询快照，可能已被
		// 后台取消/过期/另一通道入账；不能仅凭旧快照放行。
		var current model.Order
		if err := d.Where("id = ?", order.ID).First(&current).Error; err != nil {
			return err
		}
		// 已支付、取消和宽限期结束都是终态。必须在幂等记录写入前拒绝，
		// 否则会留下“流水已认领但订单未支付”的脏记录。
		if current.Status == model.StatusPaid ||
			current.Status == model.StatusCanceled ||
			(current.Status == model.StatusExpired && current.ExpireFinalized) {
			return errors.New("当前订单状态不允许入账")
		}
		if !strings.EqualFold(current.Collection, tx.Provider) {
			return errors.New("收款平台与订单所选平台不一致")
		}
		if !strings.EqualFold(current.Currency, tx.Currency) {
			return errors.New("收款币种与订单所选币种不一致")
		}
		if strings.TrimSpace(current.PayAddress) == "" ||
			!strings.EqualFold(strings.TrimSpace(current.PayAddress),
				strings.TrimSpace(tx.ReceiverAddr)) {
			return errors.New("收款 UID 与订单不一致")
		}
		if !amount.MatchRounded(amount.FormatPrecision(current.PayAmount, current.AmountPrecision), tx.Amount) {
			return errors.New("收款金额与订单应付金额不一致")
		}
		if current.Status != model.StatusReady &&
			(current.Status != model.StatusExpired || current.ExpireFinalized) {
			return errors.New("当前订单状态不允许入账")
		}

		selectedAt := current.CreatedAt
		if current.SelectedAt != nil {
			selectedAt = *current.SelectedAt
		}
		if tx.Type != "MANUAL" && (tx.TransactionTime.Before(selectedAt) || tx.TransactionTime.After(current.ExpiredAt)) {
			return errors.New("收款时间不在订单锁定后的支付窗口内")
		}
		var slot model.AmountSlot
		if err := d.Where("order_id = ? AND provider = ? AND currency = ? AND amount = ? AND expires_at > ?",
			current.ID, current.Collection, current.Currency, current.PayAmount, time.Now()).
			First(&slot).Error; err != nil {
			return errors.New("订单金额占用已失效")
		}

		// 1) 幂等闸门：incoming_txs 以 (provider, transactionId) 为主键
		res := d.Clauses(clause.OnConflict{DoNothing: true}).Create(&tx)
		if res.Error != nil {
			return res.Error
		}
		if res.RowsAffected == 0 {
			return nil // 该流水已处理过
		}

		// 2) 条件更新，只有仍处于待支付/宽限期过期状态的订单才会被置为已支付。
		now := time.Now()
		upd := d.Model(&model.Order{}).
			Where("id = ? AND ((status = ?) OR (status = ? AND expire_finalized = ?)) AND collection = ? AND currency = ?",
				order.ID, model.StatusReady, model.StatusExpired, false, tx.Provider, tx.Currency).
			Updates(map[string]any{
				"status":           model.StatusPaid,
				"expire_finalized": false,
				"tx_id":            tx.TransactionID,
				"tx_time":          tx.TransactionTime,
				"receive_addr":     tx.ReceiverAddr,
				"paid_at":          now,
				"notify_status":    model.NotifyPending,
				"notify_attempts":  0,
				"next_notify_at":   now,
				"updated_at":       now,
			})
		if upd.Error != nil {
			return upd.Error
		}
		if upd.RowsAffected == 0 {
			return errors.New("订单状态已变化，撤销本次流水认领")
		}

		// 3) 释放唯一应付金额
		if err := d.Model(&model.AmountSlot{}).Where("order_id = ?", order.ID).Delete(&model.AmountSlot{}).Error; err != nil {
			return err
		}
		claimed = true
		return nil
	})
	return claimed, err
}

// MarkExpired 将超时未支付的订单置为宽限期过期。
//
// 此时订单对收银台已经结束，但仍在配置的宽限期内继续确认晚到付款；
// 因此不安排 status=3 通知，也不释放金额占用。
func (s *Store) MarkExpired(ctx context.Context) (int64, error) {
	now := time.Now()
	res := s.db.WithContext(ctx).Model(&model.Order{}).
		Where("status IN ? AND expired_at < ?", []string{model.StatusPending, model.StatusReady}, now).
		Updates(map[string]any{
			"status": model.StatusExpired, "expire_finalized": false,
			"notify_status": "", "notify_attempts": 0,
			"next_notify_at": nil, "updated_at": now,
		})
	return res.RowsAffected, res.Error
}

// FinalizeExpired 在宽限期结束后结束订单：停止确认到账、释放金额占用，
// 并安排 status=3 过期回调。
func (s *Store) FinalizeExpired(ctx context.Context, grace time.Duration) (int64, error) {
	var finalized int64
	err := s.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		now := time.Now()
		var orders []model.Order
		if err := tx.Where("status = ? AND expire_finalized = ? AND expired_at <= ?",
			model.StatusExpired, false, now.Add(-grace)).
			Where("notify_status NOT IN ?", []string{model.NotifySuccess, model.NotifyCanceled}).
			Find(&orders).Error; err != nil {
			return err
		}
		if len(orders) == 0 {
			return nil
		}

		ids := make([]uint, 0, len(orders))
		for i := range orders {
			ids = append(ids, orders[i].ID)
			res := tx.Model(&model.Order{}).
				Where("id = ? AND status = ? AND expire_finalized = ?",
					orders[i].ID, model.StatusExpired, false).
				Where("notify_status NOT IN ?", []string{model.NotifySuccess, model.NotifyCanceled}).
				Updates(map[string]any{
					"expire_finalized": true,
					"notify_status":    model.NotifyPending,
					"notify_attempts":  0,
					"next_notify_at":   now,
					"updated_at":       now,
				})
			if res.Error != nil {
				return res.Error
			}
			if res.RowsAffected > 0 {
				finalized++
			}
		}
		if err := tx.Model(&model.AmountSlot{}).Where("order_id IN ?", ids).Update("order_id", 0).Error; err != nil {
			return err
		}
		return nil
	})
	return finalized, err
}

// -----------------------------------------------------------------------------
// 回调通知
// -----------------------------------------------------------------------------

// DueNotifications 取出到期待通知的订单。
func (s *Store) DueNotifications(ctx context.Context, maxAttempts, limit int) ([]model.Order, error) {
	var out []model.Order
	now := time.Now()
	err := s.db.WithContext(ctx).
		Where("protocol = ?", "bepusdt").
		Where("status IN ? AND notify_status IN ? AND notify_attempts < ?",
			[]string{model.StatusPaid, model.StatusExpired}, []string{model.NotifyPending, ""}, maxAttempts).
		Where("(status = ? OR expire_finalized = ?)", model.StatusPaid, true).
		Where("next_notify_at IS NULL OR next_notify_at <= ?", now).
		Order("next_notify_at ASC").
		Limit(limit).
		Find(&out).Error
	return out, err
}

// FinishNotify 记录一次回调结果。
//
// 发送请求期间订单可能被替换、取消或进入另一终态，因此只有仍处于该次
// 通知对应状态的订单才能写入结果，防止旧请求覆盖新状态。
func (s *Store) FinishNotify(ctx context.Context, o *model.Order, attempt int, success bool,
	respBody string, statusCode int, reqURL, errMsg string, nextAt *time.Time) error {

	return s.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		updates := map[string]any{
			"notify_attempts":  attempt,
			"last_notify_resp": truncate(respBody, 500),
			"updated_at":       time.Now(),
		}
		if success {
			updates["notify_status"] = model.NotifySuccess
			updates["notified_at"] = time.Now()
			updates["next_notify_at"] = nil
		} else if nextAt != nil {
			updates["notify_status"] = model.NotifyPending
			updates["next_notify_at"] = *nextAt
		} else {
			updates["notify_status"] = model.NotifyFailed
			updates["next_notify_at"] = nil
		}

		res := tx.Model(&model.Order{}).
			Where("id = ? AND status = ? AND notify_attempts = ?",
				o.ID, o.Status, o.NotifyAttempts).
			Updates(updates)
		if res.Error != nil {
			return res.Error
		}
		if res.RowsAffected == 0 {
			// 订单已变化：保留审计日志，但不回写旧请求结果。
			return createNotifyLog(tx, o, attempt, success, statusCode, respBody, reqURL, errMsg)
		}
		if err := createNotifyLog(tx, o, attempt, success, statusCode, respBody, reqURL, errMsg); err != nil {
			return err
		}
		return nil
	})
}

func createNotifyLog(tx *gorm.DB, o *model.Order, attempt int, success bool,
	statusCode int, respBody, reqURL, errMsg string) error {
	return tx.Create(&model.NotifyLog{
		OrderID:         o.ID,
		TradeNo:         o.TradeNo,
		Attempt:         attempt,
		URL:             reqURL,
		Success:         success,
		StatusCode:      statusCode,
		Response:        truncate(respBody, 2000),
		Error:           errMsg,
		Token:           "-",
		MerchantOrderID: o.OutTradeNo,
		OK:              success,
	}).Error
}

func truncate(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n]
}

func normalizeCurrencyLimit(raw string) string {
	parts := strings.FieldsFunc(strings.ToUpper(strings.TrimSpace(raw)), func(r rune) bool {
		return r == ',' || r == '，' || r == ' ' || r == '/' || r == '|' || r == ';'
	})
	if len(parts) == 0 {
		return ""
	}
	out := make([]string, 0, len(parts))
	seen := make(map[string]struct{}, len(parts))
	for _, item := range parts {
		item = strings.TrimSpace(item)
		if item == "" {
			continue
		}
		key := item
		if _, ok := seen[key]; ok {
			continue
		}
		seen[key] = struct{}{}
		out = append(out, item)
	}
	return strings.Join(out, ",")
}

// WithCurrentNotification serializes delivery with replacement of the same
// merchant order. Different merchant orders proceed independently.
func (s *Store) WithCurrentNotification(ctx context.Context, snapshot *model.Order, deliver func(*model.Order)) error {
	unlock := s.lockOrder(snapshot.Pid, snapshot.OutTradeNo)
	defer unlock()
	current, err := s.GetByTradeNo(ctx, snapshot.TradeNo)
	if err != nil {
		return err
	}
	if current == nil || current.Status != snapshot.Status || current.NotifyAttempts != snapshot.NotifyAttempts ||
		(current.NotifyStatus != model.NotifyPending && current.NotifyStatus != "") {
		return nil
	}
	if current.Status != model.StatusPaid && !(current.Status == model.StatusExpired && current.ExpireFinalized) {
		return nil
	}
	deliver(current)
	return nil
}

func (s *Store) lockOrder(pid, outTradeNo string) func() {
	key := pid + "\x00" + outTradeNo
	s.orderLocksMu.Lock()
	lock := s.orderLocks[key]
	if lock == nil {
		lock = &orderLock{}
		s.orderLocks[key] = lock
	}
	lock.refs++
	s.orderLocksMu.Unlock()

	lock.mu.Lock()
	return func() {
		lock.mu.Unlock()
		s.orderLocksMu.Lock()
		lock.refs--
		if lock.refs == 0 {
			delete(s.orderLocks, key)
		}
		s.orderLocksMu.Unlock()
	}
}

// HasActivePayments is called while settings.Manager serializes configuration writes and checkout selection.
func (s *Store) HasActivePayments(ctx context.Context) (bool, error) {
	var count int64
	err := s.db.WithContext(ctx).Model(&model.AmountSlot{}).Where("order_id <> 0 AND expires_at > ?", time.Now()).Count(&count).Error
	return count > 0, err
}
