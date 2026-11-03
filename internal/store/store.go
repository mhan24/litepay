// Package store 封装 SQLite 数据访问（Gorm）。
package store

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"log"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
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
	db      *gorm.DB
	workers workerHealth

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
	dsn := dbPath + "?_pragma=busy_timeout(5000)&_pragma=journal_mode(WAL)&_pragma=synchronous(FULL)"
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
		&model.TxObservation{}, &model.ScanCursor{}, &model.NotifyLog{}, &model.Setting{},
		&model.Credential{}); err != nil {
		return nil, fmt.Errorf("数据库迁移失败: %w", err)
	}
	if err := migrateFixedAmounts(db); err != nil {
		return nil, err
	}
	if err := migrateObservationLedger(db); err != nil {
		return nil, fmt.Errorf("回填未归属流水账本失败: %w", err)
	}
	return &Store{db: db, orderLocks: make(map[string]*orderLock)}, nil
}

// migrateObservationLedger 为旧库补齐观察账本的状态位与原因优先级。
//
// status / reason_rank 是后加的列，历史行迁移后只会取默认值。若不回填，
// 旧库里一条 claim_failed 的优先级仍是 0，下一次扫描的 no_match 就会把
// 真正的核销失败原因覆盖掉。
func migrateObservationLedger(db *gorm.DB) error {
	if err := db.Model(&model.TxObservation{}).
		Where("status IS NULL OR status = ''").
		Update("status", model.ObservationOpen).Error; err != nil {
		return err
	}
	for _, reason := range model.TxObservationReasons() {
		rank := model.TxObservationRank(reason)
		if rank == 0 {
			continue
		}
		if err := db.Model(&model.TxObservation{}).
			Where("reason = ? AND reason_rank < ?", reason, rank).
			Update("reason_rank", rank).Error; err != nil {
			return err
		}
	}
	return nil
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
			// An already displayed payment is immutable to merchant request changes.
			if (existing.Status == model.StatusReady || (existing.Status == model.StatusExpired && existing.Collection != "")) && !existing.ExpireFinalized {
				if !paymentContractUnchanged(&existing, in) {
					return errors.New("商户订单已有有效支付约定；参数冲突，请使用新的商户订单号")
				}
				result = &existing
				return nil
			}
			if (existing.Status == model.StatusPending || existing.Status == model.StatusReady) &&
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

// paymentContractUnchanged 判断重复建单请求是否与原订单的支付约定一致。
//
// 金额、法币、币种限制和回调地址共同决定了用户会看到什么、商户会收到什么；
// 只要其中任何一项变化，就是新的支付约定，必须重建订单。全部一致时说明这
// 只是商户侧的重试，复用原订单才不会让已在进行的付款失去归属。
func paymentContractUnchanged(o *model.Order, in CreateOrderInput) bool {
	if !amountMatch(o.OriginMoney, in.OriginMoney) {
		return false
	}
	return strings.EqualFold(strings.TrimSpace(o.OriginCurrency), strings.TrimSpace(in.OriginCurrency)) &&
		normalizeCurrencyLimit(o.CurrencyLimit) == normalizeCurrencyLimit(in.CurrencyLimit) &&
		strings.TrimSpace(o.NotifyURL) == strings.TrimSpace(in.NotifyURL) &&
		strings.TrimSpace(o.ReturnURL) == strings.TrimSpace(in.ReturnURL)
}

// amountMatch 按十进制修约比较两个金额字符串，避免 "2.5" 与 "2.50" 被当成
// 不同金额而触发不必要的重建。
func amountMatch(a, b string) bool {
	left, err := amount.Decimal(a)
	if err != nil {
		return false
	}
	right, err := amount.Decimal(b)
	return err == nil && left.Cmp(right) == 0
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
	err := s.db.WithContext(ctx).Model(&model.Order{}).
		Joins("JOIN amount_slots ON amount_slots.order_id = orders.id").
		Where("orders.collection = ? AND amount_slots.provider = ?", provider, provider).
		Where("amount_slots.currency = orders.currency AND ((orders.pay_units > 0 AND amount_slots.amount_units = orders.pay_units) OR (orders.pay_units = 0 AND amount_slots.amount = orders.pay_amount))").
		Where("orders.origin_currency = ?", "USD").
		Where("amount_slots.order_id <> 0").
		Distinct("orders.*").
		// EXPIRED 在宽限期内仍可入账，覆盖付款已发出但交易所记录稍晚的情况。
		// CANCELED/SUPERSEDED 永不进入这里。
		Where("(orders.status = ? OR (orders.status = ? AND orders.expire_finalized = ? ))",
			model.StatusReady, model.StatusExpired, false).
		Order("orders.created_at ASC").
		Find(&out).Error
	return out, err
}

// SelectPayment 原子切换订单收款平台与币种，并重新分配唯一应付金额。
// 同一订单可多次切换（reselect），切换过程中保持原订单号和有效期。
func (s *Store) SelectPayment(ctx context.Context, tradeNo, provider, currency string,
	payAddress string, baseAmount, step float64, maxOffset int, minAmount float64,
	amountPrecision int, slotGrace time.Duration) (*model.Order, error) {
	return s.selectPaymentDecimal(ctx, tradeNo, provider, currency, payAddress, strconv.FormatFloat(baseAmount, 'f', -1, 64), step, maxOffset, minAmount, amountPrecision, slotGrace)
}

// SelectPaymentDecimal is the HTTP path: keep merchant amount as decimal text.
func (s *Store) SelectPaymentDecimal(ctx context.Context, tradeNo, provider, currency, payAddress, baseText string,
	step float64, maxOffset int, minAmount float64, amountPrecision int, slotGrace time.Duration) (*model.Order, error) {
	return s.selectPaymentDecimal(ctx, tradeNo, provider, currency, payAddress, baseText, step, maxOffset, minAmount, amountPrecision, slotGrace)
}
func (s *Store) selectPaymentDecimal(ctx context.Context, tradeNo, provider, currency, payAddress, baseText string,
	step float64, maxOffset int, minAmount float64, amountPrecision int, slotGrace time.Duration) (*model.Order, error) {
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

		if err := tx.Where("order_id = 0 AND expires_at < ?", time.Now()).Delete(&model.AmountSlot{}).Error; err != nil {
			return err
		}

		if amountPrecision < 2 || amountPrecision > 6 {
			amountPrecision = 2
		}
		// Different rounding grids can overlap even when exact amounts differ.
		// Only the currently bound slot is replaced. Detached slots belong
		// to canceled/superseded/finalized orders or to this order's earlier
		// selections, and stay reserved until their original payment window
		// ends, preventing late transfers from being credited to another order.
		var incompatible int64
		if err := tx.Model(&model.AmountSlot{}).
			Where("amount_slots.provider = ? AND amount_slots.currency = ? AND (amount_slots.order_id <> 0 OR amount_slots.expires_at > ?) AND amount_slots.amount_precision <> ?", provider, currency, time.Now(), amountPrecision).
			Count(&incompatible).Error; err != nil {
			return err
		}
		if incompatible > 0 {
			return errors.New("该通道仍有其他精度的金额占用，请等待其结束后重试")
		}
		stepUnits, err := amount.FloatUnits(step, amountPrecision)
		if err != nil {
			return err
		}
		baseUnits, err := amount.Units(baseText, amountPrecision)
		if err != nil {
			return err
		}
		step = amount.Float(stepUnits)
		if step <= 0 {
			return errors.New("金额步长必须大于 0")
		}
		base := amount.Float(baseUnits)
		if base < amount.Round(minAmount, amountPrecision) {
			return fmt.Errorf("订单应付金额低于最低金额 %.2f %s", minAmount, currency)
		}
		for i := 1; i <= maxOffset; i++ {
			if stepUnits > (amount.MaxUnits-baseUnits)/int64(i) {
				return errors.New("金额档位溢出")
			}
			candidateUnits := baseUnits + int64(i)*stepUnits
			candidate := amount.Float(candidateUnits)
			if roundTrip, err := amount.FloatUnits(candidate, amountPrecision); err != nil || roundTrip != candidateUnits {
				return errors.New("金额无法兼容商户 JSON 数值精度")
			}
			slot := model.AmountSlot{
				Provider: provider, Currency: currency, Amount: candidate, AmountUnits: candidateUnits, TradeNo: order.TradeNo, AmountPrecision: amountPrecision,
				OrderID: order.ID, ExpiresAt: order.ExpiredAt.Add(slotGrace),
			}
			res := tx.Clauses(clause.OnConflict{DoNothing: true}).Create(&slot)
			if res.Error != nil {
				return res.Error
			}
			if res.RowsAffected == 0 {
				// 金额已被占用。占用者是本订单此前释放的档位（重选同一金额）
				// 时重新绑定；否则该金额处于冷却期，尝试下一个偏移。
				var existing model.AmountSlot
				if err := tx.Where("provider = ? AND currency = ? AND amount_units = ?", provider, currency, candidateUnits).
					First(&existing).Error; err != nil {
					return err
				}
				if existing.TradeNo != order.TradeNo {
					continue
				}
				if err := tx.Model(&model.AmountSlot{}).Where("id = ?", existing.ID).Updates(map[string]any{
					"order_id": order.ID, "expires_at": order.ExpiredAt.Add(slotGrace),
					"amount_precision": amountPrecision,
				}).Error; err != nil {
					return err
				}
				slot.ID = existing.ID
			}
			selectedAt := time.Now().Truncate(time.Millisecond)
			if err := tx.Model(&model.AmountSlot{}).Where("order_id = ? AND id <> ?", order.ID, slot.ID).
				Update("order_id", 0).Error; err != nil {
				return err
			}
			if err := tx.Model(&order).Updates(map[string]any{
				"currency":   currency,
				"collection": provider, "pay_address": payAddress,
				"pay_amount": candidate, "pay_units": candidateUnits, "slot_offset": i,
				"amount_precision": amountPrecision, "selected_at": selectedAt,
				"status": model.StatusReady, "updated_at": time.Now(),
			}).Error; err != nil {
				return err
			}
			order.Currency, order.Collection = currency, provider
			order.PayAddress = payAddress
			order.PayAmount, order.SlotOffset = candidate, i
			order.PayUnits = candidateUnits
			order.AmountPrecision, order.SelectedAt = amountPrecision, &selectedAt
			order.Status = model.StatusReady
			updated = order
			return nil
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
		return false, rejectClaim("收款流水号不能为空")
	}
	if tx.Type != "MANUAL" && !((tx.Provider == "binance" && (tx.Type == "PAY" || tx.Type == "C2C")) || (tx.Provider == "okx" && tx.Type == "3")) {
		return false, rejectClaim("不支持的收款流水类型")
	}

	err := s.db.WithContext(ctx).Transaction(func(d *gorm.DB) error {
		// Check consumption before validating a stale order snapshot.
		var consumed int64
		if err := d.Model(&model.IncomingTx{}).Where("provider = ? AND transaction_id = ?", tx.Provider, tx.TransactionID).Count(&consumed).Error; err != nil {
			return err
		}
		if consumed != 0 {
			return nil
		}
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
			return rejectClaim("当前订单状态不允许入账")
		}
		if !strings.EqualFold(current.Collection, tx.Provider) {
			return rejectClaim("收款平台与订单所选平台不一致")
		}
		if !strings.EqualFold(current.Currency, tx.Currency) {
			return rejectClaim("收款币种与订单所选币种不一致")
		}
		if strings.TrimSpace(current.PayAddress) == "" ||
			!strings.EqualFold(strings.TrimSpace(current.PayAddress),
				strings.TrimSpace(tx.ReceiverAddr)) {
			return rejectClaim("收款 UID 与订单不一致")
		}
		actualText := tx.AmountText
		if actualText == "" {
			actualText = strconv.FormatFloat(tx.Amount, 'f', -1, 64)
		}
		tx.AmountText = actualText
		expected := amount.FormatPrecision(current.PayAmount, current.AmountPrecision)
		if current.PayUnits != 0 {
			expected = amount.Text(current.PayUnits, current.AmountPrecision)
		}
		if !amount.MatchDecimal(expected, actualText) {
			return rejectClaim("收款金额与订单应付金额不一致")
		}
		if current.Status != model.StatusReady &&
			(current.Status != model.StatusExpired || current.ExpireFinalized) {
			return rejectClaim("当前订单状态不允许入账")
		}

		selectedAt := current.CreatedAt
		if current.SelectedAt != nil {
			selectedAt = *current.SelectedAt
		}
		if tx.Type != "MANUAL" && (tx.TransactionTime.Before(selectedAt) || tx.TransactionTime.After(current.ExpiredAt)) {
			return rejectClaim("收款时间不在订单锁定后的支付窗口内")
		}
		var slot model.AmountSlot
		units := current.PayUnits
		if units == 0 {
			var err error
			units, err = amount.FloatUnits(current.PayAmount, current.AmountPrecision)
			if err != nil {
				return rejectClaim("订单金额无效")
			}
		}
		if err := d.Where("order_id = ? AND provider = ? AND currency = ? AND amount_units = ?",
			current.ID, current.Collection, current.Currency, units).
			First(&slot).Error; err != nil {
			if errors.Is(err, gorm.ErrRecordNotFound) {
				return rejectClaim("订单金额占用已失效")
			}
			return err
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
			return rejectClaim("订单状态已变化，撤销本次流水认领")
		}

		// 3) 释放唯一应付金额的占用。档位行保留至原支付窗口结束（冷却期），
		// 防止付款人重复转账在窗口内被错误归到复用该金额的新订单。
		if err := d.Model(&model.AmountSlot{}).Where("order_id = ?", order.ID).
			Update("order_id", 0).Error; err != nil {
			return err
		}
		// 与支付状态同一事务提交，崩溃也不会留下已入账的未归属记录。
		if err := d.Model(&model.TxObservation{}).
			Where("provider = ? AND transaction_id = ?", tx.Provider, tx.TransactionID).
			Updates(map[string]any{"status": model.ObservationResolved,
				"resolved_trade_no": current.TradeNo, "resolved_at": now, "updated_at": now}).Error; err != nil {
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
			if orders[i].Collection != "" {
				var cursor model.ScanCursor
				if err := tx.Where("provider = ?", orders[i].Collection).First(&cursor).Error; err != nil {
					if errors.Is(err, gorm.ErrRecordNotFound) {
						if err := tx.Model(&orders[i]).Update("reconciliation_reason", "等待交易所成功补扫；超过24小时请人工对账").Error; err != nil {
							return err
						}
						continue
					}
					return err
				}
				if cursor.LastError != "" || cursor.ScannedUntil.Before(orders[i].ExpiredAt.Add(grace)) ||
					(cursor.GapEnd != nil && cursor.GapEnd.After(orders[i].CreatedAt)) {
					if err := tx.Model(&orders[i]).Update("reconciliation_reason", "扫描未覆盖付款窗口或存在历史缺口；超过24小时请人工对账").Error; err != nil {
						return err
					}
					continue
				}
			}
			ids = append(ids, orders[i].ID)
			res := tx.Model(&model.Order{}).
				Where("id = ? AND status = ? AND expire_finalized = ?",
					orders[i].ID, model.StatusExpired, false).
				Where("notify_status NOT IN ?", []string{model.NotifySuccess, model.NotifyCanceled}).
				Updates(map[string]any{
					"expire_finalized":      true,
					"reconciliation_reason": "",
					"notify_status":         model.NotifyPending,
					"notify_attempts":       0,
					"next_notify_at":        now,
					"updated_at":            now,
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
		Where("status IN ? AND notify_attempts < ?",
			[]string{model.StatusPaid, model.StatusExpired}, maxAttempts).
		// SENDING 表示已领取但结果未落库（进程崩溃或正在发送）。租约到期后
		// 必须重新参与调度，否则这次回调会永久卡在“发送中”。
		Where("(notify_status IN ? OR (notify_status = ? AND (next_notify_at IS NULL OR next_notify_at <= ?)))",
			[]string{model.NotifyPending, ""}, model.NotifySending, now).
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
			"notify_lease_id":  "",
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
			Where("notify_lease_id = ?", o.NotifyLeaseID).
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
	return tx.Create(&model.NotifyLog{LeaseID: o.NotifyLeaseID,
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
		(current.NotifyStatus != model.NotifyPending && current.NotifyStatus != model.NotifySending &&
			current.NotifyStatus != "") {
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

// RecordTxObservation 记录未归属交易的观察证据（迟到付款、币种/金额不符、
// 被过滤器拒绝、核销被拒等）。
//
// 同一笔流水每轮扫描都会被重新读到，而每次看到的上下文可能不同：先因订单
// 状态被拒（claim_failed），订单终结后同一笔流水就只剩 no_match。因此这里
// 遵守两条规则，避免账本给出错误结论：
//   - 只升级不降级：ReasonRank 更高的原因不会被更弱的原因覆盖；
//   - 已入账即定稿：status=resolved 的记录不会被改回未归属。
func (s *Store) RecordTxObservation(ctx context.Context, obs model.TxObservation) error {
	now := time.Now()
	obs.Reason = model.NormalizeTxObservationReason(obs.Reason)
	obs.ReasonRank = model.TxObservationRank(obs.Reason)
	obs.Status = model.ObservationOpen

	return s.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		var existing []model.TxObservation
		if err := tx.Where("provider = ? AND transaction_id = ?", obs.Provider, obs.TransactionID).
			Limit(1).Find(&existing).Error; err != nil {
			return err
		}
		if len(existing) == 0 {
			obs.CreatedAt, obs.UpdatedAt = now, now
			return tx.Create(&obs).Error
		}
		if existing[0].Status == model.ObservationResolved {
			// 已入账的流水会随着历史窗口反复出现（幂等认领返回 false），
			// 这里只刷新时间，保留“已归属”的结论。
			return tx.Model(&model.TxObservation{}).
				Where("provider = ? AND transaction_id = ?", obs.Provider, obs.TransactionID).
				Update("updated_at", now).Error
		}
		if obs.ReasonRank < existing[0].ReasonRank {
			return nil
		}
		return tx.Model(&model.TxObservation{}).
			Where("provider = ? AND transaction_id = ?", obs.Provider, obs.TransactionID).
			Updates(map[string]any{
				"reason":           obs.Reason,
				"reason_rank":      obs.ReasonRank,
				"order_trade_no":   obs.OrderTradeNo,
				"transaction_time": obs.TransactionTime,
				"updated_at":       now,
			}).Error
	})
}

// ResolveTxObservation 把观察账本中的流水标记为已入账。
//
// 核销成功的流水不属于未归属流水；账本行保留，用于事后核对迟到付款是在
// 哪一轮扫描里补上的。tradeNo 为空时只标记状态，不改写已记录的订单号。
func (s *Store) ResolveTxObservation(ctx context.Context, provider, transactionID, tradeNo string) error {
	provider = strings.ToLower(strings.TrimSpace(provider))
	transactionID = strings.TrimSpace(transactionID)
	if provider == "" || transactionID == "" {
		return nil
	}
	now := time.Now()
	updates := map[string]any{
		"status":      model.ObservationResolved,
		"resolved_at": now,
		"updated_at":  now,
	}
	if tradeNo = strings.TrimSpace(tradeNo); tradeNo != "" {
		updates["resolved_trade_no"] = tradeNo
	}
	return s.db.WithContext(ctx).Model(&model.TxObservation{}).
		Where("provider = ? AND transaction_id = ?", provider, transactionID).
		Updates(updates).Error
}

// RecentTxObservations 返回最近仍未归属的流水（后台排查用）。
//
// 除状态位外，这里再排除一次已存在于 incoming_txs 的流水：账本上线前写入的
// 旧行没有状态位，靠这条 JOIN 也不会在后台误报成未归属。
func (s *Store) RecentTxObservations(ctx context.Context, limit int) ([]model.TxObservation, error) {
	if limit <= 0 || limit > 100 {
		limit = 20
	}
	var out []model.TxObservation
	err := s.db.WithContext(ctx).
		Where("status IS NULL OR status <> ?", model.ObservationResolved).
		Where(`NOT EXISTS (SELECT 1 FROM incoming_txs WHERE incoming_txs.provider = tx_observations.provider` +
			` AND incoming_txs.transaction_id = tx_observations.transaction_id)`).
		Order("transaction_time DESC, provider ASC, transaction_id ASC").
		Limit(limit).Find(&out).Error
	return out, err
}

// CountOpenTxObservations 返回仍未归属的流水数量（后台概览用）。
func (s *Store) CountOpenTxObservations(ctx context.Context) (int64, error) {
	var count int64
	err := s.db.WithContext(ctx).Model(&model.TxObservation{}).
		Where("status IS NULL OR status <> ?", model.ObservationResolved).
		Where(`NOT EXISTS (SELECT 1 FROM incoming_txs WHERE incoming_txs.provider = tx_observations.provider` +
			` AND incoming_txs.transaction_id = tx_observations.transaction_id)`).
		Count(&count).Error
	return count, err
}

// PruneTxObservations 删除超过保留期的观察记录。已归属与未归属都会清理：
// 账本只服务于近期排查，长期运行不能无限增长。
func (s *Store) PruneTxObservations(ctx context.Context, before time.Time) (int64, error) {
	res := s.db.WithContext(ctx).Where("updated_at < ?", before).Delete(&model.TxObservation{})
	return res.RowsAffected, res.Error
}

// -----------------------------------------------------------------------------
// 扫描游标
// -----------------------------------------------------------------------------

// ScanCursor 返回指定通道的扫描进度；从未扫描过时返回 nil。
func (s *Store) ScanCursor(ctx context.Context, provider string) (*model.ScanCursor, error) {
	provider = strings.ToLower(strings.TrimSpace(provider))
	var rows []model.ScanCursor
	if err := s.db.WithContext(ctx).Where("provider = ?", provider).Limit(1).Find(&rows).Error; err != nil {
		return nil, err
	}
	if len(rows) == 0 {
		return nil, nil
	}
	return &rows[0], nil
}

// SaveScanCursor 记录一次完整扫描的结束时间，并把失败原因清空。
//
// 只有整轮扫描（含补扫窗口）都成功才会调用，失败时游标必须原地不动，
// 否则重启后那段未扫到的时间就永久丢了。
func (s *Store) SaveScanCursor(ctx context.Context, provider string, until time.Time) error {
	provider = strings.ToLower(strings.TrimSpace(provider))
	now := time.Now()
	return s.db.WithContext(ctx).Clauses(clause.OnConflict{
		Columns: []clause.Column{{Name: "provider"}},
		DoUpdates: clause.Assignments(map[string]any{
			"scanned_until": gorm.Expr("MAX(scan_cursors.scanned_until, excluded.scanned_until)"),
			"last_run_at":   now, "last_error": "", "updated_at": now,
		}),
	}).Create(&model.ScanCursor{
		Provider: provider, ScannedUntil: until, LastRunAt: now, UpdatedAt: now,
	}).Error
}

// SaveScanError 记录扫描失败原因，游标保持不变。
func (s *Store) SaveScanError(ctx context.Context, provider, message string) error {
	provider = strings.ToLower(strings.TrimSpace(provider))
	now := time.Now()
	return s.db.WithContext(ctx).Clauses(clause.OnConflict{
		Columns:   []clause.Column{{Name: "provider"}},
		DoUpdates: clause.AssignmentColumns([]string{"last_error", "last_run_at", "updated_at"}),
	}).Create(&model.ScanCursor{
		Provider: provider, LastError: truncate(message, 500), LastRunAt: now, UpdatedAt: now,
	}).Error
}

// ClaimNotify 领取一次回调任务：把订单从待发送状态原子地切到“发送中”，
// 并写入租约到期时间。
//
// 回调是外部副作用，进程可能在请求发出后、结果落库前崩溃。用租约而不是
// 纯内存标记，重启后租约到期就能重新领取；同一订单不会被两个 worker
// 同时发送（条件更新保证只有一方 RowsAffected = 1）。
func (s *Store) ClaimNotify(ctx context.Context, o *model.Order, lease time.Duration) (bool, error) {
	if lease <= 0 {
		return false, errors.New("回调租约必须为正数")
	}
	var token [16]byte
	if _, err := rand.Read(token[:]); err != nil {
		return false, err
	}
	leaseID := hex.EncodeToString(token[:])
	now := time.Now()
	res := s.db.WithContext(ctx).Model(&model.Order{}).
		Where("id = ? AND status = ? AND notify_attempts = ?", o.ID, o.Status, o.NotifyAttempts).
		Where("(notify_status IN ? OR (notify_status = ? AND (next_notify_at IS NULL OR next_notify_at <= ?)))",
			[]string{model.NotifyPending, ""}, model.NotifySending, now).
		Updates(map[string]any{
			"notify_status":   model.NotifySending,
			"notify_lease_id": leaseID,
			"next_notify_at":  now.Add(lease),
			"updated_at":      now,
		})
	if res.Error != nil {
		return false, res.Error
	}
	if res.RowsAffected == 1 {
		o.NotifyLeaseID = leaseID
		return true, nil
	}
	return false, nil
}

// RecordScanGap preserves the union of unrecoverable windows across restarts.
func (s *Store) RecordScanGap(ctx context.Context, provider string, start, end time.Time) error {
	return s.db.WithContext(ctx).Clauses(clause.OnConflict{Columns: []clause.Column{{Name: "provider"}}, DoUpdates: clause.Assignments(map[string]any{
		"gap_start": gorm.Expr("CASE WHEN scan_cursors.gap_start IS NULL OR excluded.gap_start < scan_cursors.gap_start THEN excluded.gap_start ELSE scan_cursors.gap_start END"),
		"gap_end":   gorm.Expr("CASE WHEN scan_cursors.gap_end IS NULL OR excluded.gap_end > scan_cursors.gap_end THEN excluded.gap_end ELSE scan_cursors.gap_end END"),
	})}).Create(&model.ScanCursor{Provider: provider, GapStart: &start, GapEnd: &end}).Error
}
