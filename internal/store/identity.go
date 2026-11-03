package store

import (
	"encoding/json"
	"fmt"
	"strings"

	"gorm.io/gorm"
	"gorm.io/gorm/clause"
	"litepay/internal/model"
)

// rawEvent reads only exchange-provided identifiers. No suffix stripping or cross-namespace inference.
func rawEvent(provider, raw string) (event, reference string, ambiguous bool) {
	var row map[string]json.RawMessage
	if json.Unmarshal([]byte(raw), &row) != nil {
		return
	}
	text := func(key string) string {
		var s string
		if json.Unmarshal(row[key], &s) == nil {
			return strings.TrimSpace(s)
		}
		var n json.Number
		if json.Unmarshal(row[key], &n) == nil {
			return n.String()
		}
		return ""
	}
	if provider == "binance" {
		event = text("transactionId")
		reference = text("orderId")
		var details []json.RawMessage
		json.Unmarshal(row["fundsDetail"], &details)
		ambiguous = len(details) > 1
	} else if provider == "okx" {
		event = text("depId")
		reference = text("fromWdId")
	}
	return
}

func normalizeClaimIdentity(tx *model.IncomingTx) error {
	if tx.IdentityIssue != "" {
		return rejectClaim("交易身份不明或资金事件存在歧义")
	}
	if tx.Raw != "" {
		event, ref, ambiguous := rawEvent(tx.Provider, tx.Raw)
		if ambiguous || event == "" || (tx.EventID != "" && event != tx.EventID) {
			return rejectClaim("交易资金事件身份不完整或存在歧义")
		}
		tx.EventID = event
		if tx.ReferenceID == "" {
			tx.ReferenceID = ref
		}
	}
	// Internal/test callers may supply their already authenticated, stable ID without raw JSON.
	if tx.EventID == "" {
		tx.EventID = tx.TransactionID
	}
	if tx.CanonicalID == "" {
		tx.CanonicalID = tx.TransactionID
	}
	if tx.Provider == "binance" && tx.Raw != "" {
		tx.CanonicalID = tx.EventID
	}
	if tx.Provider == "okx" && tx.Raw != "" {
		tx.CanonicalID = "okx:" + tx.EventID
	}
	if tx.TransactionID != tx.CanonicalID {
		return rejectClaim("核销唯一键与标准交易身份不一致")
	}
	return nil
}

// migrateEventIdentity is additive and transactional: paid orders, amounts and historical keys are untouched.
// Repeated legacy detail claims remain intact, but their shared event becomes blocked for future claims.
func migrateEventIdentity(db *gorm.DB) error {
	return db.Transaction(func(d *gorm.DB) error {
		var rows []model.IncomingTx
		if err := d.Where("canonical_id IS NULL OR canonical_id = ''").Find(&rows).Error; err != nil {
			return err
		}
		for _, row := range rows {
			event, ref, _ := rawEvent(row.Provider, row.Raw)
			issue := ""
			if event == "" {
				event = row.TransactionID
				issue = "legacy_unknown"
			}
			if err := d.Model(&model.IncomingTx{}).Where("provider = ? AND transaction_id = ?", row.Provider, row.TransactionID).Updates(map[string]any{"event_id": event, "reference_id": ref, "canonical_id": row.TransactionID, "identity_issue": issue}).Error; err != nil {
				return err
			}
			guard := model.EventClaim{Provider: row.Provider, EventID: event, CanonicalID: row.TransactionID, TradeNo: row.OrderTradeNo, State: "claimed"}
			res := d.Clauses(clause.OnConflict{DoNothing: true}).Create(&guard)
			if res.Error != nil {
				return res.Error
			}
			if res.RowsAffected == 0 {
				if err := d.Model(&model.EventClaim{}).Where("provider = ? AND event_id = ? AND trade_no <> ?", row.Provider, event, row.OrderTradeNo).Update("state", "legacy_conflict").Error; err != nil {
					return err
				}
			}
		}
		var obs []model.TxObservation
		if err := d.Where("canonical_id IS NULL OR canonical_id = ''").Find(&obs).Error; err != nil {
			return err
		}
		for _, row := range obs {
			event, ref, ambiguous := rawEvent(row.Provider, row.Raw)
			issue := ""
			if event == "" {
				issue = "legacy_unknown"
			}
			if ambiguous {
				issue = "ambiguous"
				if event != "" {
					if err := d.Clauses(clause.OnConflict{DoNothing: true}).Create(&model.EventClaim{Provider: row.Provider, EventID: event, CanonicalID: row.TransactionID, State: "ambiguous"}).Error; err != nil {
						return err
					}
				}
			}
			if err := d.Model(&model.TxObservation{}).Where("provider = ? AND transaction_id = ?", row.Provider, row.TransactionID).Updates(map[string]any{"event_id": event, "reference_id": ref, "canonical_id": row.TransactionID, "identity_issue": issue}).Error; err != nil {
				return err
			}
		}
		return d.Exec(`INSERT OR IGNORE INTO event_claims(provider,event_id,canonical_id,state)
 SELECT provider,event_id,MIN(canonical_id),'ambiguous' FROM tx_observations
 WHERE identity_issue = 'ambiguous' AND event_id <> '' GROUP BY provider,event_id`).Error
	})
}

func guardLegacyIdentity(d *gorm.DB, tx *model.IncomingTx) error {
	var count int64
	q := d.Model(&model.IncomingTx{}).Where("provider = ? AND identity_issue = ?", tx.Provider, "legacy_unknown")
	if tx.Provider == "binance" {
		pattern := strings.NewReplacer("\\", "\\\\", "%", "\\%", "_", "\\_").Replace(tx.EventID) + ":%"
		q = q.Where("transaction_id = ? OR transaction_id = ? OR transaction_id LIKE ? ESCAPE '\\'", tx.EventID, tx.CanonicalID, pattern)
	} else {
		q = q.Where("transaction_id IN ?", []string{tx.CanonicalID, "okx:" + tx.ReferenceID})
	}
	if err := q.Count(&count).Error; err != nil {
		return err
	}
	if count > 0 {
		return rejectClaim("历史交易身份未确认，需人工对账")
	}
	return nil
}

func eventIdentityError(err error) error {
	return fmt.Errorf("交易事件身份迁移失败: %w", err)
}
