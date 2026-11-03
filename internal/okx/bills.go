package okx

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"litepay/internal/amount"
	"net/url"
	"strconv"
	"strings"
	"time"
)

var ErrBillsInconsistent = errors.New("OKX 资金账单返回冲突记录")

// FundingBill excludes notes, balances and personal information.
type FundingBill struct {
	BillID, Currency, ChangeText, Type string
	Timestamp                          time.Time
	Incoming                           bool
}

// billId is the overlap identity. The documented /asset/bills after parameter is a timestamp.
func (c *Client) FundingBillsSince(ctx context.Context, currency string, since time.Time) ([]FundingBill, error) {
	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	c.billsMu.Lock()
	defer c.billsMu.Unlock()
	var all []FundingBill
	seen := map[string]FundingBill{}
	var after int64
	for page := 0; page < 10; page++ {
		if page > 0 {
			timer := time.NewTimer(250 * time.Millisecond)
			select {
			case <-ctx.Done():
				timer.Stop()
				return all, ctx.Err()
			case <-timer.C:
			}
		}
		q := url.Values{"ccy": {currency}, "type": {"72"}, "limit": {strconv.Itoa(c.limit)}}
		if after != 0 {
			q.Set("after", strconv.FormatInt(after, 10))
		}
		body, err := c.get(ctx, "/api/v5/asset/bills", q)
		if err != nil {
			return all, err
		}
		var rows []struct {
			ID       flexString `json:"billId"`
			Currency flexString `json:"ccy"`
			Change   flexString `json:"balChg"`
			Type     flexString `json:"type"`
			TS       flexInt64  `json:"ts"`
		}
		if err := json.Unmarshal(body, &rows); err != nil || rows == nil {
			return all, fmt.Errorf("OKX 资金账单数据不完整")
		}
		oldest := int64(0)
		for _, row := range rows {
			id := strings.TrimSpace(string(row.ID))
			if id == "" || row.TS <= 0 {
				return all, fmt.Errorf("OKX 资金账单缺少身份或时间")
			}
			if string(row.Currency) != currency || string(row.Type) != "72" {
				return all, ErrBillsInconsistent
			}
			value, err := amount.Decimal(string(row.Change))
			if err != nil {
				return all, fmt.Errorf("OKX 资金账单金额无效")
			}
			bill := FundingBill{BillID: id, Currency: currency, ChangeText: string(row.Change), Type: "72", Timestamp: time.UnixMilli(int64(row.TS)), Incoming: value.Sign() > 0}
			if old, ok := seen[id]; ok {
				if old.ChangeText != bill.ChangeText || !old.Timestamp.Equal(bill.Timestamp) {
					return all, ErrBillsInconsistent
				}
			} else {
				seen[id] = bill
				if !bill.Timestamp.Before(since) {
					all = append(all, bill)
				}
			}
			if oldest == 0 || int64(row.TS) < oldest {
				oldest = int64(row.TS)
			}
		}
		if len(rows) < c.limit || oldest < since.UnixMilli() {
			return all, nil
		}
		next := oldest + 1
		if oldest == 0 || (after != 0 && next >= after) {
			return all, fmt.Errorf("OKX 账单分页未前进，同毫秒边界需人工对账")
		}
		after = next
	}
	return all, fmt.Errorf("OKX 账单超过分页预算，数据不完整")
}
