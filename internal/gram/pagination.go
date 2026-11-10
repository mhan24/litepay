package gram

import (
	"context"
	"errors"
	"net/url"
	"strconv"

	"litepay/internal/model"
)

// HistoryPage verifies one bounded page. The caller commits evidence and the continuation
// together; a timeout, malformed response, or restart can only replay the last committed page.
func (c *Client) HistoryPage(ctx context.Context, cp model.TONScanCheckpoint) ([]model.Transaction, model.TONScanCheckpoint, error) {
	if cp.Receiver != c.receiver || !cp.Active || cp.From.After(cp.To) {
		return nil, cp, errors.New("TON 扫描检查点无效")
	}
	q := url.Values{"limit": {"100"}, "archival": {"true"}}
	var boundary uint64
	if cp.LT != "" {
		var err error
		boundary, err = strconv.ParseUint(cp.LT, 10, 64)
		if err != nil || boundary == 0 || cp.Hash == "" {
			return nil, cp, errors.New("TON 分页边界无效")
		}
		q.Set("lt", cp.LT)
		q.Set("hash", cp.Hash)
	}
	var rows []rawTransaction
	if err := c.get(ctx, "getTransactions", q, &rows); err != nil {
		return nil, cp, err
	}
	if rows == nil || len(rows) > 100 {
		return nil, cp, errors.New("TON 分页响应无效")
	}
	if boundary > 0 && (len(rows) == 0 || rows[0].ID.LT != cp.LT || rows[0].ID.Hash != cp.Hash) {
		return nil, cp, errors.New("TON 分页未返回请求的锚点")
	}
	next := cp
	next.Pages++
	if len(rows) == 0 {
		next.Active = false
		return nil, next, nil
	}
	out := []model.Transaction{}
	progress := false
	finished := false
	var previous uint64
	oldest := cp.OldestAt
	for i, raw := range rows {
		t, lt, err := c.decode(raw)
		if err != nil {
			return nil, cp, err
		}
		if i > 0 && lt >= previous || boundary > 0 && lt > boundary {
			return nil, cp, errors.New("交易分页顺序无效")
		}
		previous = lt
		if boundary > 0 && lt == boundary {
			if i != 0 || raw.ID.Hash != cp.Hash {
				return nil, cp, errors.New("TON 分页边界哈希不一致")
			}
			continue
		}
		if !oldest.IsZero() && t.OccurredAt.After(oldest) {
			return nil, cp, errors.New("TON 分页时间倒序无效")
		}
		oldest = t.OccurredAt
		progress = true
		if t.OccurredAt.Before(cp.From) {
			finished = true
			continue
		}
		if !t.OccurredAt.After(cp.To) {
			out = append(out, t)
		}
	}
	if !progress {
		if boundary > 0 && len(rows) == 1 {
			next.Active = false
			return nil, next, nil
		}
		return nil, cp, errors.New("交易分页未推进")
	}
	last := rows[len(rows)-1]
	next.LT = last.ID.LT
	next.Hash = last.ID.Hash
	next.OldestAt = oldest
	if finished || len(rows) < 100 {
		next.Active = false
	}
	return out, next, nil
}
