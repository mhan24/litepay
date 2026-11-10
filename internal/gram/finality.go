package gram

import (
	"context"
	"encoding/base64"
	"encoding/hex"
	"errors"
	"net/url"
	"strings"

	"litepay/internal/model"
)

// verifyFinality joins validated v2 BOCs to the v3 masterchain-indexed transaction rows.
// It trusts the provider's canonical chain index; it is not an independent validator proof.
func (c *Client) verifyFinality(ctx context.Context, rows []model.Transaction) error {
	var required []int
	for i, row := range rows {
		if row.State == model.Confirmed {
			required = append(required, i)
		}
	}
	if len(required) == 0 {
		return nil
	}
	base := strings.TrimSuffix(c.base, "/api/v2") + "/api/v3"
	// LT bounds avoid offsets and multiple records sharing one second. Event hashes are exact.
	var indexed struct {
		Transactions []struct {
			Account string `json:"account"`
			Hash    string `json:"hash"`
			LT      string `json:"lt"`
			Now     int64  `json:"now"`
			MC      int64  `json:"mc_block_seqno"`
			Block   struct {
				Workchain int    `json:"workchain"`
				Shard     string `json:"shard"`
				Seqno     int64  `json:"seqno"`
			} `json:"block_ref"`
		} `json:"transactions"`
	}
	// At most 100 account records occur between this page's LT endpoints.
	q := url.Values{"account": {c.receiver}, "limit": {"100"}, "sort": {"desc"}, "start_lt": {rows[len(rows)-1].LogicalTime}, "end_lt": {rows[0].LogicalTime}}
	if q.Get("start_lt") == "" || q.Get("end_lt") == "" {
		return errors.New("TON 交易缺少最终性核验 LT")
	}
	if err := c.requestJSON(ctx, base, "transactions", q, &indexed, false); err != nil {
		return err
	}
	if indexed.Transactions == nil || len(indexed.Transactions) > 100 {
		return errors.New("TON 最终性索引响应不完整")
	}
	var info struct {
		Last struct {
			Workchain int   `json:"workchain"`
			Seqno     int64 `json:"seqno"`
		} `json:"last"`
	}
	if err := c.requestJSON(ctx, base, "masterchainInfo", url.Values{}, &info, false); err != nil {
		return err
	}
	if info.Last.Workchain != -1 || info.Last.Seqno <= 0 {
		return errors.New("TON 主链确认高度无效")
	}
	for _, i := range required {
		found := false
		for _, item := range indexed.Transactions {
			hash, err := base64.StdEncoding.DecodeString(item.Hash)
			if err != nil {
				continue
			}
			if hex.EncodeToString(hash) != rows[i].EventID {
				continue
			}
			a, err := ParseAddress(item.Account)
			if err != nil || !a.Equals(c.addr) || item.LT != rows[i].LogicalTime || item.Now != rows[i].OccurredAt.Unix() || item.MC <= 0 || item.MC > info.Last.Seqno || item.Block.Workchain != 0 || item.Block.Shard == "" || item.Block.Seqno <= 0 {
				return errors.New("TON 主链归属与交易身份不一致")
			}
			if found {
				return errors.New("TON 最终性索引交易重复")
			}
			rows[i].MasterchainSeqno = item.MC
			found = true
		}
		if !found {
			return errors.New("TON 交易尚未获得可核验的主链归属")
		}
	}
	return nil
}
