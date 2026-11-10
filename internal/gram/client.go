// Package gram reads native GRAM payments from Toncenter; it never signs transfers.
package gram

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/xssnick/tonutils-go/address"
	"github.com/xssnick/tonutils-go/tlb"
	"github.com/xssnick/tonutils-go/tvm/cell"

	"litepay/internal/amount"
	"litepay/internal/httpx"
	"litepay/internal/model"
)

const DefaultAddress = "UQAVr5gouSRmaPaNPNmNFqO7VicqWr56h-qyn735bfrOYisp"

func ParseAddress(raw string) (*address.Address, error) {
	a, err := address.ParseAddr(raw)
	if err != nil {
		a, err = address.ParseRawAddr(raw)
	}
	if err != nil || a == nil || a.IsTestnetOnly() || a.Workchain() != 0 {
		return nil, errors.New("请填写 TON 主网工作链 0 的有效收款地址")
	}
	return a, nil
}

type Client struct {
	base, key, receiver string
	addr                *address.Address
	http                *httpx.Client
	test                bool
}

func New(base, key, receiver string) *Client {
	a, _ := ParseAddress(receiver)
	return &Client{base: strings.TrimRight(base, "/"), key: key, receiver: receiver, addr: a, http: httpx.NewClient(20*time.Second, false)}
}
func (c *Client) UseTestTransport() { c.http = httpx.NewClient(5*time.Second, true); c.test = true }

// Balance and history share the anonymous Toncenter request budget.
var requestBudget struct {
	sync.Mutex
	next time.Time
}

func (c *Client) get(ctx context.Context, method string, q url.Values, out any) error {
	if c.addr == nil {
		return errors.New("GRAM 收款地址无效")
	}
	if !c.test {
		requestBudget.Lock()
		delay := time.Until(requestBudget.next)
		requestBudget.next = time.Now().Add(max(delay, 0) + 1100*time.Millisecond)
		requestBudget.Unlock()
		if delay > 0 {
			t := time.NewTimer(delay)
			defer t.Stop()
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-t.C:
			}
		}
	}
	q.Set("address", c.receiver)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, c.base+"/"+method+"?"+q.Encode(), nil)
	if err != nil {
		return err
	}
	if c.key != "" {
		req.Header.Set("X-API-Key", c.key)
	}
	status, body, err := c.http.Do(req)
	if err != nil {
		return errors.New("Toncenter 请求失败")
	}
	if status != 200 {
		return fmt.Errorf("Toncenter HTTP %d", status)
	}
	var envelope struct {
		OK     bool            `json:"ok"`
		Result json.RawMessage `json:"result"`
	}
	if json.Unmarshal(body, &envelope) != nil || !envelope.OK || len(envelope.Result) == 0 || bytes.Equal(envelope.Result, []byte("null")) {
		return errors.New("Toncenter 响应不完整")
	}
	return json.Unmarshal(envelope.Result, out)
}

func (c *Client) AccountUID(context.Context) (string, error) {
	if c.addr == nil {
		return "", errors.New("GRAM 收款地址无效")
	}
	return c.receiver, nil
}
func (c *Client) Balance(ctx context.Context) (string, error) {
	var raw string
	if err := c.get(ctx, "getAddressBalance", url.Values{}, &raw); err != nil {
		return "", err
	}
	n, err := strconv.ParseUint(raw, 10, 64)
	if err != nil {
		return "", errors.New("GRAM 余额格式无效")
	}
	return fmt.Sprintf("%d.%09d", n/1_000_000_000, n%1_000_000_000), nil
}

type rawTransaction struct {
	Data  string `json:"data"`
	Utime int64  `json:"utime"`
	ID    struct {
		LT   string `json:"lt"`
		Hash string `json:"hash"`
	} `json:"transaction_id"`
}

func (c *Client) decode(raw rawTransaction) (model.Transaction, uint64, error) {
	var result model.Transaction
	b, err := base64.StdEncoding.DecodeString(raw.Data)
	if err != nil {
		return result, 0, errors.New("交易 BOC 无效")
	}
	root, err := cell.FromBOC(b)
	if err != nil {
		return result, 0, err
	}
	var tx tlb.Transaction
	if err = tlb.Parse(&tx, root); err != nil {
		return result, 0, err
	}
	hash, err := base64.StdEncoding.DecodeString(raw.ID.Hash)
	lt, le := strconv.ParseUint(raw.ID.LT, 10, 64)
	if err != nil || le != nil || lt == 0 || tx.LT != lt || int64(tx.Now) != raw.Utime || raw.Utime <= 0 || !bytes.Equal(hash, root.Hash()) || !bytes.Equal(tx.AccountAddr, c.addr.Data()) {
		return result, 0, errors.New("交易身份与 BOC 不一致")
	}
	result = model.Transaction{Provider: "ton", EventID: hex.EncodeToString(hash), Reference: hex.EncodeToString(hash), Receiver: c.receiver, Currency: "GRAM", OccurredAt: time.Unix(raw.Utime, 0), State: model.Rejected, Reason: "not_incoming_payment"}
	if tx.IO.In == nil || tx.IO.In.MsgType != tlb.MsgTypeInternal {
		return result, lt, nil
	}
	msg := tx.IO.In.AsInternal()
	result.RawAmount = msg.Amount.String()
	if msg.DstAddr == nil || !msg.DstAddr.Equals(c.addr) || msg.SrcAddr == nil || msg.SrcAddr.IsAddrNone() || msg.Bounced {
		return result, lt, nil
	}
	dsc, ok := tx.Description.(tlb.TransactionDescriptionOrdinary)
	if !ok || dsc.Destroyed || dsc.BouncePhase != nil {
		result.Reason = "failed_or_bounced"
		return result, lt, nil
	}
	switch phase := dsc.ComputePhase.Phase.(type) {
	case tlb.ComputePhaseVM:
		if dsc.Aborted || !phase.Success || dsc.ActionPhase != nil && !dsc.ActionPhase.Success {
			result.Reason = "failed_execution"
			return result, lt, nil
		}
	case tlb.ComputePhaseSkipped:
		// A non-bounceable no-state transfer can credit an uninitialized account even though
		// aborted=true (no action phase ran). Prove the positive credit/fee balance explicitly.
		if phase.Reason.Type != tlb.ComputeSkipReasonNoState || msg.Bounce || !dsc.CreditFirst || dsc.ActionPhase != nil ||
			(tx.OrigStatus != tlb.AccountStatusUninit && tx.OrigStatus != tlb.AccountStatusNonExist) || tx.EndStatus != tlb.AccountStatusUninit ||
			dsc.CreditPhase == nil || dsc.StoragePhase == nil || dsc.StoragePhase.StatusChange.Type != tlb.AccStatusChangeUnchanged ||
			dsc.StoragePhase.StorageFeesDue != nil && dsc.StoragePhase.StorageFeesDue.Nano().Sign() != 0 ||
			dsc.CreditPhase.DueFeesCollected != nil && dsc.CreditPhase.DueFeesCollected.Nano().Sign() != 0 ||
			!dsc.CreditPhase.Credit.ExtraCurrencies.IsEmpty() || dsc.CreditPhase.Credit.Coins.Nano().Cmp(msg.Amount.Nano()) != 0 ||
			!tx.TotalFees.ExtraCurrencies.IsEmpty() || tx.TotalFees.Coins.Nano().Cmp(dsc.StoragePhase.StorageFeesCollected.Nano()) != 0 ||
			msg.Amount.Nano().Cmp(tx.TotalFees.Coins.Nano()) <= 0 {
			result.Reason = "unproven_skipped_credit"
			return result, lt, nil
		}
	default:
		result.Reason = "failed_execution"
		return result, lt, nil
	}
	// Contract calls and forwarding transactions are not plain deposits.
	if tx.OutMsgCount != 0 || !msg.ExtraCurrencies.IsEmpty() || msg.StateInit != nil {
		result.Reason = "not_plain_deposit"
		return result, lt, nil
	}
	if msg.Body != nil && msg.Body.BitsSize() > 0 {
		s, err := msg.Body.BeginParse()
		if err != nil {
			return result, 0, err
		}
		op, err := s.LoadUInt(32)
		if err != nil || op != 0 {
			result.Reason = "contract_message"
			return result, lt, nil
		}
	}
	units, err := amount.Parse(result.RawAmount)
	if err != nil || units <= 0 {
		result.Reason = "invalid_amount"
		return result, lt, nil
	}
	result.Units = units
	result.State = model.Confirmed
	result.Reason = ""
	return result, lt, nil
}

func (c *Client) History(ctx context.Context, start, end time.Time) ([]model.Transaction, error) {
	if c.addr == nil {
		return nil, errors.New("GRAM 收款地址无效")
	}
	var out []model.Transaction
	q := url.Values{"limit": {"100"}, "archival": {"true"}}
	seen := map[uint64]bool{}
	var boundary uint64
	for page := 0; page < 100; page++ {
		var rows []rawTransaction
		if err := c.get(ctx, "getTransactions", q, &rows); err != nil {
			return nil, err
		}
		if rows == nil {
			return nil, errors.New("交易列表缺失")
		}
		if len(rows) == 0 {
			return out, nil
		}
		progress, finished := false, false
		var previous uint64
		for i, raw := range rows {
			t, lt, err := c.decode(raw)
			if err != nil {
				return nil, err
			}
			if i > 0 && lt >= previous || boundary > 0 && lt > boundary {
				return nil, errors.New("交易分页顺序无效")
			}
			previous = lt
			if seen[lt] {
				if i == 0 && lt == boundary {
					continue
				}
				return nil, errors.New("交易分页重复")
			}
			seen[lt] = true
			progress = true
			if t.OccurredAt.Before(start) {
				finished = true
				continue
			}
			if !t.OccurredAt.After(end) {
				out = append(out, t)
			}
		}
		if !progress {
			return nil, errors.New("交易分页未推进")
		}
		if finished || len(rows) < 100 {
			return out, nil
		}
		last := rows[len(rows)-1]
		boundary = previous
		q.Set("lt", last.ID.LT)
		q.Set("hash", last.ID.Hash)
	}
	return nil, errors.New("交易历史超过单轮分页上限")
}
