package worker

import (
	"context"
	"encoding/json"
	"errors"
	"litepay/internal/amount"
	"litepay/internal/maintenance"
	"litepay/internal/model"
	"litepay/internal/okx"
	"time"
)

// ReconcileOKXBills is an explicit admin diagnostic. No background dual polling and no payment writes.
func (p *Poller) ReconcileOKXBills(ctx context.Context) error {
	if maintenance.Blocked() || p.scanWaiting.Load() || !p.workMu.TryLock() {
		return ErrInquiryBusy
	}
	defer p.workMu.Unlock()
	cfg := p.cfg.Get()
	if !cfg.OKXEnabled() {
		return ErrInquiryClosed
	}
	if err := p.store.ReserveAuxiliary(ctx); err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(ctx, 15*time.Second)
	defer cancel()
	cli := p.okxClientFor(cfg)
	since := time.Now().Add(-24 * time.Hour)
	uid, err := cli.AccountUID(ctx)
	if err != nil || uid != cfg.Checkout.OKX.UID {
		p.store.SaveAuxiliary(context.Background(), "incomplete", "账户核验失败，未执行辅助对账", nil)
		return ErrInquiryClosed
	}
	result, diagnostic := "needs_review", "辅助账单仅作排查，不作为自动核销依据"
	var evidence []model.FundingBillEvidence
	for _, ccy := range cfg.Checkout.Currencies {
		bills, err := cli.FundingBillsSince(ctx, ccy, since)
		if err != nil {
			result = "incomplete"
			diagnostic = "资金账单查询失败或分页不完整"
			if errors.Is(err, okx.ErrBillsInconsistent) {
				result = "inconsistent"
				diagnostic = "资金账单字段或同一账单内容不一致"
			}
		}
		deposits, depErr := cli.DepositHistorySince(ctx, ccy, since)
		if depErr != nil {
			result = "incomplete"
			diagnostic = "充值历史查询失败或分页不完整"
		}
		for _, bill := range bills {
			if !bill.Incoming {
				continue
			}
			row := model.FundingBillEvidence{BillID: bill.BillID, Currency: bill.Currency, ChangeText: bill.ChangeText, Timestamp: bill.Timestamp, Result: "unassigned"}
			candidates := []string{}
			for _, dep := range deposits {
				delta := dep.Timestamp.Sub(bill.Timestamp)
				if dep.DepositID != "" && dep.State == "2" && dep.Type == "3" && delta >= -5*time.Minute && delta <= 5*time.Minute && amount.MatchDecimal(bill.ChangeText, dep.AmountText) {
					candidates = append(candidates, dep.DepositID)
				}
			}
			if len(candidates) > 0 {
				row.Result = "candidate_only"
			}
			if err != nil || depErr != nil {
				row.Result = "incomplete"
			}
			raw, _ := json.Marshal(candidates)
			row.Candidates = string(raw)
			evidence = append(evidence, row)
		}
		if err != nil || depErr != nil {
			break
		}
	}
	auditCtx, stop := context.WithTimeout(context.Background(), 3*time.Second)
	defer stop()
	return p.store.SaveAuxiliary(auditCtx, result, diagnostic, evidence)
}
