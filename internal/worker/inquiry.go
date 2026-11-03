package worker

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"time"
	"unicode"
	"unicode/utf8"

	"litepay/internal/binance"
	"litepay/internal/config"
	"litepay/internal/maintenance"
	"litepay/internal/model"
	"litepay/internal/store"
)

var (
	ErrInquiryBusy    = errors.New("查款繁忙，请稍后重试")
	ErrInquiryInvalid = errors.New("订单或转账编号无效")
	ErrInquiryEarly   = errors.New("选择付款方式满 1 分钟后可提交查款")
	ErrInquiryClosed  = errors.New("订单已关闭，请联系商户人工核对，勿重复付款")
)

func validPaymentReference(ref string) bool {
	return len(ref) > 0 && len(ref) <= 128 && utf8.ValidString(ref) &&
		!strings.ContainsAny(ref, "<>\"'\\") && strings.IndexFunc(ref, func(r rune) bool { return unicode.IsSpace(r) || unicode.IsControl(r) }) < 0
}

// CheckPayment only consumes authenticated exchange evidence, never the submitted reference itself.
// It does not advance scan cursors, release scan gaps, or resend merchant callbacks.
func (p *Poller) CheckPayment(ctx context.Context, tradeNo, reference string) (string, error) {
	reference = strings.TrimSpace(reference)
	if !validPaymentReference(reference) {
		return "", ErrInquiryInvalid
	}
	if maintenance.Blocked() || p.scanWaiting.Load() || !p.workMu.TryLock() {
		return "", ErrInquiryBusy
	}
	defer p.workMu.Unlock()
	ctx, cancel := context.WithTimeout(ctx, 8*time.Second)
	defer cancel()
	o, err := p.store.GetByTradeNo(ctx, tradeNo)
	if err != nil {
		return "", err
	}
	if o == nil {
		return "", ErrInquiryInvalid
	}
	if o.Status == model.StatusPaid {
		return "paid", nil
	}
	if o.ExpireFinalized || (o.Status != model.StatusReady && o.Status != model.StatusExpired) {
		return "", ErrInquiryClosed
	}
	if o.SelectedAt == nil || time.Since(*o.SelectedAt) < time.Minute {
		return "", ErrInquiryEarly
	}
	cfg := p.cfg.Get()
	provider := o.Collection
	if !cfg.ProviderEnabled(provider) || !cfg.CurrencyEnabled(o.Currency) {
		return "", ErrInquiryClosed
	}
	if time.Now().Before(p.inquiryNext[provider]) {
		return "", ErrInquiryBusy
	}
	if err := p.store.ReserveInquiry(ctx, tradeNo, reference); err != nil {
		return "", err
	}
	if p.inquiryNext == nil {
		p.inquiryNext = make(map[string]time.Time)
	}
	p.inquiryNext[provider] = time.Now().Add(30 * time.Second)
	result := "api_error"
	// Persist even if the browser disconnects. A crash can leave 'checking', never a fake success.
	defer func() {
		auditCtx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		if err := p.store.FinishInquiry(auditCtx, tradeNo, result); err != nil {
			slog.Error("保存用户查款结果失败")
		}
	}()
	var txs []incomingTransaction
	if provider == config.ProviderBinance {
		ctx = binance.WithRequestBudget(ctx, 4)
		end := time.Now()
		if o.ExpiredAt.Before(end) {
			end = o.ExpiredAt
		}
		txs, err = p.binanceTransactions(ctx, cfg, *o.SelectedAt, end)
		if err != nil {
			return "", err
		}
		var matched []incomingTransaction
		for _, tx := range txs {
			if tx.EventID == reference || (tx.OrderID != "" && tx.OrderID == reference) {
				matched = append(matched, tx)
			}
		}
		txs = matched
	} else {
		cli := p.okxClientFor(cfg)
		uid, e := cli.AccountUID(ctx)
		if e != nil {
			return "", e
		}
		if uid != cfg.Checkout.OKX.UID {
			return "", fmt.Errorf("OKX 收款账户核验失败")
		}
		rows, e := cli.DepositBySenderReference(ctx, o.Currency, reference)
		if e != nil {
			return "", e
		}
		for _, row := range rows {
			if row.FromWithdrawID != reference {
				continue
			}
			// Keep the exact same canonical depId as the normal scanner.
			if row.DepositID == "" || row.State != "2" || row.Type != "3" {
				result = "needs_review"
				continue
			}
			txs = append(txs, incomingTransaction{ID: "okx:" + row.DepositID, EventID: row.DepositID, CanonicalID: "okx:" + row.DepositID, ReferenceID: row.FromWithdrawID, Type: row.Type, Amount: row.Amount, AmountText: row.AmountText, Currency: row.Currency, Time: row.Timestamp, ReceiverID: uid, ReceiverAddr: uid, Incoming: true, Raw: row.Raw})
		}
	}
	if result == "needs_review" {
		return result, nil
	}
	if len(txs) == 0 {
		if result != "needs_review" {
			result = "not_found"
		}
		return result, nil
	}
	if len(txs) != 1 {
		for _, tx := range txs {
			if err := p.store.RecordTxObservation(ctx, model.TxObservation{Provider: provider, TransactionID: tx.ID, EventID: tx.EventID, DetailID: tx.DetailID, CanonicalID: tx.CanonicalID, ReferenceID: reference, IdentityIssue: "ambiguous", Type: tx.Type, Amount: tx.Amount, AmountText: tx.AmountText, Currency: tx.Currency, ReceiverID: tx.ReceiverID, TransactionTime: tx.Time, Raw: tx.Raw, Reason: model.ObservationReasonIdentity, OrderTradeNo: o.TradeNo}); err != nil {
				return "", err
			}
		}
		result = "needs_review"
		return result, nil
	}
	tx := txs[0]
	if rejectTransaction(provider, cfg, &tx) != transactionAccepted || !strings.EqualFold(o.Currency, tx.Currency) || !orderMatchesTransaction(o, &tx, cfg.Order.AmountPrecision) || tx.Time.Before(*o.SelectedAt) || tx.Time.After(o.ExpiredAt) || tx.Time.After(time.Now().Add(scanClockSkew)) {
		result = "needs_review"
		if tx.Incoming {
			if err := p.store.RecordTxObservation(ctx, model.TxObservation{Provider: provider, TransactionID: tx.ID, EventID: tx.EventID, DetailID: tx.DetailID, CanonicalID: tx.CanonicalID, ReferenceID: reference, IdentityIssue: tx.IdentityIssue, Type: tx.Type, Amount: tx.Amount, AmountText: tx.AmountText, Currency: tx.Currency, ReceiverID: tx.ReceiverID, TransactionTime: tx.Time, Raw: tx.Raw, Reason: model.ObservationReasonClaimFailed, OrderTradeNo: o.TradeNo}); err != nil {
				return "", err
			}
		}
		return result, nil
	}
	ok, err := p.store.ClaimAndPay(ctx, o, model.IncomingTx{Provider: provider, TransactionID: tx.ID, EventID: tx.EventID, DetailID: tx.DetailID, CanonicalID: tx.CanonicalID, ReferenceID: reference, IdentityIssue: tx.IdentityIssue, Type: tx.Type, Amount: tx.Amount, AmountText: tx.AmountText, Currency: tx.Currency, ReceiverID: tx.ReceiverID, ReceiverAddr: tx.ReceiverAddr, TransactionTime: tx.Time, Raw: tx.Raw}, cfg.Order.AmountPrecision)
	if err != nil {
		if store.IsClaimRejection(err) {
			result = "needs_review"
			return result, nil
		}
		return "", err
	}
	if !ok {
		current, e := p.store.GetByTradeNo(ctx, tradeNo)
		if e != nil {
			return "", e
		}
		if current == nil || current.Status != model.StatusPaid {
			result = "needs_review"
			return result, nil
		}
	}
	result = "paid"
	return result, nil
}
