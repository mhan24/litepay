package worker

import (
	"context"
	"errors"
	"time"

	"gorm.io/gorm"
	"litepay/internal/config"
	"litepay/internal/model"
)

// Called under the same provider lock as the regular scan; each step is bounded and durable.
func (p *Poller) scanHistorical(parent context.Context, cfg *config.Config, provider string) error {
	ctx, cancel := context.WithTimeout(parent, 35*time.Second)
	defer cancel()
	receiver := cfg.Checkout.Binance.UID
	if provider == config.ProviderOKX {
		receiver = cfg.Checkout.OKX.UID
	}
	if provider == config.ProviderTON {
		receiver = cfg.TON.Address
	}
	if err := p.st.QueueSettlementScan(ctx, provider, receiver, time.Duration(cfg.ProviderLookback(provider))*time.Minute); err != nil {
		return err
	}
	job, err := p.st.NextHistoricalScan(ctx, provider)
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return nil
	}
	if err != nil {
		return err
	}
	if receiver != job.Receiver {
		return p.st.HistoricalScanFailure(ctx, job, true)
	}
	cli, err := p.client(ctx, cfg, provider)
	after := job
	if after.ChunkTo.IsZero() {
		after.ChunkTo = job.Cursor.Add(5 * time.Minute)
		if after.ChunkTo.After(job.To) {
			after.ChunkTo = job.To
		}
	}
	var rows []model.Transaction
	if err == nil {
		if provider == config.ProviderTON {
			pages, ok := cli.(interface {
				HistoryPage(context.Context, model.TONScanCheckpoint) ([]model.Transaction, model.TONScanCheckpoint, error)
			})
			if !ok {
				err = errors.New("TON 历史补扫需要可恢复分页")
			} else {
				cp := model.TONScanCheckpoint{Receiver: job.Receiver, From: job.Cursor, To: after.ChunkTo, LT: job.LT, Hash: job.Hash, OldestAt: job.OldestAt, Pages: job.Pages, Active: true}
				var next model.TONScanCheckpoint
				rows, next, err = pages.HistoryPage(ctx, cp)
				if err == nil {
					after.LT = next.LT
					after.Hash = next.Hash
					after.OldestAt = next.OldestAt
					after.Pages = next.Pages
					if !next.Active {
						after.Cursor = after.ChunkTo
						after.ChunkTo = time.Time{}
						after.LT = ""
						after.Hash = ""
						after.OldestAt = time.Time{}
					}
				}
			}
		} else {
			rows, err = cli.History(ctx, job.Cursor, after.ChunkTo)
			if err == nil {
				after.Cursor = after.ChunkTo
				after.ChunkTo = time.Time{}
				after.Pages++
			}
		}
	}
	save, done := context.WithTimeout(context.WithoutCancel(parent), 2*time.Second)
	defer done()
	if err != nil {
		if e := p.st.HistoricalScanFailure(save, job, false); e != nil {
			return e
		}
		return err
	}
	markAmbiguous(rows)
	if !after.Cursor.Before(job.To) {
		after.State = "completed"
	}
	return p.st.SaveHistoricalScan(save, job, after, rows)
}
