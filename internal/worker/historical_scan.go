package worker

import (
	"context"
	"errors"
	"time"

	"gorm.io/gorm"
	"litepay/internal/binance"
	"litepay/internal/config"
	"litepay/internal/model"
)

// Called under the same provider lock as the regular scan; each step is bounded and durable.
func (p *Poller) scanHistorical(parent context.Context, cfg *config.Config, provider string) error {
	ctx, cancel := context.WithTimeout(parent, 12*time.Second)
	defer cancel()
	receiver := cfg.Checkout.Binance.UID
	if provider == config.ProviderOKX {
		receiver = cfg.Checkout.OKX.UID
	}
	if provider == config.ProviderBybit {
		receiver = cfg.Checkout.Bybit.UID
	}
	if provider == config.ProviderBitget {
		receiver = cfg.Checkout.Bitget.UID
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
		save, done := context.WithTimeout(context.WithoutCancel(parent), 2*time.Second)
		defer done()
		return p.st.HistoricalScanFailure(save, job, true)
	}
	job, err = p.st.ReuseHistoricalCoverage(ctx, job)
	if err != nil || job.State == "completed" {
		return err
	}
	after := job
	if after.ChunkTo.IsZero() {
		step := time.Duration(job.ChunkMillis) * time.Millisecond
		if step <= 0 {
			step = time.Hour
		}
		after.ChunkTo = job.Cursor.Add(step)
		if after.ChunkTo.After(job.To) {
			after.ChunkTo = job.To
		}
	}
	after.ChunkTo, err = p.st.UncoveredChunkEnd(ctx, job, after.ChunkTo)
	if err != nil {
		return err
	}
	cli, err := p.client(ctx, cfg, provider)
	var rows []model.Transaction
	if err == nil {
		if provider == config.ProviderBinance {
			ctx = binance.WithRequestBudget(ctx, 4)
		}
		rows, err = cli.History(ctx, job.Cursor, after.ChunkTo)
		if err == nil {
			after.Cursor = after.ChunkTo
			after.ChunkTo = time.Time{}
			after.Pages++
		}
	}
	save, done := context.WithTimeout(context.WithoutCancel(parent), 2*time.Second)
	defer done()
	if err != nil {
		var apiErr *binance.APIError
		if errors.As(err, &apiErr) && (apiErr.Kind == binance.ErrorIncomplete || apiErr.Kind == binance.ErrorBudget) {
			return p.st.ShrinkHistoricalChunk(save, job, after.ChunkTo)
		}
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
