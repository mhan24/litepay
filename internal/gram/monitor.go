package gram

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math/big"
	"net"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"time"

	"litepay/internal/amount"
	"litepay/internal/httpx"
)

const PriceInterval = 5 * time.Second
const BalanceInterval = 15 * time.Second
const MaxPriceAge = 15 * time.Second

type Snapshot struct {
	Address      string    `json:"address"`
	Balance      string    `json:"balance_gram"`
	BalanceUSD   string    `json:"balance_usd,omitempty"`
	BalanceAt    time.Time `json:"balance_checked_at"`
	BalanceError string    `json:"balance_error,omitempty"`
	BalanceFresh bool      `json:"balance_fresh"`
	USD          string    `json:"usd_per_gram"`
	Bid          string    `json:"bid_usd"`
	Ask          string    `json:"ask_usd"`
	PriceAt      time.Time `json:"price_checked_at"`
	PriceError   string    `json:"price_error,omitempty"`
	Fresh        bool      `json:"price_fresh"`
}

type Monitor struct {
	mu       sync.RWMutex
	snapshot Snapshot
	http     *httpx.Client
	priceURL string
}

func NewMonitor() *Monitor {
	return &Monitor{http: httpx.NewClient(8*time.Second, false), priceURL: "https://api.kraken.com/0/public/Ticker?pair=TONUSD"}
}

// UseTestPriceSource redirects price checks only to a loopback HTTP fixture.
// Call before starting the monitor; production configuration never uses this hook.
func (m *Monitor) UseTestPriceSource(endpoint string) error {
	u, err := url.Parse(endpoint)
	if err != nil || u.Scheme != "http" || !net.ParseIP(u.Hostname()).IsLoopback() || u.User != nil {
		return errors.New("行情测试源必须为本机 HTTP 地址")
	}
	m.priceURL = endpoint
	m.http = httpx.NewClient(8*time.Second, true)
	return nil
}

func positiveDecimal(raw string) (*big.Rat, error) {
	if len(raw) == 0 || len(raw) > 40 || strings.Count(raw, ".") > 1 {
		return nil, errors.New("行情价格无效")
	}
	for _, r := range raw {
		if r != '.' && (r < '0' || r > '9') {
			return nil, errors.New("行情价格格式无效")
		}
	}
	n, ok := new(big.Rat).SetString(raw)
	if !ok || n.Sign() <= 0 {
		return nil, errors.New("行情价格必须大于零")
	}
	return n, nil
}

func (m *Monitor) CheckPrice(ctx context.Context) error {
	started := time.Now()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, m.priceURL+"&nonce="+strconv.FormatInt(started.UnixNano(), 10), nil)
	var bid, ask, price string
	if err == nil {
		var status int
		var body []byte
		status, body, err = m.http.Do(req)
		if err == nil && status != 200 {
			err = fmt.Errorf("行情 HTTP %d", status)
		}
		if err == nil {
			var response struct {
				Error  []string `json:"error"`
				Result map[string]struct {
					Ask []string `json:"a"`
					Bid []string `json:"b"`
				} `json:"result"`
			}
			if json.Unmarshal(body, &response) != nil || response.Error == nil || len(response.Error) != 0 || len(response.Result) != 1 || len(response.Result["TONUSD"].Ask) == 0 || len(response.Result["TONUSD"].Bid) == 0 {
				err = errors.New("GRAM/USD 行情响应不完整")
			} else {
				row := response.Result["TONUSD"]
				bid, ask = row.Bid[0], row.Ask[0]
				b, be := positiveDecimal(bid)
				a, ae := positiveDecimal(ask)
				if be != nil || ae != nil {
					err = errors.New("GRAM/USD 行情价格无效")
				} else {
					mid := new(big.Rat).Quo(new(big.Rat).Add(a, b), big.NewRat(2, 1))
					spread := new(big.Rat).Quo(new(big.Rat).Sub(a, b), mid)
					if spread.Sign() < 0 || spread.Cmp(big.NewRat(5, 1000)) > 0 {
						err = errors.New("GRAM/USD 买卖价差超过 0.5%，暂停新报价")
					} else {
						price = mid.FloatString(18)
					}
				}
			}
		}
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if err != nil {
		m.snapshot.PriceError = "GRAM/USD 行情检测失败，暂停新报价"
		return err
	}
	m.snapshot.USD = price
	m.snapshot.Bid = bid
	m.snapshot.Ask = ask
	m.snapshot.PriceAt = started
	m.snapshot.PriceError = ""
	return nil
}

func (m *Monitor) CheckBalance(ctx context.Context, cli *Client) error {
	started := time.Now()
	b, err := cli.Balance(ctx)
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.snapshot.Address != cli.receiver {
		m.snapshot.Balance = ""
		m.snapshot.BalanceAt = time.Time{}
	}
	m.snapshot.Address = cli.receiver
	if err != nil {
		m.snapshot.BalanceError = "GRAM 余额检测失败"
		return err
	}
	m.snapshot.Balance = b
	m.snapshot.BalanceAt = started
	m.snapshot.BalanceError = ""
	return nil
}

func (m *Monitor) Snapshot() Snapshot {
	m.mu.RLock()
	defer m.mu.RUnlock()
	s := m.snapshot
	s.Fresh = s.PriceError == "" && !s.PriceAt.IsZero() && time.Since(s.PriceAt) >= 0 && time.Since(s.PriceAt) <= MaxPriceAge
	s.BalanceFresh = s.BalanceError == "" && !s.BalanceAt.IsZero() && time.Since(s.BalanceAt) >= 0 && time.Since(s.BalanceAt) <= 2*BalanceInterval
	if s.Fresh && s.BalanceFresh {
		b, ok := new(big.Rat).SetString(s.Balance)
		p, valid := new(big.Rat).SetString(s.USD)
		if ok && valid {
			s.BalanceUSD = new(big.Rat).Mul(b, p).FloatString(8)
		}
	}
	return s
}

// Quote rounds up by less than one micro-GRAM; no binary floating point is used.
func Quote(usdUnits int64, s Snapshot) (int64, error) {
	if !s.Fresh || s.PriceError != "" || s.PriceAt.IsZero() || time.Since(s.PriceAt) < 0 || time.Since(s.PriceAt) > MaxPriceAge {
		return 0, errors.New("GRAM/USD 行情已过期，请稍后重试")
	}
	rate, err := positiveDecimal(s.USD)
	if err != nil {
		return 0, err
	}
	q := new(big.Rat).Quo(new(big.Rat).SetInt64(usdUnits), rate)
	n, rem := new(big.Int), new(big.Int)
	n.QuoRem(q.Num(), q.Denom(), rem)
	if rem.Sign() > 0 {
		n.Add(n, big.NewInt(1))
	}
	if new(big.Rat).Sub(new(big.Rat).SetInt(n), q).Cmp(new(big.Rat).Quo(q, big.NewRat(1000, 1))) > 0 {
		return 0, errors.New("订单过小，GRAM 换算尾差超过 0.1%")
	}
	if usdUnits <= 0 || !n.IsInt64() || !amount.MallCompatible(n.Int64()) {
		return 0, errors.New("GRAM 应付金额超出可核销范围")
	}
	return n.Int64(), nil
}

// MaxQuoteUnits caps the combined rounding and order-identification offset at 0.1%.
func MaxQuoteUnits(usdUnits int64, s Snapshot) (int64, error) {
	if _, err := Quote(usdUnits, s); err != nil {
		return 0, err
	}
	rate, _ := positiveDecimal(s.USD)
	n := new(big.Rat).Quo(new(big.Rat).SetInt64(usdUnits), rate)
	n.Mul(n, big.NewRat(1001, 1000))
	limit := new(big.Int).Quo(n.Num(), n.Denom())
	if !limit.IsInt64() {
		return 0, errors.New("GRAM 报价超出范围")
	}
	return limit.Int64(), nil
}
