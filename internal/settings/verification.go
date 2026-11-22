package settings

import (
	"context"
	"fmt"
	"strings"

	"litepay/internal/binance"
	"litepay/internal/bitget"
	"litepay/internal/bybit"
	"litepay/internal/config"
	"litepay/internal/gram"
	"litepay/internal/okx"
)

// AccountVerification records a completed check separately from configuration completeness.
type AccountVerification struct {
	Provider string
	Verified bool
	Missing  []string
}

func verificationTargets(c *config.Config) []AccountVerification {
	missing := func(fields ...string) []string {
		var out []string
		for i := 0; i < len(fields); i += 2 {
			if strings.TrimSpace(fields[i+1]) == "" { // #nosec G602 -- Every call below supplies literal label/value pairs; this local helper has no external callers.
				out = append(out, fields[i])
			}
		}
		return out
	}
	tonMissing := missing("收款地址", c.TON.Address)
	if !c.ProviderCurrency(config.ProviderTON, "GRAM") {
		tonMissing = append(tonMissing, "启用 GRAM 币种")
	}
	out := []AccountVerification{
		{Provider: config.ProviderBinance, Missing: missing("API Key", c.Binance.APIKey, "API Secret", c.Binance.APISecret, "收款 UID", c.Checkout.Binance.UID)},
		{Provider: config.ProviderOKX, Missing: missing("API Key", c.OKX.APIKey, "API Secret", c.OKX.APISecret, "Passphrase", c.OKX.Passphrase, "收款 UID", c.Checkout.OKX.UID)},
		{Provider: config.ProviderBybit, Missing: missing("API Key", c.Bybit.APIKey, "API Secret", c.Bybit.APISecret, "收款 UID", c.Checkout.Bybit.UID)},
		{Provider: config.ProviderBitget, Missing: missing("API Key", c.Bitget.APIKey, "API Secret", c.Bitget.APISecret, "Passphrase", c.Bitget.Passphrase, "收款 UID", c.Checkout.Bitget.UID)},
		{Provider: config.ProviderTON, Missing: tonMissing},
	}
	for i := range out {
		if out[i].Provider != config.ProviderTON && len(c.ProviderCurrencies(out[i].Provider)) == 0 {
			out[i].Missing = append(out[i].Missing, "开放币种")
		}
	}
	return out
}

func verifyCollectionAccount(ctx context.Context, c *config.Config, provider string) error {
	switch provider {
	case config.ProviderBinance:
		cli := binance.New(c.Binance.APIKey, c.Binance.APISecret, c.Binance.BaseURL, c.Binance.RecvWindow, c.Binance.Limit)
		uid, err := cli.AccountUID(ctx)
		if err != nil {
			return fmt.Errorf("Binance 账户核验失败: %w，请检查 API 凭据、读取权限、IP 白名单和网络", err)
		}
		if uid != c.Checkout.Binance.UID {
			return fmt.Errorf("Binance API Key 所属账户与收款 UID 不一致")
		}
	case config.ProviderOKX:
		cli := okx.New(c.OKX.APIKey, c.OKX.APISecret, c.OKX.Passphrase, c.OKX.BaseURL, c.OKX.Limit)
		uid, err := cli.AccountUID(ctx)
		if err != nil {
			return fmt.Errorf("OKX 账户核验失败: %w，请检查 API 凭据、读取权限、IP 白名单和网络", err)
		}
		if uid != c.Checkout.OKX.UID {
			return fmt.Errorf("OKX API Key 所属账户与收款 UID 不一致")
		}
	case config.ProviderBybit:
		cli := bybit.New(c.Bybit.APIKey, c.Bybit.APISecret, c.Bybit.BaseURL, c.Bybit.RecvWindow, c.Bybit.Limit)
		uid, err := cli.AccountUID(ctx)
		if err != nil {
			return fmt.Errorf("Bybit 账户核验失败: %w，请检查 API 凭据、读取权限、IP 白名单和网络", err)
		}
		if uid != c.Checkout.Bybit.UID {
			return fmt.Errorf("Bybit API Key 所属账户与收款 UID 不一致")
		}
	case config.ProviderBitget:
		cli := bitget.New(c.Bitget.APIKey, c.Bitget.APISecret, c.Bitget.Passphrase, c.Bitget.BaseURL, c.Bitget.Limit)
		uid, err := cli.AccountUID(ctx)
		if err != nil {
			return fmt.Errorf("Bitget 账户核验失败: %w，请检查 API 凭据、读取权限、IP 白名单和网络", err)
		}
		if uid != c.Checkout.Bitget.UID {
			return fmt.Errorf("Bitget API Key 所属账户与收款 UID 不一致")
		}
	case config.ProviderTON:
		cli := gram.New(c.TON.BaseURL, c.TON.APIKey, c.TON.Address)
		if _, err := cli.Balance(ctx); err != nil {
			return fmt.Errorf("GRAM 余额接口核验失败: %w，请检查地址、Toncenter API Key 和网络", err)
		}
	default:
		return fmt.Errorf("不支持的收款平台")
	}
	return nil
}
