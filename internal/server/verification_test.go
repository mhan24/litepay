package server

import (
	"strings"
	"testing"

	"litepay/internal/settings"
)

func TestVerificationMessageDoesNotClaimSkippedOKXSucceeded(t *testing.T) {
	message, partial := verificationMessage([]settings.AccountVerification{
		{Provider: "binance", Verified: true},
		{Provider: "okx", Missing: []string{"API Key", "API Secret", "Passphrase"}},
	})
	if !partial || !strings.Contains(message, "Binance 核验通过") || !strings.Contains(message, "OKX 未核验") {
		t.Fatal(message, partial)
	}
	for _, field := range []string{"API Key", "API Secret", "Passphrase"} {
		if !strings.Contains(message, field) {
			t.Fatal("missing reason", message)
		}
	}
	if strings.Contains(message, "OKX 核验通过") || strings.Contains(message, "收款接口核验通过") {
		t.Fatal("misleading global success", message)
	}
	message, partial = verificationMessage([]settings.AccountVerification{{Provider: "okx", Verified: true}})
	if partial || !strings.Contains(message, "OKX 核验通过") {
		t.Fatal(message, partial)
	}
}
