package config

import "testing"

func TestProductionCallbackTransportAndExactHost(t *testing.T) {
	c := Default()
	c.Server.BaseURL = "https://pay.example"
	c.Server.NotifyHosts = []string{"shop.example"}
	for _, raw := range []string{"http://shop.example/notify", "https://shop.example.evil/notify", "https://evil.shop.example/notify", "https://shop.example@evil.example/notify", "https://shop.example:444/notify", "https://shop.example./notify", "https://shop.example/notify#fragment"} {
		if err := c.ValidateNotifyURL(raw); err == nil {
			t.Fatal(raw)
		}
	}
	for _, raw := range []string{"https://shop.example/notify", "https://SHOP.example:443/notify"} {
		if err := c.ValidateNotifyURL(raw); err != nil {
			t.Fatal(raw, err)
		}
	}
	c.Server.NotifyHosts = nil
	if c.ValidateNotifyURL("https://shop.example/notify") == nil {
		t.Fatal("empty allowlist failed open")
	}
	c.Server.Production = false
	c.Server.BaseURL = "http://127.0.0.1:3000"
	if err := c.ValidateNotifyURL("http://127.0.0.1:1234/notify"); err != nil {
		t.Fatal(err)
	}
}

func TestExplicitProductionModeCannotBeDisabledByHTTPBaseURL(t *testing.T) {
	c := Default()
	c.Server.BaseURL = "http://pay.example"
	c.Server.NotifyHosts = []string{"shop.example"}
	if err := c.normalize(); err == nil {
		t.Fatal("production HTTP base_url accepted")
	}
	if c.ValidateNotifyURL("http://shop.example/notify") == nil {
		t.Fatal("HTTP base_url weakened callback policy")
	}
	c = Default()
	c.Server.BaseURL = "https://pay.example"
	if err := c.normalize(); err == nil {
		t.Fatal("production missing allowlist accepted")
	}
	c.Server.NotifyHosts = []string{"shop.example"}
	if err := c.normalize(); err != nil {
		t.Fatal(err)
	}
	c = Default()
	c.Server.Production = false
	if err := c.normalize(); err != nil {
		t.Fatal("explicit local development failed", err)
	}
}
