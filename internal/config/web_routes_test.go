package config

import "testing"

func TestWebRouteValidation(t *testing.T) {
	for _, root := range []string{"/", "/pay", "/api", "/assets", "/healthz", "/readyz", "/x/y", "//hidden", "/hidden?x=1", "/hidden/", "/:param", "/中文"} {
		c := Default()
		c.Admin.Path = root
		if c.ValidateWebRoutes() == nil {
			t.Errorf("invalid admin path accepted: %s", root)
		}
	}
	for _, target := range []string{"javascript:alert(1)", "//shop.test", "https://u:p@shop.test", "https://pay.test/", "https://pay.test/pay/anything", "https://shop.test/\r\nx"} {
		c := Default()
		c.Server.BaseURL = "https://pay.test"
		c.Server.StoreURL = target
		if c.ValidateWebRoutes() == nil {
			t.Errorf("invalid store URL accepted: %q", target)
		}
	}
	c := Default()
	c.Admin.Path = "/workspace-123"
	c.Server.StoreURL = "https://shop.test/catalog?q=1"
	if err := c.ValidateWebRoutes(); err != nil {
		t.Fatal(err)
	}
}
