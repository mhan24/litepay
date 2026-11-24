// Package httpx 提供只访问公网、限制响应大小并禁止重定向的 HTTP 客户端。
package httpx

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/netip"
	"time"
)

const maxResponseBytes = 4 << 20

// Client 是交易所 API 使用的 HTTP 客户端。
type Client struct {
	inner *http.Client
}

// NewClient 返回只访问公网地址、不跟随重定向的 HTTP 客户端。
// testsOnly 仅用于本地测试注入；生产代码不得传入 true。
func NewClient(timeout time.Duration, testsOnly bool) *Client {
	if timeout <= 0 {
		timeout = 15 * time.Second
	}
	transport := http.DefaultTransport.(*http.Transport).Clone()
	transport.Proxy = nil
	if !testsOnly {
		transport.DialContext = publicDialer
	}
	return &Client{inner: &http.Client{
		Transport: transport,
		Timeout:   timeout,
		CheckRedirect: func(*http.Request, []*http.Request) error {
			return http.ErrUseLastResponse
		},
	}}
}

// CloseIdleConnections 关闭当前客户端连接池中的空闲连接。
func (c *Client) CloseIdleConnections() {
	if c != nil && c.inner != nil {
		c.inner.CloseIdleConnections()
	}
}

// Do 执行请求，并完整读取不超过 4 MiB 的响应正文。
func (c *Client) Do(req *http.Request) (int, []byte, error) {
	resp, err := c.inner.Do(req) // #nosec G704 -- Production uses publicDialer to reject non-public DNS results and dial validated IPs directly; proxies and redirects are disabled.
	if err != nil {
		return 0, nil, err
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(io.LimitReader(resp.Body, maxResponseBytes+1))
	if err != nil {
		return resp.StatusCode, nil, err
	}
	if len(body) > maxResponseBytes {
		return resp.StatusCode, nil, errors.New("交易所响应超过 4 MiB")
	}
	return resp.StatusCode, body, nil
}

func publicDialer(ctx context.Context, network, address string) (net.Conn, error) {
	host, port, err := net.SplitHostPort(address)
	if err != nil {
		return nil, err
	}
	ips, err := net.DefaultResolver.LookupNetIP(ctx, "ip", host)
	if err != nil {
		return nil, err
	}
	if len(ips) == 0 {
		return nil, fmt.Errorf("域名没有可用地址")
	}
	for _, ip := range ips {
		if !publicIP(ip) {
			return nil, fmt.Errorf("不允许访问内部或保留网络")
		}
	}
	dialer := &net.Dialer{Timeout: 10 * time.Second, KeepAlive: 30 * time.Second}
	var lastErr error
	for _, ip := range ips {
		if ctxErr := ctx.Err(); ctxErr != nil {
			return nil, ctxErr
		}
		conn, dialErr := dialer.DialContext(ctx, network, net.JoinHostPort(ip.String(), port))
		if dialErr == nil {
			return conn, nil
		}
		lastErr = dialErr
	}
	return nil, lastErr
}

var reservedPrefixes = []netip.Prefix{
	netip.MustParsePrefix("0.0.0.0/8"),
	netip.MustParsePrefix("100.64.0.0/10"),
	netip.MustParsePrefix("192.0.0.0/24"),
	netip.MustParsePrefix("192.0.2.0/24"),
	netip.MustParsePrefix("192.88.99.0/24"),
	netip.MustParsePrefix("198.18.0.0/15"),
	netip.MustParsePrefix("198.51.100.0/24"),
	netip.MustParsePrefix("203.0.113.0/24"),
	netip.MustParsePrefix("240.0.0.0/4"),
	netip.MustParsePrefix("::/96"),
	netip.MustParsePrefix("100::/64"),
	netip.MustParsePrefix("2001::/23"),
	netip.MustParsePrefix("2001:db8::/32"),
	netip.MustParsePrefix("64:ff9b::/96"),
	netip.MustParsePrefix("64:ff9b:1::/48"),
	netip.MustParsePrefix("2002::/16"),
	netip.MustParsePrefix("fec0::/10"),
}

func publicIP(ip netip.Addr) bool {
	if ip.Zone() != "" {
		return false
	}
	ip = ip.Unmap()
	if !ip.IsGlobalUnicast() || ip.IsPrivate() || ip.IsLoopback() || ip.IsLinkLocalUnicast() {
		return false
	}
	for _, prefix := range reservedPrefixes {
		if prefix.Contains(ip) {
			return false
		}
	}
	return true
}
