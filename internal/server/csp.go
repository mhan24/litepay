package server

import (
	"crypto/rand"
	"encoding/base64"
	"log/slog"
	"net/http"

	"github.com/gin-gonic/gin"
)

const cspNonceKey = "csp_nonce"

func contentSecurityPolicy(c *gin.Context) {
	var random [24]byte
	if _, err := rand.Read(random[:]); err != nil {
		slog.Error("生成页面安全随机数失败", "err", err)
		c.AbortWithStatus(http.StatusInternalServerError)
		return
	}
	nonce := base64.RawStdEncoding.EncodeToString(random[:])
	c.Set(cspNonceKey, nonce)
	c.Header("Content-Security-Policy", "default-src 'none'; script-src 'nonce-"+nonce+"'; style-src 'nonce-"+nonce+"'; img-src 'self' data:; connect-src 'self'; form-action 'self'; base-uri 'none'; frame-ancestors 'none'; object-src 'none'")
	c.Next()
}

// Only trusted template data receives the nonce; never stamp it onto rendered HTML.
func renderHTML(c *gin.Context, status int, name string, data any) {
	nonce := c.GetString(cspNonceKey)
	switch value := data.(type) {
	case gin.H:
		value["Nonce"] = nonce
	case CashierData:
		value.Nonce = nonce
		data = value
	}
	c.HTML(status, name, data)
}
