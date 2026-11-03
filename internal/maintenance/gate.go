package maintenance

import (
	"context"
	"os"
	"time"
)

// GatePath is set only for deployments that withhold traffic and workers until health checks pass.
func Blocked() bool {
	path := os.Getenv("LITEPAY_DEPLOY_GATE")
	if path == "" {
		return false
	}
	_, err := os.Stat(path)
	return !os.IsNotExist(err)
}
func Wait(ctx context.Context) bool {
	ticker := time.NewTicker(200 * time.Millisecond)
	defer ticker.Stop()
	for Blocked() {
		select {
		case <-ctx.Done():
			return false
		case <-ticker.C:
		}
	}
	return ctx.Err() == nil
}
