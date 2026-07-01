// Package periodic runs a function on a fixed interval until its context is
// cancelled. It is the shared cadence for the background sweepers (expiry,
// monitor): the ticker, the select, and the cancellation lifecycle live here
// once, so each caller supplies only the work to do per tick. Keeping the loop
// in one place makes it testable with testing/synctest — fake time, no database
// — because the work is an injected callback rather than a direct store call.
package periodic

import (
	"context"
	"log/slog"
	"time"
)

// Run calls tick once per interval until ctx is cancelled, then returns. A
// non-positive interval is a misconfiguration: Run logs it and returns without
// starting (the background loops are best-effort, so this never aborts startup).
// Errors are the tick's own concern — Run only drives cadence and cancellation.
func Run(ctx context.Context, interval time.Duration, log *slog.Logger, name string, tick func(context.Context)) {
	if interval <= 0 {
		log.Error(name+" interval must be positive", "interval", interval)
		return
	}
	t := time.NewTicker(interval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			tick(ctx)
		}
	}
}
