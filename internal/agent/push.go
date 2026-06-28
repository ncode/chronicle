package agent

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"math/rand/v2"
	"net/http"
	"strconv"
	"time"

	"github.com/ncode/chronicle/internal/wire"
)

// pushWithRetry POSTs the payload, retrying only on 503 and transport failures
// with jittered backoff (honoring Retry-After) up to RetryAttempts. Guard
// rejects (stale/skewed) and oversize are terminal for this snapshot — retrying
// the same body cannot help. After exhausting retries it defers to the next
// timer; there is no durable spool (data path is loss-tolerant, gaps visible).
func (a *Agent) pushWithRetry(ctx context.Context, payload wire.Push) {
	body, err := json.Marshal(payload)
	if err != nil {
		a.log.Error("marshal push", "err", err)
		return
	}

	var retryAfter time.Duration
	for attempt := 0; attempt <= a.cfg.RetryAttempts; attempt++ {
		if attempt > 0 {
			if !sleep(ctx, a.backoff(attempt, retryAfter)) {
				return
			}
			retryAfter = 0
		}
		retry, ra := a.attempt(ctx, body)
		if !retry {
			return
		}
		retryAfter = ra
	}
	a.log.Warn("push gave up after retries; deferring to next timer",
		"attempts", a.cfg.RetryAttempts+1)
}

// attempt makes one push. It returns (retry, retryAfter): retry true means a
// transient failure worth another attempt.
func (a *Agent) attempt(ctx context.Context, body []byte) (retry bool, retryAfter time.Duration) {
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, a.pushURL, bytes.NewReader(body))
	if err != nil {
		a.log.Error("build request", "err", err)
		return false, 0
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := a.client.Do(req)
	if err != nil {
		a.log.Warn("push transport error; will retry", "err", err)
		return true, 0 // transport failure: retry
	}
	defer resp.Body.Close()

	switch resp.StatusCode {
	case http.StatusOK:
		var pr wire.PushResponse
		_ = json.NewDecoder(resp.Body).Decode(&pr)
		a.log.Info("push applied", "opened", pr.Opened, "closed", pr.Closed,
			"tombstoned", pr.Tombstoned, "unchanged", pr.Unchanged)
		return false, 0
	case http.StatusServiceUnavailable:
		_, _ = io.Copy(io.Discard, resp.Body)
		return true, parseRetryAfter(resp) // backpressure: retry
	case http.StatusRequestEntityTooLarge:
		a.log.Error("push rejected as oversized (terminal); operator-visible", "status", resp.StatusCode)
		return false, 0 // do not blindly retry the same oversized body
	default:
		var pr wire.PushResponse
		_ = json.NewDecoder(resp.Body).Decode(&pr)
		a.log.Warn("push rejected", "status", resp.StatusCode, "reason", pr.Reason)
		return false, 0 // guard reject / rate-limited: defer to next timer
	}
}

// backoff is Retry-After if the server set it, else an exponential base*2^(n-1)
// plus jitter, capped at 30s. The doubling saturates at the cap so a large
// attempt count can never overflow int64 and wrap negative.
func (a *Agent) backoff(attempt int, retryAfter time.Duration) time.Duration {
	if retryAfter > 0 {
		return retryAfter
	}
	const maxBackoff = 30 * time.Second
	base := a.cfg.RetryBackoff.D()
	d := base
	for i := 1; i < attempt && d < maxBackoff; i++ {
		d *= 2
	}
	return min(min(d, maxBackoff)+rand.N(base), maxBackoff) // jitter, still capped
}

func parseRetryAfter(resp *http.Response) time.Duration {
	v := resp.Header.Get("Retry-After")
	if v == "" {
		return 0
	}
	if secs, err := strconv.Atoi(v); err == nil && secs >= 0 {
		return time.Duration(secs) * time.Second
	}
	return 0
}
