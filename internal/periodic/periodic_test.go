package periodic

import (
	"bytes"
	"context"
	"io"
	"log/slog"
	"strings"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"
)

func discardLog() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

// TestRunTicksEachIntervalUntilCancel exercises the loop with fake time: the
// tick fires once per interval, and ctx cancellation makes Run return. synctest
// drives the clock with no real sleeping and no database.
func TestRunTicksEachIntervalUntilCancel(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		var ticks atomic.Int64
		done := make(chan struct{})
		go func() {
			Run(ctx, time.Second, discardLog(), "test", func(context.Context) {
				ticks.Add(1)
			})
			close(done)
		}()

		// Advance ~3 intervals of fake time while the loop is parked on the ticker.
		time.Sleep(3*time.Second + 100*time.Millisecond)
		synctest.Wait()
		if got := ticks.Load(); got != 3 {
			t.Fatalf("ticks after ~3s = %d, want 3", got)
		}

		cancel()
		synctest.Wait()
		select {
		case <-done:
		default:
			t.Fatal("Run did not return after ctx cancel")
		}
	})
}

// TestRunRejectsNonPositiveInterval: a misconfigured interval returns immediately
// (the inline call returns or the test would hang) without ticking, and logs the
// misconfig — the best-effort contract: log, don't start, don't abort.
func TestRunRejectsNonPositiveInterval(t *testing.T) {
	var buf bytes.Buffer
	log := slog.New(slog.NewTextHandler(&buf, nil))
	var called atomic.Bool
	Run(context.Background(), 0, log, "sweep", func(context.Context) {
		called.Store(true)
	})
	if called.Load() {
		t.Fatal("non-positive interval must not tick")
	}
	if !strings.Contains(buf.String(), "sweep interval must be positive") {
		t.Fatalf("misconfig must be logged, got: %q", buf.String())
	}
}
