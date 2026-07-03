package agent

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ncode/chronicle/internal/config"
	"github.com/ncode/chronicle/internal/wire"
)

func testAgent(srv *httptest.Server) *Agent {
	return &Agent{
		cfg:     &config.AgentConfig{RetryAttempts: 3, RetryBackoff: config.Duration(time.Millisecond)},
		log:     slog.New(slog.NewTextHandler(io.Discard, nil)),
		client:  srv.Client(),
		pushURL: srv.URL + "/v1/push",
	}
}

func emptyBody() []byte {
	b, _ := json.Marshal(wire.Push{ProducerTimestamp: time.Now(), Tree: json.RawMessage(`{}`)})
	return b
}

func TestPushRetryThenSuccess(t *testing.T) {
	var n atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		if n.Add(1) == 1 {
			w.Header().Set("Retry-After", "0")
			w.WriteHeader(http.StatusServiceUnavailable) // first try: backpressure
			return
		}
		_ = json.NewEncoder(w).Encode(wire.PushResponse{Applied: true})
	}))
	defer srv.Close()

	testAgent(srv).pushWithRetry(context.Background(), emptyBody())
	if got := n.Load(); got != 2 {
		t.Fatalf("expected retry then success (2 calls), got %d", got)
	}
}

func TestPushOversizedIsTerminal(t *testing.T) {
	var n atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		n.Add(1)
		w.WriteHeader(http.StatusRequestEntityTooLarge)
	}))
	defer srv.Close()

	testAgent(srv).pushWithRetry(context.Background(), emptyBody())
	if got := n.Load(); got != 1 {
		t.Fatalf("413 must be terminal (1 call), got %d", got)
	}
}

func TestPushGivesUpAfterBoundedRetries(t *testing.T) {
	var n atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		n.Add(1)
		w.Header().Set("Retry-After", "0")
		w.WriteHeader(http.StatusServiceUnavailable) // always saturated
	}))
	defer srv.Close()

	a := testAgent(srv) // RetryAttempts=3 => 4 total attempts
	a.pushWithRetry(context.Background(), emptyBody())
	if got := n.Load(); got != 4 {
		t.Fatalf("expected bounded 4 attempts, got %d", got)
	}
}
