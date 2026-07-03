package main

import (
	"context"
	"errors"
	"net"
	"net/http"
	"sync/atomic"
	"testing"
	"time"

	"golang.org/x/sync/errgroup"
)

// awaitShutdown, run as an errgroup member alongside the server, blocks g.Wait()
// until an in-flight request has drained — so main (and the pool close) never
// races ahead of active requests (task 1.4).
func TestAwaitShutdownDrainsInFlight(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}

	var completed atomic.Bool
	started := make(chan struct{})
	srv := &http.Server{
		Handler: http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			close(started)
			time.Sleep(150 * time.Millisecond) // in-flight work during shutdown
			completed.Store(true)
			_, _ = w.Write([]byte("ok"))
		}),
	}

	ctx, cancel := context.WithCancel(context.Background())
	g, gctx := errgroup.WithContext(ctx)
	g.Go(func() error {
		if err := srv.Serve(ln); err != nil && !errors.Is(err, http.ErrServerClosed) {
			return err
		}
		return nil
	})
	g.Go(func() error { return awaitShutdown(gctx, srv) })

	reqDone := make(chan struct{})
	go func() {
		defer close(reqDone)
		resp, err := http.Get("http://" + ln.Addr().String())
		if err != nil {
			t.Errorf("in-flight request failed: %v", err)
			return
		}
		resp.Body.Close()
	}()

	<-started // the request is now inside the handler
	cancel()  // trigger shutdown while it is in-flight

	if err := g.Wait(); err != nil {
		t.Fatalf("server group returned an error: %v", err)
	}
	// g.Wait() returned only after awaitShutdown finished draining.
	if !completed.Load() {
		t.Fatal("shutdown returned before the in-flight request completed")
	}
	<-reqDone
}
