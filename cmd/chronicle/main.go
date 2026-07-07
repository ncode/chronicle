// Command chronicle is the server: the mTLS ingest endpoint (nodes push facts)
// plus the server-TLS read/admin endpoint (people/automation query) and a
// plain-HTTP ops endpoint (health + Prometheus metrics). It is stateless and
// replicable behind a load balancer (ADR-0009 §5).
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"sync/atomic"
	"syscall"
	"time"

	"golang.org/x/sync/errgroup"

	"github.com/ncode/chronicle/internal/classify"
	"github.com/ncode/chronicle/internal/config"
	"github.com/ncode/chronicle/internal/ingest"
	"github.com/ncode/chronicle/internal/metrics"
	"github.com/ncode/chronicle/internal/monitor"
	"github.com/ncode/chronicle/internal/periodic"
	"github.com/ncode/chronicle/internal/query"
	"github.com/ncode/chronicle/internal/store"
	"github.com/ncode/chronicle/internal/version"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, "chronicle:", err)
		os.Exit(1)
	}
}

func run() error {
	cfgPath := flag.String("config", "/etc/chronicle/server.json", "path to server config JSON")
	showVersion := flag.Bool("version", false, "print version and exit")
	flag.Parse()
	if *showVersion {
		fmt.Println(version.Version)
		return nil
	}

	cfg, err := config.LoadServer(*cfgPath)
	if err != nil {
		return err
	}
	log := cfg.Log.Logger()

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	volatilePolicy, err := classify.New(cfg.VolatilePaths)
	if err != nil {
		return err
	}
	var volatile atomic.Pointer[classify.Policy]
	volatile.Store(volatilePolicy)

	st, err := store.Open(ctx, cfg.DatabaseURL, cfg.PoolMaxConns)
	if err != nil {
		return err
	}
	defer st.Close()
	if err := st.Migrate(ctx); err != nil {
		return fmt.Errorf("migrate: %w", err)
	}
	log.Info("migrations applied")

	m := metrics.New()

	ingestSvc, err := ingest.New(st, cfg, log, &volatile)
	if err != nil {
		return err
	}
	ingestSvc.SetMetrics(m)
	ingestTLS, crl, err := ingest.ServerTLSConfig(cfg)
	if err != nil {
		return fmt.Errorf("ingest TLS: %w", err)
	}
	if crl == nil {
		log.Warn("no CRL configured; revocation not enforced at TLS")
	}

	querySvc, err := query.NewService(ctx, st, cfg, log, &volatile)
	if err != nil {
		return fmt.Errorf("read service: %w", err)
	}
	readTLS, err := query.ReadServerTLSConfig(cfg)
	if err != nil {
		return fmt.Errorf("read TLS: %w", err)
	}

	mon := monitor.New(st, log)

	// Timeouts so a slow client can't tie up connections (Slowloris). Ingest
	// bodies are larger, so its read/write windows are wider than the read/ops
	// endpoints'.
	ingestSrv := &http.Server{
		Addr: cfg.IngestAddr, Handler: ingestSvc.Handler(), TLSConfig: ingestTLS,
		ReadHeaderTimeout: 10 * time.Second, ReadTimeout: 60 * time.Second,
		WriteTimeout: 60 * time.Second, IdleTimeout: 120 * time.Second, MaxHeaderBytes: 1 << 20,
	}
	readSrv := &http.Server{
		Addr: cfg.ReadAddr, Handler: querySvc.Handler(), TLSConfig: readTLS,
		ReadHeaderTimeout: 10 * time.Second, ReadTimeout: 30 * time.Second,
		WriteTimeout: 30 * time.Second, IdleTimeout: 120 * time.Second, MaxHeaderBytes: 1 << 20,
	}
	opsSrv := &http.Server{
		Addr: cfg.OpsAddr, Handler: opsMux(st, m),
		ReadHeaderTimeout: 5 * time.Second, ReadTimeout: 10 * time.Second,
		WriteTimeout: 10 * time.Second, IdleTimeout: 60 * time.Second, MaxHeaderBytes: 1 << 20,
	}

	go reloadOnHUP(ctx, log, *cfgPath, &volatile, querySvc, crl)

	// Each server runs two group goroutines: one serves, one waits for shutdown
	// and drains. Both are in the errgroup, so g.Wait() blocks until every drain
	// completes — main (and thus `defer st.Close()`) only proceeds after the pool
	// has no in-flight request left (task 1.4).
	g, gctx := errgroup.WithContext(ctx)
	for _, srv := range []*http.Server{ingestSrv, readSrv} {
		g.Go(func() error { return serveTLS(srv) })
		g.Go(func() error { return awaitShutdown(gctx, srv) })
	}
	g.Go(func() error { return serve(opsSrv) })
	g.Go(func() error { return awaitShutdown(gctx, opsSrv) })
	g.Go(func() error {
		periodic.Run(gctx, time.Hour, log, "expiry sweep", func(ctx context.Context) {
			ttl := cfg.ExpiryTTL.D()
			if ttl <= 0 {
				log.Error("expiry sweep", "err", fmt.Errorf("expiry ttl must be positive, got %s", ttl))
				return
			}
			n, err := st.ExpireStale(ctx, ttl)
			if err != nil {
				log.Error("expiry sweep", "err", err)
				return
			}
			if n > 0 {
				log.Info("expiry sweep", "newly_expired", n)
			}
		})
		return nil
	})
	g.Go(func() error { mon.Run(gctx, time.Hour); return nil })

	log.Info("chronicle running",
		"ingest", cfg.IngestAddr, "read", cfg.ReadAddr, "ops", cfg.OpsAddr)
	if err := g.Wait(); err != nil && !errors.Is(err, context.Canceled) {
		return err
	}
	log.Info("chronicle stopped")
	return nil
}

func opsMux(st *store.Store, m *metrics.Metrics) http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) {
		_, _ = io.WriteString(w, "ok")
	})
	mux.HandleFunc("GET /readyz", func(w http.ResponseWriter, r *http.Request) {
		ctx, cancel := context.WithTimeout(r.Context(), 2*time.Second)
		defer cancel()
		if err := st.Pool().Ping(ctx); err != nil {
			http.Error(w, "not ready", http.StatusServiceUnavailable)
			return
		}
		_, _ = io.WriteString(w, "ready")
	})
	mux.Handle("GET /metrics", m.Handler())
	return mux
}

// reloadOnHUP re-reads the config on SIGHUP and hot-swaps the volatile policy
// and the CRL. Other knobs (caps, skew) take effect on restart.
func reloadOnHUP(ctx context.Context, log *slog.Logger, cfgPath string, volatile *atomic.Pointer[classify.Policy], querySvc *query.Service, crl *ingest.CRLChecker) {
	ch := make(chan os.Signal, 1)
	signal.Notify(ch, syscall.SIGHUP)
	for {
		select {
		case <-ctx.Done():
			return
		case <-ch:
			cfg, err := config.LoadServer(cfgPath)
			if err != nil {
				log.Error("reload: config", "err", err)
				continue
			}
			cl, err := classify.New(cfg.VolatilePaths)
			if err != nil {
				log.Error("reload: volatile policy", "err", err)
			} else {
				volatile.Store(cl)
				log.Info("reloaded volatile policy", "patterns", len(cfg.VolatilePaths))
			}
			// Auth reload: the static-token half always swaps (revoking a leaked
			// token takes effect now); an error means only the OIDC verifier was
			// kept fail-closed because the IdP was unreachable (task 4.1). Bound the
			// OIDC discovery with a timeout so an unreachable/hanging IdP cannot
			// wedge this reload goroutine (and thus later SIGHUPs and the CRL
			// reload below) indefinitely.
			authCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
			authErr := querySvc.ReloadAuth(authCtx, cfg)
			cancel()
			if authErr != nil {
				log.Error("reload: oidc verifier kept fail-closed (static tokens still swapped)", "err", authErr)
			} else {
				log.Info("reloaded authenticator", "static_tokens", len(cfg.StaticTokens))
			}
			if crl != nil {
				if err := crl.Reload(); err != nil {
					log.Error("reload: CRL", "err", err)
				} else {
					log.Info("reloaded CRL")
				}
			}
		}
	}
}

func serveTLS(srv *http.Server) error {
	if err := srv.ListenAndServeTLS("", ""); err != nil && !errors.Is(err, http.ErrServerClosed) {
		return err
	}
	return nil
}

func serve(srv *http.Server) error {
	if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		return err
	}
	return nil
}

// awaitShutdown blocks until ctx is cancelled, then drains srv within a bounded
// window. Run as an errgroup member so its return value is awaited: g.Wait()
// does not return — and the pool is not closed — until the drain finishes.
func awaitShutdown(ctx context.Context, srv *http.Server) error {
	<-ctx.Done()
	drainCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	return srv.Shutdown(drainCtx)
}
