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
	"syscall"
	"time"

	"golang.org/x/sync/errgroup"

	"github.com/ncode/chronicle/internal/config"
	"github.com/ncode/chronicle/internal/ingest"
	"github.com/ncode/chronicle/internal/lifecycle"
	"github.com/ncode/chronicle/internal/metrics"
	"github.com/ncode/chronicle/internal/monitor"
	"github.com/ncode/chronicle/internal/query"
	"github.com/ncode/chronicle/internal/store"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, "chronicle:", err)
		os.Exit(1)
	}
}

func run() error {
	cfgPath := flag.String("config", "/etc/chronicle/server.json", "path to server config JSON")
	flag.Parse()

	cfg, err := config.LoadServer(*cfgPath)
	if err != nil {
		return err
	}
	log := cfg.Log.Logger()

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	st, err := store.Open(ctx, cfg.DatabaseURL)
	if err != nil {
		return err
	}
	defer st.Close()
	if err := st.Migrate(ctx); err != nil {
		return fmt.Errorf("migrate: %w", err)
	}
	log.Info("migrations applied")

	m := metrics.New()

	ingestSvc, err := ingest.New(st, cfg, log)
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

	querySvc, err := query.NewService(ctx, st, cfg, log)
	if err != nil {
		return fmt.Errorf("read service: %w", err)
	}
	readTLS, err := query.ReadServerTLSConfig(cfg)
	if err != nil {
		return fmt.Errorf("read TLS: %w", err)
	}

	lc := lifecycle.NewManager(st, log, cfg.ExpiryTTL.D())
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

	go reloadOnHUP(ctx, log, *cfgPath, ingestSvc, querySvc, crl)

	g, gctx := errgroup.WithContext(ctx)
	g.Go(func() error { return serveTLS(gctx, ingestSrv) })
	g.Go(func() error { return serveTLS(gctx, readSrv) })
	g.Go(func() error { return serve(gctx, opsSrv) })
	g.Go(func() error { lc.Run(gctx, time.Hour); return nil })
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
// (on BOTH ingest and the read service, so they stay consistent) and the CRL
// (task 7.1). Other knobs (caps, skew) take effect on restart.
func reloadOnHUP(ctx context.Context, log *slog.Logger, cfgPath string, ingestSvc *ingest.Service, querySvc *query.Service, crl *ingest.CRLChecker) {
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
			ingestErr := ingestSvc.ReloadVolatilePolicy(cfg.VolatilePaths)
			queryErr := querySvc.ReloadVolatilePolicy(cfg.VolatilePaths)
			if ingestErr != nil || queryErr != nil {
				log.Error("reload: volatile policy", "ingest_err", ingestErr, "query_err", queryErr)
			} else {
				log.Info("reloaded volatile policy", "patterns", len(cfg.VolatilePaths))
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

func serveTLS(ctx context.Context, srv *http.Server) error {
	go shutdownOnDone(ctx, srv)
	if err := srv.ListenAndServeTLS("", ""); err != nil && !errors.Is(err, http.ErrServerClosed) {
		return err
	}
	return nil
}

func serve(ctx context.Context, srv *http.Server) error {
	go shutdownOnDone(ctx, srv)
	if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		return err
	}
	return nil
}

func shutdownOnDone(ctx context.Context, srv *http.Server) {
	<-ctx.Done()
	sctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	_ = srv.Shutdown(sctx)
}
