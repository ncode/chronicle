// Command chronicle-agent is the per-node collector: on a self-jittered timer
// it runs facts.Discover() (library mode), assembles a snapshot + producer
// timestamp + discovery-status report, and pushes it to chronicle over
// facts-ca mTLS (ADR-0002). Dumb node, smart center.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"strings"
	"syscall"

	"github.com/ncode/chronicle/internal/agent"
	"github.com/ncode/chronicle/internal/config"
	"github.com/ncode/chronicle/internal/version"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, "chronicle-agent:", err)
		os.Exit(1)
	}
}

func run() error {
	cfgPath := flag.String("config", "/etc/chronicle/agent.json", "path to agent config JSON")
	dryRun := flag.Bool("dry-run", false, "discover facts once and print the push payload; no identity, no server")
	externalDirs := flag.String("external-dirs", "", "comma-separated external fact dirs (dry-run convenience)")
	showVersion := flag.Bool("version", false, "print version and exit")
	flag.Parse()

	if *showVersion {
		fmt.Println(version.Version)
		return nil
	}
	if *dryRun {
		return runDryRun(*cfgPath, *externalDirs)
	}

	cfg, err := config.LoadAgent(*cfgPath)
	if err != nil {
		return err
	}
	log := cfg.Log.Logger()

	ag, err := agent.New(cfg, log)
	if err != nil {
		return err
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	log.Info("chronicle-agent starting",
		"server_url", cfg.ServerURL, "certname", cfg.Certname, "interval", cfg.Interval.D().String())
	if err := ag.Run(ctx); err != nil && !errors.Is(err, context.Canceled) {
		return err
	}
	log.Info("chronicle-agent stopped")
	return nil
}

// runDryRun discovers facts once and prints the payload. Works without a config
// file (defaults) so it can smoke-test facts.Discover() on any OS.
func runDryRun(cfgPath, externalDirs string) error {
	cfg, err := config.LoadAgent(cfgPath)
	if err != nil {
		if !errors.Is(err, os.ErrNotExist) {
			return err
		}
		cfg = config.DefaultAgent() // no config file: defaults are enough for discovery
	}
	if externalDirs != "" {
		cfg.ExternalFactsDirs = strings.Split(externalDirs, ",")
	}
	return agent.DryRun(context.Background(), cfg, cfg.Log.Logger(), os.Stdout)
}
