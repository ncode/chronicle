// Package agent is the per-node collector (ADR-0002): on a jittered timer it
// runs facts.Discover() in library mode, builds the discovery-status report,
// stamps a producer_timestamp, and pushes over facts-ca mTLS. Dumb node: no
// inbound port, no durable spool, identity is the facts-ca cert alone.
package agent

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"math/rand/v2"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/ncode/facts"
	fca "github.com/ncode/facts-ca/agent"

	"github.com/ncode/chronicle/internal/config"
	"github.com/ncode/chronicle/internal/wire"
)

// Agent collects and pushes snapshots for one node.
type Agent struct {
	cfg     *config.AgentConfig
	log     *slog.Logger
	eng     *facts.Engine
	client  *http.Client
	pushURL string
	limURL  string
	limits  wire.Limits
}

// New builds an agent: loads the pre-provisioned facts-ca identity (no enroll),
// constructs the facts engine, and wires an mTLS client.
func New(cfg *config.AgentConfig, log *slog.Logger) (*Agent, error) {
	id, err := fca.Load(cfg.SSLDir, cfg.Certname)
	if err != nil {
		return nil, fmt.Errorf("load facts-ca identity (ssldir=%s certname=%s): %w", cfg.SSLDir, cfg.Certname, err)
	}
	// Require an absolute https URL: a http:// (or hostless) server_url would
	// silently bypass the mTLS identity we just loaded.
	u, err := url.Parse(cfg.ServerURL)
	if err != nil || u.Scheme != "https" || u.Hostname() == "" {
		return nil, fmt.Errorf("server_url must be an absolute https URL with a host: %q", cfg.ServerURL)
	}
	serverName := cfg.ServerName
	if serverName == "" {
		serverName = u.Hostname()
	}
	client := &http.Client{
		Timeout:   30 * time.Second,
		Transport: &http.Transport{TLSClientConfig: id.ClientTLSConfig(serverName)},
	}

	engOpts := []facts.Option{facts.WithLogger(log)}
	if len(cfg.ExternalFactsDirs) > 0 {
		engOpts = append(engOpts, facts.WithExternalDirs(cfg.ExternalFactsDirs...))
	}
	eng, err := facts.New(engOpts...)
	if err != nil {
		return nil, fmt.Errorf("build facts engine: %w", err)
	}

	base := strings.TrimRight(cfg.ServerURL, "/")
	return &Agent{
		cfg:     cfg,
		log:     log,
		eng:     eng,
		client:  client,
		pushURL: base + "/v1/push",
		limURL:  base + "/v1/limits",
		limits:  generousLimits(),
	}, nil
}

// Run is the collection loop. An initial small jitter desynchronizes a fleet
// that boots together; each subsequent cycle waits interval ± jitter.
func (a *Agent) Run(ctx context.Context) error {
	if l, err := a.fetchLimits(ctx); err == nil {
		a.limits = l
	} else {
		a.log.Warn("limits fetch failed; using generous local defaults", "err", err)
	}

	// Initial desync delay in [0, jitter).
	if d := a.cfg.Jitter.D(); d > 0 {
		if !sleep(ctx, rand.N(d)) {
			return ctx.Err()
		}
	}
	for {
		a.collectAndPush(ctx)
		if !sleep(ctx, a.nextInterval()) {
			return ctx.Err()
		}
	}
}

// nextInterval is interval ± jitter, floored at 1s.
func (a *Agent) nextInterval() time.Duration {
	base := a.cfg.Interval.D()
	j := a.cfg.Jitter.D()
	d := base
	if j > 0 {
		d = base - j + rand.N(2*j)
	}
	if d < time.Second {
		d = time.Second
	}
	return d
}

func (a *Agent) collectAndPush(ctx context.Context) {
	push, tree, err := a.collect(ctx)
	if err != nil {
		a.log.Error("collect", "err", err)
		return
	}
	// Pre-check the server-advertised caps; don't send a body the server will
	// reject (node-agent "Bounded Snapshot Payload").
	if a.limits.MaxSnapshotBytes > 0 && int64(len(push.Tree)) > a.limits.MaxSnapshotBytes {
		a.log.Error("snapshot exceeds advertised cap; not sending",
			"cap", "snapshot-bytes", "bytes", len(push.Tree), "limit", a.limits.MaxSnapshotBytes)
		return
	}
	if over := a.exceedsLimits(tree); over != "" {
		a.log.Error("snapshot exceeds advertised cap; not sending", "cap", over)
		return
	}
	a.pushWithRetry(ctx, push)
}

// collect runs one discovery pass and assembles the push payload (snapshot +
// producer timestamp + discovery-status report). It also returns the decoded
// tree for cap pre-checks. Shared by collectAndPush and DryRun.
func (a *Agent) collect(ctx context.Context) (wire.Push, map[string]any, error) {
	producerTS := time.Now()
	snap, derr := a.eng.Discover(ctx)
	if snap == nil {
		return wire.Push{}, nil, fmt.Errorf("discovery returned no snapshot: %w", derr)
	}
	if derr != nil {
		a.log.Warn("partial discovery", "err", derr) // snapshot still usable
	}
	tree := snap.Tree()
	status := buildStatus(tree, a.cfg.ExternalFactsDirs, derr)
	treeJSON, err := json.Marshal(tree)
	if err != nil {
		return wire.Push{}, nil, fmt.Errorf("marshal tree: %w", err)
	}
	return wire.Push{ProducerTimestamp: producerTS, Tree: treeJSON, Discovery: status}, tree, nil
}

// DryRun discovers facts once and writes the push payload it WOULD send to w —
// no identity is loaded and no server is contacted. It is the cross-OS smoke
// test: a non-empty snapshot proves facts.Discover() works on this OS.
func DryRun(ctx context.Context, cfg *config.AgentConfig, log *slog.Logger, w io.Writer) error {
	engOpts := []facts.Option{facts.WithLogger(log)}
	if len(cfg.ExternalFactsDirs) > 0 {
		engOpts = append(engOpts, facts.WithExternalDirs(cfg.ExternalFactsDirs...))
	}
	eng, err := facts.New(engOpts...)
	if err != nil {
		return fmt.Errorf("build facts engine: %w", err)
	}
	a := &Agent{cfg: cfg, log: log, eng: eng}
	push, tree, err := a.collect(ctx)
	if err != nil {
		return err
	}
	if len(tree) == 0 {
		return fmt.Errorf("discovery produced an empty snapshot")
	}
	enc := json.NewEncoder(w)
	enc.SetIndent("", "  ")
	return enc.Encode(push)
}

// exceedsLimits reports the first advertised cap a flattened snapshot would
// violate (leaf-count, path-length, value-bytes), or "" if it is within bounds.
func (a *Agent) exceedsLimits(tree map[string]any) string {
	leaves := wire.Flatten(tree)
	if a.limits.MaxLeafCount > 0 && len(leaves) > a.limits.MaxLeafCount {
		return "leaf-count"
	}
	for _, lf := range leaves {
		if a.limits.MaxPathLen > 0 && len(lf.Path) > a.limits.MaxPathLen {
			return "path-length"
		}
		if a.limits.MaxValueBytes > 0 {
			if raw, err := json.Marshal(lf.Value); err == nil && len(raw) > a.limits.MaxValueBytes {
				return "value-bytes"
			}
		}
	}
	return ""
}

func (a *Agent) fetchLimits(ctx context.Context) (wire.Limits, error) {
	var lim wire.Limits
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, a.limURL, nil)
	if err != nil {
		return lim, err
	}
	resp, err := a.client.Do(req)
	if err != nil {
		return lim, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return lim, fmt.Errorf("limits status %d", resp.StatusCode)
	}
	if err := json.NewDecoder(resp.Body).Decode(&lim); err != nil {
		return lim, err
	}
	return lim, nil
}

func generousLimits() wire.Limits {
	return wire.Limits{MaxSnapshotBytes: 64 << 20, MaxLeafCount: 1_000_000, MaxPathLen: 8192, MaxValueBytes: 8 << 20}
}

// sleep waits d or until ctx is done; returns false if ctx was cancelled.
func sleep(ctx context.Context, d time.Duration) bool {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-t.C:
		return true
	}
}
