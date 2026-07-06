// Package config loads chronicle server and agent configuration from a JSON
// file (with env overrides for secrets) and sets up structured logging.
//
// JSON over YAML is deliberate: encoding/json is stdlib, no dependency. The
// config surface is the union of what every internal package needs; fields are
// grouped by the ADR/spec area they serve.
package config

import (
	"bytes"
	"encoding/json"
	"fmt"
	"log/slog"
	"os"
	"time"
)

// Duration is a time.Duration that (un)marshals as a Go duration string
// ("5m", "168h") instead of an int nanosecond count.
type Duration time.Duration

func (d Duration) D() time.Duration { return time.Duration(d) }

func (d *Duration) UnmarshalJSON(b []byte) error {
	var s string
	if err := json.Unmarshal(b, &s); err != nil {
		return err
	}
	v, err := time.ParseDuration(s)
	if err != nil {
		return fmt.Errorf("invalid duration %q: %w", s, err)
	}
	*d = Duration(v)
	return nil
}

func (d Duration) MarshalJSON() ([]byte, error) {
	return json.Marshal(time.Duration(d).String())
}

// TLS holds the cert material shared by the ingest (mTLS) and read (server-TLS)
// listeners. CACert is the facts-ca trust root for verifying node client certs;
// CRL is the revocation list enforced at TLS (ADR-0011).
type TLS struct {
	CACert     string `json:"ca_cert"`
	ServerCert string `json:"server_cert"`
	ServerKey  string `json:"server_key"`
	CRL        string `json:"crl,omitempty"`
}

// ServerConfig configures the chronicle server (ingest + read/admin).
type ServerConfig struct {
	DatabaseURL string `json:"database_url"` // pgx connection string; env CHRONICLE_DATABASE_URL wins
	IngestAddr  string `json:"ingest_addr"`  // mTLS listener (nodes push here)
	ReadAddr    string `json:"read_addr"`    // server-TLS listener (people/automation read here)
	OpsAddr     string `json:"ops_addr"`     // plain-HTTP ops listener: /healthz /readyz /metrics
	TLS         TLS    `json:"tls"`

	// Ingest guards & caps (ADR-0006 §2, ADR-0009 §2/§4).
	MaxSkew         Duration `json:"max_skew"`           // reject producer_ts > received_at + MaxSkew
	MaxSnapshotByte int64    `json:"max_snapshot_bytes"` // hard cap on decoded snapshot size
	MaxLeafCount    int      `json:"max_leaf_count"`     // hard cap on leaf paths per snapshot
	MaxPathLen      int      `json:"max_path_len"`       // hard cap on a single path_text length
	MaxValueBytes   int      `json:"max_value_bytes"`    // hard cap on a single leaf value
	RateLimitPerMin int      `json:"rate_limit_per_min"` // per-certname push rate limit
	MaxConcurrent   int      `json:"max_concurrent"`     // bounded ingest concurrency (backpressure)
	PoolMaxConns    int      `json:"pool_max_conns"`     // pgx pool size; must exceed MaxConcurrent + headroom

	// Observability (ADR-0009 §1).
	DirtyStreakThreshold int `json:"dirty_streak_threshold"` // alarm after N consecutive dirty passes

	// Durable/Volatile classification (ADR-0007). Glob patterns over leaf paths.
	VolatilePaths []string `json:"volatile_paths"`

	// Lifecycle (ADR-0011).
	ExpiryTTL Duration `json:"expiry_ttl"` // mark expired after this long with no contact

	// People auth (ADR-0010). OIDC relying-party + named static tokens.
	OIDC           OIDC          `json:"oidc"`
	StaticTokens   []StaticToken `json:"static_tokens"`     // named tokens (audit-attributable)
	AuthFailPerMin int           `json:"auth_fail_per_min"` // per-source failed-auth rate limit

	Log Log `json:"log"`
}

// StaticToken is a named bearer credential for people/automation reads. The name
// (not the secret) is the audit principal, so an admin action can be attributed
// to an operator of record without ever logging the token itself.
type StaticToken struct {
	Name  string `json:"name"`  // operator-assigned principal name (logged; never the secret)
	Token string `json:"token"` // the bearer secret
	Role  string `json:"role"`  // "reader" | "admin"
}

// OIDC configures chronicle as a relying party validating JWTs against the
// company IdP's JWKS and mapping a groups/roles claim to reader/admin. JWKS is
// discovered from the issuer (go-oidc), so no explicit JWKS URL is configured.
type OIDC struct {
	Issuer      string   `json:"issuer"`
	Audience    string   `json:"audience"`
	RolesClaim  string   `json:"roles_claim"` // claim holding groups/roles (default "groups")
	AdminGroups []string `json:"admin_groups"`
	ReaderGroup []string `json:"reader_groups"`
}

// AgentConfig configures the per-node chronicle-agent (ADR-0002). Identity is a
// facts-ca-issued cert loaded from a Puppet-style ssldir via agent.Load; the
// cert is provisioned out-of-band (dumb node), the agent never enrolls.
type AgentConfig struct {
	ServerURL         string   `json:"server_url"`          // https://chronicle:8443
	SSLDir            string   `json:"ssl_dir"`             // facts-ca ssldir (certs/, private_keys/, crl.pem)
	Certname          string   `json:"certname"`            // this node's cert CN (must match the issued cert)
	ServerName        string   `json:"server_name"`         // TLS SNI/verify name; "" => host of ServerURL
	ExternalFactsDirs []string `json:"external_facts_dirs"` // facts external dirs (may be empty)
	Interval          Duration `json:"interval"`            // base push period
	Jitter            Duration `json:"jitter"`              // +/- random spread per cycle
	RetryAttempts     int      `json:"retry_attempts"`      // bounded push retries before deferring
	RetryBackoff      Duration `json:"retry_backoff"`       // base backoff between retries
	Log               Log      `json:"log"`
}

// Log configures slog output.
type Log struct {
	Level  string `json:"level"`  // debug|info|warn|error
	Format string `json:"format"` // json|text
}

// LoadServer reads server config from path, applies defaults, and overlays
// the CHRONICLE_DATABASE_URL env var if set (so the connstring/secret need not
// live in the file).
func LoadServer(path string) (*ServerConfig, error) {
	c := &ServerConfig{}
	if err := loadJSON(path, c); err != nil {
		return nil, err
	}
	c.applyDefaults()
	if v := os.Getenv("CHRONICLE_DATABASE_URL"); v != "" {
		c.DatabaseURL = v
	}
	if err := c.validate(); err != nil {
		return nil, err
	}
	return c, nil
}

// DefaultAgent returns an agent config with only defaults applied — enough for
// `-dry-run` discovery without a config file.
func DefaultAgent() *AgentConfig {
	c := &AgentConfig{}
	c.applyDefaults()
	return c
}

// LoadAgent reads agent config from path and applies defaults.
func LoadAgent(path string) (*AgentConfig, error) {
	c := &AgentConfig{}
	if err := loadJSON(path, c); err != nil {
		return nil, err
	}
	c.applyDefaults()
	if err := c.validate(); err != nil {
		return nil, err
	}
	return c, nil
}

// poolHeadroom is the number of pool connections reserved for non-ingest work
// (read/admin queries, the lifecycle sweep, the monitor, and the migration
// runner's advisory-lock connection) on top of MaxConcurrent apply transactions.
const poolHeadroom = 8

// validate rejects values that would panic or invert a guard. Defaults only
// replace exact zeros, so an explicit negative would otherwise slip through.
func (c *ServerConfig) validate() error {
	checks := []struct {
		name string
		ok   bool
	}{
		{"max_concurrent", c.MaxConcurrent > 0},
		{"rate_limit_per_min", c.RateLimitPerMin > 0},
		{"max_snapshot_bytes", c.MaxSnapshotByte > 0},
		{"max_leaf_count", c.MaxLeafCount > 0},
		{"max_path_len", c.MaxPathLen > 0},
		{"max_value_bytes", c.MaxValueBytes > 0},
		{"max_skew", c.MaxSkew.D() > 0},
		{"expiry_ttl", c.ExpiryTTL.D() > 0},
		{"pool_max_conns", c.PoolMaxConns > 0},
		{"dirty_streak_threshold", c.DirtyStreakThreshold > 0},
		{"auth_fail_per_min", c.AuthFailPerMin > 0},
	}
	for _, ck := range checks {
		if !ck.ok {
			return fmt.Errorf("config: %s must be positive", ck.name)
		}
	}
	// The pool must serve every concurrent apply transaction plus non-ingest
	// headroom, or a saturated ingest starves reads/lifecycle and the migration
	// runner can self-deadlock (ADR-0009 §5).
	if c.MaxConcurrent+poolHeadroom > c.PoolMaxConns {
		return fmt.Errorf("config: pool_max_conns (%d) must be at least max_concurrent + %d = %d",
			c.PoolMaxConns, poolHeadroom, c.MaxConcurrent+poolHeadroom)
	}
	seenNames := make(map[string]int, len(c.StaticTokens))
	seenTokens := make(map[string]int, len(c.StaticTokens))
	for i, t := range c.StaticTokens {
		switch {
		case t.Name == "":
			return fmt.Errorf("config: static_tokens[%d] must have a name", i)
		case t.Token == "":
			return fmt.Errorf("config: static token %q must have a token", t.Name)
		case t.Role != "reader" && t.Role != "admin":
			return fmt.Errorf("config: static token %q role must be reader or admin, got %q", t.Name, t.Role)
		}
		// Duplicate names make audit attribution ambiguous; duplicate secrets
		// silently collapse to whichever entry is last (a role/principal footgun).
		if prev, dup := seenNames[t.Name]; dup {
			return fmt.Errorf("config: static token name %q duplicated at indexes %d and %d", t.Name, prev, i)
		}
		if prev, dup := seenTokens[t.Token]; dup {
			return fmt.Errorf("config: static token %q reuses the secret from index %d", t.Name, prev)
		}
		seenNames[t.Name] = i
		seenTokens[t.Token] = i
	}
	return nil
}

func (c *AgentConfig) validate() error {
	switch {
	case c.Interval.D() <= 0:
		return fmt.Errorf("config: interval must be positive")
	case c.Jitter.D() < 0:
		return fmt.Errorf("config: jitter must be >= 0")
	case c.RetryAttempts < 0:
		return fmt.Errorf("config: retry_attempts must be >= 0")
	case c.RetryBackoff.D() <= 0:
		return fmt.Errorf("config: retry_backoff must be positive")
	}
	return nil
}

func loadJSON(path string, v any) error {
	b, err := os.ReadFile(path)
	if err != nil {
		return fmt.Errorf("read config %s: %w", path, err)
	}
	dec := json.NewDecoder(bytes.NewReader(b))
	dec.DisallowUnknownFields() // typo in a config key is an error, not a silent ignore
	if err := dec.Decode(v); err != nil {
		return fmt.Errorf("parse config %s: %w", path, err)
	}
	return nil
}

func (c *ServerConfig) applyDefaults() {
	setStr(&c.IngestAddr, ":8443")
	setStr(&c.ReadAddr, ":8444")
	setStr(&c.OpsAddr, ":9090")
	setDur(&c.MaxSkew, 5*time.Minute)
	setI64(&c.MaxSnapshotByte, 8<<20) // 8 MiB
	setInt(&c.MaxLeafCount, 50_000)
	setInt(&c.MaxPathLen, 1024)
	setInt(&c.MaxValueBytes, 256<<10) // 256 KiB
	setInt(&c.RateLimitPerMin, 6)     // a push every ~10s is plenty
	setInt(&c.MaxConcurrent, 64)
	setInt(&c.PoolMaxConns, c.MaxConcurrent+poolHeadroom) // after MaxConcurrent is set
	setInt(&c.DirtyStreakThreshold, 10)
	setInt(&c.AuthFailPerMin, 60) // 1/s per source: fine for humans, hostile to brute force
	setDur(&c.ExpiryTTL, 7*24*time.Hour)
	if c.OIDC.RolesClaim == "" {
		c.OIDC.RolesClaim = "groups"
	}
	c.Log.applyDefaults()
}

func (c *AgentConfig) applyDefaults() {
	setDur(&c.Interval, 30*time.Minute)
	setDur(&c.Jitter, 5*time.Minute)
	setInt(&c.RetryAttempts, 3)
	setDur(&c.RetryBackoff, 2*time.Second)
	c.Log.applyDefaults()
}

func (l *Log) applyDefaults() {
	setStr(&l.Level, "info")
	setStr(&l.Format, "json")
}

// Logger builds an slog.Logger from the Log config.
func (l Log) Logger() *slog.Logger {
	var lvl slog.Level
	switch l.Level {
	case "debug":
		lvl = slog.LevelDebug
	case "warn":
		lvl = slog.LevelWarn
	case "error":
		lvl = slog.LevelError
	default:
		lvl = slog.LevelInfo
	}
	opts := &slog.HandlerOptions{Level: lvl}
	var h slog.Handler
	if l.Format == "text" {
		h = slog.NewTextHandler(os.Stderr, opts)
	} else {
		h = slog.NewJSONHandler(os.Stderr, opts)
	}
	return slog.New(h)
}

func setStr(p *string, def string) {
	if *p == "" {
		*p = def
	}
}
func setInt(p *int, def int) {
	if *p == 0 {
		*p = def
	}
}
func setI64(p *int64, def int64) {
	if *p == 0 {
		*p = def
	}
}
func setDur(p *Duration, def time.Duration) {
	if *p == 0 {
		*p = Duration(def)
	}
}
