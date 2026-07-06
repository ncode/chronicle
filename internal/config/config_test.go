package config

import (
	"strings"
	"testing"
	"time"
)

// validCfg is a minimal server config that passes validate() after defaults.
func validCfg() *ServerConfig {
	c := &ServerConfig{}
	c.applyDefaults()
	return c
}

// The defaults alone must validate — a zero-config server starts.
func TestDefaultsValidate(t *testing.T) {
	if err := validCfg().validate(); err != nil {
		t.Fatalf("defaults must validate, got %v", err)
	}
	// Default pool must have headroom over the default concurrency.
	c := validCfg()
	if c.PoolMaxConns < c.MaxConcurrent+poolHeadroom {
		t.Fatalf("default pool_max_conns %d < max_concurrent+headroom %d", c.PoolMaxConns, c.MaxConcurrent+poolHeadroom)
	}
}

// The pool must be able to serve every concurrent apply plus non-ingest headroom.
func TestValidateRejectsUndersizedPool(t *testing.T) {
	c := validCfg()
	c.MaxConcurrent = 64
	c.PoolMaxConns = 64 // no headroom
	err := c.validate()
	if err == nil || !strings.Contains(err.Error(), "pool_max_conns") {
		t.Fatalf("want pool sizing error, got %v", err)
	}
}

func TestValidateRejectsBadStaticToken(t *testing.T) {
	cases := []struct {
		name string
		tok  StaticToken
	}{
		{"no name", StaticToken{Token: "t", Role: "reader"}},
		{"no token", StaticToken{Name: "n", Role: "reader"}},
		{"bad role", StaticToken{Name: "n", Token: "t", Role: "root"}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			c := validCfg()
			c.StaticTokens = []StaticToken{tc.tok}
			if err := c.validate(); err == nil {
				t.Fatal("want validation error")
			}
		})
	}
	// A well-formed named token validates.
	c := validCfg()
	c.StaticTokens = []StaticToken{{Name: "op", Token: "s3cr3t", Role: "admin"}}
	if err := c.validate(); err != nil {
		t.Fatalf("valid token rejected: %v", err)
	}
}

func TestValidateRejectsDuplicateStaticTokens(t *testing.T) {
	// Duplicate name → ambiguous audit principal.
	c := validCfg()
	c.StaticTokens = []StaticToken{{Name: "op", Token: "t1", Role: "reader"}, {Name: "op", Token: "t2", Role: "admin"}}
	if err := c.validate(); err == nil {
		t.Fatal("duplicate token name must be rejected")
	}
	// Duplicate secret → silent role/principal collapse.
	c = validCfg()
	c.StaticTokens = []StaticToken{{Name: "a", Token: "same", Role: "reader"}, {Name: "b", Token: "same", Role: "admin"}}
	if err := c.validate(); err == nil {
		t.Fatal("duplicate token secret must be rejected")
	}
}

func TestValidateRejectsNonPositive(t *testing.T) {
	c := validCfg()
	c.MaxSkew = Duration(-time.Second)
	if err := c.validate(); err == nil {
		t.Fatal("negative max_skew must be rejected")
	}
}
