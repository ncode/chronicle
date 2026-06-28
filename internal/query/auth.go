package query

import (
	"context"
	"fmt"
	"net/http"
	"strings"

	"github.com/coreos/go-oidc/v3/oidc"

	"github.com/ncode/chronicle/internal/config"
)

// Role is a chronicle-owned authorization role (never inherited from facts-ca).
type Role string

const (
	RoleReader Role = "reader"
	RoleAdmin  Role = "admin"
)

// Authenticator validates bearer credentials: static API tokens (work without an
// IdP) and OIDC JWTs (chronicle as relying party, validated against the IdP's
// JWKS). It never accepts client certificates — a node cert can never read
// (ADR-0010). Claims map to chronicle's own reader/admin roles.
type Authenticator struct {
	staticTokens map[string]Role
	verifier     *oidc.IDTokenVerifier // nil if OIDC not configured
	rolesClaim   string
	adminGroups  map[string]struct{}
	readerGroups map[string]struct{}
}

// NewAuthenticator builds an Authenticator. If OIDC.Issuer is set it discovers
// the provider (network at startup); otherwise only static tokens work.
func NewAuthenticator(ctx context.Context, cfg *config.ServerConfig) (*Authenticator, error) {
	a := &Authenticator{
		staticTokens: make(map[string]Role),
		rolesClaim:   cfg.OIDC.RolesClaim,
		adminGroups:  toSet(cfg.OIDC.AdminGroups),
		readerGroups: toSet(cfg.OIDC.ReaderGroup),
	}
	for tok, role := range cfg.StaticTokens {
		a.staticTokens[tok] = Role(role)
	}
	if cfg.OIDC.Issuer != "" {
		provider, err := oidc.NewProvider(ctx, cfg.OIDC.Issuer)
		if err != nil {
			return nil, fmt.Errorf("oidc discovery for %s: %w", cfg.OIDC.Issuer, err)
		}
		a.verifier = provider.Verifier(&oidc.Config{ClientID: cfg.OIDC.Audience})
	}
	return a, nil
}

// Authenticate resolves a request's bearer credential to a role.
func (a *Authenticator) Authenticate(ctx context.Context, r *http.Request) (Role, error) {
	tok := bearerToken(r)
	if tok == "" {
		return "", fmt.Errorf("missing bearer token")
	}
	if role, ok := a.staticTokens[tok]; ok {
		return role, nil
	}
	if a.verifier == nil {
		return "", fmt.Errorf("invalid token")
	}
	idTok, err := a.verifier.Verify(ctx, tok)
	if err != nil {
		return "", fmt.Errorf("invalid JWT: %w", err)
	}
	return a.roleFromClaims(idTok)
}

func (a *Authenticator) roleFromClaims(idTok *oidc.IDToken) (Role, error) {
	var claims map[string]any
	if err := idTok.Claims(&claims); err != nil {
		return "", err
	}
	groups := stringSlice(claims[a.rolesClaim])
	admin, reader := false, false
	for _, g := range groups {
		if _, ok := a.adminGroups[g]; ok {
			admin = true
		}
		if _, ok := a.readerGroups[g]; ok {
			reader = true
		}
	}
	switch {
	case admin:
		return RoleAdmin, nil
	case reader:
		return RoleReader, nil
	default:
		return "", fmt.Errorf("token has no chronicle role")
	}
}

// allows reports whether have-role satisfies need-role (admin implies reader).
func allows(have, need Role) bool {
	if have == RoleAdmin {
		return true
	}
	return have == need
}

func bearerToken(r *http.Request) string {
	h := r.Header.Get("Authorization")
	const p = "Bearer "
	if strings.HasPrefix(h, p) {
		return strings.TrimSpace(h[len(p):])
	}
	return ""
}

func toSet(ss []string) map[string]struct{} {
	m := make(map[string]struct{}, len(ss))
	for _, s := range ss {
		m[s] = struct{}{}
	}
	return m
}

func stringSlice(v any) []string {
	switch t := v.(type) {
	case []string:
		return t
	case []any:
		out := make([]string, 0, len(t))
		for _, e := range t {
			if s, ok := e.(string); ok {
				out = append(out, s)
			}
		}
		return out
	case string:
		return []string{t}
	default:
		return nil
	}
}
