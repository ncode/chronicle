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
// (ADR-0010). Claims map to chronicle's own reader/admin roles. It is immutable:
// SIGHUP builds a fresh one and swaps it atomically (two independent halves —
// see Reload).
type Authenticator struct {
	staticTokens map[string]principal  // token secret -> {name, role}
	verifier     *oidc.IDTokenVerifier // nil if OIDC not configured
	issuer       string                // OIDC issuer this verifier was built for
	audience     string                // OIDC audience (ClientID) this verifier was built for
	rolesClaim   string
	adminGroups  map[string]struct{}
	readerGroups map[string]struct{}
}

// principal is a resolved static-token identity: the operator-assigned name
// (the audit principal, never the secret) and its role.
type principal struct {
	name string
	role Role
}

// NewAuthenticator builds an Authenticator. If OIDC.Issuer is set it discovers
// the provider (network at startup); a discovery failure at startup is fatal.
func NewAuthenticator(ctx context.Context, cfg *config.ServerConfig) (*Authenticator, error) {
	a, err := buildAuthenticator(ctx, cfg, nil)
	if err != nil {
		return nil, err // startup: fail hard rather than run without configured OIDC
	}
	return a, nil
}

// Reload builds a fresh Authenticator for a SIGHUP config reload in two
// independently-swapped halves: the static-token set and role mappings are pure
// configuration and always take effect, while the OIDC verifier is rebuilt
// separately. If IdP discovery fails, the previous verifier is retained
// (fail-closed) and the failure returned for logging — crucially, the
// static-token swap still happens, so revoking a leaked static token is never
// blocked by an IdP outage. The returned Authenticator is always usable, even
// when err is non-nil.
func (a *Authenticator) Reload(ctx context.Context, cfg *config.ServerConfig) (*Authenticator, error) {
	return buildAuthenticator(ctx, cfg, a)
}

// buildAuthenticator assembles the static-token half unconditionally, then the
// OIDC half. On OIDC discovery failure it may fall back to the previous verifier
// and returns the error; the Authenticator it returns is still usable with the
// (freshly swapped) static tokens.
func buildAuthenticator(ctx context.Context, cfg *config.ServerConfig, prev *Authenticator) (*Authenticator, error) {
	a := &Authenticator{
		staticTokens: make(map[string]principal, len(cfg.StaticTokens)),
		issuer:       cfg.OIDC.Issuer,
		audience:     cfg.OIDC.Audience,
		rolesClaim:   cfg.OIDC.RolesClaim,
		adminGroups:  toSet(cfg.OIDC.AdminGroups),
		readerGroups: toSet(cfg.OIDC.ReaderGroup),
	}
	for _, t := range cfg.StaticTokens {
		a.staticTokens[t.Token] = principal{name: t.Name, role: Role(t.Role)}
	}
	if cfg.OIDC.Issuer == "" {
		return a, nil // static tokens only
	}
	provider, err := oidc.NewProvider(ctx, cfg.OIDC.Issuer)
	if err != nil {
		// Fail-closed. Reuse the previous verifier ONLY if it was built for the
		// SAME issuer AND audience: if the operator repointed OIDC and the new IdP
		// is unreachable, keeping the old verifier would keep accepting old-IdP
		// tokens the new config meant to reject, so drop it (verifier stays nil).
		if prev != nil && prev.verifier != nil && prev.issuer == cfg.OIDC.Issuer && prev.audience == cfg.OIDC.Audience {
			a.verifier = prev.verifier
		}
		return a, fmt.Errorf("oidc discovery for %s: %w", cfg.OIDC.Issuer, err)
	}
	a.verifier = provider.Verifier(&oidc.Config{ClientID: cfg.OIDC.Audience})
	return a, nil
}

// Authenticate resolves a request's bearer credential to a role and the
// authenticated principal (the static-token name or the OIDC subject), so an
// admin action can be attributed to an operator of record.
func (a *Authenticator) Authenticate(ctx context.Context, r *http.Request) (Role, string, error) {
	tok := bearerToken(r)
	if tok == "" {
		return "", "", fmt.Errorf("missing bearer token")
	}
	if p, ok := a.staticTokens[tok]; ok {
		return p.role, p.name, nil
	}
	if a.verifier == nil {
		return "", "", fmt.Errorf("invalid token")
	}
	idTok, err := a.verifier.Verify(ctx, tok)
	if err != nil {
		return "", "", fmt.Errorf("invalid JWT: %w", err)
	}
	role, err := a.roleFromClaims(idTok)
	if err != nil {
		return "", "", err
	}
	return role, idTok.Subject, nil
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
