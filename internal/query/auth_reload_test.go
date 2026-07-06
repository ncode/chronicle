package query

import (
	"context"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"

	"golang.org/x/time/rate"

	"github.com/ncode/chronicle/internal/config"
)

func bearerReq(tok string) *http.Request {
	r := httptest.NewRequest(http.MethodGet, "/v1/query", nil)
	if tok != "" {
		r.Header.Set("Authorization", "Bearer "+tok)
	}
	return r
}

func adminTokenCfg(tok string) *config.ServerConfig {
	return &config.ServerConfig{
		StaticTokens: []config.StaticToken{{Name: "op", Token: tok, Role: "admin"}},
		OIDC:         config.OIDC{RolesClaim: "groups"},
	}
}

// A token removed from the config stops authenticating after Reload, without a
// process restart (task 4.1).
func TestAuthReloadRevokesStaticToken(t *testing.T) {
	ctx := context.Background()
	a, err := NewAuthenticator(ctx, adminTokenCfg("tok"))
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := a.Authenticate(ctx, bearerReq("tok")); err != nil {
		t.Fatalf("token should work before reload: %v", err)
	}
	a2, err := a.Reload(ctx, &config.ServerConfig{OIDC: config.OIDC{RolesClaim: "groups"}}) // token removed
	if err != nil {
		t.Fatalf("reload without OIDC must not error: %v", err)
	}
	if _, _, err := a2.Authenticate(ctx, bearerReq("tok")); err == nil {
		t.Fatal("revoked token must be rejected after reload")
	}
}

// Revoking a leaked static token takes effect even when the OIDC IdP is
// unreachable during reload: the static-token half swaps unconditionally, only
// the OIDC verifier is kept fail-closed (task 4.1, spec scenario).
func TestAuthReloadRevocationSurvivesIdPOutage(t *testing.T) {
	ctx := context.Background()
	a, err := NewAuthenticator(ctx, adminTokenCfg("tok")) // no issuer at startup
	if err != nil {
		t.Fatal(err)
	}
	// Reload removes the token AND points at an unreachable IdP.
	unreachable := &config.ServerConfig{OIDC: config.OIDC{Issuer: "https://127.0.0.1:9/nope", RolesClaim: "groups"}}
	a2, err := a.Reload(ctx, unreachable)
	if err == nil {
		t.Fatal("want an OIDC discovery error")
	}
	if a2 == nil {
		t.Fatal("reload must still return a usable authenticator")
	}
	if _, _, e := a2.Authenticate(ctx, bearerReq("tok")); e == nil {
		t.Fatal("revoked token must be rejected even during an IdP outage")
	}
}

func quietSvc(failPerMin int, auth *Authenticator) *Service {
	s := &Service{
		log:        slog.New(slog.NewTextHandler(io.Discard, nil)),
		failPerMin: failPerMin,
		failLim:    map[string]*rate.Limiter{},
	}
	s.auth.Store(auth)
	return s
}

// Once a source burns its failure budget it is locked out BEFORE the credential
// is checked — so even a correct guess is refused (real brute-force resistance),
// while a valid token from a DIFFERENT source is unaffected and its principal
// reaches the handler (tasks 4.4, 4.5).
func TestAuthFailureThrottleAndPrincipal(t *testing.T) {
	ctx := context.Background()
	auth, err := NewAuthenticator(ctx, adminTokenCfg("good"))
	if err != nil {
		t.Fatal(err)
	}
	s := quietSvc(1, auth) // budget of 1 failure per source

	var seen string
	h := s.require(RoleReader, func(w http.ResponseWriter, r *http.Request) {
		seen = principalFrom(r.Context())
		w.WriteHeader(http.StatusOK)
	})

	call := func(tok, src string) int {
		w := httptest.NewRecorder()
		r := bearerReq(tok)
		r.RemoteAddr = src
		h.ServeHTTP(w, r)
		return w.Code
	}

	// Source A exhausts its single-failure budget, then is locked out.
	if got := call("bad", "10.0.0.1:1000"); got != http.StatusUnauthorized {
		t.Fatalf("first bad token = %d, want 401", got)
	}
	if got := call("bad", "10.0.0.1:1001"); got != http.StatusTooManyRequests {
		t.Fatalf("second bad token = %d, want 429 (throttled)", got)
	}
	// A CORRECT token from the locked-out source A is still refused — the gate
	// runs before the credential is evaluated.
	if got := call("good", "10.0.0.1:1002"); got != http.StatusTooManyRequests {
		t.Fatalf("correct token from locked-out source = %d, want 429", got)
	}
	// A valid token from a different source B is unaffected; principal reaches the handler.
	if got := call("good", "10.0.0.2:2000"); got != http.StatusOK {
		t.Fatalf("valid token from fresh source = %d, want 200", got)
	}
	if seen != "op" {
		t.Fatalf("principal in handler = %q, want op", seen)
	}
}
