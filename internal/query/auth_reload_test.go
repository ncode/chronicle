package query

import (
	"context"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strconv"
	"testing"
	"time"

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
		failLim:    map[string]*authFailureBudget{},
		failGlobal: newAuthFailureBudget(failPerMin),
	}
	s.auth.Store(auth)
	return s
}

func beginConcurrently(b *authFailureBudget, at time.Time, n int) int {
	start := make(chan struct{})
	results := make(chan bool, n)
	for range n {
		go func() {
			<-start
			results <- b.begin(at)
		}()
	}
	close(start)
	admitted := 0
	for range n {
		if <-results {
			admitted++
		}
	}
	return admitted
}

func TestAuthFailureBudgetAdmission(t *testing.T) {
	at := time.Unix(0, 0)
	b := newAuthFailureBudget(2)

	if got := beginConcurrently(b, at, 3); got != 2 {
		t.Fatalf("concurrent admissions = %d, want 2", got)
	}
	if b.inFlight != 2 {
		t.Fatalf("in-flight reservations = %d, want 2", b.inFlight)
	}
	b.settle(at, true)
	b.settle(at, true)
	if b.inFlight != 0 {
		t.Fatalf("in-flight reservations after settlement = %d, want 0", b.inFlight)
	}
	if b.begin(at) {
		t.Fatal("zero-token budget admitted an attempt")
	}
	if b.begin(at.Add(15 * time.Second)) {
		t.Fatal("partial token admitted an attempt")
	}

	refilled := at.Add(30 * time.Second)
	if !b.begin(refilled) {
		t.Fatal("one refilled token did not admit an attempt")
	}
	if b.begin(refilled) {
		t.Fatal("one refilled token admitted two concurrent attempts")
	}
	b.settle(refilled, false)
	if !b.begin(refilled) {
		t.Fatal("successful settlement did not release the reservation")
	}
	b.settle(refilled, true)
	if b.inFlight != 0 || b.limiter.TokensAt(refilled) != 0 {
		t.Fatalf("settled budget = {inFlight:%d tokens:%v}, want zeros", b.inFlight, b.limiter.TokensAt(refilled))
	}
}

func TestAuthFailureBudgetMixedOutcomes(t *testing.T) {
	at := time.Unix(0, 0)
	b := newAuthFailureBudget(2)
	if !b.begin(at) || !b.begin(at) {
		t.Fatal("full budget did not admit two attempts")
	}
	b.settle(at, true)
	b.settle(at, false)
	if b.inFlight != 0 || b.limiter.TokensAt(at) != 1 {
		t.Fatalf("mixed settlement = {inFlight:%d tokens:%v}, want {0 1}", b.inFlight, b.limiter.TokensAt(at))
	}
	if got := beginConcurrently(b, at, 2); got != 1 {
		t.Fatalf("admissions after mixed settlement = %d, want 1", got)
	}
	b.settle(at, false)
	if b.inFlight != 0 || b.limiter.TokensAt(at) != 1 {
		t.Fatalf("successful release changed budget: inFlight=%d tokens=%v", b.inFlight, b.limiter.TokensAt(at))
	}
}

func TestAuthFailureBudgetDoesNotRecreditOutOfOrderTime(t *testing.T) {
	at := time.Unix(0, 0)
	b := newAuthFailureBudget(2)
	if !b.begin(at) || !b.begin(at) {
		t.Fatal("full budget did not admit two attempts")
	}
	b.settle(at, true)
	b.settle(at, true)

	first := at.Add(30 * time.Second)
	second := at.Add(time.Minute)
	if !b.begin(first) || !b.begin(second) {
		t.Fatal("refilled budget did not admit the expected attempts")
	}
	// Concurrent completions may acquire the budget lock in the opposite order
	// from when their timestamps were captured.
	b.settle(second, true)
	b.settle(first, true)
	if b.begin(second) {
		t.Fatal("out-of-order settlement recredited spent refill time")
	}
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

func TestAuthInsufficientRoleDoesNotDebitFailureBudget(t *testing.T) {
	cfg := &config.ServerConfig{
		StaticTokens: []config.StaticToken{{Name: "reader", Token: "read", Role: "reader"}},
		OIDC:         config.OIDC{RolesClaim: "groups"},
	}
	auth, err := NewAuthenticator(context.Background(), cfg)
	if err != nil {
		t.Fatal(err)
	}
	s := quietSvc(1, auth)
	called := false
	h := s.require(RoleAdmin, func(http.ResponseWriter, *http.Request) { called = true })
	call := func(tok string) int {
		w := httptest.NewRecorder()
		r := bearerReq(tok)
		r.RemoteAddr = "10.0.0.3:1000"
		h.ServeHTTP(w, r)
		return w.Code
	}

	if got := call("read"); got != http.StatusForbidden {
		t.Fatalf("reader on admin route = %d, want 403", got)
	}
	if got := call("bad"); got != http.StatusUnauthorized {
		t.Fatalf("first bad token after 403 = %d, want 401", got)
	}
	if got := call("bad"); got != http.StatusTooManyRequests {
		t.Fatalf("second bad token = %d, want 429", got)
	}
	if called {
		t.Fatal("admin handler ran without an admin credential")
	}
}

func TestAuthSuccessReleasesBeforeHandler(t *testing.T) {
	auth, err := NewAuthenticator(context.Background(), adminTokenCfg("good"))
	if err != nil {
		t.Fatal(err)
	}
	s := quietSvc(1, auth)
	entered := make(chan struct{}, 2)
	release := make(chan struct{})
	h := s.require(RoleReader, func(w http.ResponseWriter, _ *http.Request) {
		entered <- struct{}{}
		<-release
		w.WriteHeader(http.StatusOK)
	})

	call := func(port string, result chan<- int) {
		w := httptest.NewRecorder()
		r := bearerReq("good")
		r.RemoteAddr = "10.0.0.4:" + port
		h.ServeHTTP(w, r)
		result <- w.Code
	}
	first := make(chan int, 1)
	second := make(chan int, 1)
	go call("1000", first)
	select {
	case <-entered:
	case <-time.After(2 * time.Second):
		close(release)
		t.Fatal("first authenticated request did not enter handler")
	}
	go call("1001", second)
	select {
	case <-entered:
	case code := <-second:
		close(release)
		t.Fatalf("second request returned %d before entering handler", code)
	case <-time.After(2 * time.Second):
		close(release)
		t.Fatal("second authenticated request did not enter handler")
	}
	close(release)
	if code := <-first; code != http.StatusOK {
		t.Fatalf("first authenticated request = %d, want 200", code)
	}
	if code := <-second; code != http.StatusOK {
		t.Fatalf("second authenticated request = %d, want 200", code)
	}
}

func TestAuthConcurrentValidAttemptMayThrottle(t *testing.T) {
	auth, err := NewAuthenticator(context.Background(), adminTokenCfg("good"))
	if err != nil {
		t.Fatal(err)
	}
	s := quietSvc(1, auth)
	held := bearerReq("good")
	held.RemoteAddr = "10.0.0.5:1000"
	budget := s.failBudget(held)
	if !budget.begin(time.Now()) {
		t.Fatal("failed to reserve initial credential check")
	}

	h := s.require(RoleReader, func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusOK) })
	w := httptest.NewRecorder()
	r := bearerReq("good")
	r.RemoteAddr = "10.0.0.5:1001"
	h.ServeHTTP(w, r)
	budget.settle(time.Now(), false)
	if w.Code != http.StatusTooManyRequests {
		t.Fatalf("valid request beyond concurrent capacity = %d, want 429", w.Code)
	}
	if budget.inFlight != 0 {
		t.Fatalf("in-flight reservations after release = %d, want 0", budget.inFlight)
	}
}

func TestAuthFailureFallbackSharesBudget(t *testing.T) {
	s := &Service{
		failPerMin: 1,
		failLim:    make(map[string]*authFailureBudget, maxAuthFailSources),
		failGlobal: newAuthFailureBudget(1),
	}
	placeholder := newAuthFailureBudget(1)
	for i := range maxAuthFailSources {
		s.failLim[strconv.Itoa(i)] = placeholder
	}
	aReq := bearerReq("")
	aReq.RemoteAddr = "192.0.2.1:1000"
	bReq := bearerReq("")
	bReq.RemoteAddr = "192.0.2.2:2000"
	aBudget := s.failBudget(aReq)
	bBudget := s.failBudget(bReq)
	if aBudget != s.failGlobal || bBudget != s.failGlobal {
		t.Fatal("post-cap sources did not share the fallback budget")
	}

	at := time.Unix(0, 0)
	if !aBudget.begin(at) {
		t.Fatal("source A did not reserve the fallback budget")
	}
	if bBudget.begin(at) {
		t.Fatal("source B bypassed source A's in-flight fallback reservation")
	}
	aBudget.settle(at, false)
	if !bBudget.begin(at) {
		t.Fatal("source A's success did not release fallback capacity")
	}
	bBudget.settle(at, true)
	if aBudget.begin(at) {
		t.Fatal("source B's failure did not debit the shared fallback budget")
	}
}
