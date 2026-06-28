package query

import (
	"context"
	"crypto/tls"
	"net/http"
	"testing"

	"github.com/ncode/chronicle/internal/config"
	"github.com/ncode/chronicle/internal/testca"
)

func TestStaticTokenRolesAndRejection(t *testing.T) {
	cfg := &config.ServerConfig{
		StaticTokens: map[string]string{"r-tok": "reader", "a-tok": "admin"},
		OIDC:         config.OIDC{RolesClaim: "groups"}, // no Issuer => no JWT verifier
	}
	auth, err := NewAuthenticator(context.Background(), cfg)
	if err != nil {
		t.Fatal(err)
	}

	req := func(bearer string) *http.Request {
		r, _ := http.NewRequest(http.MethodGet, "/v1/query", nil)
		if bearer != "" {
			r.Header.Set("Authorization", "Bearer "+bearer)
		}
		return r
	}

	if role, err := auth.Authenticate(context.Background(), req("r-tok")); err != nil || role != RoleReader {
		t.Fatalf("reader token => %v %v", role, err)
	}
	if role, err := auth.Authenticate(context.Background(), req("a-tok")); err != nil || role != RoleAdmin {
		t.Fatalf("admin token => %v %v", role, err)
	}
	if _, err := auth.Authenticate(context.Background(), req("")); err == nil {
		t.Fatal("missing token must error")
	}
	if _, err := auth.Authenticate(context.Background(), req("bogus")); err == nil {
		t.Fatal("invalid token must error")
	}
}

func TestRoleAllows(t *testing.T) {
	if !allows(RoleAdmin, RoleReader) || !allows(RoleAdmin, RoleAdmin) || !allows(RoleReader, RoleReader) {
		t.Fatal("admin implies reader; reader is reader")
	}
	if allows(RoleReader, RoleAdmin) {
		t.Fatal("reader must not satisfy admin")
	}
}

// The read endpoint requests NO client cert, so a facts-ca node cert can never
// be presented as a read credential (ADR-0010).
func TestReadEndpointRejectsClientCerts(t *testing.T) {
	ca := testca.New(t)
	server := ca.IssueServer(t, "chronicle", "127.0.0.1")
	dir := t.TempDir()
	certPath, keyPath := server.Write(t, dir, "server")
	cfg := &config.ServerConfig{TLS: config.TLS{ServerCert: certPath, ServerKey: keyPath}}

	tc, err := ReadServerTLSConfig(cfg)
	if err != nil {
		t.Fatal(err)
	}
	if tc.ClientAuth != tls.NoClientCert {
		t.Fatalf("read endpoint must not request client certs, got ClientAuth=%v", tc.ClientAuth)
	}
}
