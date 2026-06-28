package ingest

import (
	"crypto/tls"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"

	"github.com/ncode/chronicle/internal/config"
	"github.com/ncode/chronicle/internal/testca"
)

// TestServerTLSConfigCNAndCRL exercises the real mTLS plumbing: the server
// requires+verifies a facts-ca-style client cert, identity is the chain CN, and
// a revoked cert is rejected at the TLS layer (tasks 3.1, 3.2).
func TestServerTLSConfigCNAndCRL(t *testing.T) {
	ca := testca.New(t)
	server := ca.IssueServer(t, "chronicle", "127.0.0.1")
	good := ca.IssueClient(t, "web01.example.com")
	revoked := ca.IssueClient(t, "evil.example.com")

	dir := t.TempDir()
	caPath := ca.WriteCA(t, dir)
	srvCert, srvKey := server.Write(t, dir, "server")
	crlPath := writeTemp(t, dir, "ca_crl.pem", ca.CRLPEM(t, revoked))

	cfg := &config.ServerConfig{TLS: config.TLS{
		CACert: caPath, ServerCert: srvCert, ServerKey: srvKey, CRL: crlPath,
	}}
	tc, crl, err := ServerTLSConfig(cfg)
	if err != nil {
		t.Fatalf("ServerTLSConfig: %v", err)
	}
	if crl == nil {
		t.Fatal("expected a CRL checker")
	}

	srv := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		cn, err := certnameFromChain(r)
		if err != nil {
			http.Error(w, err.Error(), http.StatusForbidden)
			return
		}
		_, _ = io.WriteString(w, cn)
	}))
	srv.TLS = tc
	srv.StartTLS()
	defer srv.Close()

	// Good client: CN echoed back.
	if body, err := mtlsGet(srv.URL, ca, &good.TLS); err != nil {
		t.Fatalf("good client: %v", err)
	} else if body != "web01.example.com" {
		t.Fatalf("CN = %q, want web01.example.com", body)
	}

	// Revoked client: rejected at TLS (no HTTP response).
	if _, err := mtlsGet(srv.URL, ca, &revoked.TLS); err == nil {
		t.Fatal("revoked client must be rejected at TLS")
	}

	// No client cert: rejected (RequireAndVerifyClientCert).
	if _, err := mtlsGet(srv.URL, ca, nil); err == nil {
		t.Fatal("no-cert client must be rejected at TLS")
	}
}

// A CRL signed by a different CA must be rejected, not trusted.
func TestCRLWrongIssuerRejected(t *testing.T) {
	ca := testca.New(t)
	other := testca.New(t)
	server := ca.IssueServer(t, "chronicle", "127.0.0.1")
	victim := ca.IssueClient(t, "victim.example.com")

	dir := t.TempDir()
	caPath := ca.WriteCA(t, dir)
	srvCert, srvKey := server.Write(t, dir, "server")
	// CRL signed by `other`, not by the trusted `ca`.
	crlPath := writeTemp(t, dir, "bad_crl.pem", other.CRLPEM(t, victim))

	cfg := &config.ServerConfig{TLS: config.TLS{
		CACert: caPath, ServerCert: srvCert, ServerKey: srvKey, CRL: crlPath,
	}}
	if _, _, err := ServerTLSConfig(cfg); err == nil {
		t.Fatal("a CRL not signed by the configured CA must be rejected")
	}
}

func mtlsGet(url string, ca *testca.CA, clientCert *tls.Certificate) (string, error) {
	tc := &tls.Config{RootCAs: ca.Pool(), ServerName: "127.0.0.1"}
	if clientCert != nil {
		tc.Certificates = []tls.Certificate{*clientCert}
	}
	c := &http.Client{Transport: &http.Transport{TLSClientConfig: tc}}
	resp, err := c.Get(url)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != http.StatusOK {
		return "", &httpStatusError{resp.StatusCode, string(b)}
	}
	return string(b), nil
}

type httpStatusError struct {
	code int
	body string
}

func (e *httpStatusError) Error() string { return e.body }

func writeTemp(t *testing.T, dir, name string, b []byte) string {
	t.Helper()
	p := filepath.Join(dir, name)
	if err := os.WriteFile(p, b, 0o600); err != nil {
		t.Fatal(err)
	}
	return p
}
