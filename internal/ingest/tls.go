package ingest

import (
	"crypto/tls"
	"crypto/x509"
	"encoding/pem"
	"fmt"
	"math/big"
	"os"
	"sync/atomic"
	"time"

	"github.com/ncode/chronicle/internal/config"
)

// ServerTLSConfig builds the ingest listener's mTLS config: it trusts the
// facts-ca CA, requires and verifies a client certificate (ADR-0010 push-only
// machine identity), and — if a CRL is configured — rejects revoked certs at the
// TLS layer (ADR-0011, task 3.2). chronicle is not the CA; it only verifies.
func ServerTLSConfig(cfg *config.ServerConfig) (*tls.Config, *CRLChecker, error) {
	caPEM, err := os.ReadFile(cfg.TLS.CACert)
	if err != nil {
		return nil, nil, fmt.Errorf("read CA cert: %w", err)
	}
	caCerts := parseCerts(caPEM)
	if len(caCerts) == 0 {
		return nil, nil, fmt.Errorf("CA cert %s contained no certificates", cfg.TLS.CACert)
	}
	pool := x509.NewCertPool()
	for _, c := range caCerts {
		pool.AddCert(c)
	}
	cert, err := tls.LoadX509KeyPair(cfg.TLS.ServerCert, cfg.TLS.ServerKey)
	if err != nil {
		return nil, nil, fmt.Errorf("load server cert/key: %w", err)
	}

	tc := &tls.Config{
		Certificates: []tls.Certificate{cert},
		ClientCAs:    pool,
		ClientAuth:   tls.RequireAndVerifyClientCert,
		MinVersion:   tls.VersionTLS12,
	}

	var crl *CRLChecker
	if cfg.TLS.CRL != "" {
		crl, err = NewCRLChecker(cfg.TLS.CRL, caCerts)
		if err != nil {
			return nil, nil, fmt.Errorf("load CRL: %w", err)
		}
		tc.VerifyConnection = func(cs tls.ConnectionState) error {
			if len(cs.VerifiedChains) == 0 || len(cs.VerifiedChains[0]) == 0 {
				return fmt.Errorf("no verified client chain")
			}
			leaf := cs.VerifiedChains[0][0]
			if crl.Revoked(leaf.SerialNumber) {
				return fmt.Errorf("client certificate %s is revoked", leaf.SerialNumber)
			}
			return nil
		}
	}
	return tc, crl, nil
}

// CRLChecker holds the set of revoked serials, reloadable at runtime so a
// freshly-revoked cert stops connecting without a restart. The CRL is
// authenticated against the CA and its validity window checked before it is
// trusted — an unsigned, wrong-issuer, or expired CRL is rejected, not applied.
type CRLChecker struct {
	path    string
	cas     []*x509.Certificate
	revoked atomic.Pointer[map[string]struct{}] // serial.String() -> {}
}

func NewCRLChecker(path string, cas []*x509.Certificate) (*CRLChecker, error) {
	c := &CRLChecker{path: path, cas: cas}
	if err := c.Reload(); err != nil {
		return nil, err
	}
	return c, nil
}

// Reload re-reads the CRL file, verifies its signature against the CA and its
// validity window, then swaps in the revoked set. On any verification failure
// the current set is left unchanged (fail closed). Wire to a ticker/SIGHUP.
func (c *CRLChecker) Reload() error {
	raw, err := os.ReadFile(c.path)
	if err != nil {
		return err
	}
	der := raw
	if block, _ := pem.Decode(raw); block != nil {
		der = block.Bytes
	}
	crl, err := x509.ParseRevocationList(der)
	if err != nil {
		return fmt.Errorf("parse CRL %s: %w", c.path, err)
	}

	// Authenticate: the CRL must be signed by one of the trusted CA certs.
	verified := false
	for _, ca := range c.cas {
		if crl.CheckSignatureFrom(ca) == nil {
			verified = true
			break
		}
	}
	if !verified {
		return fmt.Errorf("CRL %s is not signed by the configured CA", c.path)
	}
	// Validity window: reject a future-dated or expired CRL.
	now := time.Now()
	if now.Before(crl.ThisUpdate) {
		return fmt.Errorf("CRL %s is not yet valid (thisUpdate %s)", c.path, crl.ThisUpdate)
	}
	if !crl.NextUpdate.IsZero() && now.After(crl.NextUpdate) {
		return fmt.Errorf("CRL %s expired (nextUpdate %s)", c.path, crl.NextUpdate)
	}

	set := make(map[string]struct{}, len(crl.RevokedCertificateEntries))
	for _, e := range crl.RevokedCertificateEntries {
		set[e.SerialNumber.String()] = struct{}{}
	}
	c.revoked.Store(&set)
	return nil
}

// Revoked reports whether a serial is in the current CRL.
func (c *CRLChecker) Revoked(serial *big.Int) bool {
	m := c.revoked.Load()
	if m == nil {
		return false
	}
	_, ok := (*m)[serial.String()]
	return ok
}

// parseCerts decodes all CERTIFICATE blocks from a PEM bundle.
func parseCerts(pemBytes []byte) []*x509.Certificate {
	var out []*x509.Certificate
	rest := pemBytes
	for {
		var block *pem.Block
		block, rest = pem.Decode(rest)
		if block == nil {
			break
		}
		if block.Type != "CERTIFICATE" {
			continue
		}
		if cert, err := x509.ParseCertificate(block.Bytes); err == nil {
			out = append(out, cert)
		}
	}
	return out
}
