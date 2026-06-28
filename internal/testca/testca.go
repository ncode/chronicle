// Package testca generates a throwaway CA, leaf certs, and a CRL for tests that
// need real mTLS handshakes (ingest, agent, e2e). It stands in for facts-ca:
// the consuming code only ever sees PEM files and verified chains.
package testca

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"math/big"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"
)

// CA is a self-signed test certificate authority.
type CA struct {
	Cert    *x509.Certificate
	Key     *ecdsa.PrivateKey
	CertPEM []byte
}

// Leaf is an issued certificate with its key, in several forms.
type Leaf struct {
	Cert    *x509.Certificate
	CertPEM []byte
	KeyPEM  []byte
	TLS     tls.Certificate
}

// New builds a fresh CA.
func New(t *testing.T) *CA {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	tmpl := &x509.Certificate{
		SerialNumber:          serial(t),
		Subject:               pkix.Name{CommonName: "chronicle-test-ca"},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().Add(24 * time.Hour),
		KeyUsage:              x509.KeyUsageCertSign | x509.KeyUsageCRLSign,
		BasicConstraintsValid: true,
		IsCA:                  true,
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	cert, err := x509.ParseCertificate(der)
	if err != nil {
		t.Fatal(err)
	}
	return &CA{Cert: cert, Key: key, CertPEM: pemBlock("CERTIFICATE", der)}
}

// IssueServer issues a server cert valid for the given hosts (DNS names / IPs).
func (c *CA) IssueServer(t *testing.T, cn string, hosts ...string) *Leaf {
	return c.issue(t, cn, hosts, x509.ExtKeyUsageServerAuth)
}

// IssueClient issues a client cert whose CN is the node certname.
func (c *CA) IssueClient(t *testing.T, cn string) *Leaf {
	return c.issue(t, cn, nil, x509.ExtKeyUsageClientAuth)
}

func (c *CA) issue(t *testing.T, cn string, hosts []string, eku x509.ExtKeyUsage) *Leaf {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	tmpl := &x509.Certificate{
		SerialNumber: serial(t),
		Subject:      pkix.Name{CommonName: cn},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(24 * time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature,
		ExtKeyUsage:  []x509.ExtKeyUsage{eku},
	}
	for _, h := range hosts {
		if ip := net.ParseIP(h); ip != nil {
			tmpl.IPAddresses = append(tmpl.IPAddresses, ip)
		} else {
			tmpl.DNSNames = append(tmpl.DNSNames, h)
		}
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, c.Cert, &key.PublicKey, c.Key)
	if err != nil {
		t.Fatal(err)
	}
	cert, err := x509.ParseCertificate(der)
	if err != nil {
		t.Fatal(err)
	}
	keyDER, err := x509.MarshalECPrivateKey(key)
	if err != nil {
		t.Fatal(err)
	}
	certPEM := pemBlock("CERTIFICATE", der)
	keyPEM := pemBlock("EC PRIVATE KEY", keyDER)
	tlsCert, err := tls.X509KeyPair(certPEM, keyPEM)
	if err != nil {
		t.Fatal(err)
	}
	return &Leaf{Cert: cert, CertPEM: certPEM, KeyPEM: keyPEM, TLS: tlsCert}
}

// CRLPEM produces a CRL signed by the CA revoking the given leaf certs.
func (c *CA) CRLPEM(t *testing.T, revoked ...*Leaf) []byte {
	t.Helper()
	entries := make([]x509.RevocationListEntry, 0, len(revoked))
	for _, r := range revoked {
		entries = append(entries, x509.RevocationListEntry{
			SerialNumber:   r.Cert.SerialNumber,
			RevocationTime: time.Now(),
		})
	}
	tmpl := &x509.RevocationList{
		Number:                    big.NewInt(1),
		ThisUpdate:                time.Now().Add(-time.Minute),
		NextUpdate:                time.Now().Add(time.Hour),
		RevokedCertificateEntries: entries,
	}
	der, err := x509.CreateRevocationList(rand.Reader, tmpl, c.Cert, c.Key)
	if err != nil {
		t.Fatal(err)
	}
	return pemBlock("X509 CRL", der)
}

// Pool returns a cert pool trusting this CA.
func (c *CA) Pool() *x509.CertPool {
	p := x509.NewCertPool()
	p.AppendCertsFromPEM(c.CertPEM)
	return p
}

// WriteCA writes the CA cert PEM to dir/ca.pem and returns the path.
func (c *CA) WriteCA(t *testing.T, dir string) string {
	return writeFile(t, dir, "ca.pem", c.CertPEM)
}

// Write writes the leaf cert and key to dir as <name>.crt / <name>.key and
// returns their paths.
func (l *Leaf) Write(t *testing.T, dir, name string) (certPath, keyPath string) {
	return writeFile(t, dir, name+".crt", l.CertPEM), writeFile(t, dir, name+".key", l.KeyPEM)
}

func serial(t *testing.T) *big.Int {
	t.Helper()
	n, err := rand.Int(rand.Reader, new(big.Int).Lsh(big.NewInt(1), 128))
	if err != nil {
		t.Fatal(err)
	}
	return n
}

func pemBlock(typ string, der []byte) []byte {
	return pem.EncodeToMemory(&pem.Block{Type: typ, Bytes: der})
}

func writeFile(t *testing.T, dir, name string, b []byte) string {
	t.Helper()
	p := filepath.Join(dir, name)
	if err := os.WriteFile(p, b, 0o600); err != nil {
		t.Fatal(err)
	}
	return p
}
