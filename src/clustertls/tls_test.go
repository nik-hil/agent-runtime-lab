package clustertls

import (
	"crypto/tls"
	"crypto/x509"
	"path/filepath"
	"testing"
)

func TestSeparateTrustDomainsAndRoles(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "pki")
	if err := Generate(dir, []string{"127.0.0.1"}); err != nil {
		t.Fatal(err)
	}
	load := func(role, domain string) *tls.Config {
		c, err := Config(filepath.Join(dir, domain+"-ca.pem"), filepath.Join(dir, role+".pem"), filepath.Join(dir, role+".key"))
		if err != nil {
			t.Fatal(err)
		}
		return c
	}
	api := load("coordinator", "cluster")
	node := load("node", "cluster")
	operator := load("operator", "cluster")
	etcd := load("etcd-client", "etcd")
	cert := func(c *tls.Config) *x509.Certificate {
		v, err := x509.ParseCertificate(c.Certificates[0].Certificate[0])
		if err != nil {
			t.Fatal(err)
		}
		return v
	}
	server := Server(node, "coordinator")
	if err := server.VerifyConnection(tls.ConnectionState{PeerCertificates: []*x509.Certificate{cert(api)}}); err != nil {
		t.Fatal(err)
	}
	if err := server.VerifyConnection(tls.ConnectionState{PeerCertificates: []*x509.Certificate{cert(operator)}}); err == nil {
		t.Fatal("operator authorized node mutation")
	}
	if _, err := cert(etcd).Verify(x509.VerifyOptions{Roots: api.RootCAs, KeyUsages: []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth}}); err == nil {
		t.Fatal("etcd certificate trusted for cluster API")
	}
	if server.ClientAuth != tls.RequireAndVerifyClientCert || server.MinVersion < tls.VersionTLS13 {
		t.Fatal("mutual TLS not enforced")
	}
	if err := Generate(dir, []string{"127.0.0.1"}); err == nil {
		t.Fatal("existing PKI overwritten")
	}
}
