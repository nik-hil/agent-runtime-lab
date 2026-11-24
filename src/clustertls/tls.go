// Package clustertls provides separate cluster-API and etcd trust domains.
// Host agents/coordinators are trusted control-plane actors; workload VMs
// never receive these private keys.
package clustertls

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"errors"
	"math/big"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"time"
)

func Config(ca, cert, key string) (*tls.Config, error) {
	root, err := os.ReadFile(ca)
	if err != nil {
		return nil, err
	}
	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(root) {
		return nil, errors.New("invalid cluster trust root")
	}
	pair, err := tls.LoadX509KeyPair(cert, key)
	if err != nil {
		return nil, err
	}
	return &tls.Config{MinVersion: tls.VersionTLS13, RootCAs: pool, ClientCAs: pool, Certificates: []tls.Certificate{pair}}, nil
}

func Client(config *tls.Config) *http.Client {
	return &http.Client{Transport: &http.Transport{Proxy: nil, TLSClientConfig: config.Clone()}, Timeout: 30 * time.Second,
		CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
}

func Server(config *tls.Config, roles ...string) *tls.Config {
	c := config.Clone()
	c.ClientAuth = tls.RequireAndVerifyClientCert
	c.VerifyConnection = func(state tls.ConnectionState) error {
		if len(state.PeerCertificates) == 0 {
			return errors.New("client certificate required")
		}
		for _, role := range roles {
			if state.PeerCertificates[0].Subject.CommonName == role {
				return nil
			}
		}
		return errors.New("client certificate role is not authorized")
	}
	return c
}

func serial() *big.Int {
	n, err := rand.Int(rand.Reader, new(big.Int).Lsh(big.NewInt(1), 120))
	if err != nil {
		panic(err)
	}
	return n
}
func write(path string, data []byte) error {
	f, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0600)
	if err != nil {
		return err
	}
	if _, err = f.Write(data); err != nil {
		f.Close()
		return err
	}
	if err = f.Sync(); err != nil {
		f.Close()
		return err
	}
	return f.Close()
}

// Generate creates new development CA/certificates only; it never overwrites
// existing keys. Host names/IPs must name every endpoint being deployed.
func Generate(dir string, hosts []string) error {
	if len(hosts) == 0 || len(hosts) > 32 {
		return errors.New("1..32 certificate host names/IPs required")
	}
	if err := os.MkdirAll(dir, 0700); err != nil {
		return err
	}
	info, err := os.Lstat(dir)
	if err != nil || !info.IsDir() || info.Mode().Perm()&0077 != 0 {
		return errors.New("PKI directory must be private and nonsymlink")
	}
	for _, domain := range []string{"cluster", "etcd"} {
		key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
		if err != nil {
			return err
		}
		ca := &x509.Certificate{SerialNumber: serial(), Subject: pkix.Name{CommonName: "agent-runtime-lab-" + domain}, NotBefore: time.Now().Add(-5 * time.Minute), NotAfter: time.Now().AddDate(1, 0, 0), IsCA: true, BasicConstraintsValid: true, KeyUsage: x509.KeyUsageCertSign | x509.KeyUsageCRLSign}
		der, err := x509.CreateCertificate(rand.Reader, ca, ca, &key.PublicKey, key)
		if err != nil {
			return err
		}
		if err := write(filepath.Join(dir, domain+"-ca.pem"), pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})); err != nil {
			return err
		}
		roles := []string{"coordinator", "node", "operator"}
		if domain == "etcd" {
			roles = []string{"etcd-server", "etcd-client", "etcd-peer"}
		}
		for _, role := range roles {
			leafKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
			if err != nil {
				return err
			}
			leaf := &x509.Certificate{SerialNumber: serial(), Subject: pkix.Name{CommonName: role}, NotBefore: ca.NotBefore, NotAfter: ca.NotAfter, KeyUsage: x509.KeyUsageDigitalSignature, ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth, x509.ExtKeyUsageServerAuth}}
			for _, host := range hosts {
				if ip := net.ParseIP(host); ip != nil {
					leaf.IPAddresses = append(leaf.IPAddresses, ip)
				} else {
					leaf.DNSNames = append(leaf.DNSNames, host)
				}
			}
			cert, err := x509.CreateCertificate(rand.Reader, leaf, ca, &leafKey.PublicKey, key)
			if err != nil {
				return err
			}
			encoded, err := x509.MarshalECPrivateKey(leafKey)
			if err != nil {
				return err
			}
			if err := write(filepath.Join(dir, role+".pem"), pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: cert})); err != nil {
				return err
			}
			if err := write(filepath.Join(dir, role+".key"), pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: encoded})); err != nil {
				return err
			}
		}
	}
	f, err := os.Open(dir)
	if err != nil {
		return err
	}
	defer f.Close()
	return f.Sync()
}
