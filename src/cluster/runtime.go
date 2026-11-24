package cluster

import (
	"context"
	"crypto/tls"
	"errors"
	"flag"
	"net"
	"net/http"
	"path/filepath"
	"strings"
	"time"

	"agent-runtime-lab/src/clustertls"
	"agent-runtime-lab/src/coordkv"
)

type RuntimeFlags struct {
	Cluster, ID, Listen, Advertise, Etcd, PKI string
	TTL                                       int64
	Execute                                   bool
}

func Flags(f *flag.FlagSet, role string) *RuntimeFlags {
	c := &RuntimeFlags{}
	f.StringVar(&c.Cluster, "cluster", "m5-lab", "cluster namespace")
	f.StringVar(&c.ID, "id", role+"-a", "stable instance ID")
	f.StringVar(&c.Listen, "listen", "127.0.0.1:7443", "TLS listen address (private network or loopback)")
	f.StringVar(&c.Advertise, "advertise", "https://127.0.0.1:7443", "TLS address reachable by peer nodes")
	f.StringVar(&c.Etcd, "etcd", "https://127.0.0.1:2379", "comma-separated quorum client origins")
	f.StringVar(&c.PKI, "pki", ".state/m5-pki", "private certificate directory")
	f.Int64Var(&c.TTL, "lease-ttl", 15, "server-managed leader/node lease TTL seconds (3..120)")
	f.BoolVar(&c.Execute, "execute", false, "required to enable cluster operations")
	return c
}

func (c RuntimeFlags) Open(role string) (*Repository, *tls.Config, *http.Client, error) {
	if !c.Execute || !SafeID(c.ID) || !SafeID(c.Cluster) || !ValidURL(c.Advertise) || c.TTL < 3 || c.TTL > 120 {
		return nil, nil, nil, errors.New("--execute, safe IDs, TLS advertise URL and lease TTL 3..120 required")
	}
	api, err := clustertls.Config(filepath.Join(c.PKI, "cluster-ca.pem"), filepath.Join(c.PKI, role+".pem"), filepath.Join(c.PKI, role+".key"))
	if err != nil {
		return nil, nil, nil, err
	}
	kvTLS, err := clustertls.Config(filepath.Join(c.PKI, "etcd-ca.pem"), filepath.Join(c.PKI, "etcd-client.pem"), filepath.Join(c.PKI, "etcd-client.key"))
	if err != nil {
		return nil, nil, nil, err
	}
	kv, err := coordkv.New(strings.Split(c.Etcd, ","), &http.Transport{Proxy: nil, TLSClientConfig: kvTLS})
	if err != nil {
		return nil, nil, nil, err
	}
	repo, err := NewRepository(kv, c.Cluster)
	if err != nil {
		return nil, nil, nil, err
	}
	return repo, api, clustertls.Client(api), nil
}

// Serve joins the worker before returning so its journals remain open until
// all in-flight local intent updates have stopped. It never deletes VMs.
func Serve(ctx context.Context, address string, tlsConfig *tls.Config, handler http.Handler, run func(context.Context) error) error {
	listener, err := net.Listen("tcp", address)
	if err != nil {
		return err
	}
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	server := &http.Server{Handler: handler, TLSConfig: tlsConfig, ReadHeaderTimeout: 5 * time.Second, ReadTimeout: 40 * time.Second, WriteTimeout: 40 * time.Second}
	serveErr := make(chan error, 1)
	go func() { serveErr <- server.ServeTLS(listener, "", "") }()
	workerErr := make(chan error, 1)
	go func() { workerErr <- run(ctx) }()
	workerStopped := false
	select {
	case <-ctx.Done():
	case err = <-workerErr:
		workerStopped = true
	case err = <-serveErr:
		if errors.Is(err, http.ErrServerClosed) {
			err = nil
		}
	}
	cancel()
	end, stop := context.WithTimeout(context.Background(), 10*time.Second)
	defer stop()
	if closeErr := server.Shutdown(end); closeErr != nil {
		_ = server.Close()
		if err == nil {
			err = closeErr
		}
	}
	if !workerStopped {
		if worker := <-workerErr; worker != nil && err == nil {
			err = worker
		}
	}
	return err
}
