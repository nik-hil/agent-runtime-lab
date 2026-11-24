package tests

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"agent-runtime-lab/src/admission"
	"agent-runtime-lab/src/cluster"
	"agent-runtime-lab/src/clustertls"
	"agent-runtime-lab/src/coordkv"
)

const etcdImage = "gcr.io/etcd-development/etcd:v3.7.4"

type m5Quorum struct {
	dir, pki, endpoints string
	names               []string
	kv                  *coordkv.Client
	repo                *cluster.Repository
}

func availablePort(t *testing.T) string {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	address := l.Addr().String()
	l.Close()
	return address
}
func dockerM5(t *testing.T, args ...string) []byte {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	data, err := exec.CommandContext(ctx, "docker", args...).CombinedOutput()
	if err != nil {
		t.Fatalf("Docker %s failed: %v: %s", args[0], err, data)
	}
	return data
}
func eventuallyM5(t *testing.T, label string, check func() bool) {
	t.Helper()
	deadline := time.Now().Add(90 * time.Second)
	for {
		if check() {
			return
		}
		if time.Now().After(deadline) {
			t.Fatal("timed out:", label)
		}
		time.Sleep(200 * time.Millisecond)
	}
}

func newM5Quorum(t *testing.T) *m5Quorum {
	t.Helper()
	root, err := filepath.Abs("..")
	if err != nil {
		t.Fatal(err)
	}
	if err = os.MkdirAll(filepath.Join(root, ".state"), 0700); err != nil {
		t.Fatal(err)
	}
	dir, err := os.MkdirTemp(filepath.Join(root, ".state"), "m5-")
	if err != nil {
		t.Fatal(err)
	}
	q := &m5Quorum{dir: dir, pki: filepath.Join(dir, "pki")}
	t.Log("M5 retained verification files:", dir)
	if err = clustertls.Generate(q.pki, []string{"127.0.0.1", "localhost"}); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		for i, name := range q.names {
			ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
			exec.CommandContext(ctx, "docker", "unpause", name).Run()
			data, _ := exec.CommandContext(ctx, "docker", "logs", name).CombinedOutput()
			os.WriteFile(filepath.Join(dir, fmt.Sprintf("etcd-%d.log", i)), data, 0600)
			if data, err := exec.CommandContext(ctx, "docker", "rm", "-f", name).CombinedOutput(); err != nil {
				t.Errorf("remove selected etcd container: %v: %s", err, data)
			}
			cancel()
		}
	})
	dockerM5(t, "info", "--format", "{{.ServerVersion}}")
	dockerM5(t, "pull", etcdImage)
	var clients, peers, initial []string
	for i := 0; i < 3; i++ {
		clients = append(clients, "https://"+availablePort(t))
		peers = append(peers, "https://"+availablePort(t))
		initial = append(initial, fmt.Sprintf("member-%d=%s", i, peers[i]))
	}
	q.endpoints = strings.Join(clients, ",")
	token := cluster.Token()[:16]
	for i := 0; i < 3; i++ {
		dataDir := filepath.Join(dir, fmt.Sprintf("etcd-%d", i))
		if err = os.Mkdir(dataDir, 0700); err != nil {
			t.Fatal(err)
		}
		name := fmt.Sprintf("agentlab-m5-%s-%d", token, i)
		q.names = append(q.names, name)
		dockerM5(t, "run", "-d", "--name", name, "--network", "host", "--user", strconv.Itoa(os.Getuid())+":"+strconv.Itoa(os.Getgid()), "--cap-drop", "ALL", "--security-opt", "no-new-privileges", "--mount", "type=bind,src="+q.pki+",dst=/pki,readonly", "--mount", "type=bind,src="+dataDir+",dst=/data", etcdImage, "/usr/local/bin/etcd",
			"--name", fmt.Sprintf("member-%d", i), "--data-dir", "/data", "--listen-client-urls", clients[i], "--advertise-client-urls", clients[i], "--listen-peer-urls", peers[i], "--initial-advertise-peer-urls", peers[i], "--initial-cluster", strings.Join(initial, ","), "--initial-cluster-token", token, "--initial-cluster-state", "new",
			"--cert-file", "/pki/etcd-server.pem", "--key-file", "/pki/etcd-server.key", "--trusted-ca-file", "/pki/etcd-ca.pem", "--client-cert-auth", "--peer-cert-file", "/pki/etcd-peer.pem", "--peer-key-file", "/pki/etcd-peer.key", "--peer-trusted-ca-file", "/pki/etcd-ca.pem", "--peer-client-cert-auth")
	}
	tlsConfig, err := clustertls.Config(filepath.Join(q.pki, "etcd-ca.pem"), filepath.Join(q.pki, "etcd-client.pem"), filepath.Join(q.pki, "etcd-client.key"))
	if err != nil {
		t.Fatal(err)
	}
	q.kv, err = coordkv.New(clients, &http.Transport{TLSClientConfig: tlsConfig})
	if err != nil {
		t.Fatal(err)
	}
	q.repo, _ = cluster.NewRepository(q.kv, "m5-verify")
	eventuallyM5(t, "three-member TLS etcd quorum", func() bool {
		ctx, stop := context.WithTimeout(context.Background(), 10*time.Second)
		defer stop()
		_, err := q.kv.Range(ctx, q.repo.Prefix)
		return err == nil
	})
	return q
}

// This gate uses real etcd quorum and TLS, but makes no live VM claim.
func TestM5Quorum(t *testing.T) {
	if os.Getenv("LAB_CLUSTER_INTEGRATION") != "1" {
		t.Skip("set LAB_CLUSTER_INTEGRATION=1 for real three-member etcd gate")
	}
	q := newM5Quorum(t)
	ctx := context.Background()
	if err := q.repo.Bootstrap(ctx, cluster.DefaultConfig("m5-verify")); err != nil {
		t.Fatal(err)
	}
	l, _, won, err := q.repo.Campaign(ctx, "coord-a", "https://127.0.0.1:7443", 3)
	if err != nil || !won {
		t.Fatal(err)
	}
	shape := admission.Resources{CPU: 2, MemoryMiB: 2048}
	n := cluster.Node{ID: "node-a", Incarnation: cluster.Token(), URL: "https://127.0.0.1:8443", Capacity: shape, Template: shape}
	live := cluster.Live{Node: n.ID, Incarnation: n.Incarnation, Token: cluster.Token()}
	lease, err := q.kv.Grant(ctx, 120)
	if err != nil {
		t.Fatal(err)
	}
	if err = q.repo.Register(ctx, n, live, lease); err != nil {
		t.Fatal(err)
	}
	if _, _, err = q.repo.Submit(ctx, cluster.Request{ID: "owned", Tenant: "alpha", Command: "true", Timeout: 30, Resources: shape}); err != nil {
		t.Fatal(err)
	}
	if ok, err := q.repo.Schedule(ctx, l); err != nil || !ok {
		t.Fatal(err)
	}
	s, err := q.repo.Read(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if ok, err := q.repo.Claim(ctx, "owned", s.Jobs["owned"].Epoch, live); err != nil || !ok {
		t.Fatal(err)
	}
	var next cluster.Leader
	eventuallyM5(t, "server lease expiration and coordinator election", func() bool {
		var ok bool
		next, _, ok, err = q.repo.Campaign(ctx, "coord-b", "https://127.0.0.1:7444", 120)
		return err == nil && ok
	})
	if ok, err := q.repo.Schedule(ctx, l); err == nil || ok {
		t.Fatal("expired leader was allowed to mutate")
	}
	dockerM5(t, "pause", q.names[0])
	eventuallyM5(t, "quorum survives one member loss", func() bool {
		step, stop := context.WithTimeout(ctx, 12*time.Second)
		defer stop()
		_, err = q.repo.Read(step)
		return err == nil
	})
	dockerM5(t, "pause", q.names[1])
	step, stop := context.WithTimeout(ctx, 2*time.Second)
	_, err = q.repo.Schedule(step, next)
	stop()
	if err == nil {
		t.Fatal("mutation accepted without majority")
	}
	dockerM5(t, "unpause", q.names[0])
	dockerM5(t, "unpause", q.names[1])
	eventuallyM5(t, "quorum resumes without ownership replay", func() bool {
		step, stop := context.WithTimeout(ctx, 12*time.Second)
		defer stop()
		s, err := q.repo.Read(step)
		return err == nil && s.Jobs["owned"].State == "dispatched" && s.Jobs["owned"].Node == n.ID
	})
}
