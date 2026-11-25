package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"agent-runtime-lab/src/artifactstore"
	"agent-runtime-lab/src/cluster"
	"agent-runtime-lab/src/clustertls"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, "ERROR:", err)
		os.Exit(1)
	}
}
func run() error {
	f := flag.NewFlagSet("lab-node-agent", flag.ContinueOnError)
	cfg := cluster.Flags(f, "node")
	statePath := f.String("state-file", ".state/m5-node.json", "private durable node incarnation and execution intent file")
	socket := f.String("fleet-socket", ".state/m5-fleet.sock", "dedicated local M4.4 fleetd socket")
	archive := f.String("artifact-dir", "", "private replica store (default: <state-file>.artifacts)")
	quota := f.Int64("artifact-max-mib", 128, "replica storage quota MiB (1..1024)")
	hold := f.String("test-hold-claim-job", "", "TEST ONLY: remain healthy but do not claim this assigned job")
	labelText := f.String("labels", "", "placement labels, comma-separated key=value")
	if err := f.Parse(os.Args[1:]); err != nil {
		return err
	}
	if f.NArg() != 0 {
		return fmt.Errorf("no positional arguments accepted")
	}
	labels, err := cluster.ParseLabels(*labelText)
	if err != nil {
		return err
	}
	repo, tlsConfig, _, err := cfg.Open("node")
	if err != nil {
		return err
	}
	state, err := cluster.OpenLocal(*statePath, cfg.Cluster, cfg.ID)
	if err != nil {
		return err
	}
	defer state.Close()
	if *archive == "" {
		*archive = *statePath + ".artifacts"
	}
	storage, err := artifactstore.Open(*archive, *quota<<20)
	if err != nil {
		return err
	}
	defer storage.Close()
	a := &cluster.Agent{Repo: repo, State: state, Fleet: cluster.NewFleetClient(*socket), Archive: storage, Node: cluster.Node{ID: cfg.ID, Incarnation: state.Incarnation(), URL: cfg.Advertise, Labels: labels}, TTL: cfg.TTL, HoldClaim: *hold}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	fmt.Printf("Node agent running; id=%s cluster=%s listen=%s fleet_socket=%s\n", cfg.ID, cfg.Cluster, cfg.Listen, *socket)
	return cluster.Serve(ctx, cfg.Listen, clustertls.Server(tlsConfig, "coordinator"), a.Handler(), a.Run)
}
