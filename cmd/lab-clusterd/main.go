package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"syscall"

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
	f := flag.NewFlagSet("lab-clusterd", flag.ContinueOnError)
	cfg := cluster.Flags(f, "coordinator")
	replicas := f.Int("replicas", 2, "independent artifact copies required before completion (1..3)")
	if err := f.Parse(os.Args[1:]); err != nil {
		return err
	}
	if f.NArg() != 0 {
		return fmt.Errorf("no positional arguments accepted")
	}
	repo, tlsConfig, client, err := cfg.Open("coordinator")
	if err != nil {
		return err
	}
	config := cluster.DefaultConfig(cfg.Cluster)
	config.Replicas = *replicas
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if err := repo.Bootstrap(ctx, config); err != nil {
		return err
	}
	c := &cluster.Coordinator{Repo: repo, ID: cfg.ID, URL: cfg.Advertise, TTL: cfg.TTL, Client: client}
	fmt.Printf("Cluster coordinator running; id=%s cluster=%s listen=%s\n", cfg.ID, cfg.Cluster, cfg.Listen)
	return cluster.Serve(ctx, cfg.Listen, clustertls.Server(tlsConfig, "operator", "coordinator"), c.Handler(), c.Run)
}
