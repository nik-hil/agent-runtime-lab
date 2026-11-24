package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"agent-runtime-lab/src/admission"
	"agent-runtime-lab/src/cluster"
	"agent-runtime-lab/src/clustertls"
)

type paths []string

func (p *paths) String() string     { return strings.Join(*p, ",") }
func (p *paths) Set(v string) error { *p = append(*p, v); return nil }
func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, "ERROR:", err)
		os.Exit(1)
	}
}
func run() error {
	f := flag.NewFlagSet("lab-clusterctl", flag.ContinueOnError)
	servers := f.String("servers", "https://127.0.0.1:7443", "comma-separated coordinator TLS origins; mutations use first only")
	pki := f.String("pki", ".state/m5-pki", "operator TLS directory")
	id := f.String("id", "", "job ID, or node ID for node-drain")
	tenant := f.String("tenant", "alpha", "tenant")
	command := f.String("command", "", "shell command")
	timeout := f.Int("timeout", 30, "command timeout seconds (1..60)")
	cpu := f.Int64("cpu", 2, "VM CPU shape")
	memory := f.Int64("memory-mib", 2048, "VM memory shape")
	labels := f.String("labels", "", "required node labels, comma-separated key=value")
	index := f.Int("index", 0, "artifact index")
	output := f.String("output", "", "new local artifact file")
	wait := f.Duration("wait-timeout", 5*time.Minute, "maximum wait duration")
	execute := f.Bool("execute", false, "required for mutations")
	var artifacts paths
	f.Var(&artifacts, "artifact", "absolute artifact path (repeatable)")
	if err := f.Parse(os.Args[1:]); err != nil {
		return err
	}
	if f.NArg() != 1 {
		return errors.New("command: submit|status|list|cluster-status|wait|cancel|recover|result|download|node-drain")
	}
	tlsConfig, err := clustertls.Config(filepath.Join(*pki, "cluster-ca.pem"), filepath.Join(*pki, "operator.pem"), filepath.Join(*pki, "operator.key"))
	if err != nil {
		return err
	}
	c := cluster.Client{HTTP: clustertls.ClientRole(tlsConfig, "coordinator"), Servers: strings.Split(*servers, ",")}
	op := f.Arg(0)
	method, path := http.MethodGet, "/v1/jobs/"+*id
	var input any
	if op != "list" && op != "cluster-status" && op != "drain" && !cluster.SafeID(*id) {
		return errors.New("safe --id required")
	}
	switch op {
	case "submit":
		l, err := cluster.ParseLabels(*labels)
		if err != nil {
			return err
		}
		method, path, input = http.MethodPost, "/v1/jobs", cluster.Request{ID: *id, Tenant: *tenant, Command: *command, Timeout: *timeout, Artifacts: artifacts, Resources: admission.Resources{CPU: *cpu, MemoryMiB: *memory}, Labels: l}
	case "status", "wait":
	case "list":
		path = "/v1/jobs"
	case "cluster-status":
		path = "/v1/status"
	case "drain":
		method, path, input = http.MethodPost, "/v1/drain", struct{}{}
	case "cancel", "recover":
		method, path, input = http.MethodPost, path+"/"+op, struct{}{}
	case "node-drain":
		method, path, input = http.MethodPost, "/v1/nodes/"+*id+"/drain", struct{}{}
	case "result":
		path += "/result"
	case "download":
		if *index < 0 || *output == "" {
			return errors.New("--index >=0 and --output required")
		}
		path += "/artifacts/" + strconv.Itoa(*index)
	default:
		return errors.New("unknown cluster command")
	}
	if method != http.MethodGet && !*execute {
		return errors.New("--execute required")
	}
	if *wait <= 0 || *wait > 24*time.Hour {
		return errors.New("wait-timeout must be positive and at most 24h")
	}
	ctx, cancel := context.WithTimeout(context.Background(), *wait)
	defer cancel()
	if op == "wait" {
		for {
			job, err := c.Job(ctx, *id)
			if err != nil {
				return err
			}
			switch job.State {
			case "succeeded", "failed", "cancelled", "uncertain":
				data, _ := json.MarshalIndent(job, "", "  ")
				fmt.Println(string(data))
				if job.State != "succeeded" {
					return fmt.Errorf("job state %s; no automatic execution retry", job.State)
				}
				return nil
			}
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(time.Second):
			}
		}
	}
	data, headers, err := c.Request(ctx, method, path, input)
	if err != nil {
		return err
	}
	if op == "download" {
		if err := cluster.SaveDownload(*output, data, headers.Get("X-Artifact-SHA256")); err != nil {
			return err
		}
		fmt.Println("Saved verified artifact:", *output)
		return nil
	}
	var value any
	if json.Unmarshal(data, &value) != nil {
		return errors.New("invalid JSON response")
	}
	pretty, _ := json.MarshalIndent(value, "", "  ")
	fmt.Println(string(pretty))
	return nil
}
