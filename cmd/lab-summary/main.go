// lab-summary computes nearest-rank percentiles from successful raw samples.
package main

import (
	"encoding/json"
	"fmt"
	"math"
	"os"
	"sort"

	"agent-runtime-lab/src/sandbox"
)

type sample struct {
	Iteration int            `json:"iteration"`
	Warmup    bool           `json:"warmup"`
	Result    sandbox.Result `json:"result"`
	Error     string         `json:"error"`
}

func main() {
	if len(os.Args) != 2 {
		fail("usage: lab-summary samples.json")
	}
	data, err := os.ReadFile(os.Args[1])
	if err != nil {
		fail(err.Error())
	}
	var samples []sample
	if err := json.Unmarshal(data, &samples); err != nil {
		fail(err.Error())
	}
	byBackend := map[string][]sandbox.Result{}
	warmups := map[string]int{}
	for _, s := range samples {
		if s.Error != "" || s.Result.ExitCode == nil || *s.Result.ExitCode != 0 || s.Result.TimedOut || s.Result.Cancelled {
			fail("refusing to summarize unsuccessful samples")
		}
		if s.Warmup {
			warmups[s.Result.Backend]++
			continue
		}
		byBackend[s.Result.Backend] = append(byBackend[s.Result.Backend], s.Result)
	}
	if len(byBackend) == 0 {
		fail("no measured samples")
	}
	fmt.Println("| Backend | Metric (ms) | n | Mean | p50 | p95 | Min | Max |")
	fmt.Println("| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: |")
	keys := []string{}
	for key := range byBackend {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	metrics := []struct {
		name string
		get  func(sandbox.Result) *float64
	}{
		{"create", func(r sandbox.Result) *float64 { return &r.CreateMS }},
		{"attach-to-exit", func(r sandbox.Result) *float64 { return &r.ExecMS }},
		{"workload", func(r sandbox.Result) *float64 { return r.WorkloadMS }},
		{"cleanup", func(r sandbox.Result) *float64 { return &r.CleanupMS }},
		{"total", func(r sandbox.Result) *float64 { return &r.TotalMS }},
	}
	for _, backend := range keys {
		for _, metric := range metrics {
			values := []float64{}
			for _, r := range byBackend[backend] {
				if value := metric.get(r); value != nil {
					values = append(values, *value)
				}
			}
			if len(values) == 0 {
				continue
			}
			sort.Float64s(values)
			sum := 0.0
			for _, v := range values {
				sum += v
			}
			percentile := func(p float64) float64 {
				return values[int(math.Ceil(p*float64(len(values))))-1]
			}
			fmt.Printf("| %s | %s | %d | %.3f | %.3f | %.3f | %.3f | %.3f |\n", backend, metric.name, len(values), sum/float64(len(values)), percentile(.5), percentile(.95), values[0], values[len(values)-1])
		}
	}
	fmt.Println()
	for _, backend := range keys {
		fmt.Printf("Excluded %d warmups for %s.\n", warmups[backend], backend)
	}
	fmt.Println("Percentiles use nearest rank. Workload timings come from daemon timestamps; attach and total include orchestration overhead. These are local, warm-image no-op measurements.")
}

func fail(message string) {
	fmt.Fprintln(os.Stderr, message)
	os.Exit(1)
}
