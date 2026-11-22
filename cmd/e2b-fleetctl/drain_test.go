package main

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"agent-runtime-lab/src/admission"
	"agent-runtime-lab/src/fleet"
	"agent-runtime-lab/src/fleetdaemon"
)

type stubTransport func(*http.Request) (*http.Response, error)

func (f stubTransport) RoundTrip(req *http.Request) (*http.Response, error) { return f(req) }

func fakeDrainClient(t *testing.T, status func() fleetdaemon.Status) *http.Client {
	t.Helper()
	return &http.Client{Transport: stubTransport(func(r *http.Request) (*http.Response, error) {
		if r.URL.Path != "/v1/status" {
			t.Fatalf("unexpected request: %s", r.URL.Path)
		}
		payload, err := json.Marshal(status())
		if err != nil {
			t.Fatal(err)
		}
		return &http.Response{StatusCode: http.StatusOK, Body: io.NopCloser(strings.NewReader(string(payload))), Header: make(http.Header)}, nil
	})}
}

func TestPendingDrainRequiresConfirmedAbsenceAndZeroReservations(t *testing.T) {
	st := fleetdaemon.Status{Fleet: fleet.Status{Admission: admission.Snapshot{
		Used: admission.Resources{CPU: 1, MemoryMiB: 256}, Pending: 1,
	}, Jobs: map[string]fleet.JobStatus{
		"unknown": {Entry: admission.Entry{State: admission.Uncertain}, Workload: fleet.Workload{
			Desired: "deleted", CreatePending: true,
		}},
	}}}
	if got := pendingDrain(st); len(got) != 2 {
		t.Fatalf("uncertain work not reported: %v", got)
	}
	st.Fleet.Jobs["unknown"] = fleet.JobStatus{Entry: admission.Entry{State: admission.Deleted}, Workload: fleet.Workload{Desired: "deleted"}}
	st.Fleet.Admission.Used = admission.Resources{}
	st.Fleet.Admission.Pending = 0
	if got := pendingDrain(st); len(got) != 0 {
		t.Fatalf("terminal work not recognized: %v", got)
	}
}

func TestWaitForDrainEventuallyConverges(t *testing.T) {
	var calls atomic.Int32
	client := fakeDrainClient(t, func() fleetdaemon.Status {
		state := admission.Running
		resources := admission.Resources{CPU: 1, MemoryMiB: 256}
		if calls.Add(1) >= 3 {
			state = admission.Deleted
			resources = admission.Resources{}
		}
		return fleetdaemon.Status{Fleet: fleet.Status{Admission: admission.Snapshot{Used: resources}, Jobs: map[string]fleet.JobStatus{
			"test1": {Entry: admission.Entry{State: state}, Workload: fleet.Workload{Desired: "deleted"}},
		}}}
	})
	if err := waitForDrain(client, 2*time.Second, 10*time.Millisecond); err != nil {
		t.Fatal(err)
	}
	if calls.Load() < 3 {
		t.Fatal("returned before delete confirmation")
	}
}

func TestWaitForDrainTimeoutReportsUnresolvedWork(t *testing.T) {
	client := fakeDrainClient(t, func() fleetdaemon.Status {
		return fleetdaemon.Status{Fleet: fleet.Status{Admission: admission.Snapshot{Used: admission.Resources{CPU: 1, MemoryMiB: 256}}, Jobs: map[string]fleet.JobStatus{
			"test1": {Entry: admission.Entry{State: admission.Uncertain}, Workload: fleet.Workload{Desired: "deleted", CreatePending: true}},
		}}}
	})
	err := waitForDrain(client, 65*time.Millisecond, 10*time.Millisecond)
	if err == nil || !strings.Contains(err.Error(), "test1") || !strings.Contains(err.Error(), "incomplete") {
		t.Fatalf("timeout failed to report unresolved VM: %v", err)
	}
}

func TestMalformedStatusCannotPassDrain(t *testing.T) {
	client := &http.Client{Transport: stubTransport(func(*http.Request) (*http.Response, error) {
		return &http.Response{StatusCode: 200, Header: make(http.Header), Body: io.NopCloser(strings.NewReader("{}"))}, nil
	})}
	if _, err := readDrainStatus(context.Background(), client); err == nil {
		t.Fatal("accepted missing fleet data as successfully drained")
	}
}
