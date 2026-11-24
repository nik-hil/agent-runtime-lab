package main

import (
	"bytes"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"agent-runtime-lab/src/taskevidence"
)

func response(code int, body string) *http.Response {
	return &http.Response{StatusCode: code, Body: io.NopCloser(strings.NewReader(body)), Header: make(http.Header)}
}

func TestSubmitReturnsAcceptanceAndNeverRetries(t *testing.T) {
	calls := 0
	var out bytes.Buffer
	err := run([]string{"--execute", "--job-id", "one", "--id", "work", "--command", "echo marker", "submit"}, &out, func(r *http.Request) (*http.Response, error) {
		calls++
		if r.Method != http.MethodPost || r.URL.Path != "/v1/jobs" {
			t.Fatal("wrong submission route")
		}
		return response(http.StatusAccepted, `{"id":"one","state":"queued"}`), nil
	})
	if err != nil || calls != 1 || !strings.Contains(out.String(), "queued") {
		t.Fatal("queued acceptance was not successful", calls, err)
	}
}

func TestWaitOnlyPollsAndReportsUncertainty(t *testing.T) {
	for _, state := range []string{"succeeded", "failed", "uncertain", "cancelled"} {
		t.Run(state, func(t *testing.T) {
			calls := 0
			var out bytes.Buffer
			err := run([]string{"--job-id", "one", "wait"}, &out, func(r *http.Request) (*http.Response, error) {
				calls++
				if r.Method != http.MethodGet {
					t.Fatal("wait attempted a mutation")
				}
				if calls == 1 {
					return response(http.StatusOK, `{"id":"one","state":"queued"}`), nil
				}
				return response(http.StatusOK, `{"id":"one","state":"`+state+`"}`), nil
			})
			if (err == nil) != (state == "succeeded") || calls != 2 {
				t.Fatal("wait misreported terminal result", calls, err)
			}
		})
	}
}

func TestDownloadVerifiesBytesAndNeverOverwrites(t *testing.T) {
	destination := filepath.Join(t.TempDir(), "artifact.bin")
	args := []string{"--job-id", "one", "--index", "0", "--out", destination, "download"}
	send := func(r *http.Request) (*http.Response, error) {
		if r.URL.Path != "/v1/jobs/one/artifacts/0" {
			t.Fatal("wrong artifact route")
		}
		res := response(http.StatusOK, "payload")
		res.Header.Set("X-Artifact-SHA256", taskevidence.Hash([]byte("payload")))
		return res, nil
	}
	var out bytes.Buffer
	if err := run(args, &out, send); err != nil {
		t.Fatal(err)
	}
	info, err := os.Stat(destination)
	if err != nil || info.Mode().Perm() != 0600 {
		t.Fatal("download file is not private", err)
	}
	if err := run(args, &out, send); err == nil {
		t.Fatal("existing destination was overwritten")
	}
	data, _ := os.ReadFile(destination)
	if string(data) != "payload" {
		t.Fatal("existing artifact was changed")
	}
	bad := filepath.Join(t.TempDir(), "bad.bin")
	args[5] = bad
	if err := run(args, &out, func(*http.Request) (*http.Response, error) {
		res := response(http.StatusOK, "changed")
		res.Header.Set("X-Artifact-SHA256", taskevidence.Hash([]byte("original")))
		return res, nil
	}); err == nil {
		t.Fatal("invalid digest was accepted")
	}
	if _, err := os.Stat(bad); !os.IsNotExist(err) {
		t.Fatal("invalid download created output file")
	}
}

func TestMutationsRequireExecuteAndInputsAreBounded(t *testing.T) {
	for _, args := range [][]string{
		{"--job-id", "one", "cancel"}, {"--job-id", "one", "recover"},
		{"--execute", "--job-id", "../bad", "--id", "work", "--command", "id", "submit"},
		{"--job-id", "one", "--index", "0", "download"},
	} {
		if err := run(args, io.Discard, func(*http.Request) (*http.Response, error) {
			t.Fatal("invalid request reached daemon")
			return nil, nil
		}); err == nil {
			t.Fatal("invalid CLI invocation accepted", args)
		}
	}
}
