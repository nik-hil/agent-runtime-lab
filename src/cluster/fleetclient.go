package cluster

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"

	"agent-runtime-lab/src/admission"
	"agent-runtime-lab/src/fleetdaemon"
	"agent-runtime-lab/src/taskevidence"
	"agent-runtime-lab/src/taskjournal"
)

type Capabilities struct {
	Capacity admission.Resources `json:"capacity"`
	Template admission.Resources `json:"template_size"`
	Helper   string              `json:"helper_sha256"`
	Workers  int                 `json:"workers"`
}

type Fleet interface {
	Capabilities(context.Context) (Capabilities, error)
	Status(context.Context) (fleetdaemon.Status, error)
	SubmitVM(context.Context, string, string) error
	DeleteVM(context.Context, string) error
	Job(context.Context, string) (taskjournal.Entry, error)
	SubmitJob(context.Context, fleetdaemon.JobRequest) error
	Cancel(context.Context, string) error
	Recover(context.Context, string) error
	Artifact(context.Context, string, int) ([]byte, error)
}

type FleetClient struct{ client *http.Client }

func NewFleetClient(socket string) *FleetClient {
	return &FleetClient{client: &http.Client{Transport: &http.Transport{Proxy: nil, DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
		return (&net.Dialer{}).DialContext(ctx, "unix", socket)
	}}, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}}
}

func (f *FleetClient) do(ctx context.Context, method, path string, in, out any) ([]byte, error) {
	var body io.Reader
	if in != nil {
		body = bytes.NewReader(encoded(in))
	}
	req, err := http.NewRequestWithContext(ctx, method, "http://fleetd"+path, body)
	if err != nil {
		return nil, err
	}
	if in != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	resp, err := f.client.Do(req)
	if err != nil {
		return nil, errors.New("local fleet response unconfirmed")
	}
	defer resp.Body.Close()
	data, err := io.ReadAll(io.LimitReader(resp.Body, (2<<20)+1))
	if err != nil || len(data) > 2<<20 {
		return nil, errors.New("local fleet response unreadable or too large")
	}
	if resp.StatusCode == http.StatusNotFound {
		return nil, ErrNotFound
	}
	if resp.StatusCode == http.StatusConflict {
		return nil, ErrConflict
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return nil, fmt.Errorf("local fleet HTTP %d", resp.StatusCode)
	}
	if out != nil {
		if err := json.Unmarshal(data, out); err != nil {
			return nil, err
		}
	}
	if pathHasArtifact(path) {
		if taskevidence.Hash(data) != resp.Header.Get("X-Artifact-SHA256") {
			return nil, errors.New("local artifact digest mismatch")
		}
	}
	return data, nil
}

func pathHasArtifact(path string) bool {
	for i := 0; i+11 < len(path); i++ {
		if path[i:i+11] == "/artifacts/" {
			return true
		}
	}
	return false
}
func (f *FleetClient) Capabilities(ctx context.Context) (Capabilities, error) {
	var out Capabilities
	_, err := f.do(ctx, http.MethodGet, "/v1/cluster-capabilities", nil, &out)
	return out, err
}
func (f *FleetClient) Status(ctx context.Context) (fleetdaemon.Status, error) {
	var out fleetdaemon.Status
	_, err := f.do(ctx, http.MethodGet, "/v1/status", nil, &out)
	return out, err
}
func (f *FleetClient) SubmitVM(ctx context.Context, id, tenant string) error {
	_, err := f.do(ctx, http.MethodPost, "/v1/submit", map[string]string{"ID": id, "Tenant": tenant}, nil)
	return err
}
func (f *FleetClient) DeleteVM(ctx context.Context, id string) error {
	_, err := f.do(ctx, http.MethodPost, "/v1/intent", map[string]string{"ID": id, "Desired": "deleted"}, nil)
	return err
}
func (f *FleetClient) Job(ctx context.Context, id string) (taskjournal.Entry, error) {
	var out taskjournal.Entry
	_, err := f.do(ctx, http.MethodGet, "/v1/jobs/"+id, nil, &out)
	return out, err
}
func (f *FleetClient) SubmitJob(ctx context.Context, in fleetdaemon.JobRequest) error {
	_, err := f.do(ctx, http.MethodPost, "/v1/jobs", in, nil)
	return err
}
func (f *FleetClient) Cancel(ctx context.Context, id string) error {
	_, err := f.do(ctx, http.MethodPost, "/v1/jobs/"+id+"/cancel", struct{}{}, nil)
	return err
}
func (f *FleetClient) Recover(ctx context.Context, id string) error {
	_, err := f.do(ctx, http.MethodPost, "/v1/jobs/"+id+"/recover", struct{}{}, nil)
	return err
}
func (f *FleetClient) Artifact(ctx context.Context, id string, index int) ([]byte, error) {
	return f.do(ctx, http.MethodGet, fmt.Sprintf("/v1/jobs/%s/artifacts/%d", id, index), nil, nil)
}
