package cluster

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"

	"agent-runtime-lab/src/taskevidence"
)

// Operator requests are sent exactly once. A failed submit is inspected by ID;
// coordinator fallback is restricted to read-only requests.
type Client struct {
	HTTP    *http.Client
	Servers []string
}

func (c Client) Request(ctx context.Context, method, path string, input any) ([]byte, http.Header, error) {
	if c.HTTP == nil || len(c.Servers) == 0 {
		return nil, nil, errors.New("TLS client and coordinator URLs required")
	}
	for _, server := range c.Servers {
		if !ValidURL(server) {
			return nil, nil, errors.New("invalid TLS coordinator URL")
		}
	}
	var last error
	for _, server := range c.Servers {
		var body io.Reader
		if input != nil {
			body = bytes.NewReader(encoded(input))
		}
		req, err := http.NewRequestWithContext(ctx, method, server+path, body)
		if err != nil {
			return nil, nil, err
		}
		req.Header.Set("Content-Type", "application/json")
		response, err := c.HTTP.Do(req)
		if err == nil {
			data, readErr := io.ReadAll(io.LimitReader(response.Body, MaxTransfer+1))
			response.Body.Close()
			if readErr == nil && len(data) <= MaxTransfer && response.StatusCode >= 200 && response.StatusCode < 300 {
				return data, response.Header, nil
			}
			err = fmt.Errorf("cluster HTTP %d; inspect status by job ID before changing intent", response.StatusCode)
		}
		last = err
		if method != http.MethodGet {
			return nil, nil, fmt.Errorf("request unconfirmed or rejected: %w; never blindly replay side effects", err)
		}
	}
	return nil, nil, last
}

func (c Client) Job(ctx context.Context, id string) (Job, error) {
	if !SafeID(id) {
		return Job{}, ErrConflict
	}
	data, _, err := c.Request(ctx, http.MethodGet, "/v1/jobs/"+id, nil)
	var job Job
	if err == nil {
		err = json.Unmarshal(data, &job)
	}
	return job, err
}

// SaveDownload verifies the wire digest and publishes without replacing an
// existing file. A completed download is fsynced before publication.
func SaveDownload(path string, data []byte, digest string) error {
	if !taskevidence.IsHash(digest) || taskevidence.Hash(data) != digest {
		return errors.New("artifact download checksum mismatch")
	}
	abs, err := filepath.Abs(path)
	if err != nil {
		return err
	}
	f, err := os.CreateTemp(filepath.Dir(abs), ".cluster-download-*")
	if err != nil {
		return err
	}
	defer os.Remove(f.Name())
	defer f.Close()
	if _, err = f.Write(data); err != nil {
		return err
	}
	if err = f.Sync(); err != nil {
		return err
	}
	if err = f.Close(); err != nil {
		return err
	}
	if err = os.Link(f.Name(), abs); err != nil {
		return err
	}
	dir, err := os.Open(filepath.Dir(abs))
	if err != nil {
		return err
	}
	defer dir.Close()
	return dir.Sync()
}

func ParseLabels(value string) (map[string]string, error) {
	if value == "" {
		return nil, nil
	}
	labels := map[string]string{}
	for _, entry := range strings.Split(value, ",") {
		pair := strings.Split(entry, "=")
		if len(pair) != 2 || labels[pair[0]] != "" {
			return nil, errors.New("labels require unique key=value pairs")
		}
		labels[pair[0]] = pair[1]
	}
	if !labelsOK(labels) {
		return nil, errors.New("invalid bounded labels")
	}
	return labels, nil
}
