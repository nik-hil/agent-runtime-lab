package e2bapi

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"

	"agent-runtime-lab/src/durable"
)

var _ durable.Finder = (*Backend)(nil)

// FindByLogicalKey locates existing running/paused sandboxes. It does NOT
// guarantee that no remote sandbox was created if an eventually consistent
// list returns empty; durable.RecoverCreate deliberately keeps that fenced.
func (b *Backend) FindByLogicalKey(ctx context.Context, key string) ([]string, error) {
	if key == "" {
		return nil, errors.New("logical key required")
	}
	ids := make([]string, 0, 1)
	seenIDs := make(map[string]bool)
	seenTokens := make(map[string]bool)
	next := ""
	for page := 0; page < 50; page++ {
		query := url.Values{}
		query.Set("metadata", metadataKey+"="+key)
		query.Set("limit", "100")
		if next != "" {
			query.Set("nextToken", next)
		}
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, b.baseURL+"/v2/sandboxes?"+query.Encode(), nil)
		if err != nil {
			return nil, err
		}
		req.Header.Set("X-API-Key", b.apiKey)
		res, err := b.client.Do(req)
		if err != nil {
			return nil, fmt.Errorf("list E2B sandboxes: %w", err)
		}
		if res.StatusCode != http.StatusOK {
			res.Body.Close() // never log response: it may contain tokens
			return nil, fmt.Errorf("E2B sandbox list returned HTTP %d", res.StatusCode)
		}
		data, readErr := io.ReadAll(io.LimitReader(res.Body, 1<<20))
		res.Body.Close()
		if readErr != nil {
			return nil, readErr
		}
		var listed []struct {
			SandboxID string            `json:"sandboxID"`
			Metadata  map[string]string `json:"metadata"`
		}
		if err := json.Unmarshal(data, &listed); err != nil {
			return nil, fmt.Errorf("parse E2B sandbox list: %w", err)
		}
		for _, entry := range listed {
			if entry.Metadata[metadataKey] != key {
				continue // verify response even if server filters incorrectly
			}
			if _, err := sandboxPath(entry.SandboxID); err != nil {
				return nil, fmt.Errorf("invalid matching sandbox ID: %w", err)
			}
			if !seenIDs[entry.SandboxID] {
				seenIDs[entry.SandboxID] = true
				ids = append(ids, entry.SandboxID)
			}
		}
		next = res.Header.Get("X-Next-Token")
		if next == "" {
			return ids, nil
		}
		if seenTokens[next] {
			return nil, errors.New("cyclic E2B sandbox list cursor")
		}
		seenTokens[next] = true
	}
	return nil, errors.New("E2B list exceeded pagination limit; cannot safely recover")
}
