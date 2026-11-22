package e2bapi

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"sort"
	"strings"
)

// LabSandbox is a read-only provider inventory record. No credentials, envd
// tokens or private payloads are retained in its representation.
type LabSandbox struct {
	SandboxID  string `json:"sandbox_id"`
	LogicalKey string `json:"logical_key"`
}

// ListLabSandboxes scans the authenticated E2B team's active inventory. Only
// exact metadata under this lab's prefix is returned. It does NOT prove the
// provider list is globally exhaustive or ownership of another journal.
// A bad status, cursor cycle or too many pages aborts rather than truncating.
func (b *Backend) ListLabSandboxes(ctx context.Context) ([]LabSandbox, error) {
	var result []LabSandbox
	seenIDs := map[string]bool{}
	seenTokens := map[string]bool{}
	next := ""
	for page := 0; page < 50; page++ {
		query := url.Values{}
		query.Set("limit", "100")
		if next != "" {
			query.Set("nextToken", next)
		}
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, b.baseURL+"/v2/sandboxes?"+query.Encode(), nil)
		if err != nil {
			return nil, err
		}
		req.Header.Set("X-API-Key", b.apiKey)
		resp, err := b.client.Do(req)
		if err != nil {
			return nil, fmt.Errorf("list lab sandbox inventory: %w", err)
		}
		if resp.StatusCode != http.StatusOK {
			resp.Body.Close()
			return nil, fmt.Errorf("lab inventory returned HTTP %d", resp.StatusCode)
		}
		data, readErr := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
		resp.Body.Close()
		if readErr != nil {
			return nil, readErr
		}
		var list []struct {
			SandboxID string            `json:"sandboxID"`
			Metadata  map[string]string `json:"metadata"`
		}
		if err := json.Unmarshal(data, &list); err != nil {
			return nil, fmt.Errorf("decode lab inventory: %w", err)
		}
		for _, item := range list {
			key := item.Metadata[metadataKey]
			if !strings.HasPrefix(key, "agent-runtime-lab/") {
				continue
			}
			if _, err := sandboxPath(item.SandboxID); err != nil {
				return nil, fmt.Errorf("invalid lab sandbox identity: %w", err)
			}
			if seenIDs[item.SandboxID] {
				return nil, errors.New("duplicate sandbox ID in paginated inventory")
			}
			seenIDs[item.SandboxID] = true
			result = append(result, LabSandbox{SandboxID: item.SandboxID, LogicalKey: key})
		}
		next = resp.Header.Get("X-Next-Token")
		if next == "" {
			sort.Slice(result, func(i, j int) bool { return result[i].SandboxID < result[j].SandboxID })
			return result, nil
		}
		if seenTokens[next] {
			return nil, errors.New("cyclic provider inventory cursor")
		}
		seenTokens[next] = true
	}
	return nil, errors.New("provider inventory exceeded 50 pages")
}
