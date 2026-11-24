// Package coordkv implements the small linearizable etcd v3 JSON-gateway API
// used by the cluster. It adds no SDK dependencies and never retries an
// unconfirmed mutation. Callers reconcile durable state after an ambiguous ACK.
package coordkv

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

var ErrUnconfirmed = errors.New("coordination request unconfirmed; reconcile before any new side effect")
var ErrCluster = errors.New("etcd endpoints belong to different clusters")

type Value struct {
	Key    []byte `json:"key"`
	Data   []byte `json:"value"`
	Create int64  `json:"create_revision,string"`
	Mod    int64  `json:"mod_revision,string"`
	Lease  int64  `json:"lease,string"`
}

type Compare struct {
	Key  string
	Mod  *int64
	Data []byte
}

type Put struct {
	Key   string
	Data  []byte
	Lease int64
}

type Store interface {
	Range(context.Context, string) ([]Value, error)
	Txn(context.Context, []Compare, []Put) (bool, error)
	Grant(context.Context, int64) (int64, error)
	KeepAlive(context.Context, int64) error
	Revoke(context.Context, int64) error
}

type Client struct {
	endpoints []string
	client    *http.Client
	next      atomic.Uint64
	mu        sync.Mutex
	clusterID string
	verified  map[string]bool
}

func New(endpoints []string, transport *http.Transport) (*Client, error) {
	if len(endpoints) == 0 || len(endpoints) > 8 {
		return nil, errors.New("1..8 etcd endpoints required")
	}
	for _, endpoint := range endpoints {
		u, err := url.Parse(endpoint)
		if err != nil || u.User != nil || u.RawQuery != "" || u.Fragment != "" || u.Path != "" || u.Port() == "" {
			return nil, errors.New("etcd endpoints must be HTTP(S) origins with explicit ports")
		}
		ip := net.ParseIP(u.Hostname())
		if u.Scheme != "https" && (u.Scheme != "http" || ip == nil || !ip.IsLoopback()) {
			return nil, errors.New("remote etcd requires HTTPS; plaintext is loopback-only")
		}
	}
	if transport == nil {
		transport = &http.Transport{Proxy: nil}
	}
	return &Client{endpoints: append([]string{}, endpoints...), verified: map[string]bool{}, client: &http.Client{Transport: transport, Timeout: 5 * time.Second,
		CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}}, nil
}

func (c *Client) call(ctx context.Context, endpoint, path string, input, output any, stream bool) error {
	data, err := json.Marshal(input)
	if err != nil {
		return err
	}
	if len(data) > 1<<20 {
		return errors.New("coordination request exceeds 1 MiB")
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint+path, bytes.NewReader(data))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := c.client.Do(req)
	if err != nil {
		return ErrUnconfirmed
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("%w: HTTP %d", ErrUnconfirmed, resp.StatusCode)
	}
	decoder := json.NewDecoder(io.LimitReader(resp.Body, 8<<20))
	var raw json.RawMessage
	if err := decoder.Decode(&raw); err != nil {
		return ErrUnconfirmed
	}
	if !stream && decoder.Decode(new(any)) != io.EOF {
		return ErrUnconfirmed
	}
	var metadata struct {
		Header struct {
			Cluster string `json:"cluster_id"`
		} `json:"header"`
		Result struct {
			Header struct {
				Cluster string `json:"cluster_id"`
			} `json:"header"`
		} `json:"result"`
	}
	if json.Unmarshal(raw, &metadata) != nil {
		return ErrUnconfirmed
	}
	id := metadata.Header.Cluster
	if stream {
		id = metadata.Result.Header.Cluster
	}
	if id == "" || id == "0" {
		return ErrUnconfirmed
	}
	c.mu.Lock()
	if c.clusterID != "" && c.clusterID != id {
		c.mu.Unlock()
		return ErrCluster
	}
	c.clusterID = id
	c.verified[endpoint] = true
	c.mu.Unlock()
	if json.Unmarshal(raw, output) != nil {
		return ErrUnconfirmed
	}
	return nil
}

func (c *Client) mutate(ctx context.Context, path string, input, output any, stream bool) error {
	endpoint := c.endpoint()
	c.mu.Lock()
	verified := c.verified[endpoint]
	c.mu.Unlock()
	if !verified {
		var response map[string]any
		if err := c.call(ctx, endpoint, "/v3/kv/range", map[string]any{"key": []byte{0}, "limit": "1"}, &response, false); err != nil {
			return err
		}
	}
	return c.call(ctx, endpoint, path, input, output, stream)
}

func (c *Client) endpoint() string { return c.endpoints[int(c.next.Add(1)-1)%len(c.endpoints)] }

func PrefixEnd(prefix string) []byte {
	b := []byte(prefix)
	for i := len(b) - 1; i >= 0; i-- {
		if b[i] < 255 {
			b[i]++
			return b[:i+1]
		}
	}
	return []byte{0}
}

func (c *Client) Range(ctx context.Context, prefix string) ([]Value, error) {
	var response struct {
		KVs  []Value `json:"kvs"`
		More bool    `json:"more"`
	}
	// Repeating linearizable reads is safe. An error never means absence.
	for range c.endpoints {
		response.KVs = nil
		response.More = false
		err := c.call(ctx, c.endpoint(), "/v3/kv/range", map[string]any{"key": []byte(prefix), "range_end": PrefixEnd(prefix), "limit": "512"}, &response, false)
		if err == nil {
			if response.More {
				return nil, errors.New("cluster namespace exceeds bounded snapshot size")
			}
			return response.KVs, nil
		}
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
		if errors.Is(err, ErrCluster) {
			return nil, err
		}
	}
	return nil, ErrUnconfirmed
}

func (c *Client) Txn(ctx context.Context, compares []Compare, puts []Put) (bool, error) {
	cmp := make([]map[string]any, 0, len(compares))
	for _, compare := range compares {
		x := map[string]any{"key": []byte(compare.Key), "result": "EQUAL"}
		if compare.Mod != nil {
			x["target"], x["mod_revision"] = "MOD", strconv.FormatInt(*compare.Mod, 10)
		} else {
			x["target"], x["value"] = "VALUE", compare.Data
		}
		cmp = append(cmp, x)
	}
	ops := make([]map[string]any, 0, len(puts))
	for _, put := range puts {
		ops = append(ops, map[string]any{"request_put": map[string]any{"key": []byte(put.Key), "value": put.Data, "lease": strconv.FormatInt(put.Lease, 10)}})
	}
	var response struct {
		Succeeded bool `json:"succeeded"`
	}
	err := c.mutate(ctx, "/v3/kv/txn", map[string]any{"compare": cmp, "success": ops}, &response, false)
	return response.Succeeded, err
}

func (c *Client) Grant(ctx context.Context, ttl int64) (int64, error) {
	if ttl < 3 || ttl > 120 {
		return 0, errors.New("lease TTL must be 3..120 seconds")
	}
	var response struct {
		ID  int64 `json:"ID,string"`
		TTL int64 `json:"TTL,string"`
	}
	err := c.mutate(ctx, "/v3/lease/grant", map[string]string{"TTL": strconv.FormatInt(ttl, 10)}, &response, false)
	if err != nil {
		return 0, err
	}
	if response.ID == 0 || response.TTL <= 0 {
		return 0, ErrUnconfirmed
	}
	return response.ID, nil
}

func (c *Client) KeepAlive(ctx context.Context, lease int64) error {
	var response struct {
		Result struct {
			ID  int64 `json:"ID,string"`
			TTL int64 `json:"TTL,string"`
		} `json:"result"`
	}
	if err := c.mutate(ctx, "/v3/lease/keepalive", map[string]string{"ID": strconv.FormatInt(lease, 10)}, &response, true); err != nil {
		return err
	}
	if response.Result.ID != lease || response.Result.TTL <= 0 {
		return ErrUnconfirmed
	}
	return nil
}

func (c *Client) Revoke(ctx context.Context, lease int64) error {
	var response map[string]any
	return c.mutate(ctx, "/v3/lease/revoke", map[string]string{"ID": strconv.FormatInt(lease, 10)}, &response, false)
}

func Endpoints(value string) []string { return strings.Split(value, ",") }
