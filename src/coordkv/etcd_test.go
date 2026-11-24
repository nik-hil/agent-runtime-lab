package coordkv

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strings"
	"testing"
)

type roundTrip func(*http.Request) (*http.Response, error)

func (f roundTrip) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }
func response(value string) *http.Response {
	return &http.Response{StatusCode: 200, Header: http.Header{}, Body: io.NopCloser(strings.NewReader(value))}
}

func TestGatewayWireAndAmbiguousMutation(t *testing.T) {
	c, _ := New([]string{"http://127.0.0.1:2379", "http://127.0.0.1:2381"}, nil)
	mutations := 0
	c.client.Transport = roundTrip(func(r *http.Request) (*http.Response, error) {
		var body map[string]json.RawMessage
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			t.Fatal(err)
		}
		switch r.URL.Path {
		case "/v3/kv/range":
			var key []byte
			json.Unmarshal(body["key"], &key)
			if len(key) == 0 {
				t.Fatal("missing base64 key")
			}
			return response(`{"header":{"cluster_id":"123"},"kvs":[{"key":"L3Rlc3QvYQ==","value":"eA==","mod_revision":"12","create_revision":"1","lease":"42"}]}`), nil
		case "/v3/kv/txn":
			mutations++
			var ops []map[string]map[string]json.RawMessage
			json.Unmarshal(body["success"], &ops)
			if len(ops) != 1 || ops[0]["request_put"] == nil {
				t.Fatal("invalid gateway transaction", string(body["success"]))
			}
			return nil, io.ErrUnexpectedEOF
		case "/v3/lease/keepalive":
			return response(`{"result":{"header":{"cluster_id":"123"},"ID":"42","TTL":"6"}}` + "\n" + `{"result":{}}`), nil
		default:
			t.Fatal(r.URL.Path)
			return nil, errors.New("unexpected")
		}
	})
	values, err := c.Range(context.Background(), "/test/")
	if err != nil || len(values) != 1 || values[0].Mod != 12 || string(values[0].Data) != "x" {
		t.Fatal(values, err)
	}
	if _, err = c.Txn(context.Background(), nil, []Put{{Key: "/test/a", Data: []byte("x")}}); !errors.Is(err, ErrUnconfirmed) || mutations != 1 {
		t.Fatal("uncertain mutation retried", mutations, err)
	}
	if err = c.KeepAlive(context.Background(), 42); err != nil {
		t.Fatal("streaming keepalive not decoded", err)
	}
}

func TestDifferentEtcdClustersFailBeforeMutation(t *testing.T) {
	c, _ := New([]string{"http://127.0.0.1:2379", "http://127.0.0.1:2381"}, nil)
	mutations := 0
	c.client.Transport = roundTrip(func(r *http.Request) (*http.Response, error) {
		if r.URL.Path != "/v3/kv/range" {
			mutations++
		}
		id := "123"
		if strings.HasSuffix(r.URL.Host, ":2381") {
			id = "456"
		}
		return response(`{"header":{"cluster_id":"` + id + `"}}`), nil
	})
	if _, err := c.Range(context.Background(), "/test/"); err != nil {
		t.Fatal(err)
	}
	if _, err := c.Txn(context.Background(), nil, []Put{{Key: "x", Data: []byte("y")}}); !errors.Is(err, ErrCluster) || mutations != 0 {
		t.Fatal("different cluster mutated", err, mutations)
	}
}

func TestEndpointRestrictions(t *testing.T) {
	for _, endpoint := range []string{"http://example.com:2379", "https://host:2379/path", "https://user:pass@host:2379", "https://host"} {
		if _, err := New([]string{endpoint}, nil); err == nil {
			t.Fatal(endpoint)
		}
	}
}
