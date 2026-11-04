// Package httptransport carries Raft RPCs over HTTP with JSON bodies.
//
// Client side:
//
//	tr := httptransport.New(map[string]string{"n2": "http://127.0.0.1:7102"}, time.Second)
//	node, _ := raft.NewNode(cfg, sm, st, tr)
//
// Server side, mounted on a larger mux:
//
//	mux.Handle("/raft/", httptransport.NewHandler(node))
//
// Endpoints (all POST, JSON in and out): /raft/vote, /raft/append, /raft/snapshot.
package httptransport

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"path"
	"time"

	"github.com/supraj221/goqueue/internal/raft"
)

const maxBody = 1 << 30 // snapshots are sent whole

// Transport implements raft.Transport over HTTP.
type Transport struct {
	peers  map[string]string
	client *http.Client
}

// New builds a Transport. peers maps node ID to base URL such as
// "http://127.0.0.1:7101". timeout bounds each RPC (the caller's context may
// cut it shorter). One http.Client with keep-alive connections is shared by
// all peers.
func New(peers map[string]string, timeout time.Duration) *Transport {
	cp := make(map[string]string, len(peers))
	for id, u := range peers {
		cp[id] = u
	}
	return &Transport{
		peers: cp,
		client: &http.Client{
			Timeout: timeout,
			Transport: &http.Transport{
				MaxIdleConns:        64,
				MaxIdleConnsPerHost: 8,
				IdleConnTimeout:     90 * time.Second,
				ForceAttemptHTTP2:   false,
			},
		},
	}
}

func (t *Transport) post(ctx context.Context, to, route string, req, resp any) error {
	base, ok := t.peers[to]
	if !ok {
		return fmt.Errorf("httptransport: unknown peer %q", to)
	}
	body, err := json.Marshal(req)
	if err != nil {
		return err
	}
	hr, err := http.NewRequestWithContext(ctx, http.MethodPost, base+"/raft/"+route, bytes.NewReader(body))
	if err != nil {
		return err
	}
	hr.Header.Set("Content-Type", "application/json")
	res, err := t.client.Do(hr)
	if err != nil {
		return err
	}
	defer func() { _ = res.Body.Close() }()
	if res.StatusCode != http.StatusOK {
		msg, _ := io.ReadAll(io.LimitReader(res.Body, 512))
		return fmt.Errorf("httptransport: %s %s: %s: %s", to, route, res.Status, bytes.TrimSpace(msg))
	}
	return json.NewDecoder(res.Body).Decode(resp)
}

func (t *Transport) RequestVote(ctx context.Context, to string, req *raft.RequestVoteRequest) (*raft.RequestVoteResponse, error) {
	var resp raft.RequestVoteResponse
	if err := t.post(ctx, to, "vote", req, &resp); err != nil {
		return nil, err
	}
	return &resp, nil
}

func (t *Transport) AppendEntries(ctx context.Context, to string, req *raft.AppendEntriesRequest) (*raft.AppendEntriesResponse, error) {
	var resp raft.AppendEntriesResponse
	if err := t.post(ctx, to, "append", req, &resp); err != nil {
		return nil, err
	}
	return &resp, nil
}

func (t *Transport) InstallSnapshot(ctx context.Context, to string, req *raft.InstallSnapshotRequest) (*raft.InstallSnapshotResponse, error) {
	var resp raft.InstallSnapshotResponse
	if err := t.post(ctx, to, "snapshot", req, &resp); err != nil {
		return nil, err
	}
	return &resp, nil
}

var _ raft.Transport = (*Transport)(nil)

// NewHandler serves the Raft RPC endpoints for h. It routes on the last path
// element (vote, append, snapshot), so it works whether it is mounted at
// "/raft/" on a mux or at the root behind http.StripPrefix.
func NewHandler(h raft.RPCHandler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		body := http.MaxBytesReader(w, r.Body, maxBody)
		var resp any
		var err error
		switch path.Base(r.URL.Path) {
		case "vote":
			var req raft.RequestVoteRequest
			if err = json.NewDecoder(body).Decode(&req); err == nil {
				resp, err = h.HandleRequestVote(r.Context(), &req)
			}
		case "append":
			var req raft.AppendEntriesRequest
			if err = json.NewDecoder(body).Decode(&req); err == nil {
				resp, err = h.HandleAppendEntries(r.Context(), &req)
			}
		case "snapshot":
			var req raft.InstallSnapshotRequest
			if err = json.NewDecoder(body).Decode(&req); err == nil {
				resp, err = h.HandleInstallSnapshot(r.Context(), &req)
			}
		default:
			http.NotFound(w, r)
			return
		}
		if err != nil {
			http.Error(w, err.Error(), http.StatusServiceUnavailable)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(resp)
	})
}
