package httptransport_test

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/supraj221/goqueue/internal/raft"
	"github.com/supraj221/goqueue/internal/raft/httptransport"
)

type listSM struct {
	mu   sync.Mutex
	vals []string
}

func (s *listSM) Apply(e raft.LogEntry) any {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.vals = append(s.vals, string(e.Data))
	return len(s.vals)
}
func (s *listSM) Snapshot() ([]byte, error) { return nil, nil }
func (s *listSM) Restore([]byte) error      { return nil }
func (s *listSM) len() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.vals)
}

// TestThreeNodeClusterOverHTTP runs three nodes that talk through real HTTP
// servers, with the handler mounted at "/raft/" on a larger mux.
func TestThreeNodeClusterOverHTTP(t *testing.T) {
	ids := []string{"a", "b", "c"}
	muxes := make([]*http.ServeMux, 3)
	urls := map[string]string{}
	for i, id := range ids {
		muxes[i] = http.NewServeMux()
		muxes[i].HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) { _, _ = io.WriteString(w, "ok") })
		srv := httptest.NewServer(muxes[i])
		t.Cleanup(srv.Close)
		urls[id] = srv.URL
	}

	sms := make([]*listSM, 3)
	nodes := make([]*raft.Node, 3)
	for i, id := range ids {
		sms[i] = &listSM{}
		cfg := raft.Config{
			ID: id, Peers: ids,
			ElectionTimeoutMin: 100 * time.Millisecond, ElectionTimeoutMax: 200 * time.Millisecond,
			HeartbeatInterval: 25 * time.Millisecond,
			Logger:            slog.New(slog.NewTextHandler(io.Discard, nil)),
		}
		n, err := raft.NewNode(cfg, sms[i], raft.NewMemStorage(), httptransport.New(urls, time.Second))
		if err != nil {
			t.Fatal(err)
		}
		nodes[i] = n
		muxes[i].Handle("/raft/", httptransport.NewHandler(n))
	}
	for _, n := range nodes {
		if err := n.Start(); err != nil {
			t.Fatal(err)
		}
	}
	t.Cleanup(func() {
		for _, n := range nodes {
			n.Stop()
		}
	})

	var leader *raft.Node
	deadline := time.Now().Add(5 * time.Second)
	for leader == nil && time.Now().Before(deadline) {
		for _, n := range nodes {
			if n.Status().Role == raft.Leader {
				leader = n
			}
		}
		time.Sleep(10 * time.Millisecond)
	}
	if leader == nil {
		t.Fatal("no leader elected over HTTP")
	}

	for i := 0; i < 20; i++ {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		res, _, err := leader.Propose(ctx, []byte(fmt.Sprintf("job-%d", i)))
		cancel()
		if err != nil {
			t.Fatalf("Propose %d: %v", i, err)
		}
		if res != i+1 {
			t.Fatalf("Apply result %v, want %d", res, i+1)
		}
	}
	for i, sm := range sms {
		deadline := time.Now().Add(3 * time.Second)
		for sm.len() < 20 && time.Now().Before(deadline) {
			time.Sleep(10 * time.Millisecond)
		}
		if sm.len() != 20 {
			t.Fatalf("node %d applied %d entries, want 20", i, sm.len())
		}
	}
	if err := leader.ReadIndex(context.Background()); err != nil {
		t.Fatalf("ReadIndex over HTTP: %v", err)
	}
}

func TestHandlerRejectsBadRequests(t *testing.T) {
	n, err := raft.NewNode(raft.Config{ID: "a", Peers: []string{"a"}}, &listSM{}, raft.NewMemStorage(), nil)
	if err != nil {
		t.Fatal(err)
	}
	srv := httptest.NewServer(httptransport.NewHandler(n))
	defer srv.Close()
	if resp, _ := http.Get(srv.URL + "/raft/vote"); resp.StatusCode != http.StatusMethodNotAllowed {
		t.Errorf("GET status = %d, want 405", resp.StatusCode)
	}
	resp, _ := http.Post(srv.URL+"/raft/nope", "application/json", nil)
	if resp.StatusCode != http.StatusNotFound {
		t.Errorf("unknown route status = %d, want 404", resp.StatusCode)
	}
	// Node not started: the handler reports unavailable rather than hanging.
	resp, _ = http.Post(srv.URL+"/raft/vote", "application/json", strings.NewReader(`{"term":1}`))
	if resp.StatusCode != http.StatusServiceUnavailable {
		t.Errorf("unstarted node status = %d, want 503", resp.StatusCode)
	}
}
