package main

import (
	"context"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/supraj221/goqueue/internal/api"
	"github.com/supraj221/goqueue/internal/broker/raftbroker"
	"github.com/supraj221/goqueue/internal/fsm"
	"github.com/supraj221/goqueue/internal/raft"
)

// soloConsensus applies proposals straight to the state machine.
type soloConsensus struct {
	mu  sync.Mutex
	q   *fsm.Queue
	idx uint64
	ch  chan bool
}

func (s *soloConsensus) Propose(ctx context.Context, data []byte) (any, uint64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.idx++
	return s.q.Apply(raft.LogEntry{Index: s.idx, Term: 1, Data: data}), s.idx, nil
}
func (s *soloConsensus) ReadIndex(context.Context) error { return nil }
func (s *soloConsensus) Status() raft.Status {
	return raft.Status{ID: "n1", Role: raft.Leader, LeaderID: "n1", Term: 1}
}
func (s *soloConsensus) LeaderChanges() <-chan bool { return s.ch }

// TestLoadHarness runs the load generator against an in-process server and
// checks that its accounting is self-consistent.
func TestLoadHarness(t *testing.T) {
	q := fsm.New()
	b := raftbroker.New(&soloConsensus{q: q, ch: make(chan bool)}, q, raftbroker.Options{})
	defer b.Close()
	ts := httptest.NewServer(api.NewServer(b, api.Options{}).Handler())
	defer ts.Close()

	s := newLoadState(loadCfg{
		urls: []string{ts.URL}, producers: 4, workers: 4, duration: 1500 * time.Millisecond,
		payload: 256, queue: "bench", lease: 10 * time.Second, batch: 8, drain: 10 * time.Second, wait: 200 * time.Millisecond,
	})
	s.run(context.Background())
	v := s.verify(context.Background())
	if s.enqOK.Load() == 0 {
		t.Fatal("nothing enqueued")
	}
	if v.Lost != 0 || v.DuplicateJobs != 0 {
		t.Fatalf("verify: %+v", v)
	}
	if v.Processed+v.RecoveredByGet != v.Acknowledged {
		t.Fatalf("accounting mismatch: %+v", v)
	}
	rep := newReport("test", "fake", "test", nil)
	rep.fill(s, v)
	if rep.EnqueueLatency.Count != v.Acknowledged || rep.EndToEnd.Count == 0 || rep.EnqueueLatency.P99Ms < rep.EnqueueLatency.P50Ms {
		t.Fatalf("bad report: %+v", rep.EnqueueLatency)
	}
	t.Logf("enqueued %d, %.0f/s, e2e p99 %.1fms", rep.Enqueued, rep.EnqueuePerSec, rep.EndToEnd.P99Ms)
}

func TestSummarize(t *testing.T) {
	var s []time.Duration
	for i := 1; i <= 100; i++ {
		s = append(s, time.Duration(i)*time.Millisecond)
	}
	l := summarize(s)
	if l.P50Ms != 50 || l.P95Ms != 95 || l.P99Ms != 99 || l.MaxMs != 100 {
		t.Fatalf("%+v", l)
	}
}
