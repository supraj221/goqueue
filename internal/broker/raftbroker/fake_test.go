package raftbroker

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/supraj221/goqueue/internal/broker"
	"github.com/supraj221/goqueue/internal/broker/brokertest"
	"github.com/supraj221/goqueue/internal/fsm"
	"github.com/supraj221/goqueue/internal/raft"
)

// fakeConsensus applies proposals directly to the state machine, in order,
// and is always the leader.
type fakeConsensus struct {
	mu      sync.Mutex
	q       *fsm.Queue
	index   uint64
	leader  bool
	changes chan bool
}

func newFake(q *fsm.Queue) *fakeConsensus {
	f := &fakeConsensus{q: q, leader: true, changes: make(chan bool, 4)}
	f.changes <- true
	return f
}

func (f *fakeConsensus) Propose(ctx context.Context, data []byte) (any, uint64, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if !f.leader {
		return nil, 0, raft.ErrNotLeader
	}
	f.index++
	return f.q.Apply(raft.LogEntry{Index: f.index, Term: 1, Data: data}), f.index, nil
}

func (f *fakeConsensus) ReadIndex(ctx context.Context) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if !f.leader {
		return raft.ErrNotLeader
	}
	return nil
}

func (f *fakeConsensus) Status() raft.Status {
	f.mu.Lock()
	defer f.mu.Unlock()
	st := raft.Status{ID: "n1", Term: 1, CommitIndex: f.index, LastApplied: f.index, LastLogIndex: f.index}
	if f.leader {
		st.Role, st.LeaderID = raft.Leader, "n1"
	} else {
		st.Role = raft.Follower
	}
	return st
}

func (f *fakeConsensus) LeaderChanges() <-chan bool { return f.changes }

func (f *fakeConsensus) setLeader(v bool) {
	f.mu.Lock()
	f.leader = v
	f.mu.Unlock()
	f.changes <- v
}

func TestConformanceFake(t *testing.T) {
	brokertest.Run(t, func(t *testing.T) broker.Broker {
		q := fsm.New()
		b := New(newFake(q), q, Options{ReaperInterval: 50 * time.Millisecond})
		t.Cleanup(func() { _ = b.Close() })
		return b
	})
}

func TestNotLeaderMapping(t *testing.T) {
	q := fsm.New()
	f := newFake(q)
	b := New(f, q, Options{PeerAddrs: map[string]string{"n1": "http://n1"}})
	defer func() { _ = b.Close() }()
	f.setLeader(false)
	_, err := b.Enqueue(context.Background(), broker.EnqueueRequest{Queue: "q", Type: "t"})
	var nl *broker.NotLeaderError
	if !asNotLeader(err, &nl) {
		t.Fatalf("err = %v, want NotLeaderError", err)
	}
	if _, err := b.Dequeue(context.Background(), broker.DequeueRequest{Queues: []string{"q"}, WorkerID: "w", Lease: time.Second, Max: 1, Wait: time.Second}); !asNotLeader(err, &nl) {
		t.Fatalf("dequeue err = %v", err)
	}
	if _, err := b.Get(context.Background(), "x"); !asNotLeader(err, &nl) {
		t.Fatalf("get err = %v", err)
	}
}

func TestMetrics(t *testing.T) {
	q := fsm.New()
	b := New(newFake(q), q, Options{})
	defer func() { _ = b.Close() }()
	reg := prometheus.NewRegistry()
	if err := b.RegisterMetrics(reg); err != nil {
		t.Fatal(err)
	}
	if _, err := b.Enqueue(context.Background(), broker.EnqueueRequest{Queue: "q", Type: "t"}); err != nil {
		t.Fatal(err)
	}
	mfs, err := reg.Gather()
	n := len(mfs)
	if err != nil || n < 7 {
		t.Fatalf("gathered %d metrics, err %v", n, err)
	}
}

func asNotLeader(err error, target **broker.NotLeaderError) bool { return errors.As(err, target) }
