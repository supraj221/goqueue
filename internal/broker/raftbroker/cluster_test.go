package raftbroker

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"sync"
	"testing"
	"time"

	"github.com/supraj221/goqueue/internal/broker"
	"github.com/supraj221/goqueue/internal/broker/brokertest"
	"github.com/supraj221/goqueue/internal/fsm"
	"github.com/supraj221/goqueue/internal/job"
	"github.com/supraj221/goqueue/internal/raft"
	"github.com/supraj221/goqueue/internal/raft/memnet"
)

// testCluster is a real 3-node Raft cluster on the in-memory network, each
// node with its own state machine and broker.
type testCluster struct {
	t       *testing.T
	net     *memnet.Network
	ids     []string
	nodes   []*raft.Node
	queues  []*fsm.Queue
	brokers []*Broker
}

func newTestCluster(t *testing.T) *testCluster {
	t.Helper()
	quiet := slog.New(slog.NewTextHandler(io.Discard, nil))
	c := &testCluster{t: t, net: memnet.New(), ids: []string{"n1", "n2", "n3"}}
	peers := map[string]string{}
	for _, id := range c.ids {
		peers[id] = "http://" + id
	}
	for _, id := range c.ids {
		q := fsm.New()
		node, err := raft.NewNode(raft.Config{
			ID: id, Peers: c.ids,
			ElectionTimeoutMin: 80 * time.Millisecond, ElectionTimeoutMax: 160 * time.Millisecond,
			HeartbeatInterval: 20 * time.Millisecond, SnapshotThreshold: 400, Logger: quiet,
		}, q, raft.NewMemStorage(), c.net.Transport(id))
		if err != nil {
			t.Fatal(err)
		}
		c.net.Register(id, node)
		if err := node.Start(); err != nil {
			t.Fatal(err)
		}
		b := New(node, q, Options{PeerAddrs: peers, Logger: quiet, ReaperInterval: 50 * time.Millisecond})
		c.nodes, c.queues, c.brokers = append(c.nodes, node), append(c.queues, q), append(c.brokers, b)
	}
	t.Cleanup(func() {
		for _, b := range c.brokers {
			b.Close()
		}
		for _, n := range c.nodes {
			n.Stop()
		}
	})
	c.waitLeader(10 * time.Second)
	return c
}

func (c *testCluster) leaderIdx() int {
	for i, n := range c.nodes {
		if n.Status().Role == raft.Leader {
			return i
		}
	}
	return -1
}

// waitLeader waits for a leader that the cluster agrees on and returns its index.
func (c *testCluster) waitLeader(d time.Duration) int {
	c.t.Helper()
	deadline := time.Now().Add(d)
	for time.Now().Before(deadline) {
		if i := c.leaderIdx(); i >= 0 {
			return i
		}
		time.Sleep(10 * time.Millisecond)
	}
	c.t.Fatal("no leader elected")
	return -1
}

// router is a broker.Broker that behaves like the client SDK: it tries nodes
// until one accepts, following not-leader errors and retrying unavailability.
type router struct {
	c       *testCluster
	mu      sync.Mutex
	last    int
	attempt time.Duration
}

func (r *router) do(ctx context.Context, fn func(ctx context.Context, b broker.Broker) error) error {
	deadline := time.Now().Add(30 * time.Second)
	for {
		for k := 0; k < len(r.c.brokers); k++ {
			r.mu.Lock()
			i := (r.last + k) % len(r.c.brokers)
			r.mu.Unlock()
			actx, cancel := context.WithTimeout(ctx, r.attempt)
			err := fn(actx, r.c.brokers[i])
			cancel()
			var nl *broker.NotLeaderError
			switch {
			case err == nil:
				r.mu.Lock()
				r.last = i
				r.mu.Unlock()
				return nil
			case ctx.Err() != nil:
				return err
			case errors.As(err, &nl), errors.Is(err, broker.ErrUnavailable),
				errors.Is(err, context.DeadlineExceeded), errors.Is(err, context.Canceled):
				continue
			default:
				return err
			}
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("router: no node accepted the request in time")
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func (r *router) Enqueue(ctx context.Context, req broker.EnqueueRequest) (res broker.EnqueueResult, err error) {
	err = r.do(ctx, func(c context.Context, b broker.Broker) (e error) { res, e = b.Enqueue(c, req); return })
	return
}
func (r *router) Dequeue(ctx context.Context, req broker.DequeueRequest) (js []job.Job, err error) {
	err = r.do(ctx, func(c context.Context, b broker.Broker) (e error) { js, e = b.Dequeue(c, req); return })
	return
}
func (r *router) Heartbeat(ctx context.Context, id string, tok uint64, ext time.Duration) (j job.Job, err error) {
	err = r.do(ctx, func(c context.Context, b broker.Broker) (e error) { j, e = b.Heartbeat(c, id, tok, ext); return })
	return
}
func (r *router) Ack(ctx context.Context, id string, tok uint64) error {
	return r.do(ctx, func(c context.Context, b broker.Broker) error { return b.Ack(c, id, tok) })
}
func (r *router) Nack(ctx context.Context, id string, tok uint64, reason string, retry bool) error {
	return r.do(ctx, func(c context.Context, b broker.Broker) error { return b.Nack(c, id, tok, reason, retry) })
}
func (r *router) Get(ctx context.Context, id string) (j job.Job, err error) {
	err = r.do(ctx, func(c context.Context, b broker.Broker) (e error) { j, e = b.Get(c, id); return })
	return
}
func (r *router) Stats(ctx context.Context) (s broker.Stats, err error) {
	err = r.do(ctx, func(c context.Context, b broker.Broker) (e error) { s, e = b.Stats(c); return })
	return
}
func (r *router) Close() error { return nil }

func TestConformanceRaftCluster(t *testing.T) {
	if testing.Short() {
		t.Skip("real cluster")
	}
	brokertest.Run(t, func(t *testing.T) broker.Broker {
		c := newTestCluster(t)
		return &router{c: c, attempt: 5 * time.Second, last: c.leaderIdx()}
	})
}

func TestFollowerRedirects(t *testing.T) {
	c := newTestCluster(t)
	l := c.leaderIdx()
	f := (l + 1) % 3
	// Followers learn the leader through heartbeats; give them a moment.
	var err error
	for range 100 {
		_, err = c.brokers[f].Enqueue(context.Background(), broker.EnqueueRequest{Queue: "q", Type: "t"})
		var nl *broker.NotLeaderError
		if errors.As(err, &nl) && nl.LeaderAddr == "http://"+c.ids[l] {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("follower error = %v, want NotLeaderError pointing at %s", err, c.ids[l])
}

// TestFailoverExactlyOnce enqueues 500 jobs with idempotency keys while the
// leader is cut off from the network. Every enqueue is retried with the same
// key until it succeeds, so each job must exist exactly once afterwards.
func TestFailoverExactlyOnce(t *testing.T) {
	if testing.Short() {
		t.Skip("real cluster")
	}
	c := newTestCluster(t)
	r := &router{c: c, attempt: time.Second, last: c.leaderIdx()}
	const total = 500
	oldLeader := c.leaderIdx()

	ids := make([]string, total)
	errs := make(chan error, total)
	var wg sync.WaitGroup
	sem := make(chan struct{}, 8)
	var done int
	var dmu sync.Mutex
	for i := 0; i < total; i++ {
		wg.Add(1)
		sem <- struct{}{}
		go func() {
			defer wg.Done()
			defer func() { <-sem }()
			res, err := r.Enqueue(context.Background(), broker.EnqueueRequest{
				Queue: "q", Type: "t", Payload: []byte(fmt.Sprint(i)), IdempotencyKey: fmt.Sprintf("key-%d", i),
			})
			if err != nil {
				errs <- fmt.Errorf("enqueue %d: %w", i, err)
				return
			}
			ids[i] = res.Job.ID
			dmu.Lock()
			done++
			if done == 150 {
				c.net.Disconnect(c.ids[oldLeader]) // fail the leader mid-flight
			}
			dmu.Unlock()
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Error(err)
	}
	if t.Failed() {
		return
	}
	if nl := c.leaderIdx(); nl < 0 {
		t.Fatal("no leader after failover")
	}

	seen := map[string]int{}
	for _, id := range ids {
		seen[id]++
	}
	if len(seen) != total {
		t.Fatalf("distinct job IDs = %d, want %d", len(seen), total)
	}

	c.net.Connect(c.ids[oldLeader])
	// All three replicas must converge on exactly 500 jobs.
	deadline := time.Now().Add(10 * time.Second)
	for {
		ok := true
		for _, q := range c.queues {
			if q.Len() != total {
				ok = false
			}
		}
		if ok {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("replicas did not converge: %d %d %d jobs", c.queues[0].Len(), c.queues[1].Len(), c.queues[2].Len())
		}
		time.Sleep(20 * time.Millisecond)
	}
	// Dequeue everything: each job claimed exactly once.
	claimed := map[string]int{}
	for len(claimed) < total {
		js, err := r.Dequeue(context.Background(), broker.DequeueRequest{Queues: []string{"q"}, WorkerID: "w", Lease: time.Minute, Max: 100})
		if err != nil {
			t.Fatal(err)
		}
		if len(js) == 0 {
			break
		}
		for _, j := range js {
			claimed[j.ID]++
		}
	}
	for id, n := range claimed {
		if n != 1 || seen[id] != 1 {
			t.Fatalf("job %s claimed %d times", id, n)
		}
	}
	if len(claimed) != total {
		t.Fatalf("claimed %d, want %d", len(claimed), total)
	}
}

// TestLeaderLoopsFollowLeadership checks that only the current leader reaps
// expired leases and that a new leader takes over the reaper.
func TestLeaderLoopsFollowLeadership(t *testing.T) {
	if testing.Short() {
		t.Skip("real cluster")
	}
	c := newTestCluster(t)
	r := &router{c: c, attempt: time.Second, last: c.leaderIdx()}
	ctx := context.Background()
	res, err := r.Enqueue(ctx, broker.EnqueueRequest{Queue: "q", Type: "t", MaxAttempts: 1})
	if err != nil {
		t.Fatal(err)
	}
	js, err := r.Dequeue(ctx, broker.DequeueRequest{Queues: []string{"q"}, WorkerID: "w", Lease: 300 * time.Millisecond, Max: 1})
	if err != nil || len(js) != 1 {
		t.Fatalf("dequeue: %v %v", js, err)
	}
	// Kill the leader before the lease expires; the new leader must reap it.
	old := c.leaderIdx()
	c.net.Disconnect(c.ids[old])
	deadline := time.Now().Add(10 * time.Second)
	for {
		j, err := r.Get(ctx, res.Job.ID)
		if err == nil && j.State == job.StateDead {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("job never reaped after failover: %+v %v", j, err)
		}
		time.Sleep(50 * time.Millisecond)
	}
}
