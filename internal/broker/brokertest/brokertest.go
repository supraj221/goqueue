// Package brokertest is the conformance suite every broker.Broker backend
// must pass. It only checks behavior defined in broker.go and docs/PLAN.md
// section 4.3, using real time and short (about 300 ms) leases.
package brokertest

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/supraj221/goqueue/internal/broker"
	"github.com/supraj221/goqueue/internal/job"
)

const shortLease = 300 * time.Millisecond

// Run executes the conformance suite. newBroker must return a fresh, empty
// broker for every call and register any cleanup (including Close) itself
// with t.Cleanup.
func Run(t *testing.T, newBroker func(t *testing.T) broker.Broker) {
	tests := []struct {
		name string
		fn   func(t *testing.T, b broker.Broker)
	}{
		{"EnqueueAndGet", testEnqueueGet},
		{"EnqueueValidation", testValidation},
		{"GetNotFound", testGetNotFound},
		{"IdempotentEnqueue", testIdempotent},
		{"PriorityAndFIFO", testOrdering},
		{"ScheduledNotClaimableEarly", testScheduled},
		{"MaxBatching", testMax},
		{"MultipleQueues", testMultiQueue},
		{"LeaseExpiryReclaim", testLeaseExpiry},
		{"HeartbeatExtendsLease", testHeartbeat},
		{"Ack", testAck},
		{"NackRetry", testNackRetry},
		{"NackNoRetryGoesDead", testNackDead},
		{"AttemptsExhaustedGoesDead", testExhausted},
		{"LeaseExpiryExhaustedGoesDead", testLeaseExpiryExhausted},
		{"LongPollReturnsEarly", testLongPoll},
		{"LongPollTimesOutEmpty", testLongPollTimeout},
		{"StatsCounts", testStats},
		{"NoDoubleClaims", testNoDoubleClaims},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			tc.fn(t, newBroker(t))
		})
	}
}

func bg() context.Context { return context.Background() }

func enq(t *testing.T, b broker.Broker, req broker.EnqueueRequest) job.Job {
	t.Helper()
	if req.Queue == "" {
		req.Queue = "q"
	}
	if req.Type == "" {
		req.Type = "t"
	}
	res, err := b.Enqueue(bg(), req)
	if err != nil {
		t.Fatalf("enqueue: %v", err)
	}
	return res.Job
}

func deq(t *testing.T, b broker.Broker, queues []string, max int, lease, wait time.Duration) []job.Job {
	t.Helper()
	jobs, err := b.Dequeue(bg(), broker.DequeueRequest{Queues: queues, WorkerID: "w1", Lease: lease, Max: max, Wait: wait})
	if err != nil {
		t.Fatalf("dequeue: %v", err)
	}
	return jobs
}

func deq1(t *testing.T, b broker.Broker, lease time.Duration) []job.Job {
	t.Helper()
	return deq(t, b, []string{"q"}, 10, lease, 0)
}

func mustGet(t *testing.T, b broker.Broker, id string) job.Job {
	t.Helper()
	j, err := b.Get(bg(), id)
	if err != nil {
		t.Fatalf("get %s: %v", id, err)
	}
	return j
}

// waitState polls Get until the job reaches state or the timeout passes.
func waitState(t *testing.T, b broker.Broker, id string, want job.State, timeout time.Duration) job.Job {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for {
		j := mustGet(t, b, id)
		if j.State == want {
			return j
		}
		if time.Now().After(deadline) {
			t.Fatalf("job %s: state %s, want %s after %v", id, j.State, want, timeout)
		}
		time.Sleep(50 * time.Millisecond)
	}
}

// claimEventually polls Dequeue until a job comes back or the timeout passes.
func claimEventually(t *testing.T, b broker.Broker, timeout, lease time.Duration) job.Job {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for {
		if js := deq1(t, b, lease); len(js) > 0 {
			return js[0]
		}
		if time.Now().After(deadline) {
			t.Fatalf("nothing claimable within %v", timeout)
		}
		time.Sleep(50 * time.Millisecond)
	}
}

// Enqueue returns a pending job with defaults; Get returns the same job.
func testEnqueueGet(t *testing.T, b broker.Broker) {
	res, err := b.Enqueue(bg(), broker.EnqueueRequest{Queue: "q", Type: "email", Payload: []byte(`{"to":"a"}`), Priority: 3})
	if err != nil {
		t.Fatal(err)
	}
	if !res.Created {
		t.Error("Created = false")
	}
	j := res.Job
	if len(j.ID) != 26 || j.Queue != "q" || j.Type != "email" || j.Priority != 3 {
		t.Errorf("unexpected job %+v", j)
	}
	if j.State != job.StatePending || j.Attempts != 0 || j.MaxAttempts != job.DefaultMaxAttempts {
		t.Errorf("unexpected defaults %+v", j)
	}
	if j.CreatedAt.IsZero() || j.RunAt.IsZero() {
		t.Errorf("timestamps not set %+v", j)
	}
	got := mustGet(t, b, j.ID)
	if got.ID != j.ID || got.Type != "email" || len(got.Payload) == 0 {
		t.Errorf("get mismatch: %+v", got)
	}
}

// Invalid enqueue and dequeue requests return errors matching ErrInvalid.
func testValidation(t *testing.T, b broker.Broker) {
	bad := []broker.EnqueueRequest{
		{Queue: "", Type: "t"},
		{Queue: "q", Type: ""},
		{Queue: "bad queue", Type: "t"},
		{Queue: "q", Type: "t", Payload: []byte(`{not json`)},
		{Queue: "q", Type: "t", Delay: time.Second, RunAt: time.Now().Add(time.Hour)},
		{Queue: "q", Type: "t", MaxAttempts: -1},
	}
	for i, r := range bad {
		if _, err := b.Enqueue(bg(), r); !errors.Is(err, broker.ErrInvalid) {
			t.Errorf("enqueue case %d: err = %v, want ErrInvalid", i, err)
		}
	}
	badDeq := []broker.DequeueRequest{
		{Queues: nil, WorkerID: "w", Lease: time.Second, Max: 1},
		{Queues: []string{"q"}, WorkerID: "", Lease: time.Second, Max: 1},
		{Queues: []string{"q"}, WorkerID: "w", Lease: time.Millisecond, Max: 1},
		{Queues: []string{"q"}, WorkerID: "w", Lease: time.Second, Max: 0},
	}
	for i, r := range badDeq {
		if _, err := b.Dequeue(bg(), r); !errors.Is(err, broker.ErrInvalid) {
			t.Errorf("dequeue case %d: err = %v, want ErrInvalid", i, err)
		}
	}
}

// Get of an unknown ID returns ErrNotFound.
func testGetNotFound(t *testing.T, b broker.Broker) {
	if _, err := b.Get(bg(), "01HZZZZZZZZZZZZZZZZZZZZZZZ"); !errors.Is(err, broker.ErrNotFound) {
		t.Fatalf("err = %v, want ErrNotFound", err)
	}
}

// Enqueue with a repeated idempotency key returns the original job unchanged
// with Created=false, and only one job exists.
func testIdempotent(t *testing.T, b broker.Broker) {
	r1, err := b.Enqueue(bg(), broker.EnqueueRequest{Queue: "q", Type: "t", Payload: []byte(`1`), IdempotencyKey: "k1"})
	if err != nil || !r1.Created {
		t.Fatalf("first: %+v %v", r1, err)
	}
	r2, err := b.Enqueue(bg(), broker.EnqueueRequest{Queue: "q", Type: "t", Payload: []byte(`2`), IdempotencyKey: "k1"})
	if err != nil {
		t.Fatal(err)
	}
	if r2.Created || r2.Job.ID != r1.Job.ID {
		t.Fatalf("second: created=%v id=%s want id=%s", r2.Created, r2.Job.ID, r1.Job.ID)
	}
	if string(r2.Job.Payload) != "1" {
		t.Errorf("payload = %s, want original", r2.Job.Payload)
	}
	r3 := enq(t, b, broker.EnqueueRequest{IdempotencyKey: "k2"})
	if r3.ID == r1.Job.ID {
		t.Error("different key returned same job")
	}
	if got := deq(t, b, []string{"q"}, 10, time.Minute, 0); len(got) != 2 {
		t.Errorf("claimed %d jobs, want 2", len(got))
	}
}

// Claims come back highest priority first, then FIFO by run_at/ID.
func testOrdering(t *testing.T, b broker.Broker) {
	lo1 := enq(t, b, broker.EnqueueRequest{Priority: 0})
	lo2 := enq(t, b, broker.EnqueueRequest{Priority: 0})
	hi := enq(t, b, broker.EnqueueRequest{Priority: 10})
	mid := enq(t, b, broker.EnqueueRequest{Priority: 5})
	lo3 := enq(t, b, broker.EnqueueRequest{Priority: 0})
	want := []string{hi.ID, mid.ID, lo1.ID, lo2.ID, lo3.ID}

	// Claim one at a time so the order is observable regardless of how a
	// backend orders the jobs inside a batch.
	var got []string
	for range want {
		js := deq(t, b, []string{"q"}, 1, time.Minute, 0)
		if len(js) != 1 {
			t.Fatalf("claimed %d, want 1", len(js))
		}
		got = append(got, js[0].ID)
	}
	if fmt.Sprint(got) != fmt.Sprint(want) {
		t.Errorf("order = %v, want %v", got, want)
	}
}

// A job with a future run_at is scheduled, not claimable early, and becomes
// claimable once run_at passes.
func testScheduled(t *testing.T, b broker.Broker) {
	d := enq(t, b, broker.EnqueueRequest{Delay: 700 * time.Millisecond})
	if d.State != job.StateScheduled {
		t.Errorf("delay job state = %s, want scheduled", d.State)
	}
	r := enq(t, b, broker.EnqueueRequest{RunAt: time.Now().Add(time.Hour)})
	if r.State != job.StateScheduled {
		t.Errorf("run_at job state = %s, want scheduled", r.State)
	}
	if got := deq1(t, b, time.Minute); len(got) != 0 {
		t.Fatalf("claimed %d scheduled jobs early", len(got))
	}
	j := claimEventually(t, b, 5*time.Second, time.Minute)
	if j.ID != d.ID {
		t.Errorf("claimed %s, want delayed job %s", j.ID, d.ID)
	}
	if j.Attempts != 1 || j.LeaseToken != 1 || j.State != job.StateRunning {
		t.Errorf("claimed job %+v", j)
	}
	if got := deq1(t, b, time.Minute); len(got) != 0 {
		t.Errorf("hour-delayed job claimed")
	}
}

// Dequeue Max limits the batch size.
func testMax(t *testing.T, b broker.Broker) {
	for range 10 {
		enq(t, b, broker.EnqueueRequest{})
	}
	if got := deq(t, b, []string{"q"}, 3, time.Minute, 0); len(got) != 3 {
		t.Fatalf("claimed %d, want 3", len(got))
	}
	if got := deq(t, b, []string{"q"}, 100, time.Minute, 0); len(got) != 7 {
		t.Fatalf("claimed %d, want 7", len(got))
	}
}

// Dequeue across several queues only returns jobs from the requested ones,
// highest priority first across queues.
func testMultiQueue(t *testing.T, b broker.Broker) {
	a := enq(t, b, broker.EnqueueRequest{Queue: "a"})
	c := enq(t, b, broker.EnqueueRequest{Queue: "c"})
	bb := enq(t, b, broker.EnqueueRequest{Queue: "b", Priority: 9})
	got := deq(t, b, []string{"a", "b"}, 10, time.Minute, 0)
	if len(got) != 2 {
		t.Fatalf("claimed %d, want 2", len(got))
	}
	ids := map[string]bool{got[0].ID: true, got[1].ID: true}
	if !ids[a.ID] || !ids[bb.ID] || ids[c.ID] {
		t.Errorf("wrong jobs claimed: %v", ids)
	}
	if got[0].ID != bb.ID {
		t.Errorf("first = %s, want high-priority job from queue b", got[0].ID)
	}
	if got := deq(t, b, []string{"c"}, 10, time.Minute, 0); len(got) != 1 || got[0].ID != c.ID {
		t.Errorf("queue c claim = %+v", got)
	}
}

// After a lease expires another claim succeeds with Attempts=2/token=2, and
// every operation with the stale token 1 returns ErrLeaseLost.
func testLeaseExpiry(t *testing.T, b broker.Broker) {
	e := enq(t, b, broker.EnqueueRequest{})
	first := deq1(t, b, shortLease)
	if len(first) != 1 || first[0].LeaseToken != 1 || first[0].Attempts != 1 {
		t.Fatalf("first claim %+v", first)
	}
	if first[0].LeaseOwner != "w1" || first[0].LeaseExpiresAt.IsZero() {
		t.Errorf("lease fields not set: %+v", first[0])
	}
	if got := deq1(t, b, shortLease); len(got) != 0 {
		t.Fatal("job claimed twice while lease was live")
	}
	second := claimEventually(t, b, 5*time.Second, time.Minute)
	if second.ID != e.ID || second.Attempts != 2 || second.LeaseToken != 2 {
		t.Fatalf("reclaim %+v", second)
	}
	if err := b.Ack(bg(), e.ID, 1); !errors.Is(err, broker.ErrLeaseLost) {
		t.Errorf("stale ack: %v, want ErrLeaseLost", err)
	}
	if _, err := b.Heartbeat(bg(), e.ID, 1, time.Second); !errors.Is(err, broker.ErrLeaseLost) {
		t.Errorf("stale heartbeat: %v, want ErrLeaseLost", err)
	}
	if err := b.Nack(bg(), e.ID, 1, "x", true); !errors.Is(err, broker.ErrLeaseLost) {
		t.Errorf("stale nack: %v, want ErrLeaseLost", err)
	}
	if err := b.Ack(bg(), e.ID, 2); err != nil {
		t.Errorf("ack with current token: %v", err)
	}
	if j := mustGet(t, b, e.ID); j.State != job.StateSucceeded {
		t.Errorf("state = %s, want succeeded", j.State)
	}
}

// Heartbeat pushes the lease out so the job is not reclaimed; a wrong token
// is ErrLeaseLost.
func testHeartbeat(t *testing.T, b broker.Broker) {
	e := enq(t, b, broker.EnqueueRequest{})
	c := deq1(t, b, shortLease)
	if len(c) != 1 {
		t.Fatal("no claim")
	}
	time.Sleep(150 * time.Millisecond)
	hb, err := b.Heartbeat(bg(), e.ID, c[0].LeaseToken, 3*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if !hb.LeaseExpiresAt.After(c[0].LeaseExpiresAt) {
		t.Errorf("lease not extended: %v -> %v", c[0].LeaseExpiresAt, hb.LeaseExpiresAt)
	}
	time.Sleep(700 * time.Millisecond) // well past the original lease
	if got := deq1(t, b, shortLease); len(got) != 0 {
		t.Fatal("job reclaimed despite heartbeat")
	}
	if j := mustGet(t, b, e.ID); j.State != job.StateRunning {
		t.Errorf("state = %s, want running", j.State)
	}
	if _, err := b.Heartbeat(bg(), e.ID, 99, time.Second); !errors.Is(err, broker.ErrLeaseLost) {
		t.Errorf("wrong token heartbeat: %v", err)
	}
}

// Ack moves a running job to succeeded; a second ack is ErrLeaseLost.
func testAck(t *testing.T, b broker.Broker) {
	e := enq(t, b, broker.EnqueueRequest{})
	c := deq1(t, b, time.Minute)
	if err := b.Ack(bg(), e.ID, c[0].LeaseToken); err != nil {
		t.Fatal(err)
	}
	j := mustGet(t, b, e.ID)
	if j.State != job.StateSucceeded || j.CompletedAt == nil {
		t.Errorf("after ack: %+v", j)
	}
	if err := b.Ack(bg(), e.ID, c[0].LeaseToken); !errors.Is(err, broker.ErrLeaseLost) {
		t.Errorf("second ack: %v, want ErrLeaseLost", err)
	}
	if got := deq1(t, b, time.Minute); len(got) != 0 {
		t.Error("acked job was claimable")
	}
}

// Nack(retry=true) moves the job to scheduled with RunAt in the future; once
// RunAt passes it is claimable with Attempts=2, LeaseToken=2. Acking with
// token 1 after the reclaim is ErrLeaseLost.
func testNackRetry(t *testing.T, b broker.Broker) {
	e := enq(t, b, broker.EnqueueRequest{})
	c := deq1(t, b, time.Minute)
	before := time.Now()
	if err := b.Nack(bg(), e.ID, c[0].LeaseToken, "boom", true); err != nil {
		t.Fatal(err)
	}
	j := mustGet(t, b, e.ID)
	if j.State != job.StateScheduled || j.LastError != "boom" {
		t.Fatalf("after nack: %+v", j)
	}
	if !j.RunAt.After(before) {
		t.Errorf("RunAt %v not in the future of %v", j.RunAt, before)
	}
	if got := deq1(t, b, time.Minute); len(got) != 0 {
		t.Fatal("retry claimed before backoff elapsed")
	}
	again := claimEventually(t, b, 6*time.Second, time.Minute)
	if again.ID != e.ID || again.Attempts != 2 || again.LeaseToken != 2 {
		t.Fatalf("second claim %+v", again)
	}
	if err := b.Ack(bg(), e.ID, 1); !errors.Is(err, broker.ErrLeaseLost) {
		t.Errorf("ack token 1: %v, want ErrLeaseLost", err)
	}
	if err := b.Ack(bg(), e.ID, 2); err != nil {
		t.Errorf("ack token 2: %v", err)
	}
}

// Nack(retry=false) moves the job to dead and records the reason.
func testNackDead(t *testing.T, b broker.Broker) {
	e := enq(t, b, broker.EnqueueRequest{})
	c := deq1(t, b, time.Minute)
	if err := b.Nack(bg(), e.ID, c[0].LeaseToken, "fatal", false); err != nil {
		t.Fatal(err)
	}
	j := mustGet(t, b, e.ID)
	if j.State != job.StateDead || j.LastError != "fatal" {
		t.Errorf("after nack: %+v", j)
	}
	if got := deq1(t, b, time.Minute); len(got) != 0 {
		t.Error("dead job claimable")
	}
}

// Nack(retry=true) when attempts >= max_attempts goes to dead.
func testExhausted(t *testing.T, b broker.Broker) {
	e := enq(t, b, broker.EnqueueRequest{MaxAttempts: 1})
	c := deq1(t, b, time.Minute)
	if err := b.Nack(bg(), e.ID, c[0].LeaseToken, "again", true); err != nil {
		t.Fatal(err)
	}
	if j := mustGet(t, b, e.ID); j.State != job.StateDead {
		t.Errorf("state = %s, want dead", j.State)
	}
}

// A job whose lease expires on its last attempt ends up dead (via a reaper)
// and is never claimed again.
func testLeaseExpiryExhausted(t *testing.T, b broker.Broker) {
	e := enq(t, b, broker.EnqueueRequest{MaxAttempts: 1})
	c := deq1(t, b, shortLease)
	if len(c) != 1 {
		t.Fatal("no claim")
	}
	j := waitState(t, b, e.ID, job.StateDead, 8*time.Second)
	if j.Attempts != 1 {
		t.Errorf("attempts = %d", j.Attempts)
	}
	if got := deq1(t, b, time.Minute); len(got) != 0 {
		t.Error("dead job claimed")
	}
	if err := b.Ack(bg(), e.ID, c[0].LeaseToken); !errors.Is(err, broker.ErrLeaseLost) {
		t.Errorf("ack of dead job: %v, want ErrLeaseLost", err)
	}
}

// A long-polling Dequeue returns as soon as a job is enqueued, well before Wait.
func testLongPoll(t *testing.T, b broker.Broker) {
	type result struct {
		jobs []job.Job
		err  error
		took time.Duration
	}
	ch := make(chan result, 1)
	start := time.Now()
	go func() {
		js, err := b.Dequeue(bg(), broker.DequeueRequest{Queues: []string{"q"}, WorkerID: "w", Lease: time.Minute, Max: 5, Wait: 10 * time.Second})
		ch <- result{js, err, time.Since(start)}
	}()
	time.Sleep(300 * time.Millisecond)
	e := enq(t, b, broker.EnqueueRequest{})
	select {
	case r := <-ch:
		if r.err != nil {
			t.Fatal(r.err)
		}
		if len(r.jobs) != 1 || r.jobs[0].ID != e.ID {
			t.Fatalf("got %+v", r.jobs)
		}
		if r.took > 3*time.Second {
			t.Errorf("long-poll took %v", r.took)
		}
	case <-time.After(8 * time.Second):
		t.Fatal("long-poll did not return after enqueue")
	}
}

// With nothing to claim, Dequeue returns an empty result (no error) after
// roughly Wait, and honors context cancellation.
func testLongPollTimeout(t *testing.T, b broker.Broker) {
	start := time.Now()
	js, err := b.Dequeue(bg(), broker.DequeueRequest{Queues: []string{"q"}, WorkerID: "w", Lease: time.Minute, Max: 1, Wait: 400 * time.Millisecond})
	if err != nil || len(js) != 0 {
		t.Fatalf("got %v, %v", js, err)
	}
	if d := time.Since(start); d < 300*time.Millisecond || d > 3*time.Second {
		t.Errorf("waited %v, want about 400ms", d)
	}
	ctx, cancel := context.WithTimeout(bg(), 200*time.Millisecond)
	defer cancel()
	start = time.Now()
	js, _ = b.Dequeue(ctx, broker.DequeueRequest{Queues: []string{"q"}, WorkerID: "w", Lease: time.Minute, Max: 1, Wait: 20 * time.Second})
	if time.Since(start) > 3*time.Second {
		t.Errorf("dequeue ignored ctx cancellation (%v)", time.Since(start))
	}
	if len(js) != 0 {
		t.Errorf("got jobs %v", js)
	}
	// Either an empty result or a context error is acceptable.
}

// Stats reports counts per queue and state.
func testStats(t *testing.T, b broker.Broker) {
	for range 3 {
		enq(t, b, broker.EnqueueRequest{Queue: "a"})
	}
	enq(t, b, broker.EnqueueRequest{Queue: "b", Delay: time.Hour})
	got := deq(t, b, []string{"a"}, 2, time.Minute, 0)
	if len(got) != 2 {
		t.Fatalf("claimed %d", len(got))
	}
	if err := b.Ack(bg(), got[0].ID, got[0].LeaseToken); err != nil {
		t.Fatal(err)
	}
	if err := b.Nack(bg(), got[1].ID, got[1].LeaseToken, "x", false); err != nil {
		t.Fatal(err)
	}
	s, err := b.Stats(bg())
	if err != nil {
		t.Fatal(err)
	}
	check := func(q string, st job.State, want int) {
		if n := s.Counts[q][st]; n != want {
			t.Errorf("counts[%s][%s] = %d, want %d (all: %v)", q, st, n, want, s.Counts)
		}
	}
	check("a", job.StatePending, 1)
	check("a", job.StateSucceeded, 1)
	check("a", job.StateDead, 1)
	check("a", job.StateRunning, 0)
	check("b", job.StateScheduled, 1)
}

// 32 goroutines dequeue concurrently from 2,000 jobs and ack each; every job
// must be claimed exactly once (leases are long, so no legitimate reclaim).
func testNoDoubleClaims(t *testing.T, b broker.Broker) {
	const total, workers = 2000, 32
	var wg sync.WaitGroup
	sem := make(chan struct{}, 16)
	var enqErr atomic.Value
	for i := range total {
		wg.Add(1)
		sem <- struct{}{}
		go func() {
			defer wg.Done()
			defer func() { <-sem }()
			if _, err := b.Enqueue(bg(), broker.EnqueueRequest{Queue: "q", Type: "t", Payload: []byte(fmt.Sprint(i))}); err != nil {
				enqErr.Store(err)
			}
		}()
	}
	wg.Wait()
	if err, _ := enqErr.Load().(error); err != nil {
		t.Fatalf("enqueue: %v", err)
	}

	var (
		mu      sync.Mutex
		claimed = map[string]int{}
		done    atomic.Int64
		errs    = make(chan error, workers)
	)
	deadline := time.Now().Add(40 * time.Second)
	for w := range workers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			id := fmt.Sprintf("w%d", w)
			for done.Load() < total && time.Now().Before(deadline) {
				js, err := b.Dequeue(bg(), broker.DequeueRequest{Queues: []string{"q"}, WorkerID: id, Lease: 30 * time.Second, Max: 10, Wait: 100 * time.Millisecond})
				if err != nil {
					errs <- err
					return
				}
				for _, j := range js {
					mu.Lock()
					claimed[j.ID]++
					mu.Unlock()
					if j.Attempts != 1 || j.LeaseToken != 1 {
						errs <- fmt.Errorf("job %s attempts=%d token=%d", j.ID, j.Attempts, j.LeaseToken)
						return
					}
					if err := b.Ack(bg(), j.ID, j.LeaseToken); err != nil {
						errs <- fmt.Errorf("ack %s: %w", j.ID, err)
						return
					}
					done.Add(1)
				}
			}
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Error(err)
	}
	if len(claimed) != total {
		t.Errorf("distinct jobs claimed = %d, want %d", len(claimed), total)
	}
	var dup []string
	for id, n := range claimed {
		if n != 1 {
			dup = append(dup, fmt.Sprintf("%s x%d", id, n))
		}
	}
	sort.Strings(dup)
	if len(dup) > 0 {
		t.Errorf("%d jobs claimed more than once: %v", len(dup), dup[:min(len(dup), 5)])
	}
	s, err := b.Stats(bg())
	if err != nil {
		t.Fatal(err)
	}
	if n := s.Counts["q"][job.StateSucceeded]; n != total {
		t.Errorf("succeeded = %d, want %d", n, total)
	}
}
