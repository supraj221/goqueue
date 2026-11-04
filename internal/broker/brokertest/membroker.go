package brokertest

import (
	"context"
	"fmt"
	"sort"
	"sync"
	"time"

	"github.com/supraj221/goqueue/internal/broker"
	"github.com/supraj221/goqueue/internal/job"
)

// MemBroker is a small, correct in-memory broker.Broker. It exists so the
// conformance suite itself can be tested without a database, and so the api
// and client tests have a real backend to run against. State is evaluated
// lazily from the wall clock on each call, so it has no background goroutines.
type MemBroker struct {
	mu     sync.Mutex
	jobs   map[string]*job.Job
	idem   map[string]string
	change chan struct{} // closed and replaced whenever a job may have become claimable
}

var _ broker.Broker = (*MemBroker)(nil)

// NewMemBroker returns an empty in-memory broker.
func NewMemBroker() *MemBroker {
	return &MemBroker{jobs: map[string]*job.Job{}, idem: map[string]string{}, change: make(chan struct{})}
}

func (m *MemBroker) signal() {
	close(m.change)
	m.change = make(chan struct{})
}

// sweep applies time-driven transitions. Callers hold m.mu.
func (m *MemBroker) sweep(now time.Time) {
	for _, j := range m.jobs {
		switch {
		case j.State == job.StateScheduled && !j.RunAt.After(now):
			j.State = job.StatePending
		case j.State == job.StateRunning && !j.LeaseExpiresAt.After(now) && j.Attempts >= j.MaxAttempts:
			j.State = job.StateDead
			j.LastError = "lease expired"
			t := now
			j.CompletedAt = &t
			j.UpdatedAt = now
		}
	}
}

func (m *MemBroker) Enqueue(_ context.Context, req broker.EnqueueRequest) (broker.EnqueueResult, error) {
	if err := req.Validate(); err != nil {
		return broker.EnqueueResult{}, err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if req.IdempotencyKey != "" {
		if id, ok := m.idem[req.IdempotencyKey]; ok {
			m.sweep(time.Now())
			return broker.EnqueueResult{Job: *m.jobs[id]}, nil
		}
	}
	now := time.Now()
	runAt := now
	if !req.RunAt.IsZero() {
		runAt = req.RunAt
	} else if req.Delay > 0 {
		runAt = now.Add(req.Delay)
	}
	maxAttempts := req.MaxAttempts
	if maxAttempts == 0 {
		maxAttempts = job.DefaultMaxAttempts
	}
	j := &job.Job{
		ID: job.NewID(now), Queue: req.Queue, Type: req.Type, Payload: req.Payload, Priority: req.Priority,
		State: job.StatePending, MaxAttempts: maxAttempts, RunAt: runAt, IdempotencyKey: req.IdempotencyKey,
		CreatedAt: now, UpdatedAt: now,
	}
	if runAt.After(now) {
		j.State = job.StateScheduled
	}
	m.jobs[j.ID] = j
	if j.IdempotencyKey != "" {
		m.idem[j.IdempotencyKey] = j.ID
	}
	m.signal()
	return broker.EnqueueResult{Job: *j, Created: true}, nil
}

func (m *MemBroker) claim(req broker.DequeueRequest) []job.Job {
	now := time.Now()
	m.sweep(now)
	queues := map[string]bool{}
	for _, q := range req.Queues {
		queues[q] = true
	}
	var cands []*job.Job
	for _, j := range m.jobs {
		if !queues[j.Queue] {
			continue
		}
		ready := j.State == job.StatePending ||
			(j.State == job.StateRunning && !j.LeaseExpiresAt.After(now) && j.Attempts < j.MaxAttempts)
		if ready {
			cands = append(cands, j)
		}
	}
	sort.Slice(cands, func(a, b int) bool { return job.Less(cands[a], cands[b]) })
	if len(cands) > req.Max {
		cands = cands[:req.Max]
	}
	out := make([]job.Job, 0, len(cands))
	for _, j := range cands {
		j.State = job.StateRunning
		j.Attempts++
		j.LeaseToken = uint64(j.Attempts)
		j.LeaseOwner = req.WorkerID
		j.LeaseExpiresAt = now.Add(req.Lease)
		j.UpdatedAt = now
		out = append(out, *j)
	}
	return out
}

func (m *MemBroker) Dequeue(ctx context.Context, req broker.DequeueRequest) ([]job.Job, error) {
	if err := req.Validate(); err != nil {
		return nil, err
	}
	deadline := time.Now().Add(req.Wait)
	for {
		m.mu.Lock()
		jobs := m.claim(req)
		ch := m.change
		m.mu.Unlock()
		if len(jobs) > 0 || req.Wait <= 0 {
			return jobs, nil
		}
		remaining := time.Until(deadline)
		if remaining <= 0 {
			return nil, nil
		}
		timer := time.NewTimer(min(remaining, 20*time.Millisecond))
		select {
		case <-ctx.Done():
			timer.Stop()
			return nil, ctx.Err()
		case <-ch:
			timer.Stop()
		case <-timer.C:
		}
	}
}

// fenced looks up a job and checks the lease; callers hold m.mu.
func (m *MemBroker) fenced(id string, token uint64) (*job.Job, error) {
	m.sweep(time.Now())
	j, ok := m.jobs[id]
	if !ok {
		return nil, broker.ErrNotFound
	}
	if j.State != job.StateRunning || j.LeaseToken != token {
		return nil, broker.ErrLeaseLost
	}
	return j, nil
}

func (m *MemBroker) Heartbeat(_ context.Context, id string, token uint64, extend time.Duration) (job.Job, error) {
	if extend <= 0 {
		return job.Job{}, fmt.Errorf("%w: extend must be positive", broker.ErrInvalid)
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	j, err := m.fenced(id, token)
	if err != nil {
		return job.Job{}, err
	}
	now := time.Now()
	j.LeaseExpiresAt = now.Add(extend)
	j.UpdatedAt = now
	return *j, nil
}

func (m *MemBroker) Ack(_ context.Context, id string, token uint64) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	j, err := m.fenced(id, token)
	if err != nil {
		return err
	}
	now := time.Now()
	j.State, j.CompletedAt, j.UpdatedAt = job.StateSucceeded, &now, now
	return nil
}

func (m *MemBroker) Nack(_ context.Context, id string, token uint64, reason string, retry bool) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	j, err := m.fenced(id, token)
	if err != nil {
		return err
	}
	now := time.Now()
	j.LastError, j.UpdatedAt = reason, now
	if retry && j.Attempts < j.MaxAttempts {
		j.State = job.StateScheduled
		j.RunAt = now.Add(job.DefaultBackoff(j.Attempts))
		m.signal()
		return nil
	}
	j.State, j.CompletedAt = job.StateDead, &now
	return nil
}

func (m *MemBroker) Get(_ context.Context, id string) (job.Job, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.sweep(time.Now())
	j, ok := m.jobs[id]
	if !ok {
		return job.Job{}, broker.ErrNotFound
	}
	return *j, nil
}

func (m *MemBroker) Stats(context.Context) (broker.Stats, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.sweep(time.Now())
	s := broker.Stats{Counts: map[string]map[job.State]int{}}
	for _, j := range m.jobs {
		if s.Counts[j.Queue] == nil {
			s.Counts[j.Queue] = map[job.State]int{}
		}
		s.Counts[j.Queue][j.State]++
	}
	return s, nil
}

func (m *MemBroker) Close() error { return nil }
