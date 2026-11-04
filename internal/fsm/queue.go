// Package fsm is the deterministic queue state machine replicated by Raft.
//
// Determinism rules: Apply never reads the clock, never uses randomness, and
// never lets map iteration order influence state or results. Every time comes
// from the command, which the leader fills in before proposing it.
package fsm

import (
	"encoding/json"
	"fmt"
	"sort"
	"sync"
	"time"

	"github.com/supraj221/goqueue/internal/broker"
	"github.com/supraj221/goqueue/internal/job"
	"github.com/supraj221/goqueue/internal/raft"
)

// Result is what Apply returns for every command.
type Result struct {
	Job     *job.Job  // Enqueue, Heartbeat
	Jobs    []job.Job // Claim
	Created bool      // Enqueue: false when an idempotency key matched
	N       int       // Purge: jobs removed; ExpireLeases/PromoteScheduled: jobs moved
	Err     error     // nil, or one of broker.ErrNotFound, ErrLeaseLost, ErrInvalid
}

type runItem struct {
	j   *job.Job
	idx int
}

// Queue implements raft.StateMachine.
type Queue struct {
	mu sync.RWMutex

	jobs  map[string]*job.Job
	idem  map[string]string // idempotency key -> job ID
	ready map[string]*heapT[*job.Job]
	sched *heapT[*job.Job]
	run   *heapT[*runItem]
	runBy map[string]*runItem

	counts map[string]map[job.State]int

	subMu   sync.Mutex
	subs    map[int]chan struct{}
	nextSub int
}

var _ raft.StateMachine = (*Queue)(nil)

// New returns an empty queue.
func New() *Queue {
	q := &Queue{subs: map[int]chan struct{}{}}
	q.reset()
	return q
}

func (q *Queue) reset() {
	q.jobs = map[string]*job.Job{}
	q.idem = map[string]string{}
	q.ready = map[string]*heapT[*job.Job]{}
	q.sched = &heapT[*job.Job]{less: schedLess}
	q.run = &heapT[*runItem]{
		less:  func(a, b *runItem) bool { return runLess(a.j, b.j) },
		onIdx: func(x *runItem, i int) { x.idx = i },
	}
	q.runBy = map[string]*runItem{}
	q.counts = map[string]map[job.State]int{}
}

func schedLess(a, b *job.Job) bool {
	if !a.RunAt.Equal(b.RunAt) {
		return a.RunAt.Before(b.RunAt)
	}
	return a.ID < b.ID
}

func runLess(a, b *job.Job) bool {
	if !a.LeaseExpiresAt.Equal(b.LeaseExpiresAt) {
		return a.LeaseExpiresAt.Before(b.LeaseExpiresAt)
	}
	return a.ID < b.ID
}

func (q *Queue) readyHeap(queue string) *heapT[*job.Job] {
	h := q.ready[queue]
	if h == nil {
		h = &heapT[*job.Job]{less: job.Less}
		q.ready[queue] = h
	}
	return h
}

// ---- counters ----

func (q *Queue) count(queue string, s job.State, d int) {
	m := q.counts[queue]
	if m == nil {
		m = map[job.State]int{}
		q.counts[queue] = m
	}
	m[s] += d
	if m[s] == 0 {
		delete(m, s)
		if len(m) == 0 {
			delete(q.counts, queue)
		}
	}
}

// setState changes j.State and keeps counters in step. It does not touch heaps.
func (q *Queue) setState(j *job.Job, s job.State) {
	q.count(j.Queue, j.State, -1)
	j.State = s
	q.count(j.Queue, s, 1)
}

// ---- Apply ----

// Apply implements raft.StateMachine. It always returns a Result.
func (q *Queue) Apply(e raft.LogEntry) any {
	var env envelope
	if err := json.Unmarshal(e.Data, &env); err != nil {
		return Result{Err: fmt.Errorf("%w: malformed command: %v", broker.ErrInvalid, err)}
	}
	if env.V != Version {
		return Result{Err: fmt.Errorf("%w: unsupported command version %d", broker.ErrInvalid, env.V)}
	}
	q.mu.Lock()
	defer q.mu.Unlock()
	var (
		res  Result
		wake bool
	)
	switch env.Op {
	case OpEnqueue:
		var c Enqueue
		if res.Err = decode(e.Data, &c); res.Err == nil {
			res, wake = q.enqueue(c)
		}
	case OpClaim:
		var c Claim
		if res.Err = decode(e.Data, &c); res.Err == nil {
			res, wake = q.claim(c)
		}
	case OpHeartbeat:
		var c Heartbeat
		if res.Err = decode(e.Data, &c); res.Err == nil {
			res = q.heartbeat(c)
		}
	case OpAck:
		var c Ack
		if res.Err = decode(e.Data, &c); res.Err == nil {
			res = q.ack(c)
		}
	case OpNack:
		var c Nack
		if res.Err = decode(e.Data, &c); res.Err == nil {
			res, wake = q.nack(c)
		}
	case OpExpireLeases:
		var c ExpireLeases
		if res.Err = decode(e.Data, &c); res.Err == nil {
			res.N = q.expire(c.Now)
			wake = res.N > 0
		}
	case OpPromoteScheduled:
		var c PromoteScheduled
		if res.Err = decode(e.Data, &c); res.Err == nil {
			res.N = q.promote(c.Now)
			wake = res.N > 0
		}
	case OpPurge:
		var c Purge
		if res.Err = decode(e.Data, &c); res.Err == nil {
			res.N = q.purge(c.Before)
		}
	default:
		res.Err = fmt.Errorf("%w: unknown op %q", broker.ErrInvalid, env.Op)
	}
	if wake {
		q.notify()
	}
	return res
}

func decode(data []byte, v any) error {
	if err := json.Unmarshal(data, v); err != nil {
		return fmt.Errorf("%w: malformed command: %v", broker.ErrInvalid, err)
	}
	return nil
}

func (q *Queue) enqueue(c Enqueue) (Result, bool) {
	in := c.Job
	if in.ID == "" {
		return Result{Err: fmt.Errorf("%w: job id is required", broker.ErrInvalid)}, false
	}
	if err := job.ValidateName("queue", in.Queue); err != nil {
		return Result{Err: err}, false
	}
	if err := job.ValidateName("type", in.Type); err != nil {
		return Result{Err: err}, false
	}
	if err := job.ValidatePayload(in.Payload); err != nil {
		return Result{Err: err}, false
	}
	if in.IdempotencyKey != "" {
		if id, ok := q.idem[in.IdempotencyKey]; ok {
			if ex, ok := q.jobs[id]; ok {
				cp := *ex
				return Result{Job: &cp}, false
			}
		}
	}
	if ex, ok := q.jobs[in.ID]; ok { // the same proposal applied twice
		cp := *ex
		return Result{Job: &cp}, false
	}
	j := new(job.Job)
	*j = in
	if j.MaxAttempts <= 0 {
		j.MaxAttempts = job.DefaultMaxAttempts
	}
	if j.RunAt.IsZero() {
		j.RunAt = j.CreatedAt
	}
	j.UpdatedAt = j.CreatedAt
	j.Attempts = 0
	j.LeaseOwner, j.LeaseToken, j.LeaseExpiresAt = "", 0, time.Time{}
	j.LastError, j.CompletedAt = "", nil
	q.jobs[j.ID] = j
	if j.IdempotencyKey != "" {
		q.idem[j.IdempotencyKey] = j.ID
	}
	wake := false
	if j.RunAt.After(j.CreatedAt) {
		j.State = job.StateScheduled
		q.sched.push(j)
	} else {
		j.State = job.StatePending
		q.readyHeap(j.Queue).push(j)
		wake = true
	}
	q.count(j.Queue, j.State, 1)
	cp := *j
	return Result{Job: &cp, Created: true}, wake
}

// promote moves scheduled jobs with RunAt <= now to the ready heaps.
func (q *Queue) promote(now time.Time) int {
	n := 0
	for q.sched.len() > 0 {
		j := q.sched.peek()
		if j.RunAt.After(now) {
			break
		}
		q.sched.pop()
		q.setState(j, job.StatePending)
		j.UpdatedAt = now
		q.readyHeap(j.Queue).push(j)
		n++
	}
	return n
}

// expire reclaims running jobs whose lease expired before now.
func (q *Queue) expire(now time.Time) int {
	n := 0
	for q.run.len() > 0 {
		it := q.run.peek()
		j := it.j
		if !j.LeaseExpiresAt.Before(now) {
			break
		}
		q.run.pop()
		delete(q.runBy, j.ID)
		j.LeaseOwner = ""
		j.LeaseExpiresAt = time.Time{}
		j.LastError = "lease expired"
		j.UpdatedAt = now
		if j.Attempts >= j.MaxAttempts {
			q.setState(j, job.StateDead)
			t := now
			j.CompletedAt = &t
		} else {
			q.setState(j, job.StatePending)
			q.readyHeap(j.Queue).push(j)
		}
		n++
	}
	return n
}

func (q *Queue) claim(c Claim) (Result, bool) {
	if len(c.Queues) == 0 || c.Max < 1 || c.WorkerID == "" || c.Lease <= 0 {
		return Result{Err: fmt.Errorf("%w: bad claim", broker.ErrInvalid)}, false
	}
	wake := q.promote(c.Now) > 0
	if q.expire(c.Now) > 0 {
		wake = true
	}
	var out []job.Job
	for len(out) < c.Max {
		var best *job.Job
		var bestH *heapT[*job.Job]
		for _, name := range c.Queues {
			h := q.ready[name]
			if h == nil || h.len() == 0 {
				continue
			}
			if top := h.peek(); best == nil || job.Less(top, best) {
				best, bestH = top, h
			}
		}
		if best == nil {
			break
		}
		bestH.pop()
		best.Attempts++
		best.LeaseToken = uint64(best.Attempts)
		best.LeaseOwner = c.WorkerID
		best.LeaseExpiresAt = c.Now.Add(c.Lease)
		best.UpdatedAt = c.Now
		q.setState(best, job.StateRunning)
		it := &runItem{j: best}
		q.runBy[best.ID] = it
		q.run.push(it)
		out = append(out, *best)
	}
	return Result{Jobs: out}, wake
}

// leased returns the run item if id is running with the given token.
func (q *Queue) leased(id string, token uint64) (*runItem, error) {
	j, ok := q.jobs[id]
	if !ok {
		return nil, broker.ErrNotFound
	}
	if j.State != job.StateRunning || j.LeaseToken != token {
		return nil, broker.ErrLeaseLost
	}
	return q.runBy[id], nil
}

func (q *Queue) heartbeat(c Heartbeat) Result {
	if c.Extend <= 0 {
		return Result{Err: fmt.Errorf("%w: extend must be positive", broker.ErrInvalid)}
	}
	it, err := q.leased(c.ID, c.Token)
	if err != nil {
		return Result{Err: err}
	}
	it.j.LeaseExpiresAt = c.Now.Add(c.Extend)
	it.j.UpdatedAt = c.Now
	q.run.fix(it.idx)
	cp := *it.j
	return Result{Job: &cp}
}

func (q *Queue) finish(it *runItem) {
	q.run.remove(it.idx)
	delete(q.runBy, it.j.ID)
	it.j.LeaseOwner = ""
	it.j.LeaseExpiresAt = time.Time{}
}

func (q *Queue) ack(c Ack) Result {
	it, err := q.leased(c.ID, c.Token)
	if err != nil {
		return Result{Err: err}
	}
	j := it.j
	q.finish(it)
	q.setState(j, job.StateSucceeded)
	j.Payload = nil // a finished job's payload is dead weight in every snapshot
	j.UpdatedAt = c.Now
	t := c.Now
	j.CompletedAt = &t
	return Result{}
}

func (q *Queue) nack(c Nack) (Result, bool) {
	it, err := q.leased(c.ID, c.Token)
	if err != nil {
		return Result{Err: err}, false
	}
	j := it.j
	q.finish(it)
	j.LastError = c.Reason
	j.UpdatedAt = c.Now
	if c.Retry && j.Attempts < j.MaxAttempts {
		if c.RetryAt.After(c.Now) {
			j.RunAt = c.RetryAt
			q.setState(j, job.StateScheduled)
			q.sched.push(j)
			return Result{}, false
		}
		j.RunAt = c.Now
		q.setState(j, job.StatePending)
		q.readyHeap(j.Queue).push(j)
		return Result{}, true
	}
	q.setState(j, job.StateDead)
	t := c.Now
	j.CompletedAt = &t
	return Result{}, false
}

func (q *Queue) purge(before time.Time) int {
	n := 0
	for id, j := range q.jobs {
		if (j.State == job.StateSucceeded || j.State == job.StateDead) &&
			j.CompletedAt != nil && j.CompletedAt.Before(before) {
			q.count(j.Queue, j.State, -1)
			if j.IdempotencyKey != "" && q.idem[j.IdempotencyKey] == id {
				delete(q.idem, j.IdempotencyKey)
			}
			delete(q.jobs, id) // the set removed does not depend on iteration order
			n++
		}
	}
	return n
}

// ---- snapshot ----

type snapshotDoc struct {
	V    int               `json:"v"`
	Jobs []*job.Job        `json:"jobs"`
	Idem map[string]string `json:"idem"`
}

// Snapshot serializes the full state. Output is byte-for-byte deterministic:
// jobs are sorted by ID and map keys are sorted by encoding/json.
func (q *Queue) Snapshot() ([]byte, error) {
	q.mu.RLock()
	defer q.mu.RUnlock()
	doc := snapshotDoc{V: Version, Jobs: make([]*job.Job, 0, len(q.jobs)), Idem: q.idem}
	for _, j := range q.jobs {
		doc.Jobs = append(doc.Jobs, j)
	}
	sort.Slice(doc.Jobs, func(a, b int) bool { return doc.Jobs[a].ID < doc.Jobs[b].ID })
	return json.Marshal(doc)
}

// Restore replaces the full state with a snapshot and rebuilds the heaps.
func (q *Queue) Restore(data []byte) error {
	var doc snapshotDoc
	if err := json.Unmarshal(data, &doc); err != nil {
		return fmt.Errorf("fsm: restore: %w", err)
	}
	if doc.V != Version {
		return fmt.Errorf("fsm: restore: unsupported snapshot version %d", doc.V)
	}
	q.mu.Lock()
	q.reset()
	for _, j := range doc.Jobs {
		q.jobs[j.ID] = j
		q.count(j.Queue, j.State, 1)
		switch j.State {
		case job.StatePending:
			q.readyHeap(j.Queue).push(j)
		case job.StateScheduled:
			q.sched.push(j)
		case job.StateRunning:
			it := &runItem{j: j}
			q.runBy[j.ID] = it
			q.run.push(it)
		}
	}
	if doc.Idem != nil {
		q.idem = doc.Idem
	}
	q.mu.Unlock()
	q.notify()
	return nil
}

// ---- local reads (safe concurrently with Apply) ----

// Get returns a copy of the job with the given ID.
func (q *Queue) Get(id string) (job.Job, bool) {
	q.mu.RLock()
	defer q.mu.RUnlock()
	j, ok := q.jobs[id]
	if !ok {
		return job.Job{}, false
	}
	return *j, true
}

// Stats returns job counts by queue and state.
func (q *Queue) Stats() broker.Stats {
	q.mu.RLock()
	defer q.mu.RUnlock()
	out := broker.Stats{Counts: make(map[string]map[job.State]int, len(q.counts))}
	for name, m := range q.counts {
		cp := make(map[job.State]int, len(m))
		for s, n := range m {
			cp[s] = n
		}
		out.Counts[name] = cp
	}
	return out
}

// Len returns the number of jobs held.
func (q *Queue) Len() int {
	q.mu.RLock()
	defer q.mu.RUnlock()
	return len(q.jobs)
}

// HasClaimable reports whether a Claim at now might return a job from queues.
// It may return true when the due job belongs to another queue; the caller then
// just gets an empty claim.
func (q *Queue) HasClaimable(queues []string, now time.Time) bool {
	q.mu.RLock()
	defer q.mu.RUnlock()
	for _, name := range queues {
		if h := q.ready[name]; h != nil && h.len() > 0 {
			return true
		}
	}
	if q.sched.len() > 0 && !q.sched.peek().RunAt.After(now) {
		return true
	}
	if q.run.len() > 0 && q.run.peek().j.LeaseExpiresAt.Before(now) {
		return true
	}
	return false
}

// ---- subscriptions ----

// Subscribe returns a channel that receives a (coalesced) wake-up after changes
// that may make a job claimable, and a function that cancels the subscription.
func (q *Queue) Subscribe() (<-chan struct{}, func()) {
	ch := make(chan struct{}, 1)
	q.subMu.Lock()
	id := q.nextSub
	q.nextSub++
	q.subs[id] = ch
	q.subMu.Unlock()
	return ch, func() {
		q.subMu.Lock()
		delete(q.subs, id)
		q.subMu.Unlock()
	}
}

func (q *Queue) notify() {
	q.subMu.Lock()
	for _, ch := range q.subs {
		select {
		case ch <- struct{}{}:
		default:
		}
	}
	q.subMu.Unlock()
}

// Due reports whether a PromoteScheduled (scheduled) or ExpireLeases (expired)
// command at now would change anything. The leader's reaper uses it to avoid
// writing no-op commands to the replicated log.
func (q *Queue) Due(now time.Time) (scheduled, expired bool) {
	q.mu.RLock()
	defer q.mu.RUnlock()
	scheduled = q.sched.len() > 0 && !q.sched.peek().RunAt.After(now)
	expired = q.run.len() > 0 && q.run.peek().j.LeaseExpiresAt.Before(now)
	return
}

// HasTerminalBefore reports whether any succeeded or dead job completed before t.
// It is O(n) and used only by the purge loop to skip empty purges.
func (q *Queue) HasTerminalBefore(t time.Time) bool {
	q.mu.RLock()
	defer q.mu.RUnlock()
	for _, j := range q.jobs {
		if (j.State == job.StateSucceeded || j.State == job.StateDead) && j.CompletedAt != nil && j.CompletedAt.Before(t) {
			return true
		}
	}
	return false
}
