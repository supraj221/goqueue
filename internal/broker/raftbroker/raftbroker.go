// Package raftbroker implements broker.Broker on top of a Raft node and the
// deterministic queue state machine in internal/fsm.
//
// Writes are built on the leader (which supplies IDs, timestamps and backoff
// delays, so the state machine stays deterministic), proposed through Raft, and
// answered with the state machine's Apply result. Reads go through ReadIndex
// and then read the local state machine, which makes them linearizable.
package raftbroker

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/supraj221/goqueue/internal/broker"
	"github.com/supraj221/goqueue/internal/fsm"
	"github.com/supraj221/goqueue/internal/job"
	"github.com/supraj221/goqueue/internal/raft"
)

// Consensus is the subset of *raft.Node that the broker needs.
type Consensus interface {
	Propose(ctx context.Context, data []byte) (any, uint64, error)
	ReadIndex(ctx context.Context) error
	Status() raft.Status
	LeaderChanges() <-chan bool
}

var _ Consensus = (*raft.Node)(nil)

// Options configures a Broker.
type Options struct {
	// PeerAddrs maps node ID to the node's HTTP base URL; it turns
	// Status().LeaderID into NotLeaderError.LeaderAddr.
	PeerAddrs map[string]string
	Logger    *slog.Logger
	// ReaperInterval is how often the leader promotes due scheduled jobs and
	// reclaims expired leases. Default 250ms.
	ReaperInterval time.Duration
	// Retention is how long finished (succeeded or dead) jobs are kept.
	// Default 1h.
	Retention time.Duration
	// PurgeInterval is how often the leader purges finished jobs. Default
	// 1 minute.
	PurgeInterval time.Duration
	// ClaimConcurrency bounds how many Claim commands are proposed at once.
	// Fewer in-flight claims means more jobs per claim and fewer log entries
	// when many workers are polling. Default 4.
	ClaimConcurrency int
}

const pollTick = 100 * time.Millisecond

// Broker implements broker.Broker.
type Broker struct {
	c    Consensus
	q    *fsm.Queue
	opts Options
	log  *slog.Logger

	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup

	claimSem chan struct{}
	metrics  *metrics
}

var _ broker.Broker = (*Broker)(nil)

// New creates a Broker and starts its leader-watching goroutine. q must be the
// state machine that c applies committed entries to.
func New(c Consensus, q *fsm.Queue, opts Options) *Broker {
	if opts.ReaperInterval <= 0 {
		opts.ReaperInterval = 250 * time.Millisecond
	}
	if opts.Retention <= 0 {
		opts.Retention = time.Hour
	}
	if opts.PurgeInterval <= 0 {
		opts.PurgeInterval = time.Minute
	}
	if opts.ClaimConcurrency <= 0 {
		opts.ClaimConcurrency = 4
	}
	if opts.Logger == nil {
		opts.Logger = slog.Default()
	}
	ctx, cancel := context.WithCancel(context.Background())
	b := &Broker{c: c, q: q, opts: opts, log: opts.Logger, ctx: ctx, cancel: cancel,
		claimSem: make(chan struct{}, opts.ClaimConcurrency), metrics: newMetrics(c)}
	b.wg.Add(1)
	go b.watchLeadership()
	return b
}

// Close stops the background loops. It does not stop the Consensus.
func (b *Broker) Close() error {
	b.cancel()
	b.wg.Wait()
	return nil
}

// ---- error mapping ----

func (b *Broker) leaderAddr() string {
	st := b.c.Status()
	if st.LeaderID == "" {
		return ""
	}
	return b.opts.PeerAddrs[st.LeaderID]
}

func (b *Broker) notLeader() error { return &broker.NotLeaderError{LeaderAddr: b.leaderAddr()} }

func (b *Broker) mapErr(err error) error {
	switch {
	case err == nil:
		return nil
	case errors.Is(err, raft.ErrNotLeader):
		return b.notLeader()
	case errors.Is(err, raft.ErrLeadershipLost), errors.Is(err, raft.ErrStopped):
		return fmt.Errorf("%w: %v", broker.ErrUnavailable, err)
	default:
		return err
	}
}

func (b *Broker) isLeader() bool { return b.c.Status().Role == raft.Leader }

// propose sends an encoded command through Raft and returns the Apply result.
func (b *Broker) propose(ctx context.Context, data []byte) (fsm.Result, error) {
	start := time.Now()
	res, _, err := b.c.Propose(ctx, data)
	b.metrics.propose.Observe(time.Since(start).Seconds())
	if err != nil {
		return fsm.Result{}, b.mapErr(err)
	}
	r, ok := res.(fsm.Result)
	if !ok {
		return fsm.Result{}, fmt.Errorf("raftbroker: unexpected apply result %T", res)
	}
	return r, nil
}

func now() time.Time { return time.Now().UTC() }

// ---- writes ----

func (b *Broker) Enqueue(ctx context.Context, req broker.EnqueueRequest) (broker.EnqueueResult, error) {
	if err := req.Validate(); err != nil {
		return broker.EnqueueResult{}, err
	}
	t := now()
	runAt := t
	switch {
	case !req.RunAt.IsZero():
		runAt = req.RunAt.UTC()
	case req.Delay > 0:
		runAt = t.Add(req.Delay)
	}
	max := req.MaxAttempts
	if max == 0 {
		max = job.DefaultMaxAttempts
	}
	j := job.Job{
		ID: job.NewID(t), Queue: req.Queue, Type: req.Type, Payload: req.Payload,
		Priority: req.Priority, State: job.StatePending, MaxAttempts: max, RunAt: runAt,
		IdempotencyKey: req.IdempotencyKey, CreatedAt: t, UpdatedAt: t,
	}
	r, err := b.propose(ctx, fsm.EncodeEnqueue(fsm.Enqueue{Job: j}))
	if err != nil {
		return broker.EnqueueResult{}, err
	}
	if r.Err != nil {
		return broker.EnqueueResult{}, r.Err
	}
	return broker.EnqueueResult{Job: *r.Job, Created: r.Created}, nil
}

func (b *Broker) Heartbeat(ctx context.Context, id string, token uint64, extend time.Duration) (job.Job, error) {
	if extend <= 0 {
		return job.Job{}, fmt.Errorf("%w: extend must be positive", broker.ErrInvalid)
	}
	r, err := b.propose(ctx, fsm.EncodeHeartbeat(fsm.Heartbeat{ID: id, Token: token, Now: now(), Extend: extend}))
	if err != nil {
		return job.Job{}, err
	}
	if r.Err != nil {
		return job.Job{}, r.Err
	}
	return *r.Job, nil
}

func (b *Broker) Ack(ctx context.Context, id string, token uint64) error {
	r, err := b.propose(ctx, fsm.EncodeAck(fsm.Ack{ID: id, Token: token, Now: now()}))
	if err != nil {
		return err
	}
	return r.Err
}

func (b *Broker) Nack(ctx context.Context, id string, token uint64, reason string, retry bool) error {
	t := now()
	cmd := fsm.Nack{ID: id, Token: token, Now: t, Reason: reason, Retry: retry}
	if retry {
		// Attempts come from the local state machine; the replicated Apply is
		// authoritative about whether the token is still valid.
		attempts := 1
		if j, ok := b.q.Get(id); ok && j.Attempts > 0 {
			attempts = j.Attempts
		}
		cmd.RetryAt = t.Add(job.DefaultBackoff(attempts))
	}
	r, err := b.propose(ctx, fsm.EncodeNack(cmd))
	if err != nil {
		return err
	}
	return r.Err
}

// Dequeue claims jobs, long-polling when req.Wait > 0.
func (b *Broker) Dequeue(ctx context.Context, req broker.DequeueRequest) ([]job.Job, error) {
	if err := req.Validate(); err != nil {
		return nil, err
	}
	sub, cancel := b.q.Subscribe() // subscribe before checking, so no wake-up is lost
	defer cancel()
	deadline := time.Now().Add(req.Wait)
	for {
		if !b.isLeader() {
			return nil, b.notLeader()
		}
		if b.q.HasClaimable(req.Queues, time.Now()) {
			jobs, err := b.claim(ctx, req)
			if err != nil {
				return nil, err
			}
			if len(jobs) > 0 {
				return jobs, nil
			}
		}
		remaining := time.Until(deadline)
		if req.Wait <= 0 || remaining <= 0 {
			return nil, nil
		}
		timer := time.NewTimer(min(pollTick, remaining))
		select {
		case <-sub:
		case <-timer.C:
		case <-ctx.Done():
			timer.Stop()
			return nil, ctx.Err()
		case <-b.ctx.Done():
			timer.Stop()
			return nil, fmt.Errorf("%w: broker closed", broker.ErrUnavailable)
		}
		timer.Stop()
	}
}

// claim proposes one Claim command. It waits for a slot first, then re-checks
// the local state machine: by the time a slot frees up, earlier claims have
// usually taken the jobs, and proposing an empty claim would only add log entries.
func (b *Broker) claim(ctx context.Context, req broker.DequeueRequest) ([]job.Job, error) {
	select {
	case b.claimSem <- struct{}{}:
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-b.ctx.Done():
		return nil, fmt.Errorf("%w: broker closed", broker.ErrUnavailable)
	}
	defer func() { <-b.claimSem }()
	if !b.q.HasClaimable(req.Queues, time.Now()) {
		return nil, nil
	}
	r, err := b.propose(ctx, fsm.EncodeClaim(fsm.Claim{
		Queues: req.Queues, WorkerID: req.WorkerID, Max: req.Max, Now: now(), Lease: req.Lease,
	}))
	if err != nil {
		return nil, err
	}
	return r.Jobs, r.Err
}

// ---- linearizable reads ----

func (b *Broker) Get(ctx context.Context, id string) (job.Job, error) {
	if err := b.c.ReadIndex(ctx); err != nil {
		return job.Job{}, b.mapErr(err)
	}
	j, ok := b.q.Get(id)
	if !ok {
		return job.Job{}, broker.ErrNotFound
	}
	return j, nil
}

func (b *Broker) Stats(ctx context.Context) (broker.Stats, error) {
	if err := b.c.ReadIndex(ctx); err != nil {
		return broker.Stats{}, b.mapErr(err)
	}
	return b.q.Stats(), nil
}

// ---- leader loops ----

func (b *Broker) watchLeadership() {
	defer b.wg.Done()
	var stop context.CancelFunc
	var loops sync.WaitGroup
	start := func() {
		if stop != nil {
			return
		}
		var ctx context.Context
		ctx, stop = context.WithCancel(b.ctx)
		loops.Add(2)
		go func() { defer loops.Done(); b.reaperLoop(ctx) }()
		go func() { defer loops.Done(); b.purgeLoop(ctx) }()
		b.log.Info("raftbroker: leader loops started")
	}
	halt := func() {
		if stop == nil {
			return
		}
		stop()
		loops.Wait()
		stop = nil
		b.log.Info("raftbroker: leader loops stopped")
	}
	defer halt()

	changes := b.c.LeaderChanges()
	// The channel may drop intermediate values, so also reconcile with Status.
	reconcile := time.NewTicker(500 * time.Millisecond)
	defer reconcile.Stop()
	for {
		select {
		case <-b.ctx.Done():
			return
		case v, ok := <-changes:
			if !ok {
				changes = nil
				continue
			}
			if v {
				start()
			} else {
				halt()
			}
		case <-reconcile.C:
			if b.isLeader() {
				start()
			} else {
				halt()
			}
		}
	}
}

func (b *Broker) reaperLoop(ctx context.Context) {
	t := time.NewTicker(b.opts.ReaperInterval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
		n := now()
		sched, expired := b.q.Due(n)
		if sched {
			b.maintain(ctx, fsm.EncodePromoteScheduled(fsm.PromoteScheduled{Now: n}))
		}
		if expired {
			b.maintain(ctx, fsm.EncodeExpireLeases(fsm.ExpireLeases{Now: n}))
		}
	}
}

func (b *Broker) purgeLoop(ctx context.Context) {
	t := time.NewTicker(b.opts.PurgeInterval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
		before := now().Add(-b.opts.Retention)
		if b.q.HasTerminalBefore(before) {
			b.maintain(ctx, fsm.EncodePurge(fsm.Purge{Before: before}))
		}
	}
}

func (b *Broker) maintain(ctx context.Context, data []byte) {
	pctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	if _, err := b.propose(pctx, data); err != nil && ctx.Err() == nil &&
		!errors.Is(err, broker.ErrUnavailable) {
		var nl *broker.NotLeaderError
		if !errors.As(err, &nl) {
			b.log.Warn("raftbroker: maintenance command failed", "err", err)
		}
	}
}
