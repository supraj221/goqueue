// Package pgbroker implements broker.Broker on PostgreSQL. Jobs are claimed
// with a single CTE using FOR UPDATE SKIP LOCKED; leases and "now" always use
// the database clock.
package pgbroker

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"sort"
	"sync"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/supraj221/goqueue/internal/broker"
	"github.com/supraj221/goqueue/internal/job"
)

const (
	notifyChannel = "goqueue_jobs"
	pollInterval  = 250 * time.Millisecond
)

// Options configures a Broker.
type Options struct {
	// Schema holds the tables; created if missing. Default "public".
	Schema string
	Logger *slog.Logger
	// ReaperInterval is how often exhausted expired leases are moved to dead.
	// Default 500ms.
	ReaperInterval time.Duration
	// MaxConns bounds the connection pool. Default 20.
	MaxConns int32
}

// Broker is a PostgreSQL-backed broker.Broker.
type Broker struct {
	pool    *pgxpool.Pool
	connCfg *pgx.ConnConfig
	log     *slog.Logger
	notify  *notifier

	reaperInterval time.Duration

	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup
	once   sync.Once
}

var _ broker.Broker = (*Broker)(nil)

// New connects, applies migrations, and starts the LISTEN and reaper goroutines.
func New(ctx context.Context, dsn string, opts Options) (*Broker, error) {
	if opts.Schema == "" {
		opts.Schema = "public"
	}
	if opts.Logger == nil {
		opts.Logger = slog.New(slog.NewTextHandler(io.Discard, nil))
	}
	if opts.ReaperInterval <= 0 {
		opts.ReaperInterval = 500 * time.Millisecond
	}
	if opts.MaxConns <= 0 {
		opts.MaxConns = 20
	}

	cfg, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		return nil, fmt.Errorf("pgbroker: parse dsn: %w", err)
	}
	cfg.MaxConns = opts.MaxConns
	if cfg.ConnConfig.RuntimeParams == nil {
		cfg.ConnConfig.RuntimeParams = map[string]string{}
	}
	cfg.ConnConfig.RuntimeParams["search_path"] = pgx.Identifier{opts.Schema}.Sanitize()
	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		return nil, fmt.Errorf("pgbroker: connect: %w", err)
	}
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("pgbroker: ping: %w", err)
	}
	if err := migrate(ctx, pool, opts.Schema); err != nil {
		pool.Close()
		return nil, fmt.Errorf("pgbroker: migrate: %w", err)
	}

	bctx, cancel := context.WithCancel(context.Background())
	b := &Broker{
		pool:           pool,
		connCfg:        cfg.ConnConfig.Copy(),
		log:            opts.Logger,
		notify:         newNotifier(),
		reaperInterval: opts.ReaperInterval,
		ctx:            bctx,
		cancel:         cancel,
	}
	b.wg.Add(2)
	go func() { defer b.wg.Done(); b.listen(bctx) }()
	go func() { defer b.wg.Done(); b.reapLoop(bctx) }()
	return b, nil
}

// Close stops background goroutines and closes the pool.
func (b *Broker) Close() error {
	b.once.Do(func() {
		b.cancel()
		b.wg.Wait()
		b.pool.Close()
	})
	return nil
}

// Ping reports whether the database is reachable (for /readyz).
func (b *Broker) Ping(ctx context.Context) error { return b.wrap(b.pool.Ping(ctx)) }

// wrap converts driver errors to the broker's sentinel errors.
func (b *Broker) wrap(err error) error {
	if err == nil {
		return nil
	}
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) ||
		errors.Is(err, broker.ErrNotFound) || errors.Is(err, broker.ErrLeaseLost) || errors.Is(err, broker.ErrInvalid) {
		return err
	}
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) {
		return fmt.Errorf("pgbroker: %w", err)
	}
	var ne net.Error
	if errors.As(err, &ne) || errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) || b.ctx.Err() != nil {
		return fmt.Errorf("%w: %v", broker.ErrUnavailable, err)
	}
	// Connection-level failures from pgconn/puddle (closed pool, dial errors).
	return fmt.Errorf("%w: %v", broker.ErrUnavailable, err)
}

// effState reports scheduled jobs whose run_at has passed as pending.
const effState = `CASE WHEN state = 'scheduled' AND run_at <= now() THEN 'pending' ELSE state END`

const cols = `id, queue, type, payload, priority, ` + effState + `, attempts, max_attempts, run_at,
	lease_owner, lease_token, lease_expires_at, last_error, COALESCE(idempotency_key, ''),
	created_at, updated_at, completed_at`

type scanner interface{ Scan(dest ...any) error }

func scanJob(r scanner) (job.Job, error) {
	var (
		j        job.Job
		payload  []byte
		state    string
		token    int64
		leaseExp *time.Time
		done     *time.Time
	)
	err := r.Scan(&j.ID, &j.Queue, &j.Type, &payload, &j.Priority, &state, &j.Attempts, &j.MaxAttempts, &j.RunAt,
		&j.LeaseOwner, &token, &leaseExp, &j.LastError, &j.IdempotencyKey, &j.CreatedAt, &j.UpdatedAt, &done)
	if err != nil {
		return job.Job{}, err
	}
	j.State = job.State(state)
	j.LeaseToken = uint64(token)
	if len(payload) > 0 {
		j.Payload = payload
	}
	j.RunAt = j.RunAt.UTC()
	j.CreatedAt = j.CreatedAt.UTC()
	j.UpdatedAt = j.UpdatedAt.UTC()
	if leaseExp != nil {
		j.LeaseExpiresAt = leaseExp.UTC()
	}
	if done != nil {
		t := done.UTC()
		j.CompletedAt = &t
	}
	return j, nil
}

func micros(d time.Duration) int64 { return d.Microseconds() }

// Enqueue inserts a job, or returns the existing job for a repeated
// idempotency key.
func (b *Broker) Enqueue(ctx context.Context, req broker.EnqueueRequest) (broker.EnqueueResult, error) {
	if err := req.Validate(); err != nil {
		return broker.EnqueueResult{}, err
	}
	maxAttempts := req.MaxAttempts
	if maxAttempts == 0 {
		maxAttempts = job.DefaultMaxAttempts
	}
	var payload any
	if len(req.Payload) > 0 {
		payload = []byte(req.Payload)
	}
	var key any
	if req.IdempotencyKey != "" {
		key = req.IdempotencyKey
	}
	var runAt any
	if !req.RunAt.IsZero() {
		runAt = req.RunAt
	}
	id := job.NewID(time.Now())

	const insert = `
INSERT INTO jobs (id, queue, type, payload, priority, state, max_attempts, run_at, idempotency_key, created_at, updated_at)
SELECT $1::text, $2::text, $3::text, $4::json, $5::int,
       CASE WHEN r.run_at > now() THEN 'scheduled' ELSE 'pending' END,
       $6::int, r.run_at, $7::text, now(), now()
FROM (SELECT COALESCE($8::timestamptz, now() + $9::bigint * interval '1 microsecond') AS run_at) r
ON CONFLICT (idempotency_key) WHERE idempotency_key IS NOT NULL DO NOTHING
RETURNING ` + cols

	j, err := scanJob(b.pool.QueryRow(ctx, insert, id, req.Queue, req.Type, payload, req.Priority, maxAttempts, key, runAt, micros(req.Delay)))
	if err == nil {
		// Wake long-pollers on every node (and this one) listening on the channel.
		if _, nerr := b.pool.Exec(ctx, "SELECT pg_notify($1, $2)", notifyChannel, req.Queue); nerr != nil {
			b.log.Warn("pgbroker: notify failed", "err", nerr)
		}
		return broker.EnqueueResult{Job: j, Created: true}, nil
	}
	if !errors.Is(err, pgx.ErrNoRows) || req.IdempotencyKey == "" {
		return broker.EnqueueResult{}, b.wrap(err)
	}
	// Conflict: the key already exists. If the winning transaction was still
	// in flight, DO NOTHING waited for its outcome, so the row is visible now.
	j, err = scanJob(b.pool.QueryRow(ctx, "SELECT "+cols+" FROM jobs WHERE idempotency_key = $1", req.IdempotencyKey))
	if err != nil {
		return broker.EnqueueResult{}, b.wrap(err)
	}
	return broker.EnqueueResult{Job: j, Created: false}, nil
}

// claimSQL claims in one statement: expired-lease jobs first (cheap, uses the
// lease index), then ready jobs up to Max, both with FOR UPDATE SKIP LOCKED so
// concurrent claimers never block or double-claim.
const claimSQL = `
WITH exp AS (
    SELECT id FROM jobs
    WHERE state = 'running' AND lease_expires_at <= now() AND attempts < max_attempts
      AND queue = ANY($1)
    ORDER BY priority DESC, run_at, id
    LIMIT $2::bigint
    FOR UPDATE SKIP LOCKED
), rdy AS (
    SELECT id FROM jobs
    WHERE state IN ('pending', 'scheduled') AND run_at <= now()
      AND queue = ANY($1)
    ORDER BY priority DESC, run_at, id
    LIMIT GREATEST($2::bigint - (SELECT count(*) FROM exp), 0)
    FOR UPDATE SKIP LOCKED
), picked AS (
    SELECT id AS cid FROM exp UNION ALL SELECT id AS cid FROM rdy
)
UPDATE jobs SET
    state = 'running',
    attempts = attempts + 1,
    lease_token = attempts + 1,
    lease_owner = $3,
    lease_expires_at = now() + $4::bigint * interval '1 microsecond',
    updated_at = now()
FROM picked
WHERE jobs.id = picked.cid
RETURNING ` + cols

func (b *Broker) claim(ctx context.Context, req broker.DequeueRequest) ([]job.Job, error) {
	rows, err := b.pool.Query(ctx, claimSQL, req.Queues, req.Max, req.WorkerID, micros(req.Lease))
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []job.Job
	for rows.Next() {
		j, err := scanJob(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, j)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	sort.Slice(out, func(i, k int) bool { return job.Less(&out[i], &out[k]) })
	return out, nil
}

// Dequeue claims up to req.Max jobs, long-polling for req.Wait if none are ready.
func (b *Broker) Dequeue(ctx context.Context, req broker.DequeueRequest) ([]job.Job, error) {
	if err := req.Validate(); err != nil {
		return nil, err
	}
	var wake <-chan struct{}
	if req.Wait > 0 {
		var unsub func()
		wake, unsub = b.notify.subscribe()
		defer unsub()
	}
	deadline := time.Now().Add(req.Wait)
	for {
		jobs, err := b.claim(ctx, req)
		if err != nil {
			return nil, b.wrap(err)
		}
		if len(jobs) > 0 || req.Wait <= 0 {
			return jobs, nil
		}
		remaining := time.Until(deadline)
		if remaining <= 0 {
			return nil, nil
		}
		timer := time.NewTimer(min(pollInterval, remaining))
		select {
		case <-ctx.Done():
			timer.Stop()
			return nil, ctx.Err()
		case <-b.ctx.Done():
			timer.Stop()
			return nil, fmt.Errorf("%w: broker closed", broker.ErrUnavailable)
		case <-wake:
			timer.Stop()
		case <-timer.C:
		}
	}
}

// fenced runs a single UPDATE guarded by id, state='running', lease_token and
// maps zero affected rows to ErrLeaseLost (or ErrNotFound if the job is gone).
func (b *Broker) fenced(ctx context.Context, id string, query string, args ...any) (job.Job, error) {
	j, err := scanJob(b.pool.QueryRow(ctx, query, args...))
	if err == nil {
		return j, nil
	}
	if !errors.Is(err, pgx.ErrNoRows) {
		return job.Job{}, b.wrap(err)
	}
	var one int
	err = b.pool.QueryRow(ctx, "SELECT 1 FROM jobs WHERE id = $1", id).Scan(&one)
	if errors.Is(err, pgx.ErrNoRows) {
		return job.Job{}, broker.ErrNotFound
	}
	if err != nil {
		return job.Job{}, b.wrap(err)
	}
	return job.Job{}, broker.ErrLeaseLost
}

// Heartbeat extends the lease to now+extend.
func (b *Broker) Heartbeat(ctx context.Context, id string, token uint64, extend time.Duration) (job.Job, error) {
	if extend <= 0 || extend > time.Hour {
		return job.Job{}, fmt.Errorf("%w: extend must be between 0 and 1h", broker.ErrInvalid)
	}
	return b.fenced(ctx, id, `
UPDATE jobs SET lease_expires_at = now() + $3::bigint * interval '1 microsecond', updated_at = now()
WHERE id = $1 AND state = 'running' AND lease_token = $2
RETURNING `+cols, id, int64(token), micros(extend))
}

// Ack marks the job succeeded.
func (b *Broker) Ack(ctx context.Context, id string, token uint64) error {
	_, err := b.fenced(ctx, id, `
UPDATE jobs SET state = 'succeeded', completed_at = now(), updated_at = now()
WHERE id = $1 AND state = 'running' AND lease_token = $2
RETURNING `+cols, id, int64(token))
	return err
}

// Nack records a failure: scheduled again after job.DefaultBackoff if retry is
// set and attempts remain, otherwise dead. Because lease_token equals attempts
// for the current claim, the backoff can be computed from the token without
// reading the row first.
func (b *Broker) Nack(ctx context.Context, id string, token uint64, reason string, retry bool) error {
	backoff := job.DefaultBackoff(int(token))
	_, err := b.fenced(ctx, id, `
UPDATE jobs SET
    state = CASE WHEN $3 AND attempts < max_attempts THEN 'scheduled' ELSE 'dead' END,
    run_at = CASE WHEN $3 AND attempts < max_attempts THEN now() + $5::bigint * interval '1 microsecond' ELSE run_at END,
    completed_at = CASE WHEN $3 AND attempts < max_attempts THEN NULL ELSE now() END,
    last_error = $4,
    updated_at = now()
WHERE id = $1 AND state = 'running' AND lease_token = $2
RETURNING `+cols, id, int64(token), retry, reason, micros(backoff))
	return err
}

// Get returns a job by ID.
func (b *Broker) Get(ctx context.Context, id string) (job.Job, error) {
	j, err := scanJob(b.pool.QueryRow(ctx, "SELECT "+cols+" FROM jobs WHERE id = $1", id))
	if errors.Is(err, pgx.ErrNoRows) {
		return job.Job{}, broker.ErrNotFound
	}
	if err != nil {
		return job.Job{}, b.wrap(err)
	}
	return j, nil
}

// Stats counts jobs by queue and state.
func (b *Broker) Stats(ctx context.Context) (broker.Stats, error) {
	rows, err := b.pool.Query(ctx, "SELECT queue, "+effState+" AS st, count(*) FROM jobs GROUP BY 1, 2")
	if err != nil {
		return broker.Stats{}, b.wrap(err)
	}
	defer rows.Close()
	s := broker.Stats{Counts: map[string]map[job.State]int{}}
	for rows.Next() {
		var q, st string
		var n int
		if err := rows.Scan(&q, &st, &n); err != nil {
			return broker.Stats{}, b.wrap(err)
		}
		if s.Counts[q] == nil {
			s.Counts[q] = map[job.State]int{}
		}
		s.Counts[q][job.State(st)] = n
	}
	return s, b.wrap(rows.Err())
}

// reapLoop moves running jobs whose lease expired on their last attempt to
// dead. Jobs with attempts remaining are reclaimed by the claim query.
func (b *Broker) reapLoop(ctx context.Context) {
	t := time.NewTicker(b.reaperInterval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
		tag, err := b.pool.Exec(ctx, `
UPDATE jobs SET state = 'dead', last_error = 'lease expired', completed_at = now(), updated_at = now()
WHERE state = 'running' AND lease_expires_at <= now() AND attempts >= max_attempts`)
		if err != nil {
			if ctx.Err() == nil {
				b.log.Warn("pgbroker: reaper failed", "err", err)
			}
			continue
		}
		if n := tag.RowsAffected(); n > 0 {
			b.log.Info("pgbroker: reaped exhausted jobs", "count", n)
		}
	}
}
