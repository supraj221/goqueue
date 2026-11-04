// Package broker defines the storage-agnostic queue contract. Every backend
// (pgbroker, raftbroker) implements Broker and must pass brokertest.Run.
package broker

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/supraj221/goqueue/internal/job"
)

// Broker is a durable job queue with leased, at-least-once delivery.
type Broker interface {
	// Enqueue adds a job. If req.IdempotencyKey matches an existing job, that
	// job is returned unchanged with Created=false and nothing is added.
	Enqueue(ctx context.Context, req EnqueueRequest) (EnqueueResult, error)

	// Dequeue claims up to req.Max claimable jobs from req.Queues, ordered by
	// job.Less across all requested queues. Each claimed job moves to running,
	// gets Attempts incremented, and gets LeaseToken = Attempts. A running job
	// whose lease has expired is claimable again. If nothing is claimable and
	// req.Wait > 0, it blocks until a job becomes claimable, Wait elapses, or
	// ctx is done; an empty result is not an error.
	Dequeue(ctx context.Context, req DequeueRequest) ([]job.Job, error)

	// Heartbeat extends a running job's lease to now+extend. It returns
	// ErrLeaseLost if token is not the current lease token or the job is not
	// running.
	Heartbeat(ctx context.Context, id string, token uint64, extend time.Duration) (job.Job, error)

	// Ack marks a running job succeeded. ErrLeaseLost on a stale token.
	Ack(ctx context.Context, id string, token uint64) error

	// Nack records a failure. With retry=true and attempts remaining, the job
	// is scheduled again after job.DefaultBackoff(attempts); otherwise it is
	// dead. ErrLeaseLost on a stale token.
	Nack(ctx context.Context, id string, token uint64, reason string, retry bool) error

	// Get returns a job by ID, or ErrNotFound.
	Get(ctx context.Context, id string) (job.Job, error)

	// Stats returns job counts by queue and state.
	Stats(ctx context.Context) (Stats, error)

	Close() error
}

// EnqueueRequest describes a new job. Exactly one of RunAt and Delay may be
// set; if neither is, the job is claimable immediately.
type EnqueueRequest struct {
	Queue          string          `json:"queue"`
	Type           string          `json:"type"`
	Payload        json.RawMessage `json:"payload,omitempty"`
	Priority       int             `json:"priority,omitempty"`
	RunAt          time.Time       `json:"run_at,omitzero"`
	Delay          time.Duration   `json:"-"`
	MaxAttempts    int             `json:"max_attempts,omitempty"` // 0 means job.DefaultMaxAttempts
	IdempotencyKey string          `json:"idempotency_key,omitempty"`
}

// Validate checks the request. It does not apply defaults.
func (r EnqueueRequest) Validate() error {
	if err := job.ValidateName("queue", r.Queue); err != nil {
		return err
	}
	if err := job.ValidateName("type", r.Type); err != nil {
		return err
	}
	if err := job.ValidatePayload(r.Payload); err != nil {
		return err
	}
	if !r.RunAt.IsZero() && r.Delay != 0 {
		return fmt.Errorf("%w: set run_at or delay, not both", ErrInvalid)
	}
	if r.Delay < 0 {
		return fmt.Errorf("%w: delay must not be negative", ErrInvalid)
	}
	if r.MaxAttempts < 0 || r.MaxAttempts > 1000 {
		return fmt.Errorf("%w: max_attempts must be between 0 and 1000", ErrInvalid)
	}
	if len(r.IdempotencyKey) > 256 {
		return fmt.Errorf("%w: idempotency_key longer than 256 bytes", ErrInvalid)
	}
	return nil
}

// EnqueueResult is the stored job and whether this call created it.
type EnqueueResult struct {
	Job     job.Job `json:"job"`
	Created bool    `json:"created"`
}

// DequeueRequest asks for up to Max jobs from any of Queues.
type DequeueRequest struct {
	Queues   []string      `json:"queues"`
	WorkerID string        `json:"worker_id"`
	Lease    time.Duration `json:"-"`
	Max      int           `json:"max"`
	Wait     time.Duration `json:"-"`
}

// Validate checks the request.
func (r DequeueRequest) Validate() error {
	if len(r.Queues) == 0 || len(r.Queues) > 32 {
		return fmt.Errorf("%w: between 1 and 32 queues required", ErrInvalid)
	}
	for _, q := range r.Queues {
		if err := job.ValidateName("queue", q); err != nil {
			return err
		}
	}
	if err := job.ValidateName("worker_id", r.WorkerID); err != nil {
		return err
	}
	if r.Lease < 100*time.Millisecond || r.Lease > time.Hour {
		return fmt.Errorf("%w: lease must be between 100ms and 1h", ErrInvalid)
	}
	if r.Max < 1 || r.Max > 1000 {
		return fmt.Errorf("%w: max must be between 1 and 1000", ErrInvalid)
	}
	if r.Wait < 0 || r.Wait > 30*time.Second {
		return fmt.Errorf("%w: wait must be between 0 and 30s", ErrInvalid)
	}
	return nil
}

// Stats holds job counts. Counts[queue][state] = n.
type Stats struct {
	Counts map[string]map[job.State]int `json:"counts"`
}

var (
	ErrNotFound  = errors.New("not found")
	ErrLeaseLost = errors.New("lease lost")
	// ErrInvalid is the same value as job.ErrInvalid, so errors.Is matches
	// validation errors from either package.
	ErrInvalid     = job.ErrInvalid
	ErrUnavailable = errors.New("unavailable")
)

// NotLeaderError means this node cannot serve the request and the client
// should retry against LeaderAddr (empty if no leader is known yet).
type NotLeaderError struct {
	LeaderAddr string
}

func (e *NotLeaderError) Error() string {
	if e.LeaderAddr == "" {
		return "not leader: no leader known"
	}
	return "not leader: leader is " + e.LeaderAddr
}
