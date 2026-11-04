// Package job defines the job model shared by every goqueue backend.
package job

import (
	"encoding/json"
	"errors"
	"fmt"
	"time"
)

// State is the lifecycle state of a job. See docs/PLAN.md section 4.3 for the
// full transition table.
type State string

const (
	StatePending   State = "pending"   // claimable now
	StateScheduled State = "scheduled" // claimable once RunAt is reached
	StateRunning   State = "running"   // leased to a worker
	StateSucceeded State = "succeeded" // acked
	StateDead      State = "dead"      // retries exhausted or nacked without retry
)

// AllStates lists every state in a stable order, for stats and metrics.
var AllStates = []State{StatePending, StateScheduled, StateRunning, StateSucceeded, StateDead}

const (
	DefaultMaxAttempts = 5
	MaxPayloadBytes    = 512 << 10
	maxNameLen         = 128
)

// Job is a unit of work. Field names in JSON are part of the public HTTP API.
type Job struct {
	ID             string          `json:"id"`
	Queue          string          `json:"queue"`
	Type           string          `json:"type"`
	Payload        json.RawMessage `json:"payload,omitempty"`
	Priority       int             `json:"priority"`
	State          State           `json:"state"`
	Attempts       int             `json:"attempts"`
	MaxAttempts    int             `json:"max_attempts"`
	RunAt          time.Time       `json:"run_at"`
	LeaseOwner     string          `json:"lease_owner,omitempty"`
	LeaseToken     uint64          `json:"lease_token,omitempty"`
	LeaseExpiresAt time.Time       `json:"lease_expires_at,omitzero"`
	LastError      string          `json:"last_error,omitempty"`
	IdempotencyKey string          `json:"idempotency_key,omitempty"`
	CreatedAt      time.Time       `json:"created_at"`
	UpdatedAt      time.Time       `json:"updated_at"`
	CompletedAt    *time.Time      `json:"completed_at,omitempty"`
}

// Less reports whether a should be claimed before b: priority DESC, run_at ASC,
// id ASC. IDs are time-ordered, so equal priority and run_at falls back to FIFO.
func Less(a, b *Job) bool {
	if a.Priority != b.Priority {
		return a.Priority > b.Priority
	}
	if !a.RunAt.Equal(b.RunAt) {
		return a.RunAt.Before(b.RunAt)
	}
	return a.ID < b.ID
}

// ErrInvalid is wrapped by every validation error.
var ErrInvalid = errors.New("invalid")

// ValidateName checks a queue or job type name.
func ValidateName(field, v string) error {
	if v == "" {
		return fmt.Errorf("%w: %s is required", ErrInvalid, field)
	}
	if len(v) > maxNameLen {
		return fmt.Errorf("%w: %s longer than %d bytes", ErrInvalid, field, maxNameLen)
	}
	for _, r := range v {
		ok := r == '-' || r == '_' || r == '.' || r == ':' ||
			(r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z') || (r >= '0' && r <= '9')
		if !ok {
			return fmt.Errorf("%w: %s has invalid character %q", ErrInvalid, field, r)
		}
	}
	return nil
}

// ValidatePayload checks that a payload is empty or valid JSON within limits.
func ValidatePayload(p json.RawMessage) error {
	if len(p) == 0 {
		return nil
	}
	if len(p) > MaxPayloadBytes {
		return fmt.Errorf("%w: payload larger than %d bytes", ErrInvalid, MaxPayloadBytes)
	}
	if !json.Valid(p) {
		return fmt.Errorf("%w: payload is not valid JSON", ErrInvalid)
	}
	return nil
}
