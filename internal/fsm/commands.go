package fsm

import (
	"encoding/json"
	"time"

	"github.com/supraj221/goqueue/internal/job"
)

// Version is the command envelope version.
const Version = 1

// Operation names in the command envelope.
const (
	OpEnqueue          = "enqueue"
	OpClaim            = "claim"
	OpHeartbeat        = "heartbeat"
	OpAck              = "ack"
	OpNack             = "nack"
	OpExpireLeases     = "expire_leases"
	OpPromoteScheduled = "promote_scheduled"
	OpPurge            = "purge"
)

// Every command carries the time it should be evaluated at. The state machine
// never reads a clock; the leader that builds the command supplies it.

// Enqueue adds a fully formed job. The leader assigns ID, CreatedAt, RunAt and
// MaxAttempts. The state machine decides pending vs scheduled by comparing
// RunAt with CreatedAt.
type Enqueue struct {
	Job job.Job `json:"job"`
}

// Claim leases up to Max claimable jobs from Queues to WorkerID.
type Claim struct {
	Queues   []string      `json:"queues"`
	WorkerID string        `json:"worker_id"`
	Max      int           `json:"max"`
	Now      time.Time     `json:"now"`
	Lease    time.Duration `json:"lease"`
}

// Heartbeat extends a lease to Now+Extend.
type Heartbeat struct {
	ID     string        `json:"id"`
	Token  uint64        `json:"token"`
	Now    time.Time     `json:"now"`
	Extend time.Duration `json:"extend"`
}

// Ack marks a running job succeeded.
type Ack struct {
	ID    string    `json:"id"`
	Token uint64    `json:"token"`
	Now   time.Time `json:"now"`
}

// Nack records a failure. With Retry and attempts remaining the job is
// rescheduled at RetryAt (computed by the leader, including jitter).
type Nack struct {
	ID      string    `json:"id"`
	Token   uint64    `json:"token"`
	Now     time.Time `json:"now"`
	Reason  string    `json:"reason,omitempty"`
	Retry   bool      `json:"retry"`
	RetryAt time.Time `json:"retry_at,omitzero"`
}

// ExpireLeases reclaims running jobs whose lease expired before Now.
type ExpireLeases struct {
	Now time.Time `json:"now"`
}

// PromoteScheduled moves scheduled jobs with RunAt <= Now to pending.
type PromoteScheduled struct {
	Now time.Time `json:"now"`
}

// Purge removes succeeded and dead jobs completed before Before.
type Purge struct {
	Before time.Time `json:"before"`
}

type envelope struct {
	V  int    `json:"v"`
	Op string `json:"op"`
}

// encode produces {"v":1,"op":"<op>", <fields of body>...}.
func encode(op string, body any) []byte {
	b, err := json.Marshal(body)
	if err != nil {
		// Command structs only contain marshalable types; a failure here is a
		// programming error (for example an invalid json.RawMessage payload).
		panic("fsm: encode " + op + ": " + err.Error())
	}
	head := `{"v":1,"op":"` + op + `"`
	if len(b) <= 2 { // "{}"
		return []byte(head + "}")
	}
	out := make([]byte, 0, len(head)+len(b)+1)
	out = append(out, head...)
	out = append(out, ',')
	out = append(out, b[1:]...)
	return out
}

func EncodeEnqueue(c Enqueue) []byte                   { return encode(OpEnqueue, c) }
func EncodeClaim(c Claim) []byte                       { return encode(OpClaim, c) }
func EncodeHeartbeat(c Heartbeat) []byte               { return encode(OpHeartbeat, c) }
func EncodeAck(c Ack) []byte                           { return encode(OpAck, c) }
func EncodeNack(c Nack) []byte                         { return encode(OpNack, c) }
func EncodeExpireLeases(c ExpireLeases) []byte         { return encode(OpExpireLeases, c) }
func EncodePromoteScheduled(c PromoteScheduled) []byte { return encode(OpPromoteScheduled, c) }
func EncodePurge(c Purge) []byte                       { return encode(OpPurge, c) }
