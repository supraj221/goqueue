# goqueue — Build Plan

A distributed, fault-tolerant job queue written in Go, built in two stages:

1. **Postgres backend (baseline).** A durable single-database queue using
   `SELECT … FOR UPDATE SKIP LOCKED`, leases, and fencing tokens.
2. **Raft backend (the main event).** A 3- or 5-node cluster that replicates the
   queue state machine with a **from-scratch implementation of the Raft
   consensus algorithm**. No external consensus library and no external database:
   the cluster is its own source of truth and survives leader crashes without
   losing acknowledged jobs.

Both backends implement the same `broker.Broker` interface and pass the same
conformance test suite. Then they are benchmarked against each other and the
Raft cluster is put through chaos tests (killing the leader under load,
partitioning nodes). The measured numbers go in `docs/BENCHMARKS.md` and the
resume bullets.

---

## 1. Goals and non-goals

### Goals
- **At-least-once delivery** with **fencing tokens**, so a worker whose lease
  expired cannot ack or nack a job another worker now owns.
- **Exactly-once enqueue** through idempotency keys.
- Leases with heartbeats, automatic reclaim of jobs from crashed workers,
  exponential backoff with jitter, max attempts, and a dead-letter state.
- Priorities, delayed or scheduled jobs (`run_at`), and multiple named queues.
- Raft written from scratch: leader election, log replication, persistence,
  fast log backtracking, snapshots with `InstallSnapshot`, and linearizable
  reads through `ReadIndex`.
- **Zero acknowledged-job loss across leader failovers**, verified by a chaos
  harness rather than claimed.
- Production-style observability: Prometheus metrics, structured `slog` logs,
  health and readiness endpoints, and a cluster status endpoint.
- Reproducible benchmarks with p50, p95, and p99 latency and throughput.

### Non-goals (documented as future work)
- Dynamic cluster membership changes (joint consensus). The cluster is static,
  configured with `--peers`.
- Exactly-once *processing*. That isn't achievable in general, so handlers must
  be idempotent. The docs say so plainly.
- Multi-Raft or sharding across Raft groups.
- gRPC. Everything uses HTTP/JSON with no code generation, so it builds with
  just `go build`.
- Authentication and TLS between nodes.

---

## 2. Architecture

```
                 ┌──────────────────────── producers / workers ───────────────────────┐
                 │   client SDK: Enqueue(), Worker{Handle(type, fn), Run(ctx)}         │
                 │   follows leader hints (HTTP 421 + X-Goqueue-Leader), retries        │
                 └───────────────┬───────────────────────────────┬────────────────────┘
                                 │ HTTP/JSON /v1/...              │
          ┌──────────────────────▼───────────┐     ┌──────────────▼──────────────┐
          │ goqueued --backend=postgres      │     │ goqueued --backend=raft (×3) │
          │  api.Server ─► pgbroker          │     │  api.Server ─► raftbroker    │
          │                 │                │     │                 │            │
          │                 ▼                │     │        ┌────────▼────────┐   │
          │           PostgreSQL             │     │        │ raft.Node        │   │
          │   (SKIP LOCKED, LISTEN/NOTIFY)   │     │        │  log + WAL       │◄──┼──► peers
          └──────────────────────────────────┘     │        │  snapshots       │   │  /raft/* RPC
                                                   │        └────────┬────────┘   │
                                                   │                 ▼ Apply()    │
                                                   │        fsm.Queue (deterministic│
                                                   │        in-memory state machine)│
                                                   └──────────────────────────────┘
```

### Request path on the Raft backend
1. A client calls `POST /v1/jobs` on any node. A follower answers
   `421 Misdirected Request` with an `X-Goqueue-Leader: <url>` header, and the
   SDK retries against the leader.
2. The leader's `raftbroker` builds a `fsm.Command`. It assigns the job ID,
   timestamp, and backoff values up front, because the state machine must be
   deterministic: no `time.Now()` and no randomness inside `Apply`.
3. `raft.Node.Propose(cmd)` appends the command to the leader's log, replicates
   it with `AppendEntries`, and commits it once a majority has persisted it.
4. Every node applies committed entries to `fsm.Queue` in log order. The
   leader's `Propose` returns the `Apply` result to the waiting HTTP handler.
5. Reads (`GET /v1/jobs/{id}`, `/v1/stats`) go through `ReadIndex`. The leader
   confirms it is still leader with a heartbeat quorum, waits until
   `lastApplied >= readIndex`, and then reads local state. That makes reads
   linearizable without writing to the log.

### Time on the Raft backend
`Apply` never reads the clock. Each command carries its own `Now`. The leader
runs a **reaper loop** every 250 ms that proposes `ExpireLeases{Now}` and
`PromoteScheduled{Now}`, and a **purge loop** that proposes
`Purge{Before: now - retention}`. A newly elected leader starts these loops
when it takes over and stops them when it loses leadership.

---

## 3. Repository layout and ownership

Module: `github.com/supraj221/goqueue`

| Path | What it holds | Owner |
|---|---|---|
| `internal/job/` | `Job`, `State`, validation, ID generation, backoff | scaffold (Opus) |
| `internal/broker/` | `Broker` interface, request types, sentinel errors | scaffold (Opus) |
| `internal/broker/brokertest/` | Shared conformance suite for any `Broker` | **Agent B** |
| `internal/broker/pgbroker/` | Postgres backend, embedded migrations | **Agent B** |
| `internal/api/` | HTTP server and handlers, error mapping, long-poll | **Agent B** |
| `internal/metrics/` | Prometheus collectors and HTTP middleware | **Agent B** |
| `client/` | Go SDK: producer client and `Worker` | **Agent B** |
| `examples/worker/` | Example worker binary | **Agent B** |
| `internal/raft/` | Raft core, storage, in-memory chaos network, tests | **Agent A** |
| `internal/raft/httptransport/` | HTTP transport for real deployments | **Agent A** |
| `internal/fsm/` | Deterministic queue state machine, snapshots | **Agent C** |
| `internal/broker/raftbroker/` | `Broker` on top of `raft.Node` + `fsm`, leader loops | **Agent C** |
| `cmd/goqueued/` | Server binary (`--backend=postgres\|raft`) | **Agent C** |
| `cmd/goqueue-bench/` | Load generator, chaos harness, report writer | **Agent C** |
| `deploy/`, `Makefile`, `.github/workflows/` | docker-compose, CI | **Agent C** |
| `docs/DESIGN.md`, `docs/RAFT.md` | Design write-up and Raft explainer | Agents A and C |
| `docs/BENCHMARKS.md`, `README.md` | Results and front page | Integration (Opus) |

**Rules for the agents**
- Write only inside the paths you own. The scaffold packages (`job`, `broker`,
  and the public API types in `raft/types.go`) are the contract. Change them
  only by reporting the need back, not by editing them.
- All agents share one working tree, so don't run `go build ./...` or
  `go test ./...` across the whole repo while the others are mid-edit. Test
  your own packages, for example `go test -race ./internal/raft/...`.
- Every dependency is already in `go.mod`. Don't add any without saying so in
  your report.
- Don't run `git commit`. The integration step makes the commits.

---

## 4. Contracts (written in the scaffold)

### 4.1 Job model (`internal/job`)
```go
type State string // "pending" | "scheduled" | "running" | "succeeded" | "dead"

type Job struct {
    ID, Queue, Type string
    Payload         json.RawMessage
    Priority        int        // higher runs first
    State           State
    Attempts        int        // number of times the job has been claimed
    MaxAttempts     int        // default 5
    RunAt           time.Time  // not claimable before this time
    LeaseOwner      string
    LeaseToken      uint64     // fencing token; equals Attempts at claim time
    LeaseExpiresAt  time.Time
    LastError       string
    IdempotencyKey  string
    CreatedAt, UpdatedAt time.Time
    CompletedAt     *time.Time
}
```
- **Ordering among claimable jobs:** `priority DESC, run_at ASC, id ASC`. IDs
  sort by time (a ULID-style 26-character Crockford base32 value), so ties fall
  back to FIFO.
- **Fencing:** every claim increments `Attempts` and sets
  `LeaseToken = Attempts`. `Heartbeat`, `Ack`, and `Nack` must present the
  current token or get `ErrLeaseLost`.
- **Backoff:** `job.Backoff(attempt, base=1s, max=10m, jitter=0.2)`. This is
  computed outside the state machine.

### 4.2 Broker (`internal/broker`)
```go
type Broker interface {
    Enqueue(ctx, EnqueueRequest) (EnqueueResult, error) // {Job, Created}; same IdempotencyKey returns the existing job
    Dequeue(ctx, DequeueRequest) ([]job.Job, error) // claim up to Max; Wait means long-poll
    Heartbeat(ctx, id string, token uint64, extend time.Duration) (job.Job, error)
    Ack(ctx, id string, token uint64) error
    Nack(ctx, id string, token uint64, reason string, retry bool) error
    Get(ctx, id string) (job.Job, error)
    Stats(ctx) (Stats, error)
    Close() error
}
```
Sentinel errors: `ErrNotFound`, `ErrLeaseLost`, `ErrInvalid`, `ErrUnavailable`, and
`*NotLeaderError{LeaderAddr}`, matched with `errors.Is` and `errors.As`.

### 4.3 State transitions (both backends must match)
```
Enqueue ──► pending (run_at <= now) | scheduled (run_at > now)
scheduled ──(run_at reached)──► pending
pending ──Dequeue──► running (attempts++, token=attempts, lease set)
running ──Ack──► succeeded
running ──Nack(retry=true, attempts<max)──► scheduled (run_at = now + backoff)
running ──Nack(retry=false) or attempts>=max──► dead
running ──lease expired, attempts<max──► pending  (reclaimed; old token is now stale)
running ──lease expired, attempts>=max──► dead
```

### 4.4 Raft public API (`internal/raft/types.go`)
```go
type StateMachine interface {
    Apply(e LogEntry) any         // deterministic
    Snapshot() ([]byte, error)
    Restore(data []byte) error
}
type Storage interface { /* hard state, log append/truncate/read, snapshot */ }
type Transport interface {
    RequestVote(ctx, to string, *RequestVoteRequest) (*RequestVoteResponse, error)
    AppendEntries(ctx, to string, *AppendEntriesRequest) (*AppendEntriesResponse, error)
    InstallSnapshot(ctx, to string, *InstallSnapshotRequest) (*InstallSnapshotResponse, error)
}
func NewNode(cfg Config, sm StateMachine, st Storage, tr Transport) (*Node, error)
func (n *Node) Start() error
func (n *Node) Stop()
func (n *Node) Propose(ctx, data []byte) (result any, index uint64, err error) // ErrNotLeader
func (n *Node) ReadIndex(ctx) error
func (n *Node) Status() Status
func (n *Node) LeaderChanges() <-chan bool                // true means this node became leader
func (n *Node) HandleRequestVote / HandleAppendEntries / HandleInstallSnapshot  // inbound RPCs
```

---

## 5. Work packages

### Agent A — Raft from scratch (`internal/raft`)
The hardest and most self-contained piece. It follows Figure 2 of the Raft paper
(Ongaro and Ousterhout, 2014) exactly.

1. **Core loop.** One goroutine owns all mutable state and is driven by a
   ticker, RPC requests and responses, and proposals, all arriving over
   channels. That's the event-loop design: no lock-ordering bugs, and it's
   deterministic enough to test. Any mutex usage must be documented.
2. **Leader election.** Randomized election timeout (default 150–300 ms),
   `RequestVote` with the up-to-date log check, votes persisted before replying,
   and step-down when a higher term is seen. Implement **PreVote** to stop a
   partitioned node from inflating terms when it rejoins.
3. **Log replication.** `AppendEntries` with the consistency check and
   per-follower `nextIndex` and `matchIndex`. Use **fast backtracking**
   (`ConflictTerm` and `ConflictIndex` in the reply). Advance `commitIndex`
   only for entries from the **current term** (the Figure 8 rule). The leader
   appends a no-op entry when it takes over. Send batches of up to
   `MaxEntriesPerAppend` entries.
4. **Persistence.** `MemStorage`, which keeps state across a simulated crash in
   tests, and `FileStorage`: an append-only WAL of length-prefixed, CRC32-checked
   records, `fsync` before acknowledging, a separate `hardstate` file written
   atomically (write to temp, fsync, rename), and truncation of torn tails on
   load.
5. **Snapshots.** When there are more than `SnapshotThreshold` applied entries
   since the last snapshot, call `sm.Snapshot()`, save it, and compact the log
   prefix. Send `InstallSnapshot` to a follower whose `nextIndex` is at or below
   the snapshot index.
6. **Apply pipeline.** Apply committed entries in order, and deliver results to
   the waiting `Propose` callers keyed by index and term. If a different term's
   entry is committed at that index, return `ErrLeadershipLost`.
7. **`ReadIndex`.** Record `commitIndex`, confirm leadership with a heartbeat
   round acknowledged by a majority, then wait for `lastApplied >= readIndex`.
8. **`memnet` chaos network** for tests: `Disconnect(id)`, `Connect(id)`,
   `Partition(groups...)`, `SetDropRate(p)`, `SetDelay(min, max)`, and
   reordering. Crash and restart a node while keeping its `MemStorage`.
9. **Tests**, all run with `-race`, modeled on MIT 6.5840 Labs 3A–3D:
   initial election; re-election after the leader disconnects; no leader
   without quorum; basic agreement; agreement despite a follower disconnecting;
   no agreement without quorum; concurrent proposals; rejoin of a partitioned
   leader; fast backtracking with 50+ divergent entries; persistence across a
   full-cluster restart; the Figure 8 unreliable scenario; snapshot install to a
   lagging follower; and `ReadIndex` refusing on a deposed leader. A **safety
   checker** asserts that every node's applied sequence is a prefix of the
   longest one (State Machine Safety), and that there is at most one leader per
   term (Election Safety). Each test finishes in under 10 seconds.
10. **`httptransport`.** `POST /raft/vote|append|snapshot` with JSON bodies,
    a peer-address map, per-RPC timeouts, and connection reuse.
11. **`docs/RAFT.md`.** A study guide to how this implementation maps to the
    paper, with a section per mechanism and the invariants each test checks.

### Agent B — Postgres backend, API, SDK, metrics
1. **`brokertest.Run(t, newBroker func(t) broker.Broker)`.** The shared
   conformance suite, which uses real time with short leases (around 300 ms).
   It covers: enqueue and get; validation errors; idempotent enqueue; priority
   and FIFO ordering; scheduled jobs not being claimable early; `Max` batching;
   `Dequeue` across multiple queues; lease expiry reclaim with the stale token
   getting `ErrLeaseLost`; heartbeat extending the lease; ack; nack with retry
   moving to scheduled with backoff; nack without retry or with attempts
   exhausted moving to dead; long-poll `Wait` returning early when a job
   arrives; **no double claims under 32 concurrent dequeuers × 2,000 jobs**; and
   `Stats` counts.
2. **`pgbroker`.** pgx/v5 pool, migrations embedded with `embed` and tracked in
   a `schema_migrations` table, a partial index on claimable rows, and claiming
   with a CTE using `FOR UPDATE SKIP LOCKED`. Lease expiry is handled inside the
   claim query as well as by a reaper goroutine that moves exhausted jobs to
   dead. `LISTEN goqueue_jobs`/`NOTIFY` wakes long-poll waiters. Tests run only
   when `GOQUEUE_PG_DSN` is set, and use a separate schema per test.
3. **`api`.** Routes are listed in section 6. It maps errors to status codes,
   caps request bodies at 1 MiB, takes `wait_ms` up to 30 s for long-polls,
   wraps handlers in a metrics middleware, logs requests with `slog`, and
   supports graceful shutdown. On `*NotLeaderError` it returns 421 with the
   `X-Goqueue-Leader` header.
4. **`metrics`.** The full list is in section 7.
5. **`client`.** `client.New(urls ...string)` follows 421 leader hints and
   rotates through URLs on connection errors, with bounded retries and backoff.
   `Worker` has a concurrency limit, batch dequeue, a long-poll loop, a
   heartbeat goroutine per job (at lease/3), panic recovery that turns a panic
   into a nack, and graceful shutdown that stops claiming, waits for in-flight
   jobs up to a deadline, then nacks what's left.
6. **`examples/worker`.** A demo handler for the `email.send` job type.

### Agent C — State machine, Raft broker, binaries, benchmarks
1. **`fsm.Queue`**, which implements `raft.StateMachine`. Commands are
   versioned JSON: `Enqueue`, `Claim`, `Heartbeat`, `Ack`, `Nack`,
   `ExpireLeases`, `PromoteScheduled`, and `Purge`. State includes
   `jobs map[id]*Job`, a ready heap per queue (priority, run_at, id), a
   scheduled min-heap by `run_at`, a running set by lease expiry, and an
   idempotency index. Results are deterministic: no reliance on map iteration
   order, no clock, no randomness. Snapshot and restore cover the whole state.
   It also exposes `Subscribe()`, a wake-up channel for long-poll waiters.
   Tests: table tests for every transition in section 4.3, a
   determinism property test (the same command sequence applied to two
   instances gives identical snapshots), a snapshot round-trip, and benchmarks
   (`BenchmarkApplyEnqueue`, `BenchmarkApplyClaim` at 1M jobs).
2. **`raftbroker`**, which implements `broker.Broker` over a small
   `Consensus` interface (`Propose`, `ReadIndex`, `IsLeader`, `LeaderAddr`,
   `LeaderChanges`). The real one is `*raft.Node`, and a `fakeConsensus` that
   applies directly lets it pass `brokertest` before Agent A finishes. It runs
   the reaper and purge loops only while leader, and serves long-poll through
   `fsm.Subscribe()` on the leader.
3. **`cmd/goqueued`.** Flags: `--backend`, `--http`, `--pg-dsn`, `--node-id`,
   `--peers=n1=http://127.0.0.1:7101,…`, `--data-dir`, and
   `--snapshot-threshold`. The raft RPC handlers mount on the same HTTP server
   under `/raft/`. Handles SIGINT and SIGTERM for graceful shutdown.
4. **`cmd/goqueue-bench`.**
   - `load`: P producers and W workers for a set duration or job count, with a
     configurable payload size. Reports enqueue throughput, processed
     throughput, and p50/p95/p99 for enqueue latency, queue wait, and
     end-to-end latency (HDR-style histograms).
   - `chaos`: spawns an N-node `goqueued` raft cluster as child processes
     (portable, no Docker), keeps load running, **kills the leader every K
     seconds** and restarts it, and optionally SIGSTOPs or pauses a follower.
     It measures **failover time** (the longest window with no successful
     enqueue) and verifies **lost jobs = 0** (every enqueue that returned 2xx is
     eventually processed) and counts duplicate deliveries.
   - Writes JSON and Markdown results to `bench/results/`.
5. **`deploy/docker-compose.yml`** (Postgres 17 and a 3-node cluster), a
   `Makefile`, and **GitHub Actions CI** (`go vet`, `golangci-lint`,
   `go test -race`, and pgbroker tests against a Postgres service container).
6. **`docs/DESIGN.md`.** Architecture, the delivery-semantics argument, and
   tradeoffs: Postgres vs Raft, why the clock lives outside the state machine,
   fencing tokens, and ReadIndex vs lease reads.

---

## 6. HTTP API

| Method and path | Body | Success |
|---|---|---|
| `POST /v1/jobs` | `{queue, type, payload, priority?, run_at? \| delay_ms?, max_attempts?, idempotency_key?}` | 201 with the job (200 if idempotent) |
| `GET /v1/jobs/{id}` | none | 200 with the job |
| `POST /v1/dequeue` | `{queues[], worker_id, lease_ms, max, wait_ms?}` | 200 `{jobs: [...]}` |
| `POST /v1/jobs/{id}/heartbeat` | `{lease_token, extend_ms}` | 200 with the job |
| `POST /v1/jobs/{id}/ack` | `{lease_token}` | 204 |
| `POST /v1/jobs/{id}/nack` | `{lease_token, error, retry}` | 204 |
| `GET /v1/stats` | none | 200 with counts per queue and state |
| `GET /v1/cluster` | none | 200 with raft status (id, term, role, leader, commit, applied) |
| `GET /healthz`, `/readyz`, `/metrics` | none | liveness, readiness (known leader or DB reachable), Prometheus |

Errors return `{ "error": "<code>", "message": "..." }` with: 400 `invalid`,
404 `not_found`, 409 `lease_lost`, 421 `not_leader` (plus the
`X-Goqueue-Leader` header), 503 `unavailable`.

---

## 7. Metrics (Prometheus)

Queue: `goqueue_jobs_enqueued_total{queue}`, `goqueue_jobs_claimed_total{queue}`,
`goqueue_jobs_acked_total{queue}`, `goqueue_jobs_nacked_total{queue,retry}`,
`goqueue_jobs_dead_total{queue}`, `goqueue_leases_expired_total{queue}`,
`goqueue_queue_depth{queue,state}`,
`goqueue_job_queue_wait_seconds{queue}` (from enqueue to first claim), and
`goqueue_job_e2e_seconds{queue}` (from enqueue to ack).
HTTP: `goqueue_http_request_duration_seconds{route,method,code}`.
Raft: `goqueue_raft_term`, `goqueue_raft_is_leader`,
`goqueue_raft_commit_index`, `goqueue_raft_applied_index`,
`goqueue_raft_leader_changes_total`, `goqueue_raft_propose_seconds`, and
`goqueue_raft_snapshot_total`.

---

## 8. Benchmark matrix (filled in by integration)

| Scenario | Backend | Measure |
|---|---|---|
| Enqueue throughput, 1 KB payload, 16 producers | pg, raft-3, raft-5 | jobs/s, p50/p99 |
| End-to-end, 16 producers + 32 workers | pg, raft-3 | processed/s, e2e p50/p99 |
| Leader kill every 10 s for 2 min under load | raft-3, raft-5 | failover p50/max, **lost = 0**, duplicates |
| Follower partition and heal | raft-3 | catch-up time, snapshot installs |
| FSM micro-benchmarks | in-proc | ns/op for apply enqueue and claim |
| Raft micro-benchmark (memnet, 3 nodes) | in-proc | commits/s |

Every number in the README links back to the command that produced it.

---

## 9. Integration and definition of done
1. `go vet ./...` and `go test -race ./...` are green, including pgbroker
   against Docker Postgres.
2. The Raft test suite passes 20 times in a row (`-count=20`), so it isn't flaky.
3. The chaos run reports **0 lost jobs**.
4. `docs/BENCHMARKS.md` is filled in from real runs on this machine, with the
   hardware noted.
5. The README covers a 60-second quickstart, the architecture diagram, the
   guarantees, and the results.
6. The history is in logical commits, pushed to `github.com/supraj221/goqueue`
   (public), with CI green.
7. Resume bullets are drafted from the measured results only.
