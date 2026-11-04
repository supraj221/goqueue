# goqueue design

goqueue is a job queue with two interchangeable backends behind one `broker.Broker` interface: a single Postgres database, and a cluster of nodes that replicate a queue state machine with a from-scratch Raft implementation. Both backends pass the same conformance suite (`internal/broker/brokertest`). This document covers the architecture, the delivery guarantees and the argument for them, and the trade-offs between the backends. `docs/RAFT.md` covers the consensus layer itself.

## 1. Architecture

```
 clients ──HTTP/JSON──► api.Server ──► broker.Broker ─┬─► pgbroker ──► PostgreSQL
 (producers, workers)   (/v1/*, 421 redirects)        └─► raftbroker ─► raft.Node ─► log + snapshots on disk
                                                                  │          │
                                                                  │          └─ AppendEntries / RequestVote /
                                                                  │             InstallSnapshot over HTTP (/raft/*)
                                                                  └─► fsm.Queue  (reads, long-poll wake-ups)
```

On the Raft backend every node runs the same binary. Raft RPCs and the client API share one HTTP listener per node.

| Piece | Job |
|---|---|
| `internal/fsm` | The queue as a deterministic state machine: jobs, a ready heap per queue, a scheduled heap, a lease-expiry heap, an idempotency index, and counters. It implements `raft.StateMachine`. |
| `internal/broker/raftbroker` | Implements `broker.Broker` for one node. Builds commands, proposes them, and maps results and errors. Runs the leader-only reaper and purge loops. |
| `internal/raft` | Leader election (with PreVote), log replication, persistence, snapshots, `ReadIndex`. |
| `internal/api` | HTTP handlers. A follower answers `421` plus `X-Goqueue-Leader`; the client retries there. |

### Life of a write on the Raft backend

1. A client sends `POST /v1/jobs` to any node. A follower returns `421` with the leader's URL (`raftbroker` turns `Status().LeaderID` into a URL through the `--peers` map).
2. The leader's `raftbroker` creates the full command: job ID, `CreatedAt`, `RunAt`, defaulted `MaxAttempts`. It encodes it with `fsm.EncodeEnqueue` into a versioned JSON envelope `{"v":1,"op":"enqueue",...}`.
3. `raft.Node.Propose` appends it to the log, replicates it, and commits it once a majority has fsynced it.
4. Every node applies committed entries to its own `fsm.Queue` in log order. `Apply` returns an `fsm.Result`, which the leader's `Propose` hands back to the waiting HTTP request.
5. The HTTP response goes out only after the entry is committed (durable on a majority) and applied.

The commands are `enqueue`, `claim`, `heartbeat`, `ack`, `nack`, `expire_leases`, `promote_scheduled`, and `purge`. A `claim` is itself a replicated command, because claiming mutates state (attempts, lease owner, lease expiry), so two nodes can never hand the same job to two workers: the log gives claims a total order.

## 2. Delivery semantics

### What is guaranteed

- **At-least-once delivery.** A job that was acknowledged at enqueue is delivered to a worker until a worker acks it or its attempts run out (then it is `dead`, which is visible and inspectable, not silently dropped). If a worker crashes, stalls, or loses the network, its lease expires and the job becomes claimable again.
- **Idempotent enqueue**, scoped to an idempotency key and a time window. Two enqueues with the same key create one job; the second returns the first job with `Created=false`. The key index is part of the replicated state, so deduplication survives leader failover and client retries against a new leader. On the Raft backend the window lasts while the job is pending or running, plus the finished-job retention (default 1 hour; see the end of this section). On Postgres, a unique index enforces it for as long as the row exists, and rows are never deleted.
- **Fenced completion.** Every claim increments `Attempts` and sets `LeaseToken = Attempts`. `Heartbeat`, `Ack` and `Nack` must present the current token or get `ErrLeaseLost`. A worker whose lease expired and whose job was handed to someone else cannot ack, nack, or extend it.
- **No acknowledged-job loss across leader failure.** An enqueue is acknowledged only after a majority has fsynced it. Any new leader must contain every committed entry (Raft's election restriction), so an acknowledged job survives any single node failure in a 3-node cluster (any two in a 5-node cluster).

### What is not guaranteed, and why

**Exactly-once processing is not achievable in general.** A worker can finish a side effect and then crash before its ack reaches the queue; the queue cannot tell that apart from a worker that never ran. To avoid ever running a job twice, the queue would have to risk running it zero times (at-most-once). For a job queue, silently losing work is the worse failure, so goqueue picks at-least-once and pushes the last step to the handler: handlers must be idempotent, typically by using the job ID or an application key as a deduplication key in whatever they write to. The README and API docs say this plainly.

### Why the combination is sound

Three mechanisms cover the three places a duplicate or a loss could come from:

1. **Producer retries** (the producer does not know whether its enqueue landed, for example because a leader died after committing but before replying). The client retries with the same idempotency key; the state machine looks the key up in a replicated index, so the retry cannot create a second job. The benchmark relies on this: every enqueue carries a unique key and is retried with that key, and the chaos run checks that nothing is lost.
2. **Worker failure.** Leases turn a crashed worker into a delayed redelivery rather than a lost job.
3. **Zombie workers.** A worker that is merely slow keeps running after its lease expired. Fencing tokens make its late `Ack`/`Nack` fail with `ErrLeaseLost` rather than overwrite the new owner's state. A zombie can still complete its side effects, which is the at-least-once cost described above; fencing guarantees only that the queue's own state stays consistent.

The idempotency index entry lives as long as the job does. When the purge loop deletes finished jobs older than the retention (default 1 hour), their keys are released, so the deduplication window equals the retention.

## 3. Why time is supplied by the leader and kept out of the state machine

Raft guarantees every replica applies the same commands in the same order. For replicas to end up in the same state, `Apply` must be a pure function of (state, command). If `Apply` called `time.Now()`, each node would stamp different times; a lease might be expired on one node and live on another, and the replicas would diverge permanently. The same goes for randomness (job IDs, backoff jitter) and for anything derived from Go map iteration order.

So `Apply` never reads the clock. The leader reads the clock once while building the command and writes the result into it: `Claim{Now, Lease}`, `Heartbeat{Now, Extend}`, `Nack{Now, RetryAt}` with the jittered backoff already computed, and `Enqueue{Job}` with ID and timestamps assigned. Lease expiry is itself a command. The leader's reaper loop (every 250 ms, and only if something is due) proposes `PromoteScheduled{Now}` and `ExpireLeases{Now}`, and `Claim` also applies both for its own `Now` first. A replica replaying the log years later reaches the same state because the times are in the log.

The tests enforce this. `TestDeterminismAndSnapshotRoundTrip` applies the same pseudo-random command stream to two instances and requires byte-identical snapshots, and requires a third instance restored from a snapshot to match and keep matching.

Consequences worth knowing:

- Time is whatever the current leader's clock says. A leader change with skewed clocks can make leases look slightly shorter or longer to workers, but never inconsistent between replicas. Lease lengths of seconds make modest NTP-level skew irrelevant. Heavy skew (seconds) between nodes would shorten or extend effective leases at failover; it cannot violate fencing, because tokens, not time, decide who may ack.
- Timestamps in a command are not validated against a clock in `Apply`. A buggy or malicious leader could send absurd times. Nodes are trusted (no auth between nodes; see limitations).
- Lease expiry is lazy-but-prompt: the reaper acts within about 250 ms of expiry, and `Claim` reclaims expired leases itself, so a claimant never waits for the reaper.

## 4. Reads: ReadIndex

`Get` and `Stats` must not return stale data (for example, "not found" for a job that was acknowledged a moment ago, or an old state after failover). Two options:

- **Log reads.** Send reads through the log. Correct, but it makes every read a disk write plus a round trip to a majority.
- **ReadIndex** (used here). The leader records its commit index, confirms it is still the leader with one heartbeat round acknowledged by a majority, waits until its state machine has applied up to the recorded index, and then reads the local `fsm.Queue`. No log write, one network round trip, and the result reflects every write that completed before the read began. That is linearizability.
- **Lease reads** (not used). The leader skips the heartbeat round while it holds a time-based lease from the last quorum contact. It is faster but is only safe if clock drift is bounded; a paused or skewed leader could serve stale data. ReadIndex needs no clock assumptions, so it was chosen.

`Dequeue` uses the local state machine only as a hint: `HasClaimable` decides whether it is worth proposing a `Claim`, but the claim itself goes through the log, so a stale hint costs at most an empty claim and never double-delivers. Long-polling waits on `fsm.Subscribe()` wake-ups plus a 100 ms tick (so scheduled jobs come due) rather than on log traffic. At most `ClaimConcurrency` (default 4) claims per node are in flight, so many idle workers do not flood the log with empty claims.

## 5. Postgres versus Raft

| | Postgres backend | Raft backend |
|---|---|---|
| Durability | Whatever Postgres is configured for. A single primary: if its disk dies, so does the queue, unless you run replication separately | Majority fsync before acknowledging. Survives loss of a minority of nodes with no operator action |
| Availability on node loss | Depends on the Postgres failover setup you operate | A new leader is elected automatically. Measured failover in the chaos run is in `bench/results/` |
| Throughput | Higher on one box: one fsync per transaction, group commit, many connections in parallel | Bound by log replication: every command (including every claim and ack) is a replicated log entry, and latency includes a leader fsync plus a follower fsync |
| Latency | One round trip to the database | Leader fsync + network + a follower's fsync, per batch of commands |
| Operations | Needs a Postgres you already know how to run | Self-contained: three or five identical processes. No external database. Operators must understand quorum |
| Query power | Arbitrary SQL over job history | Only the API; state lives in memory (rebuilt from snapshot + log), so the working set must fit in RAM |
| Scale | Vertical, then sharding | Vertical (one Raft group); the leader serves all writes |
| Consistency | Serializable-enough via `SKIP LOCKED` + transactions | Linearizable writes and ReadIndex reads |

Postgres is the right default when you already run a durable, replicated Postgres and want simplicity or arbitrary querying. The Raft backend is the choice when you want the queue to be its own fault-tolerant system with no external dependency, and can accept a lower write ceiling and an in-memory working set.

## 6. Failure modes

| Failure | What happens | What clients see |
|---|---|---|
| **Leader crash** | Followers stop receiving heartbeats, one wins an election within about one election timeout (150 to 300 ms by default). The new leader appends a no-op, then commits older entries. Leader-only loops (reaper, purge) start on the new leader. | In-flight requests fail with a connection error or `503`. The SDK retries with the same idempotency key and follows the `421` hint. Enqueues that committed before the crash are never lost; ones that did not commit were never acknowledged. Measured as "failover time" by `goqueue-bench chaos`. |
| **Follower crash** | The cluster keeps committing with the remaining majority. On restart the follower reloads its WAL and snapshot, and the leader catches it up with `AppendEntries`, or `InstallSnapshot` if it fell behind the compacted prefix. | Nothing. |
| **Network partition, minority side holds the leader** | The old leader cannot reach a majority: it cannot commit, so writes through it stall and fail; `ReadIndex` fails, so it cannot serve stale reads, and CheckQuorum makes it step down after about one election timeout without hearing from a majority. The majority side elects a new leader. On heal, the old leader sees the higher term and steps down; its uncommitted entries are overwritten. PreVote stops the isolated node from inflating terms and disrupting the cluster on rejoin. | Requests to the stale leader time out or return `503`/`421`; the SDK rotates to the majority side. An enqueue that never committed was never acknowledged and is retried safely by key. |
| **Partition with no majority anywhere** | No leader; the cluster is unavailable for writes and linearizable reads until a majority reconnects. No data is lost or diverges. | `503`, `421` with no hint, `/readyz` failing. |
| **Worker crash or hang** | Its lease expires; the leader's reaper (or the next claim) returns the job to `pending`, or to `dead` if attempts are exhausted. | The job is redelivered, with a higher attempt number and token. The old worker's late ack gets `409 lease_lost`. |
| **Slow disk on the leader** | Every commit waits for fsync, so latency and queue depth rise, and a stall longer than the election timeout can trigger a needless election (observed during benchmarking on a laptop SSD under heavy load). Safety is unaffected. | Higher enqueue latency, occasional `503` and leader change during a stall. Tune the election timeout up on slow disks. |
| **Slow disk on a follower** | The leader needs only a majority, so one slow follower is outvoted. Two slow followers in a 3-node cluster are as bad as a slow leader. | Little or nothing for one follower. |
| **Disk corruption or torn write** | WAL records are length-prefixed and CRC32-checked. On load, the first torn or corrupt record and everything after it is cut off, and the node re-fetches the missing suffix from the leader (the entries were never acknowledged by this node, or are re-replicated from the others). A corrupt hard-state file (checksummed, written atomically) fails startup rather than being guessed at. | A node that cannot start; the other nodes continue. |
| **Whole cluster restart** | Every node reloads its snapshot and log; the state machine is rebuilt deterministically; a leader is elected. | Downtime for the restart only; no acknowledged job lost. |

## 7. Limitations

- **Static membership.** The cluster is fixed by `--peers`. There is no joint consensus; adding or replacing a node means a coordinated restart, and a replaced node must be a fresh identity with the same ID and data directory restored from a backup.
- **Single Raft group.** One leader handles all writes; there is no sharding by queue. Throughput scales with the leader's CPU and the fsync rate, not with cluster size (a 5-node cluster is slower than a 3-node one).
- **In-memory state.** The whole job set, including payloads, lives in RAM on every node and is snapshotted as one JSON document. A million small jobs is fine; tens of millions with large payloads is not. Snapshots are not chunked (`InstallSnapshot` sends the whole snapshot in one RPC) and `Snapshot()` is O(n) and runs on the apply path.
- **Retention drives snapshot size.** Finished jobs stay in the state machine (and every snapshot) until the purge loop removes them, 1 hour by default. A succeeded job drops its payload but keeps its metadata. At about 1,000 jobs/s that is millions of retained jobs and tens of megabytes per snapshot. Measured on a laptop: with 1 hour retention the 120 s chaos run saw failovers up to 7.7 s and throughput of 456 jobs/s, because a restarted follower needed a multi-megabyte `InstallSnapshot` (sent whole, in one RPC) before the surviving pair had a quorum that included it. With `--retention 30s` the same run had failovers under 0.4 s and 1,160 jobs/s. Set retention to what you actually need.
- **Every operation is a log entry.** Claims, heartbeats and acks are replicated like enqueues. A heartbeat-heavy workload (many long jobs, short lease) can dominate the log. Batching helps (the leader batches proposals, and a claim takes up to `Max` jobs) but there is no per-job-id coalescing of heartbeats.
- **At-least-once only.** Handlers must be idempotent; see section 2.
- **Clock dependence is limited, not zero.** Leases and `run_at` follow the leader's wall clock. A leader change across skewed clocks shifts lease deadlines by the skew.
- **No auth or TLS**, on the client API or between nodes. Run on a trusted network or behind a proxy.
- **Idempotency window equals retention.** Keys are forgotten when their job is purged (default after 1 hour).
- **JSON on the wire and in the log.** Simple and debuggable, and not the cheapest to encode: FSM micro-benchmarks show that decoding the command accounts for most of an `Apply`.
- **Lost claim responses.** If a leader dies after committing a `Claim` but before replying, those jobs sit leased to a worker that never saw them until the lease expires, then are redelivered. That is a delay, not a loss, and it shows up as higher queue wait during failover rather than as a duplicate.
