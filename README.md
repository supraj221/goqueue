# goqueue

[![ci](https://github.com/supraj221/goqueue/actions/workflows/ci.yml/badge.svg)](https://github.com/supraj221/goqueue/actions/workflows/ci.yml)

A distributed, fault-tolerant job queue in Go, replicated with a **from-scratch implementation of
the Raft consensus algorithm**. It has no consensus library and no external database: a 3- or 5-node
cluster is its own source of truth and keeps working when the leader crashes.

A Postgres backend (`FOR UPDATE SKIP LOCKED`) implements the same interface and passes the same
conformance suite, for comparison.

| | |
|---|---|
| **Leader killed 11 times in 2 minutes under load** | **0 of 171,228 acknowledged jobs lost** |
| Failover (kill to first successful write) | **333 ms p50**, 3-node cluster |
| Raft commit throughput, in memory | ~100k commits/s |
| Delivery | at-least-once with fencing tokens; exactly-once enqueue via idempotency keys |

See [docs/BENCHMARKS.md](docs/BENCHMARKS.md) for method, caveats, and every number.

## Architecture

```
 producers / workers ── client SDK (follows leader hints, retries) ──┐
                                                                     │ HTTP/JSON
              ┌──────────────────────────────┬───────────────────────┴──────┐
              ▼                              ▼                              ▼
        goqueued n1 (leader)           goqueued n2                    goqueued n3
        api ─► raftbroker              api ─► 421 + leader hint       ...
               │ Propose / ReadIndex
               ▼
        raft.Node ── AppendEntries / RequestVote / InstallSnapshot ──► peers
          │ WAL (CRC32, fsync) + snapshots
          ▼ Apply(committed entry)
        fsm.Queue — deterministic in-memory queue (heaps per queue, leases, idempotency index)
```

- **Writes** (enqueue, claim, heartbeat, ack, nack) are proposed to the Raft log and applied on
  every node once a majority has persisted them.
- **Reads** use ReadIndex. The leader confirms with a quorum that it's still leader before
  answering, so a deposed leader can never serve stale data.
- **Time stays out of the state machine.** The leader stamps each command with the time, the job
  ID, and the backoff delay, so every replica computes identical state. A property test checks
  byte-identical snapshots across replicas.

## What's implemented

**Raft** ([`internal/raft`](internal/raft), study guide in [docs/RAFT.md](docs/RAFT.md))
- Leader election with randomized timeouts, **PreVote**, leader stickiness, and **CheckQuorum**
- Log replication with **fast backtracking**, batching, and the Figure 8 commit rule
- Durable **WAL**: length-prefixed, CRC32-checked, fsynced; torn tails truncated on recovery
- **Snapshots** and `InstallSnapshot` for lagging or restarted followers
- **ReadIndex** linearizable reads
- An in-memory chaos network (partitions, drops, delays, reordering) and a test harness that
  continuously checks Election Safety and State Machine Safety

**Queue**
- Priorities, delayed jobs, multiple queues, retries with exponential backoff and jitter, and a
  dead-letter state
- Leases with heartbeats. Jobs from crashed workers are reclaimed automatically.
- **Fencing tokens**, so a worker whose lease expired can't ack a job someone else now owns
- Idempotent enqueue, long-poll dequeue
- Prometheus metrics (queue, HTTP, and Raft), structured logging, `/healthz`, `/readyz`, and `/v1/cluster`

## Quickstart

```bash
go build -o bin/ ./cmd/... ./examples/...

# a 3-node cluster
P=n1=http://127.0.0.1:7101,n2=http://127.0.0.1:7102,n3=http://127.0.0.1:7103
./bin/goqueued --backend=raft --node-id=n1 --http=127.0.0.1:7101 --peers=$P --data-dir=data/n1 &
./bin/goqueued --backend=raft --node-id=n2 --http=127.0.0.1:7102 --peers=$P --data-dir=data/n2 &
./bin/goqueued --backend=raft --node-id=n3 --http=127.0.0.1:7103 --peers=$P --data-dir=data/n3 &

curl -s localhost:7101/v1/cluster
curl -s -X POST localhost:7101/v1/jobs -d '{"queue":"email","type":"email.send","payload":{"to":"a@b.c"}}'
./bin/worker --url http://127.0.0.1:7101 --queues email
```

Or with Docker: `make up` starts Postgres 17, a 3-node Raft cluster, and a Postgres-mode server.

### Using the Go SDK

```go
c := client.New([]string{"http://127.0.0.1:7101", "http://127.0.0.1:7102", "http://127.0.0.1:7103"})
c.Enqueue(ctx, broker.EnqueueRequest{Queue: "email", Type: "email.send", Payload: payload, IdempotencyKey: "order-42"})

w := client.NewWorker(c, client.WorkerOptions{Queues: []string{"email"}, Concurrency: 20})
w.Handle("email.send", func(ctx context.Context, j job.Job) error { return send(j.Payload) })
w.Run(ctx) // heartbeats leases, nacks on error or panic, drains on shutdown
```

## Testing

```bash
go test ./...                              # unit, conformance, and in-memory cluster tests
go test -race -count=3 ./internal/raft/... # consensus tests, repeated to catch flakes
./bin/goqueue-bench chaos --nodes 3 --duration 120s --kill-every 10s
```

CI runs `go vet`, golangci-lint, the full suite under `-race` with a Postgres service container,
the Raft suite three times, and a chaos smoke test on every push.

## Docs

- [docs/RAFT.md](docs/RAFT.md): how Raft works, mapped to this code and its tests
- [docs/DESIGN.md](docs/DESIGN.md): delivery semantics, failure modes, Postgres vs Raft, limitations
- [docs/BENCHMARKS.md](docs/BENCHMARKS.md): results and how to reproduce them
- [docs/PLAN.md](docs/PLAN.md): the original build plan

## Limitations

Static cluster membership (no joint consensus), whole-snapshot `InstallSnapshot` (not chunked),
and no TLS or auth between nodes. Delivery is at-least-once, so handlers must be idempotent.
