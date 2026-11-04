# Benchmarks

All numbers come from runs on one laptop, executed one at a time on an otherwise idle machine. Each result
file in [`bench/results/`](../bench/results) has the full report and the exact configuration.

**Machine:** Intel Core i5-1235U (12 logical CPUs), Windows 11, Go 1.27.3. Every node of a
cluster runs on this one machine and **shares one SSD**. An `fsync` costs about 5.6 ms here, and every
Raft commit needs a majority of nodes to fsync. So the throughput numbers below measure a single disk,
not what a cluster on separate machines would do. The correctness numbers (lost jobs, duplicates,
failover) don't depend on hardware.

**Load shape** (unless noted): 16 producers, 32 workers, 1 KB payloads, dequeue batch 16,
10 s leases. Every enqueue carries a unique idempotency key and is retried with the same key.

## Headline: leader failover under load

The `chaos` harness starts the cluster as child processes and keeps load running. Every 10 seconds it
finds the current leader through `/v1/cluster`, kills it, and restarts it 2 s later on the same data
directory. After the load stops it drains the backlog, then checks **every acknowledged enqueue** with
a linearizable `GET`.

| Scenario | Leader kills | Jobs acknowledged | **Lost** | Duplicate deliveries | Failover p50 | Failover max | Throughput |
|---|---|---|---|---|---|---|---|
| 3 nodes, kill every 10 s, 120 s | 11 | 171,228 | **0** | 0 | 333 ms | 950 ms | 1,427 jobs/s |
| 5 nodes, kill every 10 s, 120 s | 11 | 149,825 | **0** | 0 | 329 ms | 342 ms | 1,248 jobs/s |
| 3 nodes, leader *frozen* 2 s every 10 s, 60 s | 5 | 71,582 | **0** | 1,088 | 2,964 ms | 3,017 ms | 1,188 jobs/s |

*Failover* is measured from the kill to the first successful enqueue afterwards. It's bounded below
by the election timeout (150–300 ms) and PreVote.

Results: [`chaos-raft-3-chaos`](../bench/results/chaos-raft-3-chaos-20261104-170138.md),
[`chaos-raft-5-chaos`](../bench/results/chaos-raft-5-chaos-20261104-170338.md),
[`chaos-raft-3-partition`](../bench/results/chaos-raft-3-partition-20261104-170442.md).

**What the frozen-leader run shows.** A frozen process (NtSuspendProcess / SIGSTOP) is worse than a
dead one. Its TCP connections stay open, so clients hang instead of failing fast. The roughly 3 s failover is
the bench client's per-attempt timeout, not the election time. Workers whose acks were stuck on the
frozen node lost their leases, so 1,088 jobs were delivered twice. That's at-least-once delivery
working as designed: no job was lost, and the fencing tokens rejected every stale ack. Still, it's the
clearest argument for idempotent handlers.

## Steady state (no faults)

| Cluster | Jobs | Throughput | Enqueue p50 | Enqueue p99 | Queue wait p50 | Queue wait p99 | Lost |
|---|---|---|---|---|---|---|---|
| Raft, 3 nodes, 30 s | 29,272 | 975 jobs/s | 5.3 ms | 123 ms | 9.3 ms | 161 ms | 0 |
| Raft, 5 nodes, 30 s | 23,280 | 776 jobs/s | 5.3 ms | 147 ms | 9.7 ms | 303 ms | 0 |

Results: [`chaos-raft-3-steady`](../bench/results/chaos-raft-3-steady-20261104-165906.md),
[`chaos-raft-5-steady`](../bench/results/chaos-raft-5-steady-20261104-165937.md).

**Why the chaos runs have *higher* throughput than steady state.** All nodes share one disk, and
fsyncs from every node queue behind each other. While a killed node is down, one fewer process is
fsyncing, so commits on the survivors get faster. On separate machines this effect would go away.
The 5-node cluster is slower than the 3-node one for the same reason: it does more fsyncs per commit
on the same disk.

## Micro-benchmarks (in process, no disk)

| Benchmark | Result | Command |
|---|---|---|
| Raft commit, 3 nodes on the in-memory network | 87k–133k commits/s (7.5–11.5 µs/op) | `go test -run '^$' -bench Propose3Nodes -benchtime 3s ./internal/raft/` |
| FSM apply: enqueue | 4.8–6.9 µs/op | `go test -run '^$' -bench ApplyEnqueue -benchtime 200000x ./internal/fsm/` |
| FSM apply: claim, 1M jobs preloaded | 3.7–4.8 µs/op | `go test -run '^$' -bench ApplyClaim -benchtime 200000x ./internal/fsm/` |

The gap between roughly 100k commits/s in memory and roughly 1k jobs/s end to end is mostly fsync on
the shared disk, plus HTTP and JSON. The consensus code isn't the bottleneck.

## A finding: snapshot size vs. failover time

An earlier chaos run kept finished jobs for the server's default **1 hour**
([`chaos-raft-3-…-retention1h`](../bench/results/chaos-raft-3-20261104-164612-retention1h.md)).
It still lost **0** jobs, but throughput fell to 456 jobs/s and failover got as bad as 7.7 s (with a
14.9 s gap between successful enqueues). Every finished job stays in the snapshot, and
`InstallSnapshot` sends the whole snapshot in one RPC. A restarted node fell behind and needed a
large snapshot, and sending it competed with normal replication.

The chaos harness now runs nodes with `--retention 30s`. The real fixes are chunked or streamed
snapshot transfer, and moving finished jobs out of the replicated state. Both are listed under
future work in [DESIGN.md](DESIGN.md).

## Postgres baseline

Not measured yet. Docker's engine wasn't available on the benchmark machine. The pgbroker
conformance tests run in CI against a `postgres:17` service container. To measure it locally:

```bash
make up            # postgres:17 from deploy/docker-compose.yml
make bench-pg
```

## Reproduce

```bash
go build -o bin/ ./cmd/...
./bin/goqueue-bench chaos --nodes 3 --duration 120s --kill-every 10s          # leader kills
./bin/goqueue-bench chaos --nodes 5 --duration 120s --kill-every 10s
./bin/goqueue-bench chaos --nodes 3 --duration 60s  --kill-every 10s --partition
./bin/goqueue-bench chaos --nodes 3 --duration 30s  --kill-every 10m          # steady state
```
