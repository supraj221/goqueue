# chaos-raft-3 (raft-3)

- Run at: 2026-11-04T21:46:12Z
- Machine: windows/amd64, 12 logical CPUs, go1.27.3
- Command: `goqueue-bench chaos --nodes 3 --binary ./bin/goqueued.exe --duration 120s --kill-every 10s`

## Throughput

| Metric | Value |
|---|---|
| Load duration | 120.4 s |
| Drain time | 0.1 s |
| Enqueued (acknowledged) | 54924 |
| Enqueue errors | 0 |
| Enqueue throughput | 456 jobs/s |
| Processed during load | 54783 (455 jobs/s) |
| Processed total | 54924 (456 jobs/s over the whole run) |

## Latency (ms)

| Measure | Count | p50 | p95 | p99 | max | mean |
|---|---|---|---|---|---|---|
| Enqueue | 54924 | 7.38 | 31.54 | 650.58 | 14967.12 | 35.06 |
| Queue wait (enqueue request to claim) | 54924 | 12.90 | 93.36 | 1340.19 | 22003.28 | 119.32 |
| End to end (enqueue request to ack) | 54924 | 87.70 | 1148.81 | 5680.64 | 22289.89 | 373.90 |

## Correctness

| Check | Value |
|---|---|
| Acknowledged enqueues | 54924 |
| Processed by workers | 54924 |
| Processed but ack response lost (verified via GET) | 0 |
| **Lost jobs** | **0** |
| Duplicate deliveries (jobs delivered more than once) | 397 (451 extra deliveries) |
| Worker acks answered lease_lost | 578 |
| Dequeue errors | 0 |

## Chaos

| Metric | Value |
|---|---|
| Nodes | 3 |
| Leader kill interval | 10s |
| Leader kills | 11 |
| Restarts | 11 |
| Failover time p50 (kill to first successful enqueue) | 1199 ms |
| Failover time max | 7665 ms |
| Longest gap between successful enqueues | 14888 ms |
| Kills with no recovery before the load ended | 0 |
| Partition scenario | not run (re-run with --partition to freeze the leader instead of killing it) |

| # | Killed node | Killed at | Failover |
|---|---|---|---|
| 1 | n1 | 16:44:22.369 | 180 ms |
| 2 | n3 | 16:44:32.387 | 1111 ms |
| 3 | n2 | 16:44:42.385 | 259 ms |
| 4 | n3 | 16:44:52.371 | 1199 ms |
| 5 | n2 | 16:45:02.572 | 2825 ms |
| 6 | n1 | 16:45:12.369 | 1859 ms |
| 7 | n2 | 16:45:22.369 | 7665 ms |
| 8 | n2 | 16:45:32.369 | 3433 ms |
| 9 | n2 | 16:45:42.371 | 339 ms |
| 10 | n1 | 16:45:52.379 | 1 ms |
| 11 | n3 | 16:46:02.369 | 1623 ms |

Final cluster status:

- n1 role=leader term=35 commit=125192 applied=125192
- n2 role=follower term=35 commit=125192 applied=125192
- n3 role=follower term=35 commit=125192 applied=125192
