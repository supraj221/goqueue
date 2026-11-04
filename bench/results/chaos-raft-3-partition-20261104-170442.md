# chaos-raft-3-partition (raft-3-partition)

- Run at: 2026-11-04T22:04:42Z
- Machine: windows/amd64, 12 logical CPUs, go1.27.3
- Command: `goqueue-bench chaos --nodes 3 --duration 60s --kill-every 10s --partition --backend-label raft-3-partition`

## Throughput

| Metric | Value |
|---|---|
| Load duration | 60.2 s |
| Drain time | 2.9 s |
| Enqueued (acknowledged) | 71582 |
| Enqueue errors | 0 |
| Enqueue throughput | 1188 jobs/s |
| Processed during load | 62770 (1042 jobs/s) |
| Processed total | 71582 (1134 jobs/s over the whole run) |

## Latency (ms)

| Measure | Count | p50 | p95 | p99 | max | mean |
|---|---|---|---|---|---|---|
| Enqueue | 71582 | 5.79 | 9.54 | 218.58 | 3024.68 | 13.45 |
| Queue wait (enqueue request to claim) | 71582 | 3892.17 | 9973.49 | 11163.80 | 11651.86 | 3984.20 |
| End to end (enqueue request to ack) | 71582 | 4024.01 | 10328.87 | 11253.82 | 11784.52 | 4160.29 |

## Correctness

| Check | Value |
|---|---|
| Acknowledged enqueues | 71582 |
| Processed by workers | 71582 |
| Processed but ack response lost (verified via GET) | 0 |
| **Lost jobs** | **0** |
| Duplicate deliveries (jobs delivered more than once) | 1088 (1088 extra deliveries) |
| Worker acks answered lease_lost | 1089 |
| Dequeue errors | 0 |

## Chaos

| Metric | Value |
|---|---|
| Nodes | 3 |
| Leader kill interval | 10s |
| Leader kills | 5 |
| Restarts | 5 |
| Failover time p50 (kill to first successful enqueue) | 2964 ms |
| Failover time max | 3017 ms |
| Longest gap between successful enqueues | 3021 ms |
| Kills with no recovery before the load ended | 0 |
| Partition scenario | leader frozen with NtSuspendProcess/SIGSTOP for the restart delay, then resumed (counted under kills) |

| # | Killed node | Killed at | Failover |
|---|---|---|---|
| 1 | n1 | 17:03:49.537 | 2964 ms |
| 2 | n2 | 17:03:59.537 | 3017 ms |
| 3 | n3 | 17:04:09.538 | 2783 ms |
| 4 | n2 | 17:04:19.537 | 2955 ms |
| 5 | n3 | 17:04:29.537 | 2983 ms |

Final cluster status:

- n1 role=leader term=6 commit=152657 applied=152657
- n2 role=follower term=6 commit=152657 applied=152657
- n3 role=follower term=6 commit=152657 applied=152488
