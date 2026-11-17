# Load benchmark: my-mini-redis vs Redis

| | |
|---|---|
| Date (Unix time) | 1794842722 |
| Commit | a6a9aa8 (uncommitted changes) |
| Machine | linux aarch64, 18 CPUs |
| my-mini-redis | fresh process per run |
| Redis | 8.0.2, fresh process per run |
| Load | 50 connections, pipeline 16, 64 B payload, 10000 keys, 5s measured after 1s warmup (sweeps: 2s after 0.5s) |

**Reading the numbers.** Both servers and the load generator share this machine. Redis runs commands on one thread; my-mini-redis uses every core, so compare *CPU per op* for efficiency and *ops/s* for capacity. *Cores* is server CPU time divided by wall time. If the load generator's cores approach the machine's CPU count, it is the bottleneck and ops/s understates both servers. Latency for raw scenarios is per pipelined batch.

## Summary

| Scenario | my-mini-redis ops/s | Redis ops/s | mini / Redis | p50 mini / Redis | p99 mini / Redis | CPU per op mini / Redis | cores mini / Redis | peak RSS mini / Redis |
|---|---:|---:|---:|---:|---:|---:|---:|---:|
| PING (raw, pipeline 16) | 8,888,922 | 4,925,654 | 1.80x | 87.0us / 163.8us | 178.2us / 229.4us | 0.37us / 0.20us | 3.29 / 1.00 | 3.2 MiB / 18.5 MiB |
| SET (raw, pipeline 16) | 1,075,734 | 3,017,584 | 0.36x | 704.5us / 278.5us | 1.62ms / 348.2us | 9.40us / 0.33us | 10.11 / 1.00 | 4.9 MiB / 19.2 MiB |
| GET (raw, pipeline 16) | 2,008,157 | 3,465,299 | 0.58x | 376.8us / 241.7us | 958.5us / 307.2us | 5.32us / 0.29us | 10.68 / 1.00 | 6.0 MiB / 19.7 MiB |
| PUBLISH, no subscribers (raw, pipeline 16) | 6,643,037 | 3,476,806 | 1.91x | 114.7us / 239.6us | 266.2us / 303.1us | 0.81us / 0.29us | 5.35 / 1.00 | 3.2 MiB / 18.5 MiB |
| SET via Client (1 in flight) | 603,611 | 492,077 | 1.23x | 79.9us / 102.4us | 163.8us / 163.8us | 4.50us / 2.03us | 2.71 / 1.00 | 5.9 MiB / 19.5 MiB |
| GET via Client (1 in flight) | 623,407 | 505,710 | 1.23x | 77.8us / 99.3us | 157.7us / 157.7us | 4.27us / 1.98us | 2.66 / 1.00 | 6.0 MiB / 19.7 MiB |
| PUBLISH to 50 subscribers (4 publishers, pipeline 16) | 220,634 | 286,960 | 0.77x | 278.5us / 223.2us | 639.0us / 294.9us | 11.77us / 3.48us | 2.60 / 1.00 | 7.0 MiB / 19.1 MiB |

### Latency detail

| Scenario | Server | p50 | p90 | p99 | p99.9 | max |
|---|---|---:|---:|---:|---:|---:|
| PING (raw, pipeline 16) | my-mini-redis | 87.0us | 133.1us | 178.2us | 215.0us | 535.2us |
| PING (raw, pipeline 16) | Redis | 163.8us | 200.7us | 229.4us | 356.4us | 754.3us |
| SET (raw, pipeline 16) | my-mini-redis | 704.5us | 1.11ms | 1.62ms | 8.65ms | 12.26ms |
| SET (raw, pipeline 16) | Redis | 278.5us | 315.4us | 348.2us | 426.0us | 1.64ms |
| GET (raw, pipeline 16) | my-mini-redis | 376.8us | 630.8us | 958.5us | 1.69ms | 8.72ms |
| GET (raw, pipeline 16) | Redis | 241.7us | 278.5us | 307.2us | 385.0us | 1.03ms |
| PUBLISH, no subscribers (raw, pipeline 16) | my-mini-redis | 114.7us | 182.3us | 266.2us | 364.5us | 1.15ms |
| PUBLISH, no subscribers (raw, pipeline 16) | Redis | 239.6us | 278.5us | 303.1us | 385.0us | 1.65ms |
| SET via Client (1 in flight) | my-mini-redis | 79.9us | 122.9us | 163.8us | 194.6us | 361.5us |
| SET via Client (1 in flight) | Redis | 102.4us | 135.2us | 163.8us | 202.8us | 745.2us |
| GET via Client (1 in flight) | my-mini-redis | 77.8us | 118.8us | 157.7us | 186.4us | 295.5us |
| GET via Client (1 in flight) | Redis | 99.3us | 131.1us | 157.7us | 192.5us | 696.9us |
| PUBLISH to 50 subscribers (4 publishers, pipeline 16) | my-mini-redis | 278.5us | 458.8us | 639.0us | 802.8us | 1.03ms |
| PUBLISH to 50 subscribers (4 publishers, pipeline 16) | Redis | 223.2us | 254.0us | 294.9us | 372.7us | 703.7us |

### Pub/sub delivery

Publishes are counted at the publishers; deliveries at the subscribers (publishes x subscribers when nothing is lost). *RSS growth* is how much the server's memory grew during the run, mostly queued messages.

| Run | Server | publishes/s | deliveries/s | delivery p50 | p99 | p99.9 | max | disconnected | peak RSS | RSS growth |
|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| summary | my-mini-redis | 220,634 | 11,031,680 | 331.8us | 761.9us | 950.3us | 1.40ms | 0 of 50 | 7.0 MiB | +4.4 MiB |
| summary | Redis | 286,960 | 14,348,000 | 323.6us | 442.4us | 516.1us | 996.5us | 0 of 50 | 19.1 MiB | +984.0 KiB |

## Memory

Resident memory (RSS) of each server process, read from the OS after letting allocations settle. Per-connection and per-key numbers are the RSS increase divided by the count, so they include allocator overhead. Freed memory is often kept by the allocator, so RSS rarely shrinks.

| Measure | my-mini-redis | Redis | mini / Redis |
|---|---:|---:|---:|
| Idle RSS | 2.7 MiB | 18.1 MiB | 0.15x |
| Per idle connection (1000 open) | 9.6 KiB | 5.4 KiB | 1.80x |
| Per key, 16 B value (100000 keys) | 213 B | 93 B | 2.29x |
|   overhead per key beyond 31 B of key + value | 182 B | 62 B | 2.94x |
| Per key, 1.0 KiB value (20000 keys) | 1.5 KiB | 1.3 KiB | 1.18x |
|   overhead per key beyond 1.0 KiB of key + value | 526 B | 285 B | 1.84x |

## Load generator

CPU used by the benchmark process itself during each summary run. Near 18 cores, the generator is saturated and the server isn't the limit.

| Scenario | Server | load generator cores | server cores | total |
|---|---|---:|---:|---:|
| PING (raw, pipeline 16) | my-mini-redis | 2.40 | 3.29 | 5.69 of 18 |
| PING (raw, pipeline 16) | Redis | 1.30 | 1.00 | 2.30 of 18 |
| SET (raw, pipeline 16) | my-mini-redis | 0.72 | 10.11 | 10.83 of 18 |
| SET (raw, pipeline 16) | Redis | 0.78 | 1.00 | 1.78 of 18 |
| GET (raw, pipeline 16) | my-mini-redis | 0.96 | 10.68 | 11.64 of 18 |
| GET (raw, pipeline 16) | Redis | 0.89 | 1.00 | 1.89 of 18 |
| PUBLISH, no subscribers (raw, pipeline 16) | my-mini-redis | 2.26 | 5.35 | 7.61 of 18 |
| PUBLISH, no subscribers (raw, pipeline 16) | Redis | 0.91 | 1.00 | 1.91 of 18 |
| SET via Client (1 in flight) | my-mini-redis | 2.66 | 2.71 | 5.38 of 18 |
| SET via Client (1 in flight) | Redis | 2.08 | 1.00 | 3.08 of 18 |
| GET via Client (1 in flight) | my-mini-redis | 2.60 | 2.66 | 5.26 of 18 |
| GET via Client (1 in flight) | Redis | 2.06 | 1.00 | 3.06 of 18 |
| PUBLISH to 50 subscribers (4 publishers, pipeline 16) | my-mini-redis | 4.14 | 2.60 | 6.73 of 18 |
| PUBLISH to 50 subscribers (4 publishers, pipeline 16) | Redis | 4.96 | 1.00 | 5.95 of 18 |

## Observations

- Largest throughput gap: **SET (raw, pipeline 16)** at 0.36x Redis.
- Closest to (or ahead of) Redis: **PUBLISH, no subscribers (raw, pipeline 16)** at 1.91x.
- my-mini-redis uses on average **8.5x** the CPU time per operation that Redis does across the summary scenarios; that is the efficiency gap to close independently of core count.
- Each key with a 16 B value costs 213 B in my-mini-redis and 93 B in Redis (2.3x).

## Profiling next steps

- CPU profile the server under one scenario: `cargo build --release`, start `target/release/my-mini-redis-server`, run `scripts/bench.sh --address 127.0.0.1:6379 --no-redis --quick --only set --duration 30`, and attach a sampling profiler to the server (`samply record -p <pid>` on macOS, `perf record -g -p <pid>` on Linux).
- Compare allocation counts with a heap profiler (`heaptrack`, or Instruments' Allocations on macOS) on the same run; a high *CPU per op* with low *cores* points at per-request allocation or syscalls rather than contention.
- `JSON` with every number is next to this report for tracking changes over time.

