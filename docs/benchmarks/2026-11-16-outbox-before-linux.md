# Load benchmark: my-redis vs Redis

| | |
|---|---|
| Date (Unix time) | 1794842823 |
| Commit | a6a9aa8 |
| Machine | linux aarch64, 18 CPUs |
| my-redis | fresh process per run |
| Redis | 8.0.2, fresh process per run |
| Load | 50 connections, pipeline 16, 64 B payload, 10000 keys, 5s measured after 1s warmup (sweeps: 2s after 0.5s) |

**Reading the numbers.** Both servers and the load generator share this machine. Redis runs commands on one thread; my-redis uses every core, so compare *CPU per op* for efficiency and *ops/s* for capacity. *Cores* is server CPU time divided by wall time. If the load generator's cores approach the machine's CPU count, it is the bottleneck and ops/s understates both servers. Latency for raw scenarios is per pipelined batch.

## Summary

| Scenario | my-redis ops/s | Redis ops/s | my-redis / Redis | p50 my-redis / Redis | p99 my-redis / Redis | CPU per op my-redis / Redis | cores my-redis / Redis | peak RSS my-redis / Redis |
|---|---:|---:|---:|---:|---:|---:|---:|---:|
| PING (raw, pipeline 16) | 7,199,158 | 4,540,973 | 1.59x | 98.3us / 172.0us | 262.1us / 380.9us | 0.45us / 0.22us | 3.24 / 1.00 | 3.2 MiB / 18.5 MiB |
| SET (raw, pipeline 16) | 1,001,590 | 3,013,699 | 0.33x | 745.5us / 278.5us | 1.74ms / 360.4us | 10.34us / 0.33us | 10.35 / 1.00 | 5.0 MiB / 19.2 MiB |
| GET (raw, pipeline 16) | 1,993,942 | 3,447,034 | 0.58x | 376.8us / 241.7us | 958.5us / 307.2us | 5.33us / 0.29us | 10.63 / 1.00 | 6.0 MiB / 19.7 MiB |
| PUBLISH, no subscribers (raw, pipeline 16) | 6,421,030 | 3,463,216 | 1.85x | 118.8us / 239.6us | 270.3us / 303.1us | 0.84us / 0.29us | 5.42 / 1.00 | 3.2 MiB / 18.5 MiB |
| SET via Client (1 in flight) | 602,724 | 488,911 | 1.23x | 79.9us / 103.4us | 163.8us / 163.8us | 4.53us / 2.04us | 2.73 / 1.00 | 5.9 MiB / 19.5 MiB |
| GET via Client (1 in flight) | 622,820 | 502,555 | 1.24x | 77.8us / 100.4us | 157.7us / 159.7us | 4.31us / 1.99us | 2.69 / 1.00 | 5.9 MiB / 19.7 MiB |
| PUBLISH to 50 subscribers (4 publishers, pipeline 16) | 61,386 | 286,624 | 0.21x | 917.5us / 223.2us | 2.98ms / 290.8us | 91.21us / 3.49us | 5.60 / 1.00 | 4.9 MiB / 19.1 MiB |

### Latency detail

| Scenario | Server | p50 | p90 | p99 | p99.9 | max |
|---|---|---:|---:|---:|---:|---:|
| PING (raw, pipeline 16) | my-redis | 98.3us | 161.8us | 262.1us | 483.3us | 211.94ms |
| PING (raw, pipeline 16) | Redis | 172.0us | 217.1us | 380.9us | 696.3us | 7.93ms |
| SET (raw, pipeline 16) | my-redis | 745.5us | 1.18ms | 1.74ms | 10.88ms | 41.17ms |
| SET (raw, pipeline 16) | Redis | 278.5us | 319.5us | 360.4us | 430.1us | 1.40ms |
| GET (raw, pipeline 16) | my-redis | 376.8us | 639.0us | 958.5us | 1.74ms | 11.16ms |
| GET (raw, pipeline 16) | Redis | 241.7us | 282.6us | 307.2us | 348.2us | 832.3us |
| PUBLISH, no subscribers (raw, pipeline 16) | my-redis | 118.8us | 188.4us | 270.3us | 368.6us | 1.30ms |
| PUBLISH, no subscribers (raw, pipeline 16) | Redis | 239.6us | 278.5us | 303.1us | 344.1us | 479.4us |
| SET via Client (1 in flight) | my-redis | 79.9us | 122.9us | 163.8us | 194.6us | 405.4us |
| SET via Client (1 in flight) | Redis | 103.4us | 137.2us | 163.8us | 192.5us | 577.4us |
| GET via Client (1 in flight) | my-redis | 77.8us | 118.8us | 157.7us | 186.4us | 293.4us |
| GET via Client (1 in flight) | Redis | 100.4us | 133.1us | 159.7us | 186.4us | 308.0us |
| PUBLISH to 50 subscribers (4 publishers, pipeline 16) | my-redis | 917.5us | 1.90ms | 2.98ms | 4.06ms | 5.21ms |
| PUBLISH to 50 subscribers (4 publishers, pipeline 16) | Redis | 223.2us | 256.0us | 290.8us | 360.4us | 496.4us |

### Pub/sub delivery

Publishes are counted at the publishers; deliveries at the subscribers (publishes x subscribers when nothing is lost). *RSS growth* is how much the server's memory grew during the run, mostly queued messages.

| Run | Server | publishes/s | deliveries/s | delivery p50 | p99 | p99.9 | max | disconnected | peak RSS | RSS growth |
|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| summary | my-redis | 61,386 | 3,069,280 | 647.2us | 2.59ms | 3.60ms | 5.64ms | 0 of 50 | 4.9 MiB | +2.3 MiB |
| summary | Redis | 286,624 | 14,331,200 | 323.6us | 442.4us | 491.5us | 693.6us | 0 of 50 | 19.1 MiB | +984.0 KiB |

## Memory

Resident memory (RSS) of each server process, read from the OS after letting allocations settle. Per-connection and per-key numbers are the RSS increase divided by the count, so they include allocator overhead. Freed memory is often kept by the allocator, so RSS rarely shrinks.

| Measure | my-redis | Redis | my-redis / Redis |
|---|---:|---:|---:|
| Idle RSS | 2.7 MiB | 18.1 MiB | 0.15x |
| Per idle connection (1000 open) | 11.1 KiB | 5.4 KiB | 2.08x |
| Per key, 16 B value (100000 keys) | 200 B | 93 B | 2.15x |
|   overhead per key beyond 31 B of key + value | 169 B | 62 B | 2.73x |
| Per key, 1.0 KiB value (20000 keys) | 1.5 KiB | 1.3 KiB | 1.18x |
|   overhead per key beyond 1.0 KiB of key + value | 520 B | 285 B | 1.82x |

## Load generator

CPU used by the benchmark process itself during each summary run. Near 18 cores, the generator is saturated and the server isn't the limit.

| Scenario | Server | load generator cores | server cores | total |
|---|---|---:|---:|---:|
| PING (raw, pipeline 16) | my-redis | 2.36 | 3.24 | 5.60 of 18 |
| PING (raw, pipeline 16) | Redis | 1.28 | 1.00 | 2.28 of 18 |
| SET (raw, pipeline 16) | my-redis | 0.76 | 10.35 | 11.12 of 18 |
| SET (raw, pipeline 16) | Redis | 0.82 | 1.00 | 1.82 of 18 |
| GET (raw, pipeline 16) | my-redis | 0.99 | 10.63 | 11.63 of 18 |
| GET (raw, pipeline 16) | Redis | 0.93 | 1.00 | 1.93 of 18 |
| PUBLISH, no subscribers (raw, pipeline 16) | my-redis | 2.36 | 5.42 | 7.78 of 18 |
| PUBLISH, no subscribers (raw, pipeline 16) | Redis | 0.92 | 1.00 | 1.93 of 18 |
| SET via Client (1 in flight) | my-redis | 2.66 | 2.73 | 5.39 of 18 |
| SET via Client (1 in flight) | Redis | 2.08 | 1.00 | 3.08 of 18 |
| GET via Client (1 in flight) | my-redis | 2.60 | 2.69 | 5.29 of 18 |
| GET via Client (1 in flight) | Redis | 2.05 | 1.00 | 3.05 of 18 |
| PUBLISH to 50 subscribers (4 publishers, pipeline 16) | my-redis | 2.75 | 5.60 | 8.34 of 18 |
| PUBLISH to 50 subscribers (4 publishers, pipeline 16) | Redis | 4.92 | 1.00 | 5.92 of 18 |

## Observations

- Largest throughput gap: **PUBLISH to 50 subscribers (4 publishers, pipeline 16)** at 0.21x Redis.
- Closest to (or ahead of) Redis: **PUBLISH, no subscribers (raw, pipeline 16)** at 1.85x.
- my-redis uses on average **12.1x** the CPU time per operation that Redis does across the summary scenarios; that is the efficiency gap to close independently of core count.
- Each key with a 16 B value costs 200 B in my-redis and 93 B in Redis (2.2x).

## Profiling next steps

- CPU profile the server under one scenario: `cargo build --release`, start `target/release/my-redis-server`, run `scripts/bench.sh --address 127.0.0.1:6379 --no-redis --quick --only set --duration 30`, and attach a sampling profiler to the server (`samply record -p <pid>` on macOS, `perf record -g -p <pid>` on Linux).
- Compare allocation counts with a heap profiler (`heaptrack`, or Instruments' Allocations on macOS) on the same run; a high *CPU per op* with low *cores* points at per-request allocation or syscalls rather than contention.
- `JSON` with every number is next to this report for tracking changes over time.

