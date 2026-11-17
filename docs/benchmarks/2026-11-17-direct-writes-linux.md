# Load benchmark: my-mini-redis vs Redis

| | |
|---|---|
| Date (Unix time) | 1794930643 |
| Commit | 958c7b7 (uncommitted changes) |
| Machine | linux aarch64, 18 CPUs |
| my-mini-redis | fresh process per run |
| Redis | 8.0.2, fresh process per run |
| Load | 50 connections, pipeline 16, 64 B payload, 10000 keys, 5s measured after 1s warmup (sweeps: 2s after 0.5s) |

**Reading the numbers.** Both servers and the load generator share this machine. Redis runs commands on one thread; my-mini-redis uses every core, so compare *CPU per op* for efficiency and *ops/s* for capacity. *Cores* is server CPU time divided by wall time. If the load generator's cores approach the machine's CPU count, it is the bottleneck and ops/s understates both servers. Latency for raw scenarios is per pipelined batch.

## Summary

| Scenario | my-mini-redis ops/s | Redis ops/s | mini / Redis | p50 mini / Redis | p99 mini / Redis | CPU per op mini / Redis | cores mini / Redis | peak RSS mini / Redis |
|---|---:|---:|---:|---:|---:|---:|---:|---:|
| PING (raw, pipeline 16) | 9,096,842 | 5,041,168 | 1.80x | 85.0us / 159.7us | 176.1us / 221.2us | 0.34us / 0.20us | 3.13 / 1.00 | 3.0 MiB / 18.5 MiB |
| SET (raw, pipeline 16) | 1,061,325 | 3,004,557 | 0.35x | 712.7us / 278.5us | 1.88ms / 372.7us | 9.64us / 0.33us | 10.23 / 1.00 | 4.7 MiB / 19.2 MiB |
| GET (raw, pipeline 16) | 1,941,645 | 3,415,373 | 0.57x | 372.7us / 241.7us | 1.11ms / 352.3us | 5.45us / 0.29us | 10.57 / 1.00 | 6.0 MiB / 19.8 MiB |
| PUBLISH, no subscribers (raw, pipeline 16) | 6,992,736 | 3,445,222 | 2.03x | 108.5us / 241.7us | 251.9us / 311.3us | 0.69us / 0.29us | 4.81 / 1.00 | 3.0 MiB / 18.5 MiB |
| SET via Client (1 in flight) | 596,386 | 482,825 | 1.24x | 80.9us / 104.4us | 167.9us / 167.9us | 4.56us / 2.07us | 2.72 / 1.00 | 5.7 MiB / 19.6 MiB |
| GET via Client (1 in flight) | 624,553 | 494,351 | 1.26x | 77.8us / 101.4us | 157.7us / 163.8us | 4.22us / 2.02us | 2.64 / 1.00 | 5.8 MiB / 19.8 MiB |
| PUBLISH to 50 subscribers (4 publishers, pipeline 16) | 254,522 | 279,843 | 0.91x | 239.6us / 227.3us | 565.2us / 319.5us | 10.45us / 3.57us | 2.66 / 1.00 | 8.5 MiB / 19.1 MiB |

### Latency detail

| Scenario | Server | p50 | p90 | p99 | p99.9 | max |
|---|---|---:|---:|---:|---:|---:|
| PING (raw, pipeline 16) | my-mini-redis | 85.0us | 130.0us | 176.1us | 215.0us | 391.4us |
| PING (raw, pipeline 16) | Redis | 159.7us | 196.6us | 221.2us | 274.4us | 484.3us |
| SET (raw, pipeline 16) | my-mini-redis | 712.7us | 1.15ms | 1.88ms | 5.51ms | 11.53ms |
| SET (raw, pipeline 16) | Redis | 278.5us | 319.5us | 372.7us | 466.9us | 1.41ms |
| GET (raw, pipeline 16) | my-mini-redis | 372.7us | 655.4us | 1.11ms | 4.19ms | 10.67ms |
| GET (raw, pipeline 16) | Redis | 241.7us | 282.6us | 352.3us | 516.1us | 2.68ms |
| PUBLISH, no subscribers (raw, pipeline 16) | my-mini-redis | 108.5us | 174.1us | 251.9us | 348.2us | 5.20ms |
| PUBLISH, no subscribers (raw, pipeline 16) | Redis | 241.7us | 278.5us | 311.3us | 368.6us | 474.1us |
| SET via Client (1 in flight) | my-mini-redis | 80.9us | 124.9us | 167.9us | 206.8us | 344.5us |
| SET via Client (1 in flight) | Redis | 104.4us | 139.3us | 167.9us | 213.0us | 907.0us |
| GET via Client (1 in flight) | my-mini-redis | 77.8us | 117.8us | 157.7us | 188.4us | 537.4us |
| GET via Client (1 in flight) | Redis | 101.4us | 135.2us | 163.8us | 204.8us | 317.4us |
| PUBLISH to 50 subscribers (4 publishers, pipeline 16) | my-mini-redis | 239.6us | 401.4us | 565.2us | 696.3us | 998.7us |
| PUBLISH to 50 subscribers (4 publishers, pipeline 16) | Redis | 227.3us | 262.1us | 319.5us | 393.2us | 495.8us |

### Pub/sub delivery

Publishes are counted at the publishers; deliveries at the subscribers (publishes x subscribers when nothing is lost). *RSS growth* is how much the server's memory grew during the run, mostly queued messages.

| Run | Server | publishes/s | deliveries/s | delivery p50 | p99 | p99.9 | max | disconnected | peak RSS | RSS growth |
|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| summary | my-mini-redis | 254,522 | 12,726,080 | 290.8us | 704.5us | 892.9us | 1.47ms | 0 of 50 | 8.5 MiB | +5.9 MiB |
| summary | Redis | 279,843 | 13,992,160 | 327.7us | 458.8us | 540.7us | 680.4us | 0 of 50 | 19.1 MiB | +988.0 KiB |

## Memory

Resident memory (RSS) of each server process, read from the OS after letting allocations settle. Per-connection and per-key numbers are the RSS increase divided by the count, so they include allocator overhead. Freed memory is often kept by the allocator, so RSS rarely shrinks.

| Measure | my-mini-redis | Redis | mini / Redis |
|---|---:|---:|---:|
| Idle RSS | 2.7 MiB | 18.1 MiB | 0.15x |
| Per idle connection (1000 open) | 5.6 KiB | 5.4 KiB | 1.03x |
| Per key, 16 B value (100000 keys) | 213 B | 95 B | 2.24x |
|   overhead per key beyond 31 B of key + value | 182 B | 64 B | 2.84x |
| Per key, 1.0 KiB value (20000 keys) | 1.5 KiB | 1.3 KiB | 1.18x |
|   overhead per key beyond 1.0 KiB of key + value | 520 B | 286 B | 1.82x |

## Load generator

CPU used by the benchmark process itself during each summary run. Near 18 cores, the generator is saturated and the server isn't the limit.

| Scenario | Server | load generator cores | server cores | total |
|---|---|---:|---:|---:|
| PING (raw, pipeline 16) | my-mini-redis | 2.41 | 3.13 | 5.54 of 18 |
| PING (raw, pipeline 16) | Redis | 1.30 | 1.00 | 2.30 of 18 |
| SET (raw, pipeline 16) | my-mini-redis | 0.73 | 10.23 | 10.96 of 18 |
| SET (raw, pipeline 16) | Redis | 0.79 | 1.00 | 1.79 of 18 |
| GET (raw, pipeline 16) | my-mini-redis | 0.99 | 10.57 | 11.56 of 18 |
| GET (raw, pipeline 16) | Redis | 0.92 | 1.00 | 1.92 of 18 |
| PUBLISH, no subscribers (raw, pipeline 16) | my-mini-redis | 2.31 | 4.81 | 7.12 of 18 |
| PUBLISH, no subscribers (raw, pipeline 16) | Redis | 0.91 | 1.00 | 1.91 of 18 |
| SET via Client (1 in flight) | my-mini-redis | 2.62 | 2.72 | 5.34 of 18 |
| SET via Client (1 in flight) | Redis | 2.07 | 1.00 | 3.08 of 18 |
| GET via Client (1 in flight) | my-mini-redis | 2.58 | 2.64 | 5.21 of 18 |
| GET via Client (1 in flight) | Redis | 2.06 | 1.00 | 3.06 of 18 |
| PUBLISH to 50 subscribers (4 publishers, pipeline 16) | my-mini-redis | 5.23 | 2.66 | 7.89 of 18 |
| PUBLISH to 50 subscribers (4 publishers, pipeline 16) | Redis | 4.70 | 1.00 | 5.70 of 18 |

## Observations

- Largest throughput gap: **SET (raw, pipeline 16)** at 0.35x Redis.
- Closest to (or ahead of) Redis: **PUBLISH, no subscribers (raw, pipeline 16)** at 2.03x.
- my-mini-redis uses on average **8.4x** the CPU time per operation that Redis does across the summary scenarios; that is the efficiency gap to close independently of core count.
- Each key with a 16 B value costs 213 B in my-mini-redis and 95 B in Redis (2.2x).

## Profiling next steps

- CPU profile the server under one scenario: `cargo build --release`, start `target/release/my-mini-redis-server`, run `scripts/bench.sh --address 127.0.0.1:6379 --no-redis --quick --only set --duration 30`, and attach a sampling profiler to the server (`samply record -p <pid>` on macOS, `perf record -g -p <pid>` on Linux).
- Compare allocation counts with a heap profiler (`heaptrack`, or Instruments' Allocations on macOS) on the same run; a high *CPU per op* with low *cores* points at per-request allocation or syscalls rather than contention.
- `JSON` with every number is next to this report for tracking changes over time.

