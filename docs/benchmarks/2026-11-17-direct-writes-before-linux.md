# Load benchmark: my-redis vs Redis

| | |
|---|---|
| Date (Unix time) | 1794930879 |
| Commit | unknown |
| Machine | linux aarch64, 18 CPUs |
| my-redis | fresh process per run |
| Redis | 8.0.2, fresh process per run |
| Load | 50 connections, pipeline 16, 64 B payload, 10000 keys, 5s measured after 1s warmup (sweeps: 2s after 0.5s) |

**Reading the numbers.** Both servers and the load generator share this machine. Redis runs commands on one thread; my-redis uses every core, so compare *CPU per op* for efficiency and *ops/s* for capacity. *Cores* is server CPU time divided by wall time. If the load generator's cores approach the machine's CPU count, it is the bottleneck and ops/s understates both servers. Latency for raw scenarios is per pipelined batch.

## Summary

| Scenario | my-redis ops/s | Redis ops/s | my-redis / Redis | p50 my-redis / Redis | p99 my-redis / Redis | CPU per op my-redis / Redis | cores my-redis / Redis | peak RSS my-redis / Redis |
|---|---:|---:|---:|---:|---:|---:|---:|---:|
| PING (raw, pipeline 16) | 8,796,371 | 5,018,022 | 1.75x | 88.1us / 159.7us | 178.2us / 221.2us | 0.37us / 0.20us | 3.28 / 1.00 | 3.2 MiB / 18.5 MiB |
| SET (raw, pipeline 16) | 1,071,600 | 2,990,310 | 0.36x | 704.5us / 282.6us | 1.72ms / 360.4us | 9.51us / 0.33us | 10.19 / 1.00 | 5.0 MiB / 19.2 MiB |
| GET (raw, pipeline 16) | 1,913,450 | 3,416,784 | 0.56x | 385.0us / 243.7us | 1.06ms / 323.6us | 5.58us / 0.29us | 10.67 / 1.00 | 6.1 MiB / 19.8 MiB |
| PUBLISH, no subscribers (raw, pipeline 16) | 6,718,518 | 3,439,600 | 1.95x | 113.7us / 239.6us | 262.1us / 311.3us | 0.76us / 0.29us | 5.13 / 1.00 | 3.2 MiB / 18.5 MiB |
| SET via Client (1 in flight) | 600,243 | 478,734 | 1.25x | 80.9us / 104.4us | 165.9us / 176.1us | 4.49us / 2.09us | 2.70 / 1.00 | 5.9 MiB / 19.6 MiB |
| GET via Client (1 in flight) | 622,568 | 493,577 | 1.26x | 77.8us / 101.4us | 157.7us / 165.9us | 4.24us / 2.03us | 2.64 / 1.00 | 6.0 MiB / 19.8 MiB |
| PUBLISH to 50 subscribers (4 publishers, pipeline 16) | 214,464 | 279,130 | 0.77x | 286.7us / 229.4us | 663.6us / 319.5us | 12.15us / 3.58us | 2.61 / 1.00 | 6.7 MiB / 19.1 MiB |

### Latency detail

| Scenario | Server | p50 | p90 | p99 | p99.9 | max |
|---|---|---:|---:|---:|---:|---:|
| PING (raw, pipeline 16) | my-redis | 88.1us | 135.2us | 178.2us | 210.9us | 502.4us |
| PING (raw, pipeline 16) | Redis | 159.7us | 196.6us | 221.2us | 270.3us | 502.2us |
| SET (raw, pipeline 16) | my-redis | 704.5us | 1.13ms | 1.72ms | 10.62ms | 12.02ms |
| SET (raw, pipeline 16) | Redis | 282.6us | 319.5us | 360.4us | 421.9us | 617.0us |
| GET (raw, pipeline 16) | my-redis | 385.0us | 663.6us | 1.06ms | 3.93ms | 12.13ms |
| GET (raw, pipeline 16) | Redis | 243.7us | 282.6us | 323.6us | 380.9us | 545.6us |
| PUBLISH, no subscribers (raw, pipeline 16) | my-redis | 113.7us | 180.2us | 262.1us | 360.4us | 1.07ms |
| PUBLISH, no subscribers (raw, pipeline 16) | Redis | 239.6us | 278.5us | 311.3us | 376.8us | 535.5us |
| SET via Client (1 in flight) | my-redis | 80.9us | 123.9us | 165.9us | 202.8us | 1.02ms |
| SET via Client (1 in flight) | Redis | 104.4us | 141.3us | 176.1us | 294.9us | 965.7us |
| GET via Client (1 in flight) | my-redis | 77.8us | 118.8us | 157.7us | 188.4us | 338.5us |
| GET via Client (1 in flight) | Redis | 101.4us | 135.2us | 165.9us | 206.8us | 319.3us |
| PUBLISH to 50 subscribers (4 publishers, pipeline 16) | my-redis | 286.7us | 475.1us | 663.6us | 811.0us | 1.06ms |
| PUBLISH to 50 subscribers (4 publishers, pipeline 16) | Redis | 229.4us | 262.1us | 319.5us | 385.0us | 514.6us |

### Pub/sub delivery

Publishes are counted at the publishers; deliveries at the subscribers (publishes x subscribers when nothing is lost). *RSS growth* is how much the server's memory grew during the run, mostly queued messages.

| Run | Server | publishes/s | deliveries/s | delivery p50 | p99 | p99.9 | max | disconnected | peak RSS | RSS growth |
|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| summary | my-redis | 214,464 | 10,723,200 | 340.0us | 778.2us | 966.7us | 1.44ms | 0 of 50 | 6.7 MiB | +4.1 MiB |
| summary | Redis | 279,130 | 13,956,480 | 327.7us | 462.8us | 540.7us | 737.9us | 0 of 50 | 19.1 MiB | +984.0 KiB |

## Memory

Resident memory (RSS) of each server process, read from the OS after letting allocations settle. Per-connection and per-key numbers are the RSS increase divided by the count, so they include allocator overhead. Freed memory is often kept by the allocator, so RSS rarely shrinks.

| Measure | my-redis | Redis | my-redis / Redis |
|---|---:|---:|---:|
| Idle RSS | 2.7 MiB | 18.1 MiB | 0.15x |
| Per idle connection (1000 open) | 9.6 KiB | 5.4 KiB | 1.78x |
| Per key, 16 B value (100000 keys) | 202 B | 95 B | 2.12x |
|   overhead per key beyond 31 B of key + value | 171 B | 64 B | 2.66x |
| Per key, 1.0 KiB value (20000 keys) | 1.6 KiB | 1.3 KiB | 1.22x |
|   overhead per key beyond 1.0 KiB of key + value | 572 B | 286 B | 2.00x |

## Load generator

CPU used by the benchmark process itself during each summary run. Near 18 cores, the generator is saturated and the server isn't the limit.

| Scenario | Server | load generator cores | server cores | total |
|---|---|---:|---:|---:|
| PING (raw, pipeline 16) | my-redis | 2.40 | 3.28 | 5.69 of 18 |
| PING (raw, pipeline 16) | Redis | 1.31 | 1.00 | 2.31 of 18 |
| SET (raw, pipeline 16) | my-redis | 0.73 | 10.19 | 10.92 of 18 |
| SET (raw, pipeline 16) | Redis | 0.79 | 1.00 | 1.79 of 18 |
| GET (raw, pipeline 16) | my-redis | 0.97 | 10.67 | 11.65 of 18 |
| GET (raw, pipeline 16) | Redis | 0.90 | 1.00 | 1.90 of 18 |
| PUBLISH, no subscribers (raw, pipeline 16) | my-redis | 2.30 | 5.13 | 7.43 of 18 |
| PUBLISH, no subscribers (raw, pipeline 16) | Redis | 0.93 | 1.00 | 1.93 of 18 |
| SET via Client (1 in flight) | my-redis | 2.63 | 2.70 | 5.33 of 18 |
| SET via Client (1 in flight) | Redis | 2.07 | 1.00 | 3.07 of 18 |
| GET via Client (1 in flight) | my-redis | 2.57 | 2.64 | 5.21 of 18 |
| GET via Client (1 in flight) | Redis | 2.04 | 1.00 | 3.04 of 18 |
| PUBLISH to 50 subscribers (4 publishers, pipeline 16) | my-redis | 4.13 | 2.61 | 6.74 of 18 |
| PUBLISH to 50 subscribers (4 publishers, pipeline 16) | Redis | 4.86 | 1.00 | 5.86 of 18 |

## Observations

- Largest throughput gap: **SET (raw, pipeline 16)** at 0.36x Redis.
- Closest to (or ahead of) Redis: **PUBLISH, no subscribers (raw, pipeline 16)** at 1.95x.
- my-redis uses on average **8.5x** the CPU time per operation that Redis does across the summary scenarios; that is the efficiency gap to close independently of core count.
- Each key with a 16 B value costs 202 B in my-redis and 95 B in Redis (2.1x).

## Profiling next steps

- CPU profile the server under one scenario: `cargo build --release`, start `target/release/my-redis-server`, run `scripts/bench.sh --address 127.0.0.1:6379 --no-redis --quick --only set --duration 30`, and attach a sampling profiler to the server (`samply record -p <pid>` on macOS, `perf record -g -p <pid>` on Linux).
- Compare allocation counts with a heap profiler (`heaptrack`, or Instruments' Allocations on macOS) on the same run; a high *CPU per op* with low *cores* points at per-request allocation or syscalls rather than contention.
- `JSON` with every number is next to this report for tracking changes over time.

