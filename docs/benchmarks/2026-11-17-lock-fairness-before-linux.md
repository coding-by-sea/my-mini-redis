# Load benchmark: my-redis vs Redis

| | |
|---|---|
| Date (Unix time) | 1794956063 |
| Commit | unknown |
| Machine | linux aarch64, 18 CPUs |
| my-redis | fresh process per run |
| Redis | 8.0.2, fresh process per run |
| Load | 50 connections, pipeline 16, 64 B payload, 10000 keys, 5s measured after 1s warmup (sweeps: 2s after 0.5s) |

**Reading the numbers.** Both servers and the load generator share this machine. Redis runs commands on one thread; my-redis uses every core, so compare *CPU per op* for efficiency and *ops/s* for capacity. *Cores* is server CPU time divided by wall time. If the load generator's cores approach the machine's CPU count, it is the bottleneck and ops/s understates both servers. Latency for raw scenarios is per pipelined batch.

## Summary

| Scenario | my-redis ops/s | Redis ops/s | my-redis / Redis | p50 my-redis / Redis | p99 my-redis / Redis | CPU per op my-redis / Redis | cores my-redis / Redis | peak RSS my-redis / Redis |
|---|---:|---:|---:|---:|---:|---:|---:|---:|
| PING (raw, pipeline 16) | 8,878,256 | 4,972,419 | 1.79x | 87.0us / 161.8us | 184.3us / 221.2us | 0.35us / 0.20us | 3.12 / 1.00 | 3.0 MiB / 18.5 MiB |
| SET (raw, pipeline 16) | 1,069,213 | 2,969,462 | 0.36x | 720.9us / 282.6us | 1.57ms / 368.6us | 9.40us / 0.34us | 10.05 / 1.00 | 4.8 MiB / 19.2 MiB |
| GET (raw, pipeline 16) | 2,043,312 | 3,488,362 | 0.59x | 368.6us / 239.6us | 950.3us / 303.1us | 5.13us / 0.29us | 10.48 / 1.00 | 6.1 MiB / 19.8 MiB |
| PUBLISH, no subscribers (raw, pipeline 16) | 7,060,896 | 3,439,264 | 2.05x | 108.5us / 239.6us | 241.7us / 319.5us | 0.67us / 0.29us | 4.74 / 1.00 | 3.0 MiB / 18.5 MiB |
| SET via Client (1 in flight) | 586,677 | 490,886 | 1.20x | 81.9us / 102.4us | 172.0us / 163.8us | 4.69us / 2.04us | 2.75 / 1.00 | 5.7 MiB / 19.6 MiB |
| GET via Client (1 in flight) | 611,349 | 499,107 | 1.22x | 78.8us / 100.4us | 161.8us / 163.8us | 4.35us / 2.01us | 2.66 / 1.00 | 5.7 MiB / 19.8 MiB |
| PUBLISH to 50 subscribers (4 publishers, pipeline 16) | 253,245 | 285,242 | 0.89x | 239.6us / 225.3us | 565.2us / 294.9us | 10.57us / 3.50us | 2.68 / 1.00 | 8.4 MiB / 19.1 MiB |
| SET (raw, pipeline 16) during PUBLISH to 50 subscribers | 951,069 | 1,599,139 | 0.59x | 794.6us / 495.6us | 1.77ms / 761.9us | 9.84us / 0.63us | 9.36 / 1.00 | 7.9 MiB / 19.9 MiB |

### Latency detail

| Scenario | Server | p50 | p90 | p99 | p99.9 | max |
|---|---|---:|---:|---:|---:|---:|
| PING (raw, pipeline 16) | my-redis | 87.0us | 135.2us | 184.3us | 229.4us | 426.0us |
| PING (raw, pipeline 16) | Redis | 161.8us | 198.7us | 221.2us | 260.1us | 493.4us |
| SET (raw, pipeline 16) | my-redis | 720.9us | 1.13ms | 1.57ms | 2.59ms | 11.33ms |
| SET (raw, pipeline 16) | Redis | 282.6us | 319.5us | 368.6us | 466.9us | 742.1us |
| GET (raw, pipeline 16) | my-redis | 368.6us | 630.8us | 950.3us | 1.44ms | 3.84ms |
| GET (raw, pipeline 16) | Redis | 239.6us | 278.5us | 303.1us | 352.3us | 539.3us |
| PUBLISH, no subscribers (raw, pipeline 16) | my-redis | 108.5us | 170.0us | 241.7us | 323.6us | 10.51ms |
| PUBLISH, no subscribers (raw, pipeline 16) | Redis | 239.6us | 278.5us | 319.5us | 438.3us | 1.30ms |
| SET via Client (1 in flight) | my-redis | 81.9us | 127.0us | 172.0us | 213.0us | 472.0us |
| SET via Client (1 in flight) | Redis | 102.4us | 137.2us | 163.8us | 190.5us | 280.5us |
| GET via Client (1 in flight) | my-redis | 78.8us | 120.8us | 161.8us | 198.7us | 385.8us |
| GET via Client (1 in flight) | Redis | 100.4us | 135.2us | 163.8us | 208.9us | 631.2us |
| PUBLISH to 50 subscribers (4 publishers, pipeline 16) | my-redis | 239.6us | 401.4us | 565.2us | 712.7us | 1.09ms |
| PUBLISH to 50 subscribers (4 publishers, pipeline 16) | Redis | 225.3us | 256.0us | 294.9us | 360.4us | 445.3us |
| SET (raw, pipeline 16) during PUBLISH to 50 subscribers | my-redis | 794.6us | 1.25ms | 1.77ms | 9.57ms | 36.65ms |
| SET (raw, pipeline 16) during PUBLISH to 50 subscribers | Redis | 495.6us | 540.7us | 761.9us | 950.3us | 1.56ms |

### Pub/sub delivery

Publishes are counted at the publishers; deliveries at the subscribers (publishes x subscribers when nothing is lost). *RSS growth* is how much the server's memory grew during the run, mostly queued messages.

| Run | Server | publishes/s | deliveries/s | delivery p50 | p99 | p99.9 | max | disconnected | peak RSS | RSS growth |
|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| PUBLISH to 50 subscribers (4 publishers, pipeline 16) | my-redis | 253,245 | 12,662,240 | 290.8us | 712.7us | 909.3us | 1.47ms | 0 of 50 | 8.4 MiB | +5.8 MiB |
| PUBLISH to 50 subscribers (4 publishers, pipeline 16) | Redis | 285,242 | 14,262,080 | 323.6us | 446.5us | 499.7us | 669.2us | 0 of 50 | 19.1 MiB | +992.0 KiB |
| SET (raw, pipeline 16) during PUBLISH to 50 subscribers | my-redis | 18,496 | 924,800 | 2.52ms | 14.81ms | 25.95ms | 62.04ms | 0 of 50 | 7.9 MiB | +5.4 MiB |
| SET (raw, pipeline 16) during PUBLISH to 50 subscribers | Redis | 127,990 | 6,399,520 | 606.2us | 942.1us | 1.13ms | 1.49ms | 0 of 50 | 19.9 MiB | +1.8 MiB |

## Memory

Resident memory (RSS) of each server process, read from the OS after letting allocations settle. Per-connection and per-key numbers are the RSS increase divided by the count, so they include allocator overhead. Freed memory is often kept by the allocator, so RSS rarely shrinks.

| Measure | my-redis | Redis | my-redis / Redis |
|---|---:|---:|---:|
| Idle RSS | 2.7 MiB | 18.1 MiB | 0.15x |
| Per idle connection (1000 open) | 5.6 KiB | 5.4 KiB | 1.03x |
| Per key, 16 B value (100000 keys) | 196 B | 95 B | 2.06x |
|   overhead per key beyond 31 B of key + value | 165 B | 64 B | 2.57x |
| Per key, 1.0 KiB value (20000 keys) | 1.5 KiB | 1.3 KiB | 1.18x |
|   overhead per key beyond 1.0 KiB of key + value | 524 B | 286 B | 1.83x |

## Load generator

CPU used by the benchmark process itself during each summary run. Near 18 cores, the generator is saturated and the server isn't the limit.

| Scenario | Server | load generator cores | server cores | total |
|---|---|---:|---:|---:|
| PING (raw, pipeline 16) | my-redis | 2.42 | 3.12 | 5.54 of 18 |
| PING (raw, pipeline 16) | Redis | 1.30 | 1.00 | 2.30 of 18 |
| SET (raw, pipeline 16) | my-redis | 0.72 | 10.05 | 10.76 of 18 |
| SET (raw, pipeline 16) | Redis | 0.77 | 1.00 | 1.77 of 18 |
| GET (raw, pipeline 16) | my-redis | 0.99 | 10.48 | 11.47 of 18 |
| GET (raw, pipeline 16) | Redis | 0.90 | 1.00 | 1.90 of 18 |
| PUBLISH, no subscribers (raw, pipeline 16) | my-redis | 2.29 | 4.74 | 7.04 of 18 |
| PUBLISH, no subscribers (raw, pipeline 16) | Redis | 0.92 | 1.00 | 1.92 of 18 |
| SET via Client (1 in flight) | my-redis | 2.66 | 2.75 | 5.42 of 18 |
| SET via Client (1 in flight) | Redis | 2.12 | 1.00 | 3.12 of 18 |
| GET via Client (1 in flight) | my-redis | 2.62 | 2.66 | 5.28 of 18 |
| GET via Client (1 in flight) | Redis | 2.08 | 1.00 | 3.08 of 18 |
| PUBLISH to 50 subscribers (4 publishers, pipeline 16) | my-redis | 5.30 | 2.68 | 7.98 of 18 |
| PUBLISH to 50 subscribers (4 publishers, pipeline 16) | Redis | 4.88 | 1.00 | 5.88 of 18 |
| SET (raw, pipeline 16) during PUBLISH to 50 subscribers | my-redis | 0.91 | 9.36 | 10.27 of 18 |
| SET (raw, pipeline 16) during PUBLISH to 50 subscribers | Redis | 2.44 | 1.00 | 3.44 of 18 |

## Observations

- Largest throughput gap: **SET (raw, pipeline 16)** at 0.36x Redis.
- Closest to (or ahead of) Redis: **PUBLISH, no subscribers (raw, pipeline 16)** at 2.05x.
- my-redis uses on average **9.1x** the CPU time per operation that Redis does across the summary scenarios; that is the efficiency gap to close independently of core count.
- Each key with a 16 B value costs 196 B in my-redis and 95 B in Redis (2.1x).

## Profiling next steps

- CPU profile the server under one scenario: `cargo build --release`, start `target/release/my-redis-server`, run `scripts/bench.sh --address 127.0.0.1:6379 --no-redis --quick --only set --duration 30`, and attach a sampling profiler to the server (`samply record -p <pid>` on macOS, `perf record -g -p <pid>` on Linux).
- Compare allocation counts with a heap profiler (`heaptrack`, or Instruments' Allocations on macOS) on the same run; a high *CPU per op* with low *cores* points at per-request allocation or syscalls rather than contention.
- `JSON` with every number is next to this report for tracking changes over time.

