# Load benchmark: my-mini-redis vs Redis

| | |
|---|---|
| Date (Unix time) | 1794966951 |
| Commit | unknown |
| Machine | linux aarch64, 18 CPUs |
| my-mini-redis | fresh process per run |
| Redis | 8.0.2, fresh process per run |
| Load | 50 connections, pipeline 16, 64 B payload, 10000 keys, 5s measured after 1s warmup (sweeps: 2s after 0.5s) |

**Reading the numbers.** Both servers and the load generator share this machine. Redis runs commands on one thread; my-mini-redis uses every core, so compare *CPU per op* for efficiency and *ops/s* for capacity. *Cores* is server CPU time divided by wall time. If the load generator's cores approach the machine's CPU count, it is the bottleneck and ops/s understates both servers. Latency for raw scenarios is per pipelined batch.

## Summary

| Scenario | my-mini-redis ops/s | Redis ops/s | mini / Redis | p50 mini / Redis | p99 mini / Redis | CPU per op mini / Redis | cores mini / Redis | peak RSS mini / Redis |
|---|---:|---:|---:|---:|---:|---:|---:|---:|
| PING (raw, pipeline 16) | 9,095,069 | 5,024,570 | 1.81x | 84.0us / 159.7us | 178.2us / 219.1us | 0.38us / 0.20us | 3.44 / 1.00 | 3.0 MiB / 18.5 MiB |
| SET (raw, pipeline 16) | 1,069,149 | 2,987,952 | 0.36x | 598.0us / 282.6us | 2.75ms / 376.8us | 9.37us / 0.33us | 10.02 / 1.00 | 4.8 MiB / 19.2 MiB |
| GET (raw, pipeline 16) | 2,088,531 | 3,453,741 | 0.60x | 311.3us / 241.7us | 1.39ms / 311.3us | 4.84us / 0.29us | 10.10 / 1.00 | 6.1 MiB / 19.8 MiB |
| PUBLISH, no subscribers (raw, pipeline 16) | 6,624,605 | 3,468,115 | 1.91x | 106.5us / 239.6us | 352.3us / 303.1us | 0.92us / 0.29us | 6.07 / 1.00 | 3.0 MiB / 18.5 MiB |
| SET via Client (1 in flight) | 593,396 | 490,495 | 1.21x | 81.9us / 102.4us | 165.9us / 163.8us | 4.61us / 2.04us | 2.74 / 1.00 | 5.7 MiB / 19.6 MiB |
| GET via Client (1 in flight) | 614,290 | 504,588 | 1.22x | 78.8us / 99.3us | 159.7us / 159.7us | 4.34us / 1.98us | 2.67 / 1.00 | 5.8 MiB / 19.8 MiB |
| PUBLISH to 50 subscribers (4 publishers, pipeline 16) | 248,176 | 280,314 | 0.89x | 243.7us / 229.4us | 573.4us / 311.3us | 10.87us / 3.57us | 2.70 / 1.00 | 8.3 MiB / 19.1 MiB |
| SET (raw, pipeline 16) during PUBLISH to 50 subscribers | 1,077,069 | 1,634,966 | 0.66x | 630.8us / 487.4us | 2.42ms / 712.7us | 8.00us / 0.61us | 8.61 / 1.00 | 12.2 MiB / 20.0 MiB |

### Latency detail

| Scenario | Server | p50 | p90 | p99 | p99.9 | max |
|---|---|---:|---:|---:|---:|---:|
| PING (raw, pipeline 16) | my-mini-redis | 84.0us | 133.1us | 178.2us | 219.1us | 481.7us |
| PING (raw, pipeline 16) | Redis | 159.7us | 196.6us | 219.1us | 254.0us | 365.5us |
| SET (raw, pipeline 16) | my-mini-redis | 598.0us | 1.46ms | 2.75ms | 6.16ms | 20.98ms |
| SET (raw, pipeline 16) | Redis | 282.6us | 319.5us | 376.8us | 753.7us | 1.74ms |
| GET (raw, pipeline 16) | my-mini-redis | 311.3us | 745.5us | 1.39ms | 2.13ms | 7.19ms |
| GET (raw, pipeline 16) | Redis | 241.7us | 278.5us | 311.3us | 380.9us | 1.68ms |
| PUBLISH, no subscribers (raw, pipeline 16) | my-mini-redis | 106.5us | 198.7us | 352.3us | 512.0us | 1.27ms |
| PUBLISH, no subscribers (raw, pipeline 16) | Redis | 239.6us | 278.5us | 303.1us | 348.2us | 514.5us |
| SET via Client (1 in flight) | my-mini-redis | 81.9us | 124.9us | 165.9us | 198.7us | 326.1us |
| SET via Client (1 in flight) | Redis | 102.4us | 137.2us | 163.8us | 192.5us | 314.8us |
| GET via Client (1 in flight) | my-mini-redis | 78.8us | 119.8us | 159.7us | 188.4us | 271.6us |
| GET via Client (1 in flight) | Redis | 99.3us | 133.1us | 159.7us | 186.4us | 315.9us |
| PUBLISH to 50 subscribers (4 publishers, pipeline 16) | my-mini-redis | 243.7us | 409.6us | 573.4us | 720.9us | 1.04ms |
| PUBLISH to 50 subscribers (4 publishers, pipeline 16) | Redis | 229.4us | 262.1us | 311.3us | 389.1us | 487.5us |
| SET (raw, pipeline 16) during PUBLISH to 50 subscribers | my-mini-redis | 630.8us | 1.38ms | 2.42ms | 3.44ms | 5.06ms |
| SET (raw, pipeline 16) during PUBLISH to 50 subscribers | Redis | 487.4us | 524.3us | 712.7us | 852.0us | 1.16ms |

### Pub/sub delivery

Publishes are counted at the publishers; deliveries at the subscribers (publishes x subscribers when nothing is lost). *RSS growth* is how much the server's memory grew during the run, mostly queued messages.

| Run | Server | publishes/s | deliveries/s | delivery p50 | p99 | p99.9 | max | disconnected | peak RSS | RSS growth |
|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| PUBLISH to 50 subscribers (4 publishers, pipeline 16) | my-mini-redis | 248,176 | 12,408,800 | 294.9us | 720.9us | 917.5us | 1.54ms | 0 of 50 | 8.3 MiB | +5.7 MiB |
| PUBLISH to 50 subscribers (4 publishers, pipeline 16) | Redis | 280,314 | 14,015,680 | 327.7us | 458.8us | 540.7us | 705.5us | 0 of 50 | 19.1 MiB | +992.0 KiB |
| SET (raw, pipeline 16) during PUBLISH to 50 subscribers | my-mini-redis | 99,731 | 4,986,560 | 557.1us | 2.56ms | 3.57ms | 6.53ms | 0 of 50 | 12.2 MiB | +9.6 MiB |
| SET (raw, pipeline 16) during PUBLISH to 50 subscribers | Redis | 130,870 | 6,543,520 | 606.2us | 892.9us | 1.04ms | 1.26ms | 0 of 50 | 20.0 MiB | +1.9 MiB |

## Memory

Resident memory (RSS) of each server process, read from the OS after letting allocations settle. Per-connection and per-key numbers are the RSS increase divided by the count, so they include allocator overhead. Freed memory is often kept by the allocator, so RSS rarely shrinks.

| Measure | my-mini-redis | Redis | mini / Redis |
|---|---:|---:|---:|
| Idle RSS | 2.7 MiB | 18.1 MiB | 0.15x |
| Per idle connection (1000 open) | 5.6 KiB | 5.4 KiB | 1.03x |
| Per key, 16 B value (100000 keys) | 213 B | 95 B | 2.24x |
|   overhead per key beyond 31 B of key + value | 182 B | 64 B | 2.84x |
| Per key, 1.0 KiB value (20000 keys) | 1.5 KiB | 1.3 KiB | 1.18x |
|   overhead per key beyond 1.0 KiB of key + value | 518 B | 286 B | 1.81x |

## Load generator

CPU used by the benchmark process itself during each summary run. Near 18 cores, the generator is saturated and the server isn't the limit.

| Scenario | Server | load generator cores | server cores | total |
|---|---|---:|---:|---:|
| PING (raw, pipeline 16) | my-mini-redis | 2.53 | 3.44 | 5.98 of 18 |
| PING (raw, pipeline 16) | Redis | 1.32 | 1.00 | 2.32 of 18 |
| SET (raw, pipeline 16) | my-mini-redis | 0.75 | 10.02 | 10.77 of 18 |
| SET (raw, pipeline 16) | Redis | 0.79 | 1.00 | 1.79 of 18 |
| GET (raw, pipeline 16) | my-mini-redis | 1.10 | 10.10 | 11.20 of 18 |
| GET (raw, pipeline 16) | Redis | 0.91 | 1.00 | 1.90 of 18 |
| PUBLISH, no subscribers (raw, pipeline 16) | my-mini-redis | 2.35 | 6.07 | 8.41 of 18 |
| PUBLISH, no subscribers (raw, pipeline 16) | Redis | 0.93 | 1.00 | 1.93 of 18 |
| SET via Client (1 in flight) | my-mini-redis | 2.67 | 2.74 | 5.41 of 18 |
| SET via Client (1 in flight) | Redis | 2.12 | 1.00 | 3.12 of 18 |
| GET via Client (1 in flight) | my-mini-redis | 2.62 | 2.67 | 5.29 of 18 |
| GET via Client (1 in flight) | Redis | 2.08 | 1.00 | 3.08 of 18 |
| PUBLISH to 50 subscribers (4 publishers, pipeline 16) | my-mini-redis | 5.28 | 2.70 | 7.98 of 18 |
| PUBLISH to 50 subscribers (4 publishers, pipeline 16) | Redis | 4.92 | 1.00 | 5.93 of 18 |
| SET (raw, pipeline 16) during PUBLISH to 50 subscribers | my-mini-redis | 2.07 | 8.61 | 10.68 of 18 |
| SET (raw, pipeline 16) during PUBLISH to 50 subscribers | Redis | 2.57 | 1.00 | 3.57 of 18 |

## Observations

- Largest throughput gap: **SET (raw, pipeline 16)** at 0.36x Redis.
- Closest to (or ahead of) Redis: **PUBLISH, no subscribers (raw, pipeline 16)** at 1.91x.
- my-mini-redis uses on average **8.8x** the CPU time per operation that Redis does across the summary scenarios; that is the efficiency gap to close independently of core count.
- Each key with a 16 B value costs 213 B in my-mini-redis and 95 B in Redis (2.2x).

## Profiling next steps

- CPU profile the server under one scenario: `cargo build --release`, start `target/release/my-mini-redis-server`, run `scripts/bench.sh --address 127.0.0.1:6379 --no-redis --quick --only set --duration 30`, and attach a sampling profiler to the server (`samply record -p <pid>` on macOS, `perf record -g -p <pid>` on Linux).
- Compare allocation counts with a heap profiler (`heaptrack`, or Instruments' Allocations on macOS) on the same run; a high *CPU per op* with low *cores* points at per-request allocation or syscalls rather than contention.
- `JSON` with every number is next to this report for tracking changes over time.

