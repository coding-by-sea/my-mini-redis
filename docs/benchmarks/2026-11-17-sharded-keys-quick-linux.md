# Load benchmark: my-redis vs Redis

| | |
|---|---|
| Date (Unix time) | 1794966594 |
| Commit | 0bcf088 (uncommitted changes) |
| Machine | linux aarch64, 18 CPUs |
| my-redis | fresh process per run |
| Redis | 8.0.2, fresh process per run |
| Load | 50 connections, pipeline 16, 64 B payload, 10000 keys, 5s measured after 1s warmup (sweeps: 2s after 0.5s) |

**Reading the numbers.** Both servers and the load generator share this machine. Redis runs commands on one thread; my-redis uses every core, so compare *CPU per op* for efficiency and *ops/s* for capacity. *Cores* is server CPU time divided by wall time. If the load generator's cores approach the machine's CPU count, it is the bottleneck and ops/s understates both servers. Latency for raw scenarios is per pipelined batch.

## Summary

| Scenario | my-redis ops/s | Redis ops/s | my-redis / Redis | p50 my-redis / Redis | p99 my-redis / Redis | CPU per op my-redis / Redis | cores my-redis / Redis | peak RSS my-redis / Redis |
|---|---:|---:|---:|---:|---:|---:|---:|---:|
| PING (raw, pipeline 16) | 9,201,283 | 4,851,450 | 1.90x | 82.9us / 165.9us | 176.1us / 225.3us | 0.38us / 0.21us | 3.45 / 1.00 | 3.0 MiB / 18.5 MiB |
| SET (raw, pipeline 16) | 7,004,442 | 3,012,202 | 2.33x | 106.5us / 278.5us | 251.9us / 348.2us | 0.79us / 0.33us | 5.55 / 1.00 | 5.2 MiB / 19.2 MiB |
| GET (raw, pipeline 16) | 7,733,811 | 3,449,610 | 2.24x | 98.3us / 241.7us | 219.1us / 307.2us | 0.60us / 0.29us | 4.64 / 1.00 | 6.1 MiB / 19.8 MiB |
| PUBLISH, no subscribers (raw, pipeline 16) | 6,967,850 | 3,436,006 | 2.03x | 103.4us / 241.7us | 311.3us / 303.1us | 0.80us / 0.29us | 5.54 / 1.00 | 3.0 MiB / 18.5 MiB |
| SET via Client (1 in flight) | 601,665 | 466,963 | 1.29x | 80.9us / 106.5us | 163.8us / 202.8us | 4.49us / 2.14us | 2.70 / 1.00 | 5.8 MiB / 19.6 MiB |
| GET via Client (1 in flight) | 565,812 | 486,412 | 1.16x | 79.9us / 103.4us | 190.5us / 165.9us | 4.73us / 2.06us | 2.68 / 1.00 | 5.8 MiB / 19.8 MiB |
| PUBLISH to 50 subscribers (4 publishers, pipeline 16) | 253,344 | 282,586 | 0.90x | 239.6us / 227.3us | 565.2us / 299.0us | 10.59us / 3.54us | 2.68 / 1.00 | 8.5 MiB / 19.1 MiB |
| SET (raw, pipeline 16) during PUBLISH to 50 subscribers | 3,246,589 | 1,671,498 | 1.94x | 227.3us / 475.1us | 630.8us / 696.3us | 1.56us / 0.60us | 5.06 / 1.00 | 10.5 MiB / 20.0 MiB |

### Latency detail

| Scenario | Server | p50 | p90 | p99 | p99.9 | max |
|---|---|---:|---:|---:|---:|---:|
| PING (raw, pipeline 16) | my-redis | 82.9us | 131.1us | 176.1us | 213.0us | 378.5us |
| PING (raw, pipeline 16) | Redis | 165.9us | 204.8us | 225.3us | 274.4us | 514.0us |
| SET (raw, pipeline 16) | my-redis | 106.5us | 176.1us | 251.9us | 327.7us | 595.7us |
| SET (raw, pipeline 16) | Redis | 278.5us | 315.4us | 348.2us | 401.4us | 876.7us |
| GET (raw, pipeline 16) | my-redis | 98.3us | 157.7us | 219.1us | 270.3us | 456.2us |
| GET (raw, pipeline 16) | Redis | 241.7us | 282.6us | 307.2us | 348.2us | 413.2us |
| PUBLISH, no subscribers (raw, pipeline 16) | my-redis | 103.4us | 184.3us | 311.3us | 450.6us | 1.20ms |
| PUBLISH, no subscribers (raw, pipeline 16) | Redis | 241.7us | 278.5us | 303.1us | 344.1us | 422.1us |
| SET via Client (1 in flight) | my-redis | 80.9us | 122.9us | 163.8us | 192.5us | 259.6us |
| SET via Client (1 in flight) | Redis | 106.5us | 143.4us | 202.8us | 434.2us | 1.94ms |
| GET via Client (1 in flight) | my-redis | 79.9us | 124.9us | 190.5us | 340.0us | 220.25ms |
| GET via Client (1 in flight) | Redis | 103.4us | 137.2us | 165.9us | 194.6us | 406.7us |
| PUBLISH to 50 subscribers (4 publishers, pipeline 16) | my-redis | 239.6us | 401.4us | 565.2us | 712.7us | 874.9us |
| PUBLISH to 50 subscribers (4 publishers, pipeline 16) | Redis | 227.3us | 260.1us | 299.0us | 380.9us | 522.3us |
| SET (raw, pipeline 16) during PUBLISH to 50 subscribers | my-redis | 227.3us | 405.5us | 630.8us | 876.5us | 1.73ms |
| SET (raw, pipeline 16) during PUBLISH to 50 subscribers | Redis | 475.1us | 512.0us | 696.3us | 827.4us | 1.17ms |

### Pub/sub delivery

Publishes are counted at the publishers; deliveries at the subscribers (publishes x subscribers when nothing is lost). *RSS growth* is how much the server's memory grew during the run, mostly queued messages.

| Run | Server | publishes/s | deliveries/s | delivery p50 | p99 | p99.9 | max | disconnected | peak RSS | RSS growth |
|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| PUBLISH to 50 subscribers (4 publishers, pipeline 16) | my-redis | 253,344 | 12,667,200 | 290.8us | 712.7us | 901.1us | 1.46ms | 0 of 50 | 8.5 MiB | +5.9 MiB |
| PUBLISH to 50 subscribers (4 publishers, pipeline 16) | Redis | 282,586 | 14,129,280 | 327.7us | 454.7us | 532.5us | 788.8us | 0 of 50 | 19.1 MiB | +992.0 KiB |
| SET (raw, pipeline 16) during PUBLISH to 50 subscribers | my-redis | 197,379 | 9,868,960 | 352.3us | 884.7us | 1.15ms | 1.91ms | 0 of 50 | 10.5 MiB | +7.9 MiB |
| SET (raw, pipeline 16) during PUBLISH to 50 subscribers | Redis | 133,744 | 6,687,200 | 589.8us | 868.4us | 1.02ms | 1.13ms | 0 of 50 | 20.0 MiB | +1.8 MiB |

## Memory

Resident memory (RSS) of each server process, read from the OS after letting allocations settle. Per-connection and per-key numbers are the RSS increase divided by the count, so they include allocator overhead. Freed memory is often kept by the allocator, so RSS rarely shrinks.

| Measure | my-redis | Redis | my-redis / Redis |
|---|---:|---:|---:|
| Idle RSS | 2.7 MiB | 18.1 MiB | 0.15x |
| Per idle connection (1000 open) | 5.6 KiB | 5.4 KiB | 1.03x |
| Per key, 16 B value (100000 keys) | 212 B | 95 B | 2.23x |
|   overhead per key beyond 31 B of key + value | 181 B | 64 B | 2.82x |
| Per key, 1.0 KiB value (20000 keys) | 1.5 KiB | 1.3 KiB | 1.16x |
|   overhead per key beyond 1.0 KiB of key + value | 497 B | 286 B | 1.74x |

## Load generator

CPU used by the benchmark process itself during each summary run. Near 18 cores, the generator is saturated and the server isn't the limit.

| Scenario | Server | load generator cores | server cores | total |
|---|---|---:|---:|---:|
| PING (raw, pipeline 16) | my-redis | 2.53 | 3.45 | 5.98 of 18 |
| PING (raw, pipeline 16) | Redis | 1.33 | 1.00 | 2.33 of 18 |
| SET (raw, pipeline 16) | my-redis | 2.38 | 5.55 | 7.93 of 18 |
| SET (raw, pipeline 16) | Redis | 0.84 | 1.00 | 1.84 of 18 |
| GET (raw, pipeline 16) | my-redis | 2.44 | 4.64 | 7.08 of 18 |
| GET (raw, pipeline 16) | Redis | 0.95 | 1.00 | 1.95 of 18 |
| PUBLISH, no subscribers (raw, pipeline 16) | my-redis | 2.43 | 5.54 | 7.97 of 18 |
| PUBLISH, no subscribers (raw, pipeline 16) | Redis | 0.95 | 1.00 | 1.95 of 18 |
| SET via Client (1 in flight) | my-redis | 2.69 | 2.70 | 5.39 of 18 |
| SET via Client (1 in flight) | Redis | 2.10 | 1.00 | 3.10 of 18 |
| GET via Client (1 in flight) | my-redis | 2.63 | 2.68 | 5.31 of 18 |
| GET via Client (1 in flight) | Redis | 2.07 | 1.00 | 3.07 of 18 |
| PUBLISH to 50 subscribers (4 publishers, pipeline 16) | my-redis | 5.40 | 2.68 | 8.08 of 18 |
| PUBLISH to 50 subscribers (4 publishers, pipeline 16) | Redis | 4.99 | 1.00 | 5.99 of 18 |
| SET (raw, pipeline 16) during PUBLISH to 50 subscribers | my-redis | 4.97 | 5.06 | 10.04 of 18 |
| SET (raw, pipeline 16) during PUBLISH to 50 subscribers | Redis | 2.59 | 1.00 | 3.60 of 18 |

## Observations

- Largest throughput gap: **PUBLISH to 50 subscribers (4 publishers, pipeline 16)** at 0.90x Redis.
- Closest to (or ahead of) Redis: **SET (raw, pipeline 16)** at 2.33x.
- my-redis uses on average **2.4x** the CPU time per operation that Redis does across the summary scenarios; that is the efficiency gap to close independently of core count.
- Each key with a 16 B value costs 212 B in my-redis and 95 B in Redis (2.2x).

## Profiling next steps

- CPU profile the server under one scenario: `cargo build --release`, start `target/release/my-redis-server`, run `scripts/bench.sh --address 127.0.0.1:6379 --no-redis --quick --only set --duration 30`, and attach a sampling profiler to the server (`samply record -p <pid>` on macOS, `perf record -g -p <pid>` on Linux).
- Compare allocation counts with a heap profiler (`heaptrack`, or Instruments' Allocations on macOS) on the same run; a high *CPU per op* with low *cores* points at per-request allocation or syscalls rather than contention.
- `JSON` with every number is next to this report for tracking changes over time.

