# Load benchmark: my-redis vs Redis

| | |
|---|---|
| Date (Unix time) | 1794957512 |
| Commit | 3d692e0 (uncommitted changes) |
| Machine | linux aarch64, 18 CPUs |
| my-redis | fresh process per run |
| Redis | 8.0.2, fresh process per run |
| Load | 50 connections, pipeline 16, 64 B payload, 10000 keys, 5s measured after 1s warmup (sweeps: 2s after 0.5s) |

**Reading the numbers.** Both servers and the load generator share this machine. Redis runs commands on one thread; my-redis uses every core, so compare *CPU per op* for efficiency and *ops/s* for capacity. *Cores* is server CPU time divided by wall time. If the load generator's cores approach the machine's CPU count, it is the bottleneck and ops/s understates both servers. Latency for raw scenarios is per pipelined batch.

## Summary

| Scenario | my-redis ops/s | Redis ops/s | my-redis / Redis | p50 my-redis / Redis | p99 my-redis / Redis | CPU per op my-redis / Redis | cores my-redis / Redis | peak RSS my-redis / Redis |
|---|---:|---:|---:|---:|---:|---:|---:|---:|
| PING (raw, pipeline 16) | 9,226,954 | 4,889,098 | 1.89x | 82.9us / 165.9us | 174.1us / 225.3us | 0.37us / 0.20us | 3.45 / 1.00 | 3.0 MiB / 18.5 MiB |
| SET (raw, pipeline 16) | 1,070,198 | 3,084,893 | 0.35x | 589.8us / 274.4us | 2.75ms / 340.0us | 9.36us / 0.32us | 10.02 / 1.00 | 4.8 MiB / 19.2 MiB |
| GET (raw, pipeline 16) | 1,810,157 | 2,971,386 | 0.61x | 319.5us / 254.0us | 1.62ms / 663.6us | 5.66us / 0.34us | 10.25 / 1.00 | 6.1 MiB / 19.8 MiB |
| PUBLISH, no subscribers (raw, pipeline 16) | 6,667,869 | 3,437,126 | 1.94x | 106.5us / 239.6us | 348.2us / 307.2us | 0.89us / 0.29us | 5.92 / 1.00 | 3.0 MiB / 18.5 MiB |
| SET via Client (1 in flight) | 585,234 | 474,900 | 1.23x | 81.9us / 106.5us | 174.1us / 172.0us | 4.68us / 2.11us | 2.74 / 1.00 | 5.7 MiB / 19.6 MiB |
| GET via Client (1 in flight) | 611,092 | 480,485 | 1.27x | 78.8us / 105.5us | 163.8us / 170.0us | 4.35us / 2.08us | 2.66 / 1.00 | 5.8 MiB / 19.8 MiB |
| PUBLISH to 50 subscribers (4 publishers, pipeline 16) | 253,110 | 281,654 | 0.90x | 239.6us / 227.3us | 573.4us / 311.3us | 10.65us / 3.55us | 2.70 / 1.00 | 8.3 MiB / 19.1 MiB |
| SET (raw, pipeline 16) during PUBLISH to 50 subscribers | 1,088,701 | 1,630,106 | 0.67x | 622.6us / 487.4us | 2.42ms / 737.3us | 7.92us / 0.61us | 8.63 / 1.00 | 12.5 MiB / 20.0 MiB |

### Latency detail

| Scenario | Server | p50 | p90 | p99 | p99.9 | max |
|---|---|---:|---:|---:|---:|---:|
| PING (raw, pipeline 16) | my-redis | 82.9us | 130.0us | 174.1us | 208.9us | 824.0us |
| PING (raw, pipeline 16) | Redis | 165.9us | 202.8us | 225.3us | 274.4us | 2.20ms |
| SET (raw, pipeline 16) | my-redis | 589.8us | 1.46ms | 2.75ms | 10.22ms | 17.70ms |
| SET (raw, pipeline 16) | Redis | 274.4us | 311.3us | 340.0us | 397.3us | 662.1us |
| GET (raw, pipeline 16) | my-redis | 319.5us | 778.2us | 1.62ms | 10.35ms | 243.24ms |
| GET (raw, pipeline 16) | Redis | 254.0us | 311.3us | 663.6us | 1.25ms | 210.11ms |
| PUBLISH, no subscribers (raw, pipeline 16) | my-redis | 106.5us | 196.6us | 348.2us | 499.7us | 959.3us |
| PUBLISH, no subscribers (raw, pipeline 16) | Redis | 239.6us | 278.5us | 307.2us | 348.2us | 553.3us |
| SET via Client (1 in flight) | my-redis | 81.9us | 127.0us | 174.1us | 219.1us | 486.2us |
| SET via Client (1 in flight) | Redis | 106.5us | 141.3us | 172.0us | 210.9us | 385.4us |
| GET via Client (1 in flight) | my-redis | 78.8us | 120.8us | 163.8us | 200.7us | 454.8us |
| GET via Client (1 in flight) | Redis | 105.5us | 139.3us | 170.0us | 213.0us | 458.0us |
| PUBLISH to 50 subscribers (4 publishers, pipeline 16) | my-redis | 239.6us | 401.4us | 573.4us | 720.9us | 1.08ms |
| PUBLISH to 50 subscribers (4 publishers, pipeline 16) | Redis | 227.3us | 262.1us | 311.3us | 405.5us | 570.8us |
| SET (raw, pipeline 16) during PUBLISH to 50 subscribers | my-redis | 622.6us | 1.38ms | 2.42ms | 3.44ms | 7.92ms |
| SET (raw, pipeline 16) during PUBLISH to 50 subscribers | Redis | 487.4us | 532.5us | 737.3us | 909.3us | 1.41ms |

### Pub/sub delivery

Publishes are counted at the publishers; deliveries at the subscribers (publishes x subscribers when nothing is lost). *RSS growth* is how much the server's memory grew during the run, mostly queued messages.

| Run | Server | publishes/s | deliveries/s | delivery p50 | p99 | p99.9 | max | disconnected | peak RSS | RSS growth |
|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| PUBLISH to 50 subscribers (4 publishers, pipeline 16) | my-redis | 253,110 | 12,655,670 | 290.8us | 704.5us | 901.1us | 1.58ms | 0 of 50 | 8.3 MiB | +5.8 MiB |
| PUBLISH to 50 subscribers (4 publishers, pipeline 16) | Redis | 281,654 | 14,082,720 | 327.7us | 458.8us | 581.6us | 852.2us | 0 of 50 | 19.1 MiB | +988.0 KiB |
| SET (raw, pipeline 16) during PUBLISH to 50 subscribers | my-redis | 100,253 | 5,012,640 | 557.1us | 2.56ms | 3.60ms | 8.35ms | 0 of 50 | 12.5 MiB | +10.0 MiB |
| SET (raw, pipeline 16) during PUBLISH to 50 subscribers | Redis | 130,461 | 6,523,040 | 598.0us | 917.5us | 1.08ms | 1.68ms | 0 of 50 | 20.0 MiB | +1.8 MiB |

## Memory

Resident memory (RSS) of each server process, read from the OS after letting allocations settle. Per-connection and per-key numbers are the RSS increase divided by the count, so they include allocator overhead. Freed memory is often kept by the allocator, so RSS rarely shrinks.

| Measure | my-redis | Redis | my-redis / Redis |
|---|---:|---:|---:|
| Idle RSS | 2.7 MiB | 18.1 MiB | 0.15x |
| Per idle connection (1000 open) | 5.6 KiB | 5.4 KiB | 1.02x |
| Per key, 16 B value (100000 keys) | 203 B | 95 B | 2.14x |
|   overhead per key beyond 31 B of key + value | 172 B | 64 B | 2.69x |
| Per key, 1.0 KiB value (20000 keys) | 1.5 KiB | 1.3 KiB | 1.18x |
|   overhead per key beyond 1.0 KiB of key + value | 518 B | 286 B | 1.81x |

## Load generator

CPU used by the benchmark process itself during each summary run. Near 18 cores, the generator is saturated and the server isn't the limit.

| Scenario | Server | load generator cores | server cores | total |
|---|---|---:|---:|---:|
| PING (raw, pipeline 16) | my-redis | 2.56 | 3.45 | 6.01 of 18 |
| PING (raw, pipeline 16) | Redis | 1.29 | 1.00 | 2.29 of 18 |
| SET (raw, pipeline 16) | my-redis | 0.75 | 10.02 | 10.77 of 18 |
| SET (raw, pipeline 16) | Redis | 0.81 | 1.00 | 1.81 of 18 |
| GET (raw, pipeline 16) | my-redis | 1.05 | 10.25 | 11.30 of 18 |
| GET (raw, pipeline 16) | Redis | 0.87 | 1.00 | 1.87 of 18 |
| PUBLISH, no subscribers (raw, pipeline 16) | my-redis | 2.32 | 5.92 | 8.24 of 18 |
| PUBLISH, no subscribers (raw, pipeline 16) | Redis | 0.97 | 1.00 | 1.97 of 18 |
| SET via Client (1 in flight) | my-redis | 2.67 | 2.74 | 5.41 of 18 |
| SET via Client (1 in flight) | Redis | 2.11 | 1.00 | 3.11 of 18 |
| GET via Client (1 in flight) | my-redis | 2.61 | 2.66 | 5.27 of 18 |
| GET via Client (1 in flight) | Redis | 2.07 | 1.00 | 3.07 of 18 |
| PUBLISH to 50 subscribers (4 publishers, pipeline 16) | my-redis | 5.26 | 2.70 | 7.96 of 18 |
| PUBLISH to 50 subscribers (4 publishers, pipeline 16) | Redis | 4.82 | 1.00 | 5.82 of 18 |
| SET (raw, pipeline 16) during PUBLISH to 50 subscribers | my-redis | 2.01 | 8.63 | 10.63 of 18 |
| SET (raw, pipeline 16) during PUBLISH to 50 subscribers | Redis | 2.52 | 1.00 | 3.52 of 18 |

## Observations

- Largest throughput gap: **SET (raw, pipeline 16)** at 0.35x Redis.
- Closest to (or ahead of) Redis: **PUBLISH, no subscribers (raw, pipeline 16)** at 1.94x.
- my-redis uses on average **8.8x** the CPU time per operation that Redis does across the summary scenarios; that is the efficiency gap to close independently of core count.
- Each key with a 16 B value costs 203 B in my-redis and 95 B in Redis (2.1x).

## Profiling next steps

- CPU profile the server under one scenario: `cargo build --release`, start `target/release/my-redis-server`, run `scripts/bench.sh --address 127.0.0.1:6379 --no-redis --quick --only set --duration 30`, and attach a sampling profiler to the server (`samply record -p <pid>` on macOS, `perf record -g -p <pid>` on Linux).
- Compare allocation counts with a heap profiler (`heaptrack`, or Instruments' Allocations on macOS) on the same run; a high *CPU per op* with low *cores* points at per-request allocation or syscalls rather than contention.
- `JSON` with every number is next to this report for tracking changes over time.

