# Load benchmark: my-mini-redis vs Redis

| | |
|---|---|
| Date (Unix time) | 1795020550 |
| Commit | 09ce574 |
| Machine | linux aarch64, 18 CPUs |
| my-mini-redis | fresh process per run |
| Redis | 8.0.2, fresh process per run |
| Load | 50 connections, pipeline 16, 64 B payload, 10000 keys, 5s measured after 1s warmup (sweeps: 2s after 0.5s) |

**Reading the numbers.** Both servers and the load generator share this machine. Redis runs commands on one thread; my-mini-redis uses every core, so compare *CPU per op* for efficiency and *ops/s* for capacity. *Cores* is server CPU time divided by wall time. If the load generator's cores approach the machine's CPU count, it is the bottleneck and ops/s understates both servers. Latency for raw scenarios is per pipelined batch.

## Summary

| Scenario | my-mini-redis ops/s | Redis ops/s | mini / Redis | p50 mini / Redis | p99 mini / Redis | CPU per op mini / Redis | cores mini / Redis | peak RSS mini / Redis |
|---|---:|---:|---:|---:|---:|---:|---:|---:|
| PING (raw, pipeline 16) | 9,159,037 | 4,839,242 | 1.89x | 84.0us / 165.9us | 178.2us / 227.3us | 0.38us / 0.21us | 3.44 / 1.00 | 3.0 MiB / 18.5 MiB |
| SET (raw, pipeline 16) | 6,958,582 | 3,002,550 | 2.32x | 107.5us / 278.5us | 256.0us / 348.2us | 0.81us / 0.33us | 5.61 / 1.00 | 5.2 MiB / 19.2 MiB |
| GET (raw, pipeline 16) | 7,710,314 | 3,436,608 | 2.24x | 98.3us / 241.7us | 219.1us / 311.3us | 0.60us / 0.29us | 4.65 / 1.00 | 6.1 MiB / 19.7 MiB |
| PUBLISH, no subscribers (raw, pipeline 16) | 7,000,246 | 3,411,773 | 2.05x | 103.4us / 243.7us | 307.2us / 307.2us | 0.79us / 0.29us | 5.55 / 1.00 | 3.0 MiB / 18.5 MiB |
| SET via Client (1 in flight) | 597,697 | 477,935 | 1.25x | 80.9us / 105.5us | 167.9us / 167.9us | 4.51us / 2.09us | 2.69 / 1.00 | 5.8 MiB / 19.5 MiB |
| GET via Client (1 in flight) | 608,390 | 489,907 | 1.24x | 78.8us / 102.4us | 165.9us / 167.9us | 4.41us / 2.04us | 2.68 / 1.00 | 5.8 MiB / 19.7 MiB |
| PUBLISH to 50 subscribers (4 publishers, pipeline 16) | 252,000 | 264,883 | 0.95x | 241.7us / 233.5us | 573.4us / 434.2us | 10.69us / 3.78us | 2.69 / 1.00 | 8.5 MiB / 19.1 MiB |
| SET (raw, pipeline 16) during PUBLISH to 50 subscribers | 3,252,234 | 1,630,480 | 1.99x | 225.3us / 487.4us | 639.0us / 712.7us | 1.56us / 0.61us | 5.06 / 1.00 | 11.5 MiB / 19.9 MiB |

### Latency detail

| Scenario | Server | p50 | p90 | p99 | p99.9 | max |
|---|---|---:|---:|---:|---:|---:|
| PING (raw, pipeline 16) | my-mini-redis | 84.0us | 131.1us | 178.2us | 215.0us | 405.6us |
| PING (raw, pipeline 16) | Redis | 165.9us | 204.8us | 227.3us | 274.4us | 488.6us |
| SET (raw, pipeline 16) | my-mini-redis | 107.5us | 178.2us | 256.0us | 335.9us | 887.2us |
| SET (raw, pipeline 16) | Redis | 278.5us | 319.5us | 348.2us | 401.4us | 721.8us |
| GET (raw, pipeline 16) | my-mini-redis | 98.3us | 159.7us | 219.1us | 274.4us | 507.4us |
| GET (raw, pipeline 16) | Redis | 241.7us | 282.6us | 311.3us | 360.4us | 642.8us |
| PUBLISH, no subscribers (raw, pipeline 16) | my-mini-redis | 103.4us | 182.3us | 307.2us | 442.4us | 797.9us |
| PUBLISH, no subscribers (raw, pipeline 16) | Redis | 243.7us | 282.6us | 307.2us | 364.5us | 566.1us |
| SET via Client (1 in flight) | my-mini-redis | 80.9us | 123.9us | 167.9us | 206.8us | 546.5us |
| SET via Client (1 in flight) | Redis | 105.5us | 139.3us | 167.9us | 194.6us | 328.0us |
| GET via Client (1 in flight) | my-mini-redis | 78.8us | 121.9us | 165.9us | 225.3us | 527.0us |
| GET via Client (1 in flight) | Redis | 102.4us | 137.2us | 167.9us | 225.3us | 613.0us |
| PUBLISH to 50 subscribers (4 publishers, pipeline 16) | my-mini-redis | 241.7us | 401.4us | 573.4us | 720.9us | 939.9us |
| PUBLISH to 50 subscribers (4 publishers, pipeline 16) | Redis | 233.5us | 290.8us | 434.2us | 589.8us | 1.63ms |
| SET (raw, pipeline 16) during PUBLISH to 50 subscribers | my-mini-redis | 225.3us | 405.5us | 639.0us | 892.9us | 1.67ms |
| SET (raw, pipeline 16) during PUBLISH to 50 subscribers | Redis | 487.4us | 524.3us | 712.7us | 852.0us | 1.13ms |

### Pub/sub delivery

Publishes are counted at the publishers; deliveries at the subscribers (publishes x subscribers when nothing is lost). *RSS growth* is how much the server's memory grew during the run, mostly queued messages.

| Run | Server | publishes/s | deliveries/s | delivery p50 | p99 | p99.9 | max | disconnected | peak RSS | RSS growth |
|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| PUBLISH to 50 subscribers (4 publishers, pipeline 16) | my-mini-redis | 252,000 | 12,600,000 | 290.8us | 720.9us | 917.5us | 1.47ms | 0 of 50 | 8.5 MiB | +5.9 MiB |
| PUBLISH to 50 subscribers (4 publishers, pipeline 16) | Redis | 264,883 | 13,244,160 | 340.0us | 614.4us | 794.6us | 2.29ms | 0 of 50 | 19.1 MiB | +984.0 KiB |
| SET (raw, pipeline 16) during PUBLISH to 50 subscribers | my-mini-redis | 197,942 | 9,897,120 | 352.3us | 892.9us | 1.20ms | 1.92ms | 0 of 50 | 11.5 MiB | +8.9 MiB |
| SET (raw, pipeline 16) during PUBLISH to 50 subscribers | Redis | 130,499 | 6,524,960 | 606.2us | 868.4us | 1.03ms | 1.39ms | 0 of 50 | 19.9 MiB | +1.8 MiB |
| 1 subscriber | my-mini-redis | 1,349,232 | 1,349,232 | 46.6us | 84.0us | 112.6us | 292.4us | 0 of 1 | 2.8 MiB | +208.0 KiB |
| 1 subscriber | Redis | 997,976 | 997,976 | 54.3us | 76.8us | 137.2us | 298.6us | 0 of 1 | 18.3 MiB | +204.0 KiB |
| 10 subscribers | my-mini-redis | 577,064 | 5,770,640 | 117.8us | 331.8us | 958.5us | 3.38ms | 0 of 10 | 3.9 MiB | +1.3 MiB |
| 10 subscribers | Redis | 718,688 | 7,186,880 | 102.4us | 184.3us | 270.3us | 520.2us | 0 of 10 | 18.4 MiB | +344.0 KiB |
| 100 subscribers | my-mini-redis | 143,184 | 14,318,400 | 516.1us | 1.36ms | 1.85ms | 3.54ms | 0 of 100 | 13.8 MiB | +11.2 MiB |
| 100 subscribers | Redis | 143,072 | 14,307,200 | 639.0us | 843.8us | 909.3us | 1.10ms | 0 of 100 | 19.8 MiB | +1.7 MiB |
| 1 channel | my-mini-redis | 141,400 | 14,140,000 | 520.2us | 1.31ms | 1.69ms | 2.80ms | 0 of 100 | 12.8 MiB | +10.2 MiB |
| 1 channel | Redis | 140,672 | 14,067,200 | 647.2us | 884.7us | 1.03ms | 1.28ms | 0 of 100 | 19.8 MiB | +1.7 MiB |
| 10 channels | my-mini-redis | 255,032 | 2,550,320 | 290.8us | 688.1us | 950.3us | 2.23ms | 0 of 100 | 4.3 MiB | +1.7 MiB |
| 10 channels | Redis | 283,680 | 2,836,800 | 270.3us | 368.6us | 401.4us | 500.2us | 0 of 100 | 18.8 MiB | +688.0 KiB |
| 100 channels | my-mini-redis | 792,152 | 792,152 | 93.2us | 184.3us | 243.7us | 478.8us | 0 of 100 | 3.5 MiB | +960.0 KiB |
| 100 channels | Redis | 623,392 | 623,392 | 97.3us | 153.6us | 180.2us | 242.5us | 0 of 100 | 18.8 MiB | +684.0 KiB |

## Memory

Resident memory (RSS) of each server process, read from the OS after letting allocations settle. Per-connection and per-key numbers are the RSS increase divided by the count, so they include allocator overhead. Freed memory is often kept by the allocator, so RSS rarely shrinks.

| Measure | my-mini-redis | Redis | mini / Redis |
|---|---:|---:|---:|
| Idle RSS | 2.7 MiB | 18.1 MiB | 0.15x |
| Per idle connection (1000 open) | 5.7 KiB | 5.4 KiB | 1.06x |
| Per key, 16 B value (100000 keys) | 215 B | 93 B | 2.31x |
|   overhead per key beyond 31 B of key + value | 184 B | 62 B | 2.96x |
| Per key, 1.0 KiB value (20000 keys) | 1.5 KiB | 1.3 KiB | 1.17x |
|   overhead per key beyond 1.0 KiB of key + value | 508 B | 285 B | 1.78x |

## Pipeline depth: SET, 50 connections

| pipeline | my-mini-redis ops/s | Redis ops/s | mini / Redis | p99 mini / Redis | CPU per op mini / Redis | cores mini / Redis | peak RSS mini / Redis |
|---|---:|---:|---:|---:|---:|---:|---:|
| 1 | 621,123 | 489,537 | 1.27x | 159.7us / 163.8us | 4.39us / 2.03us | 2.72 / 0.99 | 3.2 MiB / 18.5 MiB |
| 4 | 2,298,768 | 1,525,020 | 1.51x | 178.2us / 194.6us | 1.55us / 0.66us | 3.55 / 1.00 | 3.8 MiB / 18.6 MiB |
| 16 | 6,878,296 | 3,017,456 | 2.28x | 258.0us / 352.3us | 0.82us / 0.33us | 5.61 / 1.00 | 5.2 MiB / 19.2 MiB |
| 64 | 14,803,904 | 4,026,304 | 3.68x | 540.7us / 991.2us | 0.76us / 0.25us | 11.19 / 1.00 | 6.4 MiB / 19.5 MiB |

## Concurrent connections: SET, pipeline 16

| connections | my-mini-redis ops/s | Redis ops/s | mini / Redis | p99 mini / Redis | CPU per op mini / Redis | cores mini / Redis | peak RSS mini / Redis |
|---|---:|---:|---:|---:|---:|---:|---:|
| 1 | 481,104 | 502,592 | 0.96x | 46.1us / 42.0us | 1.26us / 0.74us | 0.60 / 0.37 | 2.8 MiB / 18.2 MiB |
| 10 | 3,957,160 | 2,718,328 | 1.46x | 85.0us / 85.0us | 0.67us / 0.37us | 2.64 / 1.00 | 3.4 MiB / 18.4 MiB |
| 50 | 6,914,928 | 2,445,928 | 2.83x | 258.0us / 884.7us | 0.81us / 0.40us | 5.60 / 0.97 | 5.3 MiB / 19.2 MiB |
| 200 | 8,695,320 | 2,898,048 | 3.00x | 884.7us / 2.03ms | 0.97us / 0.34us | 8.41 / 1.00 | 6.7 MiB / 20.2 MiB |

## Payload size: SET, 50 connections, pipeline 16

| payload | my-mini-redis ops/s | Redis ops/s | mini / Redis | p99 mini / Redis | CPU per op mini / Redis | cores mini / Redis | peak RSS mini / Redis |
|---|---:|---:|---:|---:|---:|---:|---:|
| 16 B | 6,985,232 | 3,179,792 | 2.20x | 254.0us / 340.0us | 0.79us / 0.31us | 5.53 / 1.00 | 4.9 MiB / 19.0 MiB |
| 1.0 KiB | 5,372,384 | 2,134,544 | 2.52x | 352.3us / 491.5us | 1.18us / 0.47us | 6.33 / 1.00 | 13.2 MiB / 27.1 MiB |
| 16.0 KiB | 1,335,552 | 324,296 | 4.12x | 1.47ms / 2.79ms | 5.93us / 3.07us | 7.92 / 0.99 | 202.7 MiB / 162.1 MiB |
| 64.0 KiB | 498,904 | 96,536 | 5.17x | 4.39ms / 10.49ms | 16.80us / 10.41us | 8.38 / 1.00 | 427.0 MiB / 328.7 MiB |

## Pub/sub fan-out: 4 publishers, pipeline 16

| fan-out | my-mini-redis ops/s | Redis ops/s | mini / Redis | p99 mini / Redis | CPU per op mini / Redis | cores mini / Redis | peak RSS mini / Redis |
|---|---:|---:|---:|---:|---:|---:|---:|
| 1 subscriber | 1,349,232 | 997,976 | 1.35x | 82.9us / 89.1us | 1.16us / 0.55us | 1.57 / 0.55 | 2.8 MiB / 18.3 MiB |
| 10 subscribers | 577,064 | 718,688 | 0.80x | 294.9us / 157.7us | 3.46us / 1.30us | 1.99 / 0.93 | 3.9 MiB / 18.4 MiB |
| 100 subscribers | 143,184 | 143,072 | 1.00x | 1.06ms / 540.7us | 19.51us / 6.99us | 2.79 / 1.00 | 13.8 MiB / 19.8 MiB |

## Pub/sub channels: 100 subscribers, 4 publishers, pipeline 16

| channels | my-mini-redis ops/s | Redis ops/s | mini / Redis | p99 mini / Redis | CPU per op mini / Redis | cores mini / Redis | peak RSS mini / Redis |
|---|---:|---:|---:|---:|---:|---:|---:|
| 1 channel | 141,400 | 140,672 | 1.01x | 1.03ms / 614.4us | 20.15us / 7.11us | 2.85 / 1.00 | 12.8 MiB / 19.8 MiB |
| 10 channels | 255,032 | 283,680 | 0.90x | 573.4us / 340.0us | 12.10us / 3.52us | 3.08 / 1.00 | 4.3 MiB / 18.8 MiB |
| 100 channels | 792,152 | 623,392 | 1.27x | 159.7us / 153.6us | 2.90us / 1.60us | 2.29 / 1.00 | 3.5 MiB / 18.8 MiB |

## Single-threaded: my-mini-redis with 1 worker thread vs Redis

my-mini-redis started with `TOKIO_WORKER_THREADS=1`, so both servers run commands on one thread. Redis numbers are from the summary runs.

| Scenario | mini (1 thread) ops/s | Redis ops/s | mini / Redis | p99 mini / Redis | CPU per op mini / Redis | cores mini / Redis |
|---|---:|---:|---:|---:|---:|---:|
| PING (raw, pipeline 16) | 5,248,224 | 4,839,242 | 1.08x | 290.8us / 227.3us | 0.19us / 0.21us | 1.00 / 1.00 |
| SET (raw, pipeline 16) | 3,016,480 | 3,002,550 | 1.00x | 507.9us / 348.2us | 0.33us / 0.33us | 1.00 / 1.00 |
| GET (raw, pipeline 16) | 3,591,709 | 3,436,608 | 1.05x | 421.9us / 311.3us | 0.28us / 0.29us | 1.00 / 1.00 |

## Load generator

CPU used by the benchmark process itself during each summary run. Near 18 cores, the generator is saturated and the server isn't the limit.

| Scenario | Server | load generator cores | server cores | total |
|---|---|---:|---:|---:|
| PING (raw, pipeline 16) | my-mini-redis | 2.53 | 3.44 | 5.97 of 18 |
| PING (raw, pipeline 16) | Redis | 1.33 | 1.00 | 2.33 of 18 |
| SET (raw, pipeline 16) | my-mini-redis | 2.35 | 5.61 | 7.95 of 18 |
| SET (raw, pipeline 16) | Redis | 0.83 | 1.00 | 1.83 of 18 |
| GET (raw, pipeline 16) | my-mini-redis | 2.43 | 4.65 | 7.08 of 18 |
| GET (raw, pipeline 16) | Redis | 0.95 | 1.00 | 1.95 of 18 |
| PUBLISH, no subscribers (raw, pipeline 16) | my-mini-redis | 2.40 | 5.55 | 7.95 of 18 |
| PUBLISH, no subscribers (raw, pipeline 16) | Redis | 0.94 | 1.00 | 1.94 of 18 |
| SET via Client (1 in flight) | my-mini-redis | 2.63 | 2.69 | 5.33 of 18 |
| SET via Client (1 in flight) | Redis | 2.05 | 1.00 | 3.04 of 18 |
| GET via Client (1 in flight) | my-mini-redis | 2.57 | 2.68 | 5.25 of 18 |
| GET via Client (1 in flight) | Redis | 2.05 | 1.00 | 3.05 of 18 |
| PUBLISH to 50 subscribers (4 publishers, pipeline 16) | my-mini-redis | 5.27 | 2.69 | 7.97 of 18 |
| PUBLISH to 50 subscribers (4 publishers, pipeline 16) | Redis | 4.55 | 1.00 | 5.55 of 18 |
| SET (raw, pipeline 16) during PUBLISH to 50 subscribers | my-mini-redis | 4.92 | 5.06 | 9.98 of 18 |
| SET (raw, pipeline 16) during PUBLISH to 50 subscribers | Redis | 2.50 | 1.00 | 3.50 of 18 |

## Observations

- Largest throughput gap: **PUBLISH to 50 subscribers (4 publishers, pipeline 16)** at 0.95x Redis.
- Closest to (or ahead of) Redis: **SET (raw, pipeline 16)** at 2.32x.
- my-mini-redis uses on average **2.3x** the CPU time per operation that Redis does across the summary scenarios; that is the efficiency gap to close independently of core count.
- **PING (raw, pipeline 16)**: my-mini-redis on 1 thread does 5,248,224 ops/s at 0.19us CPU/op; on all threads 9,159,037 ops/s at 0.38us CPU/op using 3.4 cores. 2.0x the CPU per op for 1.75x the throughput points at contention between threads (the shared db lock, the scheduler) rather than command cost.
- **SET (raw, pipeline 16)**: my-mini-redis on 1 thread does 3,016,480 ops/s at 0.33us CPU/op; on all threads 6,958,582 ops/s at 0.81us CPU/op using 5.6 cores. 2.4x the CPU per op for 2.31x the throughput points at contention between threads (the shared db lock, the scheduler) rather than command cost.
- **GET (raw, pipeline 16)**: my-mini-redis on 1 thread does 3,591,709 ops/s at 0.28us CPU/op; on all threads 7,710,314 ops/s at 0.60us CPU/op using 4.7 cores. 2.2x the CPU per op for 2.15x the throughput points at contention between threads (the shared db lock, the scheduler) rather than command cost.
- With a single connection (no contention), my-mini-redis needs 1.26us CPU per SET against Redis's 0.74us (1.7x): the per-request cost of parsing, allocating and writing replies.
- Each key with a 16 B value costs 215 B in my-mini-redis and 93 B in Redis (2.3x).
- Pipelining from 1 to 64 multiplies my-mini-redis throughput by 23.8x: the larger this is, the more time goes to per-request round trips and syscalls rather than command execution.

## Profiling next steps

- CPU profile the server under one scenario: `cargo build --release`, start `target/release/my-mini-redis-server`, run `scripts/bench.sh --address 127.0.0.1:6379 --no-redis --quick --only set --duration 30`, and attach a sampling profiler to the server (`samply record -p <pid>` on macOS, `perf record -g -p <pid>` on Linux).
- Compare allocation counts with a heap profiler (`heaptrack`, or Instruments' Allocations on macOS) on the same run; a high *CPU per op* with low *cores* points at per-request allocation or syscalls rather than contention.
- `JSON` with every number is next to this report for tracking changes over time.

