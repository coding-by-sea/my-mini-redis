# Load benchmark: my-mini-redis vs Redis

| | |
|---|---|
| Date (Unix time) | 1794970202 |
| Commit | f6ecede |
| Machine | linux aarch64, 18 CPUs |
| my-mini-redis | fresh process per run |
| Redis | 8.0.2, fresh process per run |
| Load | 50 connections, pipeline 16, 64 B payload, 10000 keys, 5s measured after 1s warmup (sweeps: 2s after 0.5s) |

**Reading the numbers.** Both servers and the load generator share this machine. Redis runs commands on one thread; my-mini-redis uses every core, so compare *CPU per op* for efficiency and *ops/s* for capacity. *Cores* is server CPU time divided by wall time. If the load generator's cores approach the machine's CPU count, it is the bottleneck and ops/s understates both servers. Latency for raw scenarios is per pipelined batch.

## Summary

| Scenario | my-mini-redis ops/s | Redis ops/s | mini / Redis | p50 mini / Redis | p99 mini / Redis | CPU per op mini / Redis | cores mini / Redis | peak RSS mini / Redis |
|---|---:|---:|---:|---:|---:|---:|---:|---:|
| PING (raw, pipeline 16) | 8,892,400 | 4,852,813 | 1.83x | 85.0us / 165.9us | 190.5us / 229.4us | 0.39us / 0.21us | 3.43 / 1.00 | 3.0 MiB / 18.5 MiB |
| SET (raw, pipeline 16) | 7,004,378 | 3,044,400 | 2.30x | 106.5us / 274.4us | 254.0us / 348.2us | 0.80us / 0.33us | 5.60 / 1.00 | 5.2 MiB / 19.2 MiB |
| GET (raw, pipeline 16) | 7,654,752 | 3,446,880 | 2.22x | 99.3us / 239.6us | 221.2us / 331.8us | 0.61us / 0.29us | 4.65 / 1.00 | 6.1 MiB / 19.7 MiB |
| PUBLISH, no subscribers (raw, pipeline 16) | 6,886,838 | 3,449,037 | 2.00x | 104.4us / 239.6us | 315.4us / 315.4us | 0.81us / 0.29us | 5.60 / 1.00 | 3.0 MiB / 18.5 MiB |
| SET via Client (1 in flight) | 595,850 | 488,115 | 1.22x | 80.9us / 103.4us | 165.9us / 167.9us | 4.52us / 2.05us | 2.69 / 1.00 | 5.8 MiB / 19.5 MiB |
| GET via Client (1 in flight) | 605,033 | 497,133 | 1.22x | 79.9us / 101.4us | 167.9us / 163.8us | 4.44us / 2.02us | 2.68 / 1.00 | 5.8 MiB / 19.7 MiB |
| PUBLISH to 50 subscribers (4 publishers, pipeline 16) | 252,019 | 284,250 | 0.89x | 241.7us / 225.3us | 573.4us / 327.7us | 10.60us / 3.52us | 2.67 / 1.00 | 8.9 MiB / 19.1 MiB |
| SET (raw, pipeline 16) during PUBLISH to 50 subscribers | 3,246,387 | 1,600,205 | 2.03x | 225.3us / 491.5us | 639.0us / 794.6us | 1.57us / 0.62us | 5.11 / 1.00 | 10.7 MiB / 19.8 MiB |

### Latency detail

| Scenario | Server | p50 | p90 | p99 | p99.9 | max |
|---|---|---:|---:|---:|---:|---:|
| PING (raw, pipeline 16) | my-mini-redis | 85.0us | 137.2us | 190.5us | 247.8us | 777.8us |
| PING (raw, pipeline 16) | Redis | 165.9us | 204.8us | 229.4us | 327.7us | 1.79ms |
| SET (raw, pipeline 16) | my-mini-redis | 106.5us | 176.1us | 254.0us | 331.8us | 1.21ms |
| SET (raw, pipeline 16) | Redis | 274.4us | 315.4us | 348.2us | 401.4us | 499.5us |
| GET (raw, pipeline 16) | my-mini-redis | 99.3us | 159.7us | 221.2us | 282.6us | 843.7us |
| GET (raw, pipeline 16) | Redis | 239.6us | 278.5us | 331.8us | 540.7us | 3.47ms |
| PUBLISH, no subscribers (raw, pipeline 16) | my-mini-redis | 104.4us | 186.4us | 315.4us | 454.7us | 974.2us |
| PUBLISH, no subscribers (raw, pipeline 16) | Redis | 239.6us | 278.5us | 315.4us | 405.5us | 820.3us |
| SET via Client (1 in flight) | my-mini-redis | 80.9us | 124.9us | 165.9us | 198.7us | 338.2us |
| SET via Client (1 in flight) | Redis | 103.4us | 137.2us | 167.9us | 208.9us | 502.6us |
| GET via Client (1 in flight) | my-mini-redis | 79.9us | 122.9us | 167.9us | 215.0us | 766.0us |
| GET via Client (1 in flight) | Redis | 101.4us | 135.2us | 163.8us | 208.9us | 511.1us |
| PUBLISH to 50 subscribers (4 publishers, pipeline 16) | my-mini-redis | 241.7us | 401.4us | 573.4us | 712.7us | 1.50ms |
| PUBLISH to 50 subscribers (4 publishers, pipeline 16) | Redis | 225.3us | 258.0us | 327.7us | 471.0us | 876.5us |
| SET (raw, pipeline 16) during PUBLISH to 50 subscribers | my-mini-redis | 225.3us | 405.5us | 639.0us | 892.9us | 1.55ms |
| SET (raw, pipeline 16) during PUBLISH to 50 subscribers | Redis | 491.5us | 548.9us | 794.6us | 1.06ms | 2.19ms |

### Pub/sub delivery

Publishes are counted at the publishers; deliveries at the subscribers (publishes x subscribers when nothing is lost). *RSS growth* is how much the server's memory grew during the run, mostly queued messages.

| Run | Server | publishes/s | deliveries/s | delivery p50 | p99 | p99.9 | max | disconnected | peak RSS | RSS growth |
|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| PUBLISH to 50 subscribers (4 publishers, pipeline 16) | my-mini-redis | 252,019 | 12,600,960 | 290.8us | 712.7us | 909.3us | 1.81ms | 0 of 50 | 8.9 MiB | +6.3 MiB |
| PUBLISH to 50 subscribers (4 publishers, pipeline 16) | Redis | 284,250 | 14,212,480 | 323.6us | 466.9us | 639.0us | 1.15ms | 0 of 50 | 19.1 MiB | +984.0 KiB |
| SET (raw, pipeline 16) during PUBLISH to 50 subscribers | my-mini-redis | 196,474 | 9,823,680 | 356.4us | 884.7us | 1.16ms | 1.87ms | 0 of 50 | 10.7 MiB | +8.2 MiB |
| SET (raw, pipeline 16) during PUBLISH to 50 subscribers | Redis | 128,061 | 6,403,040 | 606.2us | 974.8us | 1.25ms | 2.01ms | 0 of 50 | 19.8 MiB | +1.8 MiB |
| 1 subscriber | my-mini-redis | 1,339,600 | 1,339,605 | 47.1us | 85.0us | 114.7us | 437.5us | 0 of 1 | 2.8 MiB | +236.0 KiB |
| 1 subscriber | Redis | 1,032,888 | 1,032,888 | 52.7us | 72.7us | 111.6us | 231.3us | 0 of 1 | 18.3 MiB | +204.0 KiB |
| 10 subscribers | my-mini-redis | 782,776 | 7,827,760 | 91.1us | 213.0us | 270.3us | 428.0us | 0 of 10 | 3.9 MiB | +1.3 MiB |
| 10 subscribers | Redis | 728,488 | 7,284,885 | 101.4us | 167.9us | 260.1us | 1.53ms | 0 of 10 | 18.4 MiB | +348.0 KiB |
| 100 subscribers | my-mini-redis | 144,096 | 14,409,600 | 512.0us | 1.26ms | 1.64ms | 2.40ms | 0 of 100 | 13.2 MiB | +10.6 MiB |
| 100 subscribers | Redis | 144,160 | 14,416,000 | 630.8us | 843.8us | 1.11ms | 1.38ms | 0 of 100 | 19.8 MiB | +1.8 MiB |
| 1 channel | my-mini-redis | 145,424 | 14,542,400 | 507.9us | 1.25ms | 1.62ms | 2.47ms | 0 of 100 | 13.6 MiB | +11.0 MiB |
| 1 channel | Redis | 145,664 | 14,566,400 | 630.8us | 835.6us | 917.5us | 1.09ms | 0 of 100 | 19.8 MiB | +1.8 MiB |
| 10 channels | my-mini-redis | 303,536 | 3,035,360 | 241.7us | 532.5us | 671.7us | 1.01ms | 0 of 100 | 4.3 MiB | +1.7 MiB |
| 10 channels | Redis | 284,000 | 2,840,000 | 266.2us | 372.7us | 446.5us | 702.6us | 0 of 100 | 18.8 MiB | +684.0 KiB |
| 100 channels | my-mini-redis | 801,032 | 801,032 | 92.2us | 178.2us | 223.2us | 515.6us | 0 of 100 | 3.5 MiB | +920.0 KiB |
| 100 channels | Redis | 604,712 | 604,712 | 99.3us | 182.3us | 294.9us | 593.2us | 0 of 100 | 18.7 MiB | +704.0 KiB |

## Memory

Resident memory (RSS) of each server process, read from the OS after letting allocations settle. Per-connection and per-key numbers are the RSS increase divided by the count, so they include allocator overhead. Freed memory is often kept by the allocator, so RSS rarely shrinks.

| Measure | my-mini-redis | Redis | mini / Redis |
|---|---:|---:|---:|
| Idle RSS | 2.7 MiB | 18.1 MiB | 0.15x |
| Per idle connection (1000 open) | 5.6 KiB | 5.4 KiB | 1.04x |
| Per key, 16 B value (100000 keys) | 212 B | 93 B | 2.28x |
|   overhead per key beyond 31 B of key + value | 181 B | 62 B | 2.92x |
| Per key, 1.0 KiB value (20000 keys) | 1.5 KiB | 1.3 KiB | 1.14x |
|   overhead per key beyond 1.0 KiB of key + value | 474 B | 285 B | 1.66x |

## Pipeline depth: SET, 50 connections

| pipeline | my-mini-redis ops/s | Redis ops/s | mini / Redis | p99 mini / Redis | CPU per op mini / Redis | cores mini / Redis | peak RSS mini / Redis |
|---|---:|---:|---:|---:|---:|---:|---:|
| 1 | 619,270 | 486,317 | 1.27x | 159.7us / 184.3us | 4.41us / 2.05us | 2.73 / 0.99 | 3.2 MiB / 18.5 MiB |
| 4 | 2,275,582 | 1,535,830 | 1.48x | 180.2us / 190.5us | 1.56us / 0.65us | 3.54 / 1.00 | 3.8 MiB / 18.6 MiB |
| 16 | 6,907,128 | 3,029,656 | 2.28x | 258.0us / 356.4us | 0.81us / 0.33us | 5.59 / 1.00 | 5.2 MiB / 19.2 MiB |
| 64 | 14,671,456 | 3,875,648 | 3.79x | 540.7us / 1.15ms | 0.76us / 0.26us | 11.09 / 1.00 | 6.4 MiB / 19.5 MiB |

## Concurrent connections: SET, pipeline 16

| connections | my-mini-redis ops/s | Redis ops/s | mini / Redis | p99 mini / Redis | CPU per op mini / Redis | cores mini / Redis | peak RSS mini / Redis |
|---|---:|---:|---:|---:|---:|---:|---:|
| 1 | 499,912 | 478,992 | 1.04x | 42.5us / 42.0us | 1.21us / 0.79us | 0.60 / 0.38 | 2.8 MiB / 18.2 MiB |
| 10 | 3,951,792 | 2,751,144 | 1.44x | 84.0us / 84.0us | 0.67us / 0.36us | 2.65 / 1.00 | 3.4 MiB / 18.4 MiB |
| 50 | 6,903,984 | 3,036,144 | 2.27x | 256.0us / 344.1us | 0.81us / 0.33us | 5.58 / 1.01 | 5.2 MiB / 19.2 MiB |
| 200 | 8,870,520 | 2,892,800 | 3.07x | 843.8us / 2.03ms | 0.95us / 0.35us | 8.43 / 1.00 | 6.7 MiB / 20.4 MiB |

## Payload size: SET, 50 connections, pipeline 16

| payload | my-mini-redis ops/s | Redis ops/s | mini / Redis | p99 mini / Redis | CPU per op mini / Redis | cores mini / Redis | peak RSS mini / Redis |
|---|---:|---:|---:|---:|---:|---:|---:|
| 16 B | 6,861,592 | 3,126,240 | 2.19x | 266.2us / 417.8us | 0.81us / 0.32us | 5.58 / 1.00 | 5.0 MiB / 19.0 MiB |
| 1.0 KiB | 5,402,880 | 2,156,576 | 2.51x | 348.2us / 491.5us | 1.18us / 0.46us | 6.36 / 1.00 | 13.2 MiB / 27.1 MiB |
| 16.0 KiB | 1,346,864 | 276,136 | 4.88x | 1.47ms / 5.05ms | 6.04us / 3.62us | 8.13 / 1.00 | 196.9 MiB / 162.1 MiB |
| 64.0 KiB | 505,320 | 97,936 | 5.16x | 4.26ms / 10.22ms | 16.62us / 10.21us | 8.40 / 1.00 | 422.5 MiB / 328.7 MiB |

## Pub/sub fan-out: 4 publishers, pipeline 16

| fan-out | my-mini-redis ops/s | Redis ops/s | mini / Redis | p99 mini / Redis | CPU per op mini / Redis | cores mini / Redis | peak RSS mini / Redis |
|---|---:|---:|---:|---:|---:|---:|---:|
| 1 subscriber | 1,339,600 | 1,032,888 | 1.30x | 84.0us / 84.0us | 1.17us / 0.54us | 1.56 / 0.55 | 2.8 MiB / 18.3 MiB |
| 10 subscribers | 782,776 | 728,488 | 1.07x | 182.3us / 143.4us | 2.78us / 1.30us | 2.17 / 0.94 | 3.9 MiB / 18.4 MiB |
| 100 subscribers | 144,096 | 144,160 | 1.00x | 1.01ms / 573.4us | 19.71us / 6.94us | 2.84 / 1.00 | 13.2 MiB / 19.8 MiB |

## Pub/sub channels: 100 subscribers, 4 publishers, pipeline 16

| channels | my-mini-redis ops/s | Redis ops/s | mini / Redis | p99 mini / Redis | CPU per op mini / Redis | cores mini / Redis | peak RSS mini / Redis |
|---|---:|---:|---:|---:|---:|---:|---:|
| 1 channel | 145,424 | 145,664 | 1.00x | 983.0us / 540.7us | 19.49us / 6.90us | 2.83 / 1.00 | 13.6 MiB / 19.8 MiB |
| 10 channels | 303,536 | 284,000 | 1.07x | 446.5us / 340.0us | 10.21us / 3.50us | 3.10 / 0.99 | 4.3 MiB / 18.8 MiB |
| 100 channels | 801,032 | 604,712 | 1.32x | 155.6us / 190.5us | 2.88us / 1.66us | 2.30 / 1.00 | 3.5 MiB / 18.7 MiB |

## Single-threaded: my-mini-redis with 1 worker thread vs Redis

my-mini-redis started with `TOKIO_WORKER_THREADS=1`, so both servers run commands on one thread. Redis numbers are from the summary runs.

| Scenario | mini (1 thread) ops/s | Redis ops/s | mini / Redis | p99 mini / Redis | CPU per op mini / Redis | cores mini / Redis |
|---|---:|---:|---:|---:|---:|---:|
| PING (raw, pipeline 16) | 5,046,064 | 4,852,813 | 1.04x | 372.7us / 229.4us | 0.20us / 0.21us | 1.00 / 1.00 |
| SET (raw, pipeline 16) | 2,909,104 | 3,044,400 | 0.96x | 581.6us / 348.2us | 0.34us / 0.33us | 1.00 / 1.00 |
| GET (raw, pipeline 16) | 3,585,613 | 3,446,880 | 1.04x | 426.0us / 331.8us | 0.28us / 0.29us | 1.00 / 1.00 |

## Load generator

CPU used by the benchmark process itself during each summary run. Near 18 cores, the generator is saturated and the server isn't the limit.

| Scenario | Server | load generator cores | server cores | total |
|---|---|---:|---:|---:|
| PING (raw, pipeline 16) | my-mini-redis | 2.51 | 3.43 | 5.93 of 18 |
| PING (raw, pipeline 16) | Redis | 1.32 | 1.00 | 2.32 of 18 |
| SET (raw, pipeline 16) | my-mini-redis | 2.35 | 5.60 | 7.95 of 18 |
| SET (raw, pipeline 16) | Redis | 0.81 | 1.00 | 1.81 of 18 |
| GET (raw, pipeline 16) | my-mini-redis | 2.42 | 4.65 | 7.08 of 18 |
| GET (raw, pipeline 16) | Redis | 0.92 | 1.00 | 1.92 of 18 |
| PUBLISH, no subscribers (raw, pipeline 16) | my-mini-redis | 2.40 | 5.60 | 8.00 of 18 |
| PUBLISH, no subscribers (raw, pipeline 16) | Redis | 0.92 | 1.00 | 1.92 of 18 |
| SET via Client (1 in flight) | my-mini-redis | 2.68 | 2.69 | 5.37 of 18 |
| SET via Client (1 in flight) | Redis | 2.10 | 1.00 | 3.10 of 18 |
| GET via Client (1 in flight) | my-mini-redis | 2.60 | 2.68 | 5.29 of 18 |
| GET via Client (1 in flight) | Redis | 2.06 | 1.00 | 3.07 of 18 |
| PUBLISH to 50 subscribers (4 publishers, pipeline 16) | my-mini-redis | 5.31 | 2.67 | 7.98 of 18 |
| PUBLISH to 50 subscribers (4 publishers, pipeline 16) | Redis | 4.94 | 1.00 | 5.94 of 18 |
| SET (raw, pipeline 16) during PUBLISH to 50 subscribers | my-mini-redis | 4.93 | 5.11 | 10.04 of 18 |
| SET (raw, pipeline 16) during PUBLISH to 50 subscribers | Redis | 2.47 | 1.00 | 3.47 of 18 |

## Observations

- Largest throughput gap: **PUBLISH to 50 subscribers (4 publishers, pipeline 16)** at 0.89x Redis.
- Closest to (or ahead of) Redis: **SET (raw, pipeline 16)** at 2.30x.
- my-mini-redis uses on average **2.4x** the CPU time per operation that Redis does across the summary scenarios; that is the efficiency gap to close independently of core count.
- **PING (raw, pipeline 16)**: my-mini-redis on 1 thread does 5,046,064 ops/s at 0.20us CPU/op; on all threads 8,892,400 ops/s at 0.39us CPU/op using 3.4 cores. 1.9x the CPU per op for 1.76x the throughput points at contention between threads (the shared db lock, the scheduler) rather than command cost.
- **SET (raw, pipeline 16)**: my-mini-redis on 1 thread does 2,909,104 ops/s at 0.34us CPU/op; on all threads 7,004,378 ops/s at 0.80us CPU/op using 5.6 cores. 2.3x the CPU per op for 2.41x the throughput points at contention between threads (the shared db lock, the scheduler) rather than command cost.
- **GET (raw, pipeline 16)**: my-mini-redis on 1 thread does 3,585,613 ops/s at 0.28us CPU/op; on all threads 7,654,752 ops/s at 0.61us CPU/op using 4.7 cores. 2.2x the CPU per op for 2.13x the throughput points at contention between threads (the shared db lock, the scheduler) rather than command cost.
- With a single connection (no contention), my-mini-redis needs 1.21us CPU per SET against Redis's 0.79us (1.5x): the per-request cost of parsing, allocating and writing replies.
- Each key with a 16 B value costs 212 B in my-mini-redis and 93 B in Redis (2.3x).
- Pipelining from 1 to 64 multiplies my-mini-redis throughput by 23.7x: the larger this is, the more time goes to per-request round trips and syscalls rather than command execution.

## Profiling next steps

- CPU profile the server under one scenario: `cargo build --release`, start `target/release/my-mini-redis-server`, run `scripts/bench.sh --address 127.0.0.1:6379 --no-redis --quick --only set --duration 30`, and attach a sampling profiler to the server (`samply record -p <pid>` on macOS, `perf record -g -p <pid>` on Linux).
- Compare allocation counts with a heap profiler (`heaptrack`, or Instruments' Allocations on macOS) on the same run; a high *CPU per op* with low *cores* points at per-request allocation or syscalls rather than contention.
- `JSON` with every number is next to this report for tracking changes over time.

