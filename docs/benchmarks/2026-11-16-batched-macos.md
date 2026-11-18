# Load benchmark: my-redis vs Redis

| | |
|---|---|
| Date (Unix time) | 1794832627 |
| Commit | 22f2f31 (uncommitted changes) |
| Machine | macos aarch64, 18 CPUs |
| my-redis | fresh process per run |
| Redis | 8.10.3, fresh process per run |
| Load | 50 connections, pipeline 16, 64 B payload, 10000 keys, 5s measured after 1s warmup (sweeps: 2s after 0.5s) |

**Reading the numbers.** Both servers and the load generator share this machine. Redis runs commands on one thread; my-redis uses every core, so compare *CPU per op* for efficiency and *ops/s* for capacity. *Cores* is server CPU time divided by wall time. If the load generator's cores approach the machine's CPU count, it is the bottleneck and ops/s understates both servers. Latency for raw scenarios is per pipelined batch.

## Summary

| Scenario | my-redis ops/s | Redis ops/s | my-redis / Redis | p50 my-redis / Redis | p99 my-redis / Redis | CPU per op my-redis / Redis | cores my-redis / Redis | peak RSS my-redis / Redis |
|---|---:|---:|---:|---:|---:|---:|---:|---:|
| PING (raw, pipeline 16) | 3,178,918 | 3,080,954 | 1.03x | 251.9us / 251.9us | 401.4us / 393.2us | 1.21us / 0.32us | 3.85 / 1.00 | 5.2 MiB / 7.7 MiB |
| SET (raw, pipeline 16) | 900,157 | 1,821,139 | 0.49x | 868.4us / 413.7us | 1.74ms / 704.5us | 16.79us / 0.55us | 15.12 / 1.00 | 7.6 MiB / 8.8 MiB |
| GET (raw, pipeline 16) | 1,794,995 | 2,295,494 | 0.78x | 434.2us / 331.8us | 901.1us / 540.7us | 7.78us / 0.43us | 13.96 / 1.00 | 8.4 MiB / 9.3 MiB |
| PUBLISH, no subscribers (raw, pipeline 16) | 3,055,008 | 2,321,120 | 1.32x | 258.0us / 327.7us | 401.4us / 532.5us | 1.35us / 0.43us | 4.13 / 1.00 | 5.5 MiB / 7.7 MiB |
| SET via Client (1 in flight) | 194,760 | 253,768 | 0.77x | 256.0us / 196.6us | 409.6us / 286.7us | 21.93us / 3.93us | 4.27 / 1.00 | 7.9 MiB / 9.3 MiB |
| GET via Client (1 in flight) | 200,762 | 259,037 | 0.78x | 249.9us / 192.5us | 397.3us / 282.6us | 18.87us / 3.85us | 3.79 / 1.00 | 8.0 MiB / 9.4 MiB |
| PUBLISH to 50 subscribers (4 publishers, pipeline 16) | 94,746 | 219,133 | 0.43x | 679.9us / 290.8us | 1.15ms / 385.0us | 69.20us / 4.36us | 6.56 / 0.96 | 5.2 MiB / 8.1 MiB |

### Latency detail

| Scenario | Server | p50 | p90 | p99 | p99.9 | max |
|---|---|---:|---:|---:|---:|---:|
| PING (raw, pipeline 16) | my-redis | 251.9us | 323.6us | 401.4us | 466.9us | 691.8us |
| PING (raw, pipeline 16) | Redis | 251.9us | 344.1us | 393.2us | 430.1us | 584.0us |
| SET (raw, pipeline 16) | my-redis | 868.4us | 1.29ms | 1.74ms | 2.10ms | 2.91ms |
| SET (raw, pipeline 16) | Redis | 413.7us | 614.4us | 704.5us | 761.9us | 906.3us |
| GET (raw, pipeline 16) | my-redis | 434.2us | 655.4us | 901.1us | 1.13ms | 2.88ms |
| GET (raw, pipeline 16) | Redis | 331.8us | 479.2us | 540.7us | 581.6us | 721.0us |
| PUBLISH, no subscribers (raw, pipeline 16) | my-redis | 258.0us | 323.6us | 401.4us | 475.1us | 678.8us |
| PUBLISH, no subscribers (raw, pipeline 16) | Redis | 327.7us | 471.0us | 532.5us | 573.4us | 707.0us |
| SET via Client (1 in flight) | my-redis | 256.0us | 331.8us | 409.6us | 475.1us | 721.3us |
| SET via Client (1 in flight) | Redis | 196.6us | 247.8us | 286.7us | 323.6us | 487.7us |
| GET via Client (1 in flight) | my-redis | 249.9us | 323.6us | 397.3us | 462.8us | 632.6us |
| GET via Client (1 in flight) | Redis | 192.5us | 241.7us | 282.6us | 319.5us | 535.4us |
| PUBLISH to 50 subscribers (4 publishers, pipeline 16) | my-redis | 679.9us | 901.1us | 1.15ms | 1.39ms | 1.58ms |
| PUBLISH to 50 subscribers (4 publishers, pipeline 16) | Redis | 290.8us | 331.8us | 385.0us | 475.1us | 558.2us |

### Pub/sub delivery

Publishes are counted at the publishers; deliveries at the subscribers (publishes x subscribers when nothing is lost). *RSS growth* is how much the server's memory grew during the run, mostly queued messages.

| Run | Server | publishes/s | deliveries/s | delivery p50 | p99 | p99.9 | max | disconnected | peak RSS | RSS growth |
|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| summary | my-redis | 94,746 | 4,737,280 | 745.5us | 1.18ms | 1.36ms | 1.77ms | 0 of 50 | 5.2 MiB | +2.2 MiB |
| summary | Redis | 219,133 | 10,956,640 | 393.2us | 540.7us | 606.2us | 764.1us | 0 of 50 | 8.1 MiB | +1.2 MiB |
| 1 subscriber | my-redis | 992,496 | 992,496 | 53.2us | 153.6us | 251.9us | 214.57ms | 0 of 1 | 3.7 MiB | +928.0 KiB |
| 1 subscriber | Redis | 1,474,360 | 1,474,360 | 40.4us | 84.0us | 139.3us | 469.1us | 0 of 1 | 6.9 MiB | +48.0 KiB |
| 10 subscribers | my-redis | 316,680 | 3,166,800 | 186.4us | 438.3us | 540.7us | 757.0us | 0 of 10 | 5.3 MiB | +2.2 MiB |
| 10 subscribers | Redis | 618,960 | 6,189,600 | 109.6us | 200.7us | 307.2us | 643.3us | 0 of 10 | 7.1 MiB | +112.0 KiB |
| 100 subscribers | my-redis | 39,640 | 3,964,350 | 1.74ms | 2.39ms | 2.69ms | 3.27ms | 0 of 100 | 6.2 MiB | +3.2 MiB |
| 100 subscribers | Redis | 100,544 | 10,054,400 | 843.8us | 1.25ms | 1.59ms | 2.42ms | 0 of 100 | 9.3 MiB | +2.4 MiB |

## Memory

Resident memory (RSS) of each server process, read from the OS after letting allocations settle. Per-connection and per-key numbers are the RSS increase divided by the count, so they include allocator overhead. Freed memory is often kept by the allocator, so RSS rarely shrinks.

| Measure | my-redis | Redis | my-redis / Redis |
|---|---:|---:|---:|
| Idle RSS | 2.6 MiB | 6.6 MiB | 0.39x |
| Per idle connection (1000 open) | 23.4 KiB | 18.0 KiB | 1.30x |
| Per key, 16 B value (100000 keys) | 268 B | 88 B | 3.03x |
|   overhead per key beyond 31 B of key + value | 237 B | 57 B | 4.13x |
| Per key, 1.0 KiB value (20000 keys) | 1.9 KiB | 1.3 KiB | 1.46x |
|   overhead per key beyond 1.0 KiB of key + value | 939 B | 313 B | 3.00x |

## Pipeline depth: SET, 50 connections

| pipeline | my-redis ops/s | Redis ops/s | my-redis / Redis | p99 my-redis / Redis | CPU per op my-redis / Redis | cores my-redis / Redis | peak RSS my-redis / Redis |
|---|---:|---:|---:|---:|---:|---:|---:|
| 1 | 197,594 | 258,166 | 0.77x | 405.5us / 278.5us | 21.11us / 3.85us | 4.17 / 0.99 | 5.2 MiB / 8.0 MiB |
| 4 | 646,066 | 831,620 | 0.78x | 565.2us / 360.4us | 14.91us / 1.20us | 9.63 / 1.00 | 6.7 MiB / 8.2 MiB |
| 16 | 907,168 | 1,847,008 | 0.49x | 1.74ms / 688.1us | 16.66us / 0.54us | 15.11 / 1.00 | 7.3 MiB / 8.8 MiB |
| 64 | 897,344 | 2,644,224 | 0.34x | 6.29ms / 1.93ms | 17.25us / 0.38us | 15.48 / 1.00 | 9.0 MiB / 9.2 MiB |

## Concurrent connections: SET, pipeline 16

| connections | my-redis ops/s | Redis ops/s | my-redis / Redis | p99 my-redis / Redis | CPU per op my-redis / Redis | cores my-redis / Redis | peak RSS my-redis / Redis |
|---|---:|---:|---:|---:|---:|---:|---:|
| 1 | 794,328 | 798,640 | 0.99x | 36.4us / 32.5us | 0.66us / 0.66us | 0.52 / 0.53 | 2.9 MiB / 6.8 MiB |
| 10 | 825,584 | 1,908,808 | 0.43x | 426.0us / 107.5us | 7.58us / 0.46us | 6.26 / 0.88 | 4.8 MiB / 7.2 MiB |
| 50 | 914,128 | 1,769,416 | 0.52x | 1.75ms / 720.9us | 16.74us / 0.57us | 15.31 / 1.00 | 7.6 MiB / 8.8 MiB |
| 200 | 876,568 | 1,419,480 | 0.62x | 7.54ms / 3.28ms | 17.53us / 0.61us | 15.37 / 0.87 | 12.1 MiB / 12.0 MiB |

## Payload size: SET, 50 connections, pipeline 16

| payload | my-redis ops/s | Redis ops/s | my-redis / Redis | p99 my-redis / Redis | CPU per op my-redis / Redis | cores my-redis / Redis | peak RSS my-redis / Redis |
|---|---:|---:|---:|---:|---:|---:|---:|
| 16 B | 917,656 | 1,702,816 | 0.54x | 1.72ms / 753.7us | 16.40us / 0.58us | 15.05 / 1.00 | 6.8 MiB / 8.3 MiB |
| 1.0 KiB | 492,488 | 1,490,256 | 0.33x | 3.77ms / 786.4us | 23.79us / 0.67us | 11.71 / 1.00 | 157.3 MiB / 17.8 MiB |
| 16.0 KiB | 343,464 | 366,168 | 0.94x | 2.69ms / 3.18ms | 10.99us / 2.73us | 3.78 / 1.00 | 1.27 GiB / 159.8 MiB |
| 64.0 KiB | 131,800 | 131,640 | 1.00x | 11.40ms / 6.49ms | 17.22us / 7.52us | 2.27 / 0.99 | 368.7 MiB / 340.0 MiB |

## Pub/sub fan-out: 4 publishers, pipeline 16

| fan-out | my-redis ops/s | Redis ops/s | my-redis / Redis | p99 my-redis / Redis | CPU per op my-redis / Redis | cores my-redis / Redis | peak RSS my-redis / Redis |
|---|---:|---:|---:|---:|---:|---:|---:|
| 1 subscriber | 992,496 | 1,474,360 | 0.67x | 141.3us / 86.0us | 1.98us / 0.61us | 1.97 / 0.90 | 3.7 MiB / 6.9 MiB |
| 10 subscribers | 316,680 | 618,960 | 0.51x | 462.8us / 176.1us | 15.64us / 1.54us | 4.95 / 0.95 | 5.3 MiB / 7.1 MiB |
| 100 subscribers | 39,640 | 100,544 | 0.39x | 2.33ms / 876.5us | 174.14us / 9.51us | 6.90 / 0.96 | 6.2 MiB / 9.3 MiB |

## Single-threaded: my-redis with 1 worker thread vs Redis

my-redis started with `TOKIO_WORKER_THREADS=1`, so both servers run commands on one thread. Redis numbers are from the summary runs.

| Scenario | my-redis (1 thread) ops/s | Redis ops/s | my-redis / Redis | p99 my-redis / Redis | CPU per op my-redis / Redis | cores my-redis / Redis |
|---|---:|---:|---:|---:|---:|---:|
| PING (raw, pipeline 16) | 3,132,486 | 3,080,954 | 1.02x | 434.2us / 393.2us | 0.32us / 0.32us | 1.00 / 1.00 |
| SET (raw, pipeline 16) | 2,083,306 | 1,821,139 | 1.14x | 442.4us / 704.5us | 0.48us / 0.55us | 1.00 / 1.00 |
| GET (raw, pipeline 16) | 2,494,301 | 2,295,494 | 1.09x | 450.6us / 540.7us | 0.40us / 0.43us | 0.99 / 1.00 |

## Load generator

CPU used by the benchmark process itself during each summary run. Near 18 cores, the generator is saturated and the server isn't the limit.

| Scenario | Server | load generator cores | server cores | total |
|---|---|---:|---:|---:|
| PING (raw, pipeline 16) | my-redis | 3.48 | 3.85 | 7.33 of 18 |
| PING (raw, pipeline 16) | Redis | 2.12 | 1.00 | 3.12 of 18 |
| SET (raw, pipeline 16) | my-redis | 0.70 | 15.12 | 15.81 of 18 |
| SET (raw, pipeline 16) | Redis | 1.40 | 1.00 | 2.39 of 18 |
| GET (raw, pipeline 16) | my-redis | 1.74 | 13.96 | 15.70 of 18 |
| GET (raw, pipeline 16) | Redis | 1.65 | 1.00 | 2.64 of 18 |
| PUBLISH, no subscribers (raw, pipeline 16) | my-redis | 2.69 | 4.13 | 6.82 of 18 |
| PUBLISH, no subscribers (raw, pipeline 16) | Redis | 1.67 | 1.00 | 2.67 of 18 |
| SET via Client (1 in flight) | my-redis | 3.87 | 4.27 | 8.14 of 18 |
| SET via Client (1 in flight) | Redis | 2.83 | 1.00 | 3.82 of 18 |
| GET via Client (1 in flight) | my-redis | 3.75 | 3.79 | 7.54 of 18 |
| GET via Client (1 in flight) | Redis | 2.84 | 1.00 | 3.84 of 18 |
| PUBLISH to 50 subscribers (4 publishers, pipeline 16) | my-redis | 3.57 | 6.56 | 10.13 of 18 |
| PUBLISH to 50 subscribers (4 publishers, pipeline 16) | Redis | 4.44 | 0.96 | 5.40 of 18 |

## Observations

- Largest throughput gap: **PUBLISH to 50 subscribers (4 publishers, pipeline 16)** at 0.43x Redis.
- Closest to (or ahead of) Redis: **PUBLISH, no subscribers (raw, pipeline 16)** at 1.32x.
- my-redis uses on average **11.7x** the CPU time per operation that Redis does across the summary scenarios; that is the efficiency gap to close independently of core count.
- **PING (raw, pipeline 16)**: my-redis on 1 thread does 3,132,486 ops/s at 0.32us CPU/op; on all threads 3,178,918 ops/s at 1.21us CPU/op using 3.8 cores. 3.8x the CPU per op for 1.01x the throughput points at contention between threads (the shared db lock, the scheduler) rather than command cost.
- **SET (raw, pipeline 16)**: my-redis on 1 thread does 2,083,306 ops/s at 0.48us CPU/op; on all threads 900,157 ops/s at 16.79us CPU/op using 15.1 cores. 35.0x the CPU per op for 0.43x the throughput points at contention between threads (the shared db lock, the scheduler) rather than command cost.
- **GET (raw, pipeline 16)**: my-redis on 1 thread does 2,494,301 ops/s at 0.40us CPU/op; on all threads 1,794,995 ops/s at 7.78us CPU/op using 14.0 cores. 19.5x the CPU per op for 0.72x the throughput points at contention between threads (the shared db lock, the scheduler) rather than command cost.
- With a single connection (no contention), my-redis needs 0.66us CPU per SET against Redis's 0.66us (1.0x): the per-request cost of parsing, allocating and writing replies.
- Each key with a 16 B value costs 268 B in my-redis and 88 B in Redis (3.0x).
- During **SET (raw, pipeline 16)** on my-redis, server and load generator together used 15.8 of 18 cores: the machine was close to saturated, which also slows the generator.
- During **GET (raw, pipeline 16)** on my-redis, server and load generator together used 15.7 of 18 cores: the machine was close to saturated, which also slows the generator.
- Pipelining from 1 to 64 multiplies my-redis throughput by 4.5x: the larger this is, the more time goes to per-request round trips and syscalls rather than command execution.

## Profiling next steps

- CPU profile the server under one scenario: `cargo build --release`, start `target/release/my-redis-server`, run `scripts/bench.sh --address 127.0.0.1:6379 --no-redis --quick --only set --duration 30`, and attach a sampling profiler to the server (`samply record -p <pid>` on macOS, `perf record -g -p <pid>` on Linux).
- Compare allocation counts with a heap profiler (`heaptrack`, or Instruments' Allocations on macOS) on the same run; a high *CPU per op* with low *cores* points at per-request allocation or syscalls rather than contention.
- `JSON` with every number is next to this report for tracking changes over time.

