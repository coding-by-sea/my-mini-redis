# Load benchmark: my-redis vs Redis

| | |
|---|---|
| Date (Unix time) | 1794834641 |
| Commit | 22f2f31 (uncommitted changes) |
| Machine | linux aarch64, 18 CPUs |
| my-redis | fresh process per run |
| Redis | 8.0.2, fresh process per run |
| Load | 50 connections, pipeline 16, 64 B payload, 10000 keys, 5s measured after 1s warmup (sweeps: 2s after 0.5s) |

**Reading the numbers.** Both servers and the load generator share this machine. Redis runs commands on one thread; my-redis uses every core, so compare *CPU per op* for efficiency and *ops/s* for capacity. *Cores* is server CPU time divided by wall time. If the load generator's cores approach the machine's CPU count, it is the bottleneck and ops/s understates both servers. Latency for raw scenarios is per pipelined batch.

## Summary

| Scenario | my-redis ops/s | Redis ops/s | my-redis / Redis | p50 my-redis / Redis | p99 my-redis / Redis | CPU per op my-redis / Redis | cores my-redis / Redis | peak RSS my-redis / Redis |
|---|---:|---:|---:|---:|---:|---:|---:|---:|
| PING (raw, pipeline 16) | 8,713,542 | 4,757,789 | 1.83x | 89.1us / 170.0us | 182.3us / 237.6us | 0.38us / 0.21us | 3.33 / 1.00 | 3.2 MiB / 18.5 MiB |
| SET (raw, pipeline 16) | 1,067,261 | 2,971,610 | 0.36x | 729.1us / 282.6us | 1.59ms / 368.6us | 9.59us / 0.34us | 10.23 / 1.00 | 5.0 MiB / 19.2 MiB |
| GET (raw, pipeline 16) | 1,955,840 | 3,444,170 | 0.57x | 385.0us / 243.7us | 966.7us / 307.2us | 5.47us / 0.29us | 10.71 / 1.00 | 6.1 MiB / 19.7 MiB |
| PUBLISH, no subscribers (raw, pipeline 16) | 6,355,558 | 3,482,374 | 1.83x | 120.8us / 239.6us | 270.3us / 303.1us | 0.89us / 0.29us | 5.66 / 1.00 | 3.2 MiB / 18.5 MiB |
| SET via Client (1 in flight) | 590,171 | 475,292 | 1.24x | 81.9us / 106.5us | 170.0us / 167.9us | 4.66us / 2.10us | 2.75 / 1.00 | 6.0 MiB / 19.5 MiB |
| GET via Client (1 in flight) | 614,011 | 494,040 | 1.24x | 78.8us / 101.4us | 161.8us / 159.7us | 4.39us / 2.02us | 2.69 / 1.00 | 5.9 MiB / 19.7 MiB |
| PUBLISH to 50 subscribers (4 publishers, pipeline 16) | 61,546 | 284,582 | 0.22x | 917.5us / 225.3us | 3.01ms / 294.9us | 90.86us / 3.52us | 5.59 / 1.00 | 4.9 MiB / 19.1 MiB |

### Latency detail

| Scenario | Server | p50 | p90 | p99 | p99.9 | max |
|---|---|---:|---:|---:|---:|---:|
| PING (raw, pipeline 16) | my-redis | 89.1us | 137.2us | 182.3us | 219.1us | 335.6us |
| PING (raw, pipeline 16) | Redis | 170.0us | 208.9us | 237.6us | 299.0us | 483.6us |
| SET (raw, pipeline 16) | my-redis | 729.1us | 1.15ms | 1.59ms | 2.16ms | 5.04ms |
| SET (raw, pipeline 16) | Redis | 282.6us | 323.6us | 368.6us | 442.4us | 673.5us |
| GET (raw, pipeline 16) | my-redis | 385.0us | 647.2us | 966.7us | 1.62ms | 11.18ms |
| GET (raw, pipeline 16) | Redis | 243.7us | 282.6us | 307.2us | 348.2us | 548.0us |
| PUBLISH, no subscribers (raw, pipeline 16) | my-redis | 120.8us | 190.5us | 270.3us | 372.7us | 1.35ms |
| PUBLISH, no subscribers (raw, pipeline 16) | Redis | 239.6us | 278.5us | 303.1us | 348.2us | 446.5us |
| SET via Client (1 in flight) | my-redis | 81.9us | 126.0us | 170.0us | 204.8us | 369.8us |
| SET via Client (1 in flight) | Redis | 106.5us | 141.3us | 167.9us | 196.6us | 408.9us |
| GET via Client (1 in flight) | my-redis | 78.8us | 120.8us | 161.8us | 192.5us | 316.9us |
| GET via Client (1 in flight) | Redis | 101.4us | 133.1us | 159.7us | 186.4us | 299.8us |
| PUBLISH to 50 subscribers (4 publishers, pipeline 16) | my-redis | 917.5us | 1.88ms | 3.01ms | 4.19ms | 6.19ms |
| PUBLISH to 50 subscribers (4 publishers, pipeline 16) | Redis | 225.3us | 256.0us | 294.9us | 364.5us | 875.7us |

### Pub/sub delivery

Publishes are counted at the publishers; deliveries at the subscribers (publishes x subscribers when nothing is lost). *RSS growth* is how much the server's memory grew during the run, mostly queued messages.

| Run | Server | publishes/s | deliveries/s | delivery p50 | p99 | p99.9 | max | disconnected | peak RSS | RSS growth |
|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| summary | my-redis | 61,546 | 3,077,280 | 647.2us | 2.56ms | 3.60ms | 6.30ms | 0 of 50 | 4.9 MiB | +2.3 MiB |
| summary | Redis | 284,582 | 14,229,120 | 323.6us | 446.5us | 503.8us | 1.33ms | 0 of 50 | 19.1 MiB | +1000.0 KiB |
| 1 subscriber | my-redis | 1,076,280 | 1,076,280 | 59.9us | 130.0us | 165.9us | 249.4us | 0 of 1 | 3.1 MiB | +504.0 KiB |
| 1 subscriber | Redis | 1,028,456 | 1,028,460 | 52.7us | 70.7us | 86.0us | 143.1us | 0 of 1 | 18.3 MiB | +200.0 KiB |
| 10 subscribers | my-redis | 315,056 | 3,150,560 | 151.6us | 462.8us | 630.8us | 1.24ms | 0 of 10 | 3.6 MiB | +1.0 MiB |
| 10 subscribers | Redis | 749,664 | 7,496,640 | 99.3us | 157.7us | 174.1us | 223.8us | 0 of 10 | 18.4 MiB | +344.0 KiB |
| 100 subscribers | my-redis | 30,712 | 3,071,200 | 1.28ms | 5.77ms | 7.80ms | 11.24ms | 0 of 100 | 5.8 MiB | +3.2 MiB |
| 100 subscribers | Redis | 144,288 | 14,428,800 | 639.0us | 827.4us | 892.9us | 1.00ms | 0 of 100 | 19.8 MiB | +1.7 MiB |

## Memory

Resident memory (RSS) of each server process, read from the OS after letting allocations settle. Per-connection and per-key numbers are the RSS increase divided by the count, so they include allocator overhead. Freed memory is often kept by the allocator, so RSS rarely shrinks.

| Measure | my-redis | Redis | my-redis / Redis |
|---|---:|---:|---:|
| Idle RSS | 2.7 MiB | 18.1 MiB | 0.15x |
| Per idle connection (1000 open) | 11.1 KiB | 5.4 KiB | 2.08x |
| Per key, 16 B value (100000 keys) | 195 B | 93 B | 2.09x |
|   overhead per key beyond 31 B of key + value | 164 B | 62 B | 2.64x |
| Per key, 1.0 KiB value (20000 keys) | 1.5 KiB | 1.3 KiB | 1.15x |
|   overhead per key beyond 1.0 KiB of key + value | 490 B | 285 B | 1.72x |

## Pipeline depth: SET, 50 connections

| pipeline | my-redis ops/s | Redis ops/s | my-redis / Redis | p99 my-redis / Redis | CPU per op my-redis / Redis | cores my-redis / Redis | peak RSS my-redis / Redis |
|---|---:|---:|---:|---:|---:|---:|---:|
| 1 | 622,233 | 491,615 | 1.27x | 157.7us / 159.7us | 4.47us / 2.02us | 2.78 / 0.99 | 3.4 MiB / 18.5 MiB |
| 4 | 978,210 | 1,531,302 | 0.64x | 581.6us / 186.4us | 8.62us / 0.65us | 8.43 / 1.00 | 4.0 MiB / 18.6 MiB |
| 16 | 1,076,848 | 3,054,576 | 0.35x | 1.57ms / 340.0us | 9.50us / 0.33us | 10.23 / 0.99 | 5.0 MiB / 19.2 MiB |
| 64 | 1,243,104 | 4,017,248 | 0.31x | 4.59ms / 983.0us | 8.12us / 0.25us | 10.10 / 1.00 | 6.0 MiB / 19.5 MiB |

## Concurrent connections: SET, pipeline 16

| connections | my-redis ops/s | Redis ops/s | my-redis / Redis | p99 my-redis / Redis | CPU per op my-redis / Redis | cores my-redis / Redis | peak RSS my-redis / Redis |
|---|---:|---:|---:|---:|---:|---:|---:|
| 1 | 498,400 | 491,256 | 1.01x | 41.5us / 41.5us | 0.78us / 0.76us | 0.39 / 0.37 | 2.8 MiB / 18.2 MiB |
| 10 | 2,088,336 | 2,710,200 | 0.77x | 196.6us / 82.9us | 1.57us / 0.37us | 3.28 / 1.00 | 3.5 MiB / 18.4 MiB |
| 50 | 1,082,960 | 3,039,688 | 0.36x | 1.57ms / 344.1us | 9.41us / 0.33us | 10.19 / 1.00 | 5.0 MiB / 19.2 MiB |
| 200 | 1,074,544 | 2,896,512 | 0.37x | 6.16ms / 2.05ms | 9.58us / 0.35us | 10.30 / 1.00 | 7.6 MiB / 20.3 MiB |

## Payload size: SET, 50 connections, pipeline 16

| payload | my-redis ops/s | Redis ops/s | my-redis / Redis | p99 my-redis / Redis | CPU per op my-redis / Redis | cores my-redis / Redis | peak RSS my-redis / Redis |
|---|---:|---:|---:|---:|---:|---:|---:|
| 16 B | 1,027,144 | 3,169,096 | 0.32x | 1.65ms / 331.8us | 9.85us / 0.32us | 10.11 / 1.00 | 4.7 MiB / 19.0 MiB |
| 1.0 KiB | 843,104 | 2,267,208 | 0.37x | 1.95ms / 454.7us | 12.19us / 0.44us | 10.27 / 1.00 | 11.7 MiB / 27.1 MiB |
| 16.0 KiB | 494,064 | 343,472 | 1.44x | 3.28ms / 2.49ms | 18.36us / 2.91us | 9.07 / 1.00 | 140.8 MiB / 162.2 MiB |
| 64.0 KiB | 345,392 | 102,224 | 3.38x | 4.59ms / 10.49ms | 25.91us / 9.73us | 8.95 / 1.00 | 317.7 MiB / 328.6 MiB |

## Pub/sub fan-out: 4 publishers, pipeline 16

| fan-out | my-redis ops/s | Redis ops/s | my-redis / Redis | p99 my-redis / Redis | CPU per op my-redis / Redis | cores my-redis / Redis | peak RSS my-redis / Redis |
|---|---:|---:|---:|---:|---:|---:|---:|
| 1 subscriber | 1,076,280 | 1,028,456 | 1.05x | 124.9us / 81.9us | 1.88us / 0.54us | 2.02 / 0.55 | 3.1 MiB / 18.3 MiB |
| 10 subscribers | 315,056 | 749,664 | 0.42x | 520.2us / 133.1us | 11.44us / 1.26us | 3.60 / 0.94 | 3.6 MiB / 18.4 MiB |
| 100 subscribers | 30,712 | 144,288 | 0.21x | 6.62ms / 532.5us | 218.00us / 6.90us | 6.70 / 0.99 | 5.8 MiB / 19.8 MiB |

## Single-threaded: my-redis with 1 worker thread vs Redis

my-redis started with `TOKIO_WORKER_THREADS=1`, so both servers run commands on one thread. Redis numbers are from the summary runs.

| Scenario | my-redis (1 thread) ops/s | Redis ops/s | my-redis / Redis | p99 my-redis / Redis | CPU per op my-redis / Redis | cores my-redis / Redis |
|---|---:|---:|---:|---:|---:|---:|
| PING (raw, pipeline 16) | 4,625,818 | 4,757,789 | 0.97x | 208.9us / 237.6us | 0.22us / 0.21us | 1.00 / 1.00 |
| SET (raw, pipeline 16) | 2,554,035 | 2,971,610 | 0.86x | 372.7us / 368.6us | 0.39us / 0.34us | 1.00 / 1.00 |
| GET (raw, pipeline 16) | 3,124,042 | 3,444,170 | 0.91x | 307.2us / 307.2us | 0.32us / 0.29us | 1.00 / 1.00 |

## Load generator

CPU used by the benchmark process itself during each summary run. Near 18 cores, the generator is saturated and the server isn't the limit.

| Scenario | Server | load generator cores | server cores | total |
|---|---|---:|---:|---:|
| PING (raw, pipeline 16) | my-redis | 2.43 | 3.33 | 5.75 of 18 |
| PING (raw, pipeline 16) | Redis | 1.28 | 1.00 | 2.28 of 18 |
| SET (raw, pipeline 16) | my-redis | 0.78 | 10.23 | 11.01 of 18 |
| SET (raw, pipeline 16) | Redis | 0.80 | 1.00 | 1.80 of 18 |
| GET (raw, pipeline 16) | my-redis | 0.98 | 10.71 | 11.69 of 18 |
| GET (raw, pipeline 16) | Redis | 0.91 | 1.00 | 1.91 of 18 |
| PUBLISH, no subscribers (raw, pipeline 16) | my-redis | 2.37 | 5.66 | 8.03 of 18 |
| PUBLISH, no subscribers (raw, pipeline 16) | Redis | 0.92 | 1.00 | 1.92 of 18 |
| SET via Client (1 in flight) | my-redis | 2.68 | 2.75 | 5.42 of 18 |
| SET via Client (1 in flight) | Redis | 2.05 | 1.00 | 3.05 of 18 |
| GET via Client (1 in flight) | my-redis | 2.61 | 2.69 | 5.31 of 18 |
| GET via Client (1 in flight) | Redis | 2.03 | 1.00 | 3.03 of 18 |
| PUBLISH to 50 subscribers (4 publishers, pipeline 16) | my-redis | 2.73 | 5.59 | 8.33 of 18 |
| PUBLISH to 50 subscribers (4 publishers, pipeline 16) | Redis | 4.91 | 1.00 | 5.91 of 18 |

## Observations

- Largest throughput gap: **PUBLISH to 50 subscribers (4 publishers, pipeline 16)** at 0.22x Redis.
- Closest to (or ahead of) Redis: **PING (raw, pipeline 16)** at 1.83x.
- my-redis uses on average **11.8x** the CPU time per operation that Redis does across the summary scenarios; that is the efficiency gap to close independently of core count.
- **PING (raw, pipeline 16)**: my-redis on 1 thread does 4,625,818 ops/s at 0.22us CPU/op; on all threads 8,713,542 ops/s at 0.38us CPU/op using 3.3 cores. 1.8x the CPU per op for 1.88x the throughput points at contention between threads (the shared db lock, the scheduler) rather than command cost.
- **SET (raw, pipeline 16)**: my-redis on 1 thread does 2,554,035 ops/s at 0.39us CPU/op; on all threads 1,067,261 ops/s at 9.59us CPU/op using 10.2 cores. 24.4x the CPU per op for 0.42x the throughput points at contention between threads (the shared db lock, the scheduler) rather than command cost.
- **GET (raw, pipeline 16)**: my-redis on 1 thread does 3,124,042 ops/s at 0.32us CPU/op; on all threads 1,955,840 ops/s at 5.47us CPU/op using 10.7 cores. 17.1x the CPU per op for 0.63x the throughput points at contention between threads (the shared db lock, the scheduler) rather than command cost.
- With a single connection (no contention), my-redis needs 0.78us CPU per SET against Redis's 0.76us (1.0x): the per-request cost of parsing, allocating and writing replies.
- Each key with a 16 B value costs 195 B in my-redis and 93 B in Redis (2.1x).
- Pipelining from 1 to 64 multiplies my-redis throughput by 2.0x: the larger this is, the more time goes to per-request round trips and syscalls rather than command execution.

## Profiling next steps

- CPU profile the server under one scenario: `cargo build --release`, start `target/release/my-redis-server`, run `scripts/bench.sh --address 127.0.0.1:6379 --no-redis --quick --only set --duration 30`, and attach a sampling profiler to the server (`samply record -p <pid>` on macOS, `perf record -g -p <pid>` on Linux).
- Compare allocation counts with a heap profiler (`heaptrack`, or Instruments' Allocations on macOS) on the same run; a high *CPU per op* with low *cores* points at per-request allocation or syscalls rather than contention.
- `JSON` with every number is next to this report for tracking changes over time.

