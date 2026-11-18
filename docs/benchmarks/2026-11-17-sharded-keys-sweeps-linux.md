# Load benchmark: my-redis vs Redis

| | |
|---|---|
| Date (Unix time) | 1794966771 |
| Commit | 0bcf088 (uncommitted changes) |
| Machine | linux aarch64, 18 CPUs |
| my-redis | fresh process per run |
| Redis | 8.0.2, fresh process per run |
| Load | 50 connections, pipeline 16, 64 B payload, 10000 keys, 3s measured after 1s warmup (sweeps: 2s after 0.5s) |

**Reading the numbers.** Both servers and the load generator share this machine. Redis runs commands on one thread; my-redis uses every core, so compare *CPU per op* for efficiency and *ops/s* for capacity. *Cores* is server CPU time divided by wall time. If the load generator's cores approach the machine's CPU count, it is the bottleneck and ops/s understates both servers. Latency for raw scenarios is per pipelined batch.

## Summary

| Scenario | my-redis ops/s | Redis ops/s | my-redis / Redis | p50 my-redis / Redis | p99 my-redis / Redis | CPU per op my-redis / Redis | cores my-redis / Redis | peak RSS my-redis / Redis |
|---|---:|---:|---:|---:|---:|---:|---:|---:|
| SET (raw, pipeline 16) | 6,982,304 | 3,021,536 | 2.31x | 107.5us / 278.5us | 254.0us / 348.2us | 0.80us / 0.33us | 5.57 / 1.00 | 5.2 MiB / 19.2 MiB |

### Latency detail

| Scenario | Server | p50 | p90 | p99 | p99.9 | max |
|---|---|---:|---:|---:|---:|---:|
| SET (raw, pipeline 16) | my-redis | 107.5us | 178.2us | 254.0us | 331.8us | 708.8us |
| SET (raw, pipeline 16) | Redis | 278.5us | 315.4us | 348.2us | 393.2us | 459.6us |

### Pub/sub delivery

Publishes are counted at the publishers; deliveries at the subscribers (publishes x subscribers when nothing is lost). *RSS growth* is how much the server's memory grew during the run, mostly queued messages.

| Run | Server | publishes/s | deliveries/s | delivery p50 | p99 | p99.9 | max | disconnected | peak RSS | RSS growth |
|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| 1 subscriber | my-redis | 1,366,520 | 1,366,520 | 46.1us | 81.9us | 103.4us | 162.9us | 0 of 1 | 2.8 MiB | +244.0 KiB |
| 1 subscriber | Redis | 1,016,456 | 1,016,461 | 53.2us | 71.7us | 82.9us | 136.2us | 0 of 1 | 18.3 MiB | +204.0 KiB |
| 10 subscribers | my-redis | 787,952 | 7,879,520 | 90.1us | 208.9us | 262.1us | 373.7us | 0 of 10 | 3.7 MiB | +1000.0 KiB |
| 10 subscribers | Redis | 748,592 | 7,485,920 | 99.3us | 157.7us | 176.1us | 237.5us | 0 of 10 | 18.5 MiB | +348.0 KiB |
| 100 subscribers | my-redis | 147,712 | 14,771,200 | 499.7us | 1.25ms | 1.61ms | 2.57ms | 0 of 100 | 14.3 MiB | +11.7 MiB |
| 100 subscribers | Redis | 144,728 | 14,472,800 | 630.8us | 827.4us | 901.1us | 1.15ms | 0 of 100 | 19.9 MiB | +1.8 MiB |
| 1 channel | my-redis | 147,072 | 14,707,200 | 503.8us | 1.25ms | 1.57ms | 2.26ms | 0 of 100 | 12.8 MiB | +10.2 MiB |
| 1 channel | Redis | 144,704 | 14,470,400 | 630.8us | 827.4us | 892.9us | 1.11ms | 0 of 100 | 19.9 MiB | +1.8 MiB |
| 10 channels | my-redis | 306,024 | 3,060,240 | 239.6us | 516.1us | 639.0us | 852.6us | 0 of 100 | 4.2 MiB | +1.6 MiB |
| 10 channels | Redis | 285,656 | 2,856,560 | 266.2us | 368.6us | 397.3us | 471.3us | 0 of 100 | 18.8 MiB | +696.0 KiB |
| 100 channels | my-redis | 808,536 | 808,540 | 91.1us | 176.1us | 208.9us | 309.9us | 0 of 100 | 3.5 MiB | +952.0 KiB |
| 100 channels | Redis | 625,224 | 625,225 | 97.3us | 153.6us | 178.2us | 251.8us | 0 of 100 | 18.8 MiB | +700.0 KiB |

## Memory

Resident memory (RSS) of each server process, read from the OS after letting allocations settle. Per-connection and per-key numbers are the RSS increase divided by the count, so they include allocator overhead. Freed memory is often kept by the allocator, so RSS rarely shrinks.

| Measure | my-redis | Redis | my-redis / Redis |
|---|---:|---:|---:|
| Idle RSS | 2.7 MiB | 18.1 MiB | 0.15x |
| Per idle connection (1000 open) | 5.6 KiB | 5.4 KiB | 1.03x |
| Per key, 16 B value (100000 keys) | 212 B | 95 B | 2.23x |
|   overhead per key beyond 31 B of key + value | 181 B | 64 B | 2.82x |
| Per key, 1.0 KiB value (20000 keys) | 1.5 KiB | 1.3 KiB | 1.16x |
|   overhead per key beyond 1.0 KiB of key + value | 493 B | 286 B | 1.73x |

## Pipeline depth: SET, 50 connections

| pipeline | my-redis ops/s | Redis ops/s | my-redis / Redis | p99 my-redis / Redis | CPU per op my-redis / Redis | cores my-redis / Redis | peak RSS my-redis / Redis |
|---|---:|---:|---:|---:|---:|---:|---:|
| 1 | 623,528 | 484,981 | 1.29x | 157.7us / 163.8us | 4.36us / 2.06us | 2.72 / 1.00 | 3.2 MiB / 18.6 MiB |
| 4 | 2,291,448 | 1,515,754 | 1.51x | 178.2us / 192.5us | 1.56us / 0.66us | 3.57 / 1.00 | 3.8 MiB / 18.7 MiB |
| 16 | 6,985,160 | 3,052,976 | 2.29x | 254.0us / 344.1us | 0.79us / 0.33us | 5.51 / 1.00 | 5.2 MiB / 19.2 MiB |
| 64 | 15,149,696 | 4,069,600 | 3.72x | 512.0us / 974.8us | 0.74us / 0.24us | 11.20 / 1.00 | 6.4 MiB / 19.6 MiB |

## Concurrent connections: SET, pipeline 16

| connections | my-redis ops/s | Redis ops/s | my-redis / Redis | p99 my-redis / Redis | CPU per op my-redis / Redis | cores my-redis / Redis | peak RSS my-redis / Redis |
|---|---:|---:|---:|---:|---:|---:|---:|
| 1 | 486,848 | 508,416 | 0.96x | 44.5us / 41.0us | 1.24us / 0.72us | 0.60 / 0.36 | 2.8 MiB / 18.3 MiB |
| 10 | 3,987,504 | 2,743,152 | 1.45x | 82.9us / 84.0us | 0.67us / 0.36us | 2.67 / 0.99 | 3.4 MiB / 18.5 MiB |
| 50 | 6,961,888 | 2,982,136 | 2.33x | 256.0us / 352.3us | 0.79us / 0.34us | 5.52 / 1.00 | 5.1 MiB / 19.2 MiB |
| 200 | 8,949,080 | 3,035,504 | 2.95x | 827.4us / 1.93ms | 0.94us / 0.33us | 8.41 / 1.01 | 6.7 MiB / 20.5 MiB |

## Payload size: SET, 50 connections, pipeline 16

| payload | my-redis ops/s | Redis ops/s | my-redis / Redis | p99 my-redis / Redis | CPU per op my-redis / Redis | cores my-redis / Redis | peak RSS my-redis / Redis |
|---|---:|---:|---:|---:|---:|---:|---:|
| 16 B | 7,111,824 | 3,170,760 | 2.24x | 249.9us / 331.8us | 0.77us / 0.32us | 5.50 / 1.00 | 5.0 MiB / 19.1 MiB |
| 1.0 KiB | 5,510,304 | 2,253,992 | 2.44x | 340.0us / 458.8us | 1.12us / 0.44us | 6.20 / 0.99 | 13.0 MiB / 27.2 MiB |
| 16.0 KiB | 1,388,664 | 342,920 | 4.05x | 1.46ms / 2.52ms | 5.74us / 2.90us | 7.97 / 0.99 | 204.4 MiB / 162.4 MiB |
| 64.0 KiB | 508,728 | 102,632 | 4.96x | 4.19ms / 9.44ms | 16.34us / 9.74us | 8.31 / 1.00 | 428.6 MiB / 328.9 MiB |

## Pub/sub fan-out: 4 publishers, pipeline 16

| fan-out | my-redis ops/s | Redis ops/s | my-redis / Redis | p99 my-redis / Redis | CPU per op my-redis / Redis | cores my-redis / Redis | peak RSS my-redis / Redis |
|---|---:|---:|---:|---:|---:|---:|---:|
| 1 subscriber | 1,366,520 | 1,016,456 | 1.34x | 80.9us / 81.9us | 1.16us / 0.54us | 1.58 / 0.54 | 2.8 MiB / 18.3 MiB |
| 10 subscribers | 787,952 | 748,592 | 1.05x | 182.3us / 135.2us | 2.78us / 1.26us | 2.19 / 0.94 | 3.7 MiB / 18.5 MiB |
| 100 subscribers | 147,712 | 144,728 | 1.02x | 999.4us / 532.5us | 19.02us / 6.91us | 2.81 / 1.00 | 14.3 MiB / 19.9 MiB |

## Pub/sub channels: 100 subscribers, 4 publishers, pipeline 16

| channels | my-redis ops/s | Redis ops/s | my-redis / Redis | p99 my-redis / Redis | CPU per op my-redis / Redis | cores my-redis / Redis | peak RSS my-redis / Redis |
|---|---:|---:|---:|---:|---:|---:|---:|
| 1 channel | 147,072 | 144,704 | 1.02x | 999.4us / 524.3us | 19.10us / 6.91us | 2.81 / 1.00 | 12.8 MiB / 19.9 MiB |
| 10 channels | 306,024 | 285,656 | 1.07x | 434.2us / 340.0us | 10.09us / 3.50us | 3.09 / 1.00 | 4.2 MiB / 18.8 MiB |
| 100 channels | 808,536 | 625,224 | 1.29x | 151.6us / 151.6us | 2.84us / 1.60us | 2.30 / 1.00 | 3.5 MiB / 18.8 MiB |

## Single-threaded: my-redis with 1 worker thread vs Redis

my-redis started with `TOKIO_WORKER_THREADS=1`, so both servers run commands on one thread. Redis numbers are from the summary runs.

| Scenario | my-redis (1 thread) ops/s | Redis ops/s | my-redis / Redis | p99 my-redis / Redis | CPU per op my-redis / Redis | cores my-redis / Redis |
|---|---:|---:|---:|---:|---:|---:|
| PING (raw, pipeline 16) | 5,336,800 | - | - | 282.6us / - | 0.19us / - | 1.00 / - |
| SET (raw, pipeline 16) | 3,024,421 | 3,021,536 | 1.00x | 507.9us / 348.2us | 0.33us / 0.33us | 1.00 / 1.00 |
| GET (raw, pipeline 16) | 3,591,328 | - | - | 421.9us / - | 0.28us / - | 1.00 / - |

## Load generator

CPU used by the benchmark process itself during each summary run. Near 18 cores, the generator is saturated and the server isn't the limit.

| Scenario | Server | load generator cores | server cores | total |
|---|---|---:|---:|---:|
| SET (raw, pipeline 16) | my-redis | 2.33 | 5.57 | 7.90 of 18 |
| SET (raw, pipeline 16) | Redis | 0.82 | 1.00 | 1.82 of 18 |

## Observations

- Largest throughput gap: **SET (raw, pipeline 16)** at 2.31x Redis.
- Closest to (or ahead of) Redis: **SET (raw, pipeline 16)** at 2.31x.
- my-redis uses on average **2.4x** the CPU time per operation that Redis does across the summary scenarios; that is the efficiency gap to close independently of core count.
- **SET (raw, pipeline 16)**: my-redis on 1 thread does 3,024,421 ops/s at 0.33us CPU/op; on all threads 6,982,304 ops/s at 0.80us CPU/op using 5.6 cores. 2.4x the CPU per op for 2.31x the throughput points at contention between threads (the shared db lock, the scheduler) rather than command cost.
- With a single connection (no contention), my-redis needs 1.24us CPU per SET against Redis's 0.72us (1.7x): the per-request cost of parsing, allocating and writing replies.
- Each key with a 16 B value costs 212 B in my-redis and 95 B in Redis (2.2x).
- Pipelining from 1 to 64 multiplies my-redis throughput by 24.3x: the larger this is, the more time goes to per-request round trips and syscalls rather than command execution.

## Profiling next steps

- CPU profile the server under one scenario: `cargo build --release`, start `target/release/my-redis-server`, run `scripts/bench.sh --address 127.0.0.1:6379 --no-redis --quick --only set --duration 30`, and attach a sampling profiler to the server (`samply record -p <pid>` on macOS, `perf record -g -p <pid>` on Linux).
- Compare allocation counts with a heap profiler (`heaptrack`, or Instruments' Allocations on macOS) on the same run; a high *CPU per op* with low *cores* points at per-request allocation or syscalls rather than contention.
- `JSON` with every number is next to this report for tracking changes over time.

