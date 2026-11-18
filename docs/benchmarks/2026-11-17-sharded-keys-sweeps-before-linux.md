# Load benchmark: my-redis vs Redis

| | |
|---|---|
| Date (Unix time) | 1794967073 |
| Commit | unknown |
| Machine | linux aarch64, 18 CPUs |
| my-redis | fresh process per run |
| Redis | 8.0.2, fresh process per run |
| Load | 50 connections, pipeline 16, 64 B payload, 10000 keys, 3s measured after 1s warmup (sweeps: 2s after 0.5s) |

**Reading the numbers.** Both servers and the load generator share this machine. Redis runs commands on one thread; my-redis uses every core, so compare *CPU per op* for efficiency and *ops/s* for capacity. *Cores* is server CPU time divided by wall time. If the load generator's cores approach the machine's CPU count, it is the bottleneck and ops/s understates both servers. Latency for raw scenarios is per pipelined batch.

## Summary

| Scenario | my-redis ops/s | Redis ops/s | my-redis / Redis | p50 my-redis / Redis | p99 my-redis / Redis | CPU per op my-redis / Redis | cores my-redis / Redis | peak RSS my-redis / Redis |
|---|---:|---:|---:|---:|---:|---:|---:|---:|
| SET (raw, pipeline 16) | 1,079,291 | 3,066,859 | 0.35x | 598.0us / 274.4us | 2.65ms / 340.0us | 9.15us / 0.33us | 9.88 / 1.00 | 4.7 MiB / 19.2 MiB |

### Latency detail

| Scenario | Server | p50 | p90 | p99 | p99.9 | max |
|---|---|---:|---:|---:|---:|---:|
| SET (raw, pipeline 16) | my-redis | 598.0us | 1.46ms | 2.65ms | 3.74ms | 6.58ms |
| SET (raw, pipeline 16) | Redis | 274.4us | 311.3us | 340.0us | 393.2us | 531.7us |

### Pub/sub delivery

Publishes are counted at the publishers; deliveries at the subscribers (publishes x subscribers when nothing is lost). *RSS growth* is how much the server's memory grew during the run, mostly queued messages.

| Run | Server | publishes/s | deliveries/s | delivery p50 | p99 | p99.9 | max | disconnected | peak RSS | RSS growth |
|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| 1 subscriber | my-redis | 1,359,192 | 1,359,194 | 46.1us | 81.9us | 103.4us | 2.07ms | 0 of 1 | 2.8 MiB | +228.0 KiB |
| 1 subscriber | Redis | 1,009,888 | 1,009,888 | 53.2us | 71.7us | 86.0us | 202.2us | 0 of 1 | 18.3 MiB | +204.0 KiB |
| 10 subscribers | my-redis | 800,056 | 8,000,560 | 89.1us | 208.9us | 258.0us | 434.8us | 0 of 10 | 3.8 MiB | +1.3 MiB |
| 10 subscribers | Redis | 740,152 | 7,401,520 | 100.4us | 159.7us | 178.2us | 256.6us | 0 of 10 | 18.4 MiB | +348.0 KiB |
| 100 subscribers | my-redis | 144,096 | 14,409,600 | 507.9us | 1.26ms | 1.59ms | 2.59ms | 0 of 100 | 13.0 MiB | +10.4 MiB |
| 100 subscribers | Redis | 145,248 | 14,524,800 | 630.8us | 827.4us | 892.9us | 1.08ms | 0 of 100 | 19.9 MiB | +1.8 MiB |
| 1 channel | my-redis | 147,080 | 14,708,000 | 503.8us | 1.26ms | 1.59ms | 2.25ms | 0 of 100 | 13.3 MiB | +10.7 MiB |
| 1 channel | Redis | 144,320 | 14,432,000 | 639.0us | 835.6us | 901.1us | 3.29ms | 0 of 100 | 19.9 MiB | +1.8 MiB |
| 10 channels | my-redis | 306,160 | 3,061,645 | 239.6us | 520.2us | 639.0us | 980.1us | 0 of 100 | 4.2 MiB | +1.7 MiB |
| 10 channels | Redis | 283,000 | 2,830,000 | 270.3us | 368.6us | 397.3us | 485.6us | 0 of 100 | 18.8 MiB | +692.0 KiB |
| 100 channels | my-redis | 807,184 | 807,184 | 92.2us | 176.1us | 210.9us | 349.2us | 0 of 100 | 3.5 MiB | +920.0 KiB |
| 100 channels | Redis | 607,032 | 607,032 | 100.4us | 157.7us | 182.3us | 261.1us | 0 of 100 | 18.8 MiB | +700.0 KiB |

## Memory

Resident memory (RSS) of each server process, read from the OS after letting allocations settle. Per-connection and per-key numbers are the RSS increase divided by the count, so they include allocator overhead. Freed memory is often kept by the allocator, so RSS rarely shrinks.

| Measure | my-redis | Redis | my-redis / Redis |
|---|---:|---:|---:|
| Idle RSS | 2.7 MiB | 18.1 MiB | 0.15x |
| Per idle connection (1000 open) | 5.6 KiB | 5.4 KiB | 1.03x |
| Per key, 16 B value (100000 keys) | 202 B | 95 B | 2.12x |
|   overhead per key beyond 31 B of key + value | 171 B | 64 B | 2.66x |
| Per key, 1.0 KiB value (20000 keys) | 1.5 KiB | 1.3 KiB | 1.16x |
|   overhead per key beyond 1.0 KiB of key + value | 492 B | 286 B | 1.72x |

## Pipeline depth: SET, 50 connections

| pipeline | my-redis ops/s | Redis ops/s | my-redis / Redis | p99 my-redis / Redis | CPU per op my-redis / Redis | cores my-redis / Redis | peak RSS my-redis / Redis |
|---|---:|---:|---:|---:|---:|---:|---:|
| 1 | 621,078 | 495,094 | 1.25x | 157.7us / 159.7us | 4.43us / 2.01us | 2.75 / 0.99 | 3.2 MiB / 18.6 MiB |
| 4 | 934,076 | 1,536,464 | 0.61x | 778.2us / 190.5us | 7.96us / 0.65us | 7.43 / 1.00 | 3.8 MiB / 18.7 MiB |
| 16 | 1,083,072 | 2,980,256 | 0.36x | 2.69ms / 356.4us | 9.17us / 0.34us | 9.94 / 1.00 | 4.7 MiB / 19.2 MiB |
| 64 | 1,322,048 | 3,882,560 | 0.34x | 8.00ms / 1.02ms | 7.36us / 0.26us | 9.73 / 1.00 | 6.3 MiB / 19.6 MiB |

## Concurrent connections: SET, pipeline 16

| connections | my-redis ops/s | Redis ops/s | my-redis / Redis | p99 my-redis / Redis | CPU per op my-redis / Redis | cores my-redis / Redis | peak RSS my-redis / Redis |
|---|---:|---:|---:|---:|---:|---:|---:|
| 1 | 492,424 | 506,144 | 0.97x | 44.0us / 41.5us | 1.22us / 0.72us | 0.60 / 0.37 | 2.8 MiB / 18.3 MiB |
| 10 | 2,229,208 | 2,736,128 | 0.81x | 184.3us / 85.0us | 1.36us / 0.37us | 3.03 / 1.00 | 3.5 MiB / 18.5 MiB |
| 50 | 1,072,896 | 3,002,016 | 0.36x | 2.72ms / 352.3us | 9.28us / 0.33us | 9.96 / 1.00 | 4.8 MiB / 19.2 MiB |
| 200 | 1,080,328 | 2,913,584 | 0.37x | 11.14ms / 2.02ms | 9.40us / 0.34us | 10.15 / 1.00 | 6.6 MiB / 20.5 MiB |

## Payload size: SET, 50 connections, pipeline 16

| payload | my-redis ops/s | Redis ops/s | my-redis / Redis | p99 my-redis / Redis | CPU per op my-redis / Redis | cores my-redis / Redis | peak RSS my-redis / Redis |
|---|---:|---:|---:|---:|---:|---:|---:|
| 16 B | 1,057,744 | 3,179,512 | 0.33x | 2.75ms / 331.8us | 9.34us / 0.31us | 9.88 / 1.00 | 4.4 MiB / 19.1 MiB |
| 1.0 KiB | 1,016,288 | 2,247,936 | 0.45x | 2.62ms / 462.8us | 9.74us / 0.44us | 9.90 / 0.99 | 12.6 MiB / 27.2 MiB |
| 16.0 KiB | 527,128 | 343,136 | 1.54x | 3.83ms / 2.56ms | 16.42us / 2.93us | 8.66 / 1.01 | 150.2 MiB / 162.3 MiB |
| 64.0 KiB | 342,384 | 102,928 | 3.33x | 4.65ms / 9.70ms | 25.72us / 9.71us | 8.80 / 1.00 | 315.4 MiB / 328.9 MiB |

## Pub/sub fan-out: 4 publishers, pipeline 16

| fan-out | my-redis ops/s | Redis ops/s | my-redis / Redis | p99 my-redis / Redis | CPU per op my-redis / Redis | cores my-redis / Redis | peak RSS my-redis / Redis |
|---|---:|---:|---:|---:|---:|---:|---:|
| 1 subscriber | 1,359,192 | 1,009,888 | 1.35x | 80.9us / 84.0us | 1.15us / 0.53us | 1.56 / 0.53 | 2.8 MiB / 18.3 MiB |
| 10 subscribers | 800,056 | 740,152 | 1.08x | 178.2us / 137.2us | 2.71us / 1.28us | 2.16 / 0.94 | 3.8 MiB / 18.4 MiB |
| 100 subscribers | 144,096 | 145,248 | 0.99x | 1.01ms / 524.3us | 19.36us / 6.92us | 2.79 / 1.00 | 13.0 MiB / 19.9 MiB |

## Pub/sub channels: 100 subscribers, 4 publishers, pipeline 16

| channels | my-redis ops/s | Redis ops/s | my-redis / Redis | p99 my-redis / Redis | CPU per op my-redis / Redis | cores my-redis / Redis | peak RSS my-redis / Redis |
|---|---:|---:|---:|---:|---:|---:|---:|
| 1 channel | 147,080 | 144,320 | 1.02x | 1.02ms / 524.3us | 19.04us / 6.92us | 2.80 / 1.00 | 13.3 MiB / 19.9 MiB |
| 10 channels | 306,160 | 283,000 | 1.08x | 438.3us / 340.0us | 10.13us / 3.53us | 3.10 / 1.00 | 4.2 MiB / 18.8 MiB |
| 100 channels | 807,184 | 607,032 | 1.33x | 153.6us / 157.7us | 2.86us / 1.65us | 2.31 / 1.00 | 3.5 MiB / 18.8 MiB |

## Single-threaded: my-redis with 1 worker thread vs Redis

my-redis started with `TOKIO_WORKER_THREADS=1`, so both servers run commands on one thread. Redis numbers are from the summary runs.

| Scenario | my-redis (1 thread) ops/s | Redis ops/s | my-redis / Redis | p99 my-redis / Redis | CPU per op my-redis / Redis | cores my-redis / Redis |
|---|---:|---:|---:|---:|---:|---:|
| PING (raw, pipeline 16) | 5,340,688 | - | - | 282.6us / - | 0.19us / - | 1.00 / - |
| SET (raw, pipeline 16) | 3,055,712 | 3,066,859 | 1.00x | 507.9us / 340.0us | 0.33us / 0.33us | 1.00 / 1.00 |
| GET (raw, pipeline 16) | 3,784,357 | - | - | 405.5us / - | 0.26us / - | 1.00 / - |

## Load generator

CPU used by the benchmark process itself during each summary run. Near 18 cores, the generator is saturated and the server isn't the limit.

| Scenario | Server | load generator cores | server cores | total |
|---|---|---:|---:|---:|
| SET (raw, pipeline 16) | my-redis | 0.76 | 9.88 | 10.64 of 18 |
| SET (raw, pipeline 16) | Redis | 0.81 | 1.00 | 1.81 of 18 |

## Observations

- Largest throughput gap: **SET (raw, pipeline 16)** at 0.35x Redis.
- Closest to (or ahead of) Redis: **SET (raw, pipeline 16)** at 0.35x.
- my-redis uses on average **28.1x** the CPU time per operation that Redis does across the summary scenarios; that is the efficiency gap to close independently of core count.
- **SET (raw, pipeline 16)**: my-redis on 1 thread does 3,055,712 ops/s at 0.33us CPU/op; on all threads 1,079,291 ops/s at 9.15us CPU/op using 9.9 cores. 27.9x the CPU per op for 0.35x the throughput points at contention between threads (the shared db lock, the scheduler) rather than command cost.
- With a single connection (no contention), my-redis needs 1.22us CPU per SET against Redis's 0.72us (1.7x): the per-request cost of parsing, allocating and writing replies.
- Each key with a 16 B value costs 202 B in my-redis and 95 B in Redis (2.1x).
- Pipelining from 1 to 64 multiplies my-redis throughput by 2.1x: the larger this is, the more time goes to per-request round trips and syscalls rather than command execution.

## Profiling next steps

- CPU profile the server under one scenario: `cargo build --release`, start `target/release/my-redis-server`, run `scripts/bench.sh --address 127.0.0.1:6379 --no-redis --quick --only set --duration 30`, and attach a sampling profiler to the server (`samply record -p <pid>` on macOS, `perf record -g -p <pid>` on Linux).
- Compare allocation counts with a heap profiler (`heaptrack`, or Instruments' Allocations on macOS) on the same run; a high *CPU per op* with low *cores* points at per-request allocation or syscalls rather than contention.
- `JSON` with every number is next to this report for tracking changes over time.

