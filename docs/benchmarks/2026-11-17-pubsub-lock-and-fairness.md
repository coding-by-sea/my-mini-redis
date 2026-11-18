# Pub/sub lock and fairness (Linux)

This doc works through the three items left after [direct subscriber writes](2026-11-17-pubsub-direct-writes.md): measure many channels and SET mixed with PUBLISH, take pub/sub off the db lock, and merge publishers' batches into fewer writes.

| | |
|---|---|
| Machine | Linux aarch64 (Docker Desktop VM, kernel 7.0.14-linuxkit), 18 CPUs |
| Redis | 8.0.2, fresh process per run, in the same session as the control |
| Load | `benches/load.rs`: 4 publishers, pipeline 16, 64 B payload; 50 pipelined SET connections in the mixed scenario |
| Before | PR #8's code (`pubsub-direct-writes`), measured with this branch's benchmark |
| Full reports | [before](2026-11-17-lock-fairness-before-linux.md), [after](2026-11-17-lock-fairness-linux.md) |

## Result

Each ratio is against the Redis run from the same session (single runs; the noisier scenarios were checked with interleaved repetitions):

| Scenario | before ops/s | after ops/s | before / Redis | after / Redis | CPU per op, before / after / Redis |
|---|---:|---:|---:|---:|---:|
| **PUBLISH during SET** (publishes/s) | **18,496** | **100,253** | **0.14x** | **0.77x** | |
| SET during PUBLISH | 951,069 | 1,088,701 | 0.59x | **0.67x** | 9.84 / 7.92 / 0.61 us |
| PUBLISH to 50 subscribers | 253,245 | 253,110 | 0.89x | 0.90x | 10.57 / 10.65 / 3.55 us |
| 100 subscribers on 10 channels | 307,674 | 305,581 | 1.09x | 1.09x | 10.04 / 10.18 / 3.56 us |
| 100 subscribers on 100 channels | 816,362 | 799,485 | 1.32x | 1.32x | 2.79 / 2.87 / 1.66 us |
| PING (pipeline 16) | 8,878,256 | 9,226,954 | 1.79x | 1.89x | 0.35 / 0.37 / 0.20 us |
| SET (pipeline 16) | 1,069,213 | 1,070,198 | 0.36x | 0.35x | 9.40 / 9.36 / 0.32 us |
| GET (pipeline 16) | 2,043,312 | 1,810,157 | 0.59x | 0.61x | 5.13 / 5.66 / 0.34 us |
| PUBLISH, no subscribers (pipeline 16) | 7,060,896 | 6,667,869 | 2.05x | 1.94x | 0.67 / 0.89 / 0.29 us |
| SET via Client (1 in flight) | 586,677 | 585,234 | 1.20x | 1.23x | 4.69 / 4.68 / 2.11 us |
| GET via Client (1 in flight) | 611,349 | 611,092 | 1.22x | 1.27x | 4.35 / 4.35 / 2.08 us |

- **Mixed load.** Under SET load, delivery latency went from p50 2.52 ms, p99 14.81 ms and p99.9 25.95 ms to **557 us, 2.56 ms and 3.60 ms** (Redis: 598 us, 918 us, 1.08 ms).
- **Pipelined GET.** The −11% in this single run is noise. Over 4 interleaved runs each, GET was level (1.944M against 1.948M), with CPU per op 5.15 against 4.76 us.
- **PUBLISH with no subscribers.** Its −6% and 33% more CPU per command are a real cost of yielding after each pipelined batch; see *When to yield*.
- **Tests.** They passed 10 runs out of 10 on Linux.

## New scenarios

- **`--channels N`**: spreads the pub/sub subscribers over N channels, with publishers rotating through them. A new sweep runs 100 subscribers on 1, 10 and 100 channels.
- **`mixed`**: runs 50 pipelined SET connections while 4 publishers fan out to 50 subscribers. It reports SET throughput and pub/sub delivery side by side.

Many channels were already fine before this change (1.10x Redis with 10 channels, 1.33x with 100). The mixed scenario was not:

| Before | my-mini-redis | Redis | mini / Redis |
|---|---:|---:|---:|
| PUBLISH alone | 253k/s | 285k/s | 0.89x |
| PUBLISH during SET | **20k/s**, delivery p99 12.9 ms | 132k/s, 0.88 ms | **0.15x** |
| SET during PUBLISH | 1.00M/s | 1.65M/s | 0.61x |

## Two causes, both fixed

**One lock for keys and pub/sub.** Every PUBLISH had to win the lock that 50 SET connections kept busy, then hold it for the whole fan-out. Keys and the pub/sub registry now have separate locks. Pub/sub keeps a single lock rather than one per channel, so publishes stay totally ordered: every subscriber sees them in the same order, as in Redis.

**Unfair scheduling.** A pipelined client whose next batch has already arrived never makes its task wait on the read, so the task runs batch after batch in one poll, until tokio's budget of about 128 runs out, and socket events go uncollected meanwhile. This is the stall the direct-writes work fixed for publishers, which yield after each batch. SET connections didn't yield, so they crowded the publishers out. Now every connection yields after each batch, as Redis's event loop serves each client once per pass, except after a lone command that wrote to no subscribers (see below).

Each fix only shows its full effect once the other is in. Mixed scenario, my-mini-redis only (2 to 3 runs each):

| | PUBLISH during SET | delivery p99 | SET during PUBLISH |
|---|---:|---:|---:|
| Before (one lock, publishers yield) | 20k/s | 12.9 ms | 1.00M/s |
| Two locks only | 24k/s | 16–31 ms | 1.08M/s |
| One lock, every connection yields | 55k/s | 5.5 ms | 0.80M/s |
| **Both** | **98k/s** | **3.7 ms** | **1.06M/s** |

### When to yield

Yielding has a CPU cost: the task waits for its worker's next pass, which also polls the I/O driver. Four rules were compared, all with two locks (2 runs each, server CPU from `perf stat`):

| Rule | Mixed: PUBLISH during SET | Mixed: delivery p99 | SET via Client: CPU/op | PING: CPU/op | PUBLISH, no subscribers: CPU/op |
|---|---:|---:|---:|---:|---:|
| Only after writing to subscribers (before) | 24k/s | 12.1 ms | 4.23 us | 0.34 us | 0.62 us |
| After every batch | 97k/s | 2.64 ms | 4.73 us | 0.36 us | 0.85 us |
| tokio's cooperative budget, 1 unit per command or subscriber | 107k/s | 7.34 ms | 4.10 us | 0.33 us | 0.70 us |
| Same, 4 units per command | 52k/s | 10.1 ms | 4.11 us | 0.33 us | 0.63 us |
| **After every batch, except a lone command that wrote to nobody** | **98k/s** | **2.62 ms** | **4.02 us** | 0.35 us | 0.89 us |

The last rule was kept. One-request-at-a-time clients were the ones paying for yields they didn't need, because their next read suspends the task anyway. The cooperative budget avoided that cost too, but left 3x the mixed delivery tail. Weighting it more heavily made publishers fare worse, not better, which made it hard to tune. It would also have raised the minimum tokio version to 1.39.

The kept rule has two costs:
- **Pipelined PUBLISH to channels with no subscribers:** about 40% more CPU per command and 6% fewer commands. Each batch now yields while 50 connections contend for the pub/sub lock.
- **Pipelined SET:** p50 batch latency improves (721 -> 590 us) but p99 worsens (1.54 -> 2.65 ms), as the batches of 50 connections are interleaved more evenly.

PING, SET throughput, SET via Client and PUBLISH to subscribers are unchanged or better.

## Merging publishers' batches: tried, not adopted

Redis writes each subscriber once per event-loop pass, so several publishers' messages share one write. Here each publisher writes its own batch, about 2x Redis's writes per delivery. The experiment moved each publisher's yield to *before* its subscriber writes, so other publishers could append to the same outboxes first.

| 2 runs each | deliveries/s | CPU per delivery | writes per delivery | delivery p50 / p99.9 |
|---|---:|---:|---:|---:|
| 50 subscribers | +3.1% | −10% | −14% | unchanged |
| 100 subscribers | +2.6% | −4% | −8% | unchanged |
| 50 subscribers, 1 KiB | **−6.2%** | +12% | +2% | 541 us / 1.82 ms -> 561 us / **5.19 ms** |
| mixed | unchanged | +3% | −43% | **557 us -> 1.11 ms** / 3.70 -> 5.34 ms |

The subscriber writes now waited behind every other task on the worker, which under SET load meant many SET batches. Saving 3% at 64 B doesn't justify doubling delivery latency under mixed load, so the change was dropped. Merging across publishers needs a mechanism that doesn't delay writes behind unrelated work. One option is to flush pending subscribers when a worker is about to go idle (tokio's `on_thread_park`), with a time bound.

## Measuring

The last comparisons used a resumable harness that keeps each run's raw output: the benchmark's JSON and Markdown reports, a `perf stat` CSV of the server, and the exact arguments and binary hash. A separate step parses the JSON, so a parsing bug no longer means re-running benchmarks. An earlier run had lost its delivery counts to one, and another silently used the wrong payload size.

## What is left

- **Merging writes across publishers** (see above): about 2x Redis's writes per delivery remain.
- **SET and GET throughput:** keys are still behind one lock that serializes every SET and GET. SET runs at about 0.36x Redis on 10 cores, while my-mini-redis on one thread beats Redis. Sharding the keyspace is the next step there.
- **Delivery tail under mixed load:** 3.7 ms against Redis's 0.88 ms.
