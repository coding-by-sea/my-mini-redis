# Sharded keys (Linux)

This doc covers the item left in [pub/sub lock and fairness](2026-11-17-pubsub-lock-and-fairness.md): every SET and GET took one lock over all keys. SET ran at 0.35x Redis while using about 10 cores, and pub/sub under SET load waited behind those slow SET batches.

| | |
|---|---|
| Machine | Linux aarch64 (Docker Desktop VM, kernel 7.0.14-linuxkit), 18 CPUs |
| Redis | 8.0.2, fresh process per run, in the same session as the control |
| Before | PR #9's code (`pubsub-lock-and-merging`): keys in one `Mutex<HashMap>` |
| After | keys in a `DashMap` (dashmap 6.2.1) |
| Full reports | quick suite [before](2026-11-17-sharded-keys-quick-before-linux.md) / [after](2026-11-17-sharded-keys-quick-linux.md); sweeps [before](2026-11-17-sharded-keys-sweeps-before-linux.md) / [after](2026-11-17-sharded-keys-sweeps-linux.md) |

## Cause: a lock convoy, not the map

A call-graph profile of the server under pipelined SET (50 connections, pipeline 16) showed where the CPU went:

| Share of server CPU | pipelined SET | mixed scenario |
|---|---:|---:|
| `Session::execute` (inclusive) | 75% | 77% |
| **`futex_wake`**, called from `Session::execute` | **62%** | **60%** |
| `lock_contended` (spinning, waiting) | 5.3% | 6.8% |
| `sendto` | 18% | 14% |
| `HashMap::insert` | 0.7% | 0.8% |

Aggregating the raw stacks put 61% of all samples in `futex_wake`, called from `Session::execute`: dropping the key map's `MutexGuard`. With about 50 tasks on 18 workers contending, almost every unlock found a sleeping waiter and had to wake it through the kernel. The SET itself cost under 1%. On one thread, without contention, the same SET costs 0.48 us of CPU; here it cost 9.4 us.

## Change

Keys live in a `DashMap`: a fixed number of shards (4 per CPU, rounded to a power of two, so 128 on this machine), each a hash map behind its own read-write lock, chosen by the key's hash. SET and GET on different keys rarely meet on a lock, and GETs share a shard's read lock.

- **No ordering change.** SET and GET touch one key, so per-key ordering is unchanged and no guarantee is lost.
- **Multi-key commands.** Future commands spanning several keys (MSET, transactions) will need to lock their shards in a fixed order, or use DashMap's entry API per key, to stay atomic and deadlock-free.
- **Dependency.** dashmap 6.2.1 (MSRV 1.65) and its dependencies (hashbrown, lock_api, parking_lot_core, crossbeam-utils, once_cell) build and pass the tests at their minimal versions and on Rust 1.86. The newest dashmap, 7.0.0, is a release candidate, so the stable 6.x was used.
- **Memory.** Per key (16 B values) it went from 214 B to 213 B, so DashMap adds nothing measurable.

## Result

Single runs in the same session as Redis. Scenarios that moved little were confirmed with interleaved runs (below).

| Scenario | before | after | before / Redis | after / Redis | CPU per op, before / after / Redis |
|---|---:|---:|---:|---:|---:|
| **SET** (pipeline 16) | 1,079,291 | **6,982,304** | 0.35x | **2.31x** | 9.15 / 0.80 / 0.33 us |
| **GET** (pipeline 16) | 2,088,531 | **7,733,811** | 0.60x | **2.24x** | 4.84 / 0.60 / 0.29 us |
| **SET during PUBLISH** | 1,077,069 | **3,246,589** | 0.66x | **1.94x** | 8.00 / 1.56 / 0.60 us |
| **PUBLISH during SET** (publishes/s) | 99,731 | **197,379** | 0.75x | **1.48x** | |
| PING (pipeline 16) | 9,095,069 | 9,201,283 | 1.81x | 1.90x | 0.38 / 0.38 / 0.21 us |
| PUBLISH, no subscribers | 6,624,605 | 6,967,850 | 1.91x | 2.03x | 0.92 / 0.80 / 0.29 us |
| SET via Client (1 in flight) | 593,396 | 601,665 | 1.21x | 1.29x | 4.61 / 4.49 / 2.14 us |
| GET via Client (1 in flight) | 614,290 | 565,812 | 1.22x | 1.16x | 4.34 / 4.73 / 2.06 us |
| PUBLISH to 50 subscribers | 248,176 | 253,344 | 0.89x | 0.90x | 10.87 / 10.59 / 3.54 us |

- **Mixed delivery latency.** Under SET load it went from p50 557 us, p99 2.56 ms and p99.9 3.57 ms to **352 us, 885 us and 1.15 ms**, on par with Redis's 590 us, 868 us and 1.02 ms. With pipelined SETs from 50 connections, my-mini-redis now outperforms Redis on both SET and PUBLISH.
- **GET via Client.** The −8% in this single run is noise. Over 4 interleaved runs each it was level: 598.5k against 597.4k requests/s, at 4.07 against 4.16 us of CPU.
- **PUBLISH alone.** It was level over 4 interleaved runs: 246.6k against 248.1k.

SET sweeps, against Redis:

| Sweep point | before / Redis | after / Redis |
|---|---:|---:|
| pipeline 1 | 1.25x | 1.29x |
| pipeline 4 | 0.61x | 1.51x |
| pipeline 64 | 0.34x | **3.72x** (15.1M SET/s) |
| 1 connection | 0.97x | 0.96x |
| 10 connections | 0.81x | 1.45x |
| 200 connections | 0.37x | **2.95x** |
| 1 KiB values | 0.45x | 2.44x |
| 64 KiB values | 3.33x | 4.96x |

With one connection there is nothing to contend for, so nothing changes. The gain grows with concurrency.

Tests passed 10 runs out of 10 on Linux.

## What is left

- **Merging writes across publishers** (from the earlier work): each subscriber still gets about 2x Redis's writes per delivery.
- **CPU per operation.** It is still 2 to 4x Redis's across scenarios (SET 0.80 us against 0.33 us), now spread across cores rather than spent on one lock. my-mini-redis wins on throughput by using more cores.
