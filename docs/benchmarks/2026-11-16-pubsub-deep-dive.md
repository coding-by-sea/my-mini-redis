# Pub/sub performance deep dive (Linux)

Why my-mini-redis delivered pub/sub messages at a fraction of Redis's rate, what changed, and what is left.

| | |
|---|---|
| Machine | Linux aarch64 (Docker Desktop VM, kernel 7.0.14-linuxkit), 18 CPUs |
| Redis | 8.0.2 (jemalloc), fresh process per run, measured in the same session as the control |
| Load | `benches/load.rs` pub/sub scenario: 4 publishers, pipeline 16, one channel, 64 B payload unless noted |
| Before | `a6a9aa8` (main) |
| After | this change: per-subscriber outbox with batched wakeups, and growing reads |
| Full reports | [before](2026-11-16-outbox-before-linux.md), [after](2026-11-16-outbox-linux.md) (`--quick`, all scenarios) |

## Result

| Fan-out | before ops/s | after ops/s | Redis ops/s | before / Redis | after / Redis | CPU per op before / after / Redis |
|---|---:|---:|---:|---:|---:|---:|
| 1 subscriber | 1,034,730 | 1,314,307 | 998,899 | 1.04x | **1.32x** | 1.92 / 1.12 / 0.54 us |
| 10 subscribers | 310,387 | 708,038 | 740,515 | 0.44x | **0.96x** | 11.63 / 3.08 / 1.27 us |
| 50 subscribers | 61,386 | 220,634 | 286,960 | 0.21x | **0.77x** | 91.21 / 11.77 / 3.48 us |
| 100 subscribers | 30,070 | 133,821 | 145,085 | 0.21x | **0.92x** | 223.81 / 21.89 / 6.91 us |
| 50 subscribers, 1 KiB | not measured | 107,027 | 133,683 | | **0.80x** | — / 23.09 / 7.50 us |
| 100 subscribers, 1 KiB | not measured | 62,851 | 60,890 | | **1.03x** | |

ops/s are publishes; deliveries are publishes x subscribers. Redis's numbers move a few percent between runs, so each ratio uses the Redis run from the same session.

Delivery latency at 50 subscribers: p50 647 us -> 332 us (Redis 324 us), p99 2.59 ms -> 762 us (Redis 442 us). The tail is still behind Redis; see *What is left*.

The other scenarios didn't regress (same session, before -> after): PING 7.20M -> 8.89M, SET 1.00M -> 1.08M, GET 1.99M -> 2.01M, PUBLISH without subscribers 6.42M -> 6.64M, Client SET/GET unchanged. Memory per idle connection 11.1 KiB -> 9.6 KiB. The tests passed 10 runs out of 10.

## Finding 1: a throughput ceiling caused by small writes

Before, my-mini-redis stalled at about **3M deliveries/s whatever the fan-out** (3.10M at 10 subscribers, 3.08M at 50, 3.01M at 100), using 3.6 to 6.7 cores, while Redis reached 14M on one core. The load generator used at most 10 of 18 cores, so the server was the limit.

Syscalls per delivery, counted with `perf stat` tracepoints over 5 s at 50 subscribers:

| | deliveries/s | server cores | CPU per delivery | writes per delivery | futex per delivery | context switches per delivery |
|---|---:|---:|---:|---:|---:|---:|
| my-mini-redis before | 3.12M | 5.6 | 1.79 us | 0.320 (`sendto`) | 0.035 | 0.016 |
| my-mini-redis, outbox | 10.32M | 2.8 | 0.27 us | 0.039 | 0.010 | 0.005 |
| my-mini-redis, outbox + growing reads | 11.21M | 2.6 | 0.23 us | | | |
| Redis | 14.18M | 1.0 | 0.07 us | 0.017 (`write`) | 0 | 0 |

Before, each write carried about 3 messages, against about 60 for Redis. The CPU profile (`perf record`, frame pointers) agreed:

| Before, 50 subscribers | inclusive share of server CPU |
|---|---:|
| subscriber flush -> `sendto` -> loopback TCP | 55% |
| `subscription::publish` (under the db lock) | 13.8% |
| of which tokio `mpsc` send | 9.7% |
| of which waking idle workers (condvar -> futex) | 4.3% |
| shared `Bytes` refcount clone/drop | ~3.3% |
| db mutex contention (`lock_contended`) | 0.9% |

The global db lock was *not* the bottleneck. The cause was the delivery model. Each message was pushed onto the subscriber's `mpsc`, which woke the subscriber's task. With 18 workers, an idle worker picked it up right away and wrote the 2 to 3 messages queued so far. Redis runs every readable client's commands first and writes each pending client once per event-loop pass.

**Fix:** each connection has an outbox of encoded bytes. `PUBLISH` encodes the message once and appends it to every subscriber's outbox. A publisher only notes a subscriber when it makes that subscriber's outbox non-empty, and wakes the noted subscribers after running its whole batch of pipelined requests, outside the db lock and before flushing its own replies. A subscriber swaps its outbox for a spare buffer and sends it with one write, so steady state allocates nothing.

## Finding 2: worker count decided the batch size

With the outbox in place, fewer tokio workers still meant larger writes (50 subscribers, deliveries/s and CPU per delivery):

| `TOKIO_WORKER_THREADS` | 64 B | 1 KiB |
|---|---:|---:|
| 2 | 11.75M, 0.12 us | 7.33M, 0.22 us |
| 4 | 12.02M, 0.20 us | 5.04M, 0.41 us |
| 8 | 10.25M, 0.24 us | 2.99M, 1.06 us |
| 18 (default) | 10.23M, 0.24 us | 2.99M, 1.08 us |
| Redis | 14.18M, 0.07 us | 6.44M, 0.16 us |

At 1 KiB, my-mini-redis made 0.167 writes per delivery (about 6 messages per write), while Redis made 0.0013. Limiting threads isn't a fix, because SET and GET need the cores. The reason batches stayed small was the read buffer. `Connection` read at most 4 KiB at a time, which holds only about 4 one-KiB `PUBLISH` commands. So the publisher ran 4 commands, woke its subscribers and flushed, then read again.

**Fix:** reads grow from 4 KiB up to 64 KiB (doubling) while each read fills the space it was given, and drop back to 4 KiB otherwise. Redis reads up to 16 KiB at a time. Idle connections keep the 4 KiB buffer. At 1 KiB and 50 subscribers this took 2.99M to 5.30M deliveries/s and 1.08 us to 0.42 us per delivery.

## Is the message serialized once?

Yes, before and after: `encode_message` runs once per `PUBLISH`. What differs is the per-subscriber step.

| | per subscriber at publish time | per subscriber at write time |
|---|---|---|
| before | `Bytes` clone (atomic increment on one shared refcount) + `mpsc` push | copy into that connection's `BufWriter`, refcount decrement |
| after | copy into the subscriber's outbox | none: the outbox is written as is (through the `BufWriter`, which passes large buffers straight to the socket) |

Both versions copy the bytes once per subscriber; the kernel copies again into the socket either way. After the change, the time inside `publish` itself splits into about 39% for the per-subscriber outbox lock, 17% for cache misses on the outbox (it was last touched on the subscriber's core), and 17% at the `memcpy` call (libc `memcpy` is about 6% of total CPU). Sharing handles and sending with `writev` would keep the lock and the cache-line transfer, and add contended increments and decrements on one shared refcount, which cost about 3% before. It is only worth trying for large payloads, after measuring a 16 to 64 KiB sweep.

## Experiments that didn't help

| Experiment | Result (deliveries/s) | Kept |
|---|---|---|
| `yield_now()` before draining, with the old `mpsc` | 10 subs 3.10M -> 3.54M, 50 subs 3.12M -> 2.61M, 100 subs 3.01M -> 2.24M | no |
| `yield_now()` before draining, with the outbox | 50 subs: 10.32M -> 10.28M at 64 B, 2.99M -> 2.99M at 1 KiB | no |

## What is left

At 50 subscribers my-mini-redis still needs about 3x Redis's CPU per delivery (0.23 us against 0.07 us), and its p99 latency is higher. The profile after the change:

| After, 50 subscribers | inclusive share of server CPU |
|---|---:|
| subscriber flush -> `sendto` | 45% |
| `subscription::publish` | 18% |
| futex wakes (`try_to_wake_up`), mostly tokio unparking idle workers | 13% |
| db mutex contention | 1.6% |

Proposed next steps, by expected gain:

1. **Publishers write to subscriber sockets directly.** After its batch, a publisher would try a non-blocking write of each pending subscriber's outbox, and only wake the subscriber when the socket is full. That removes most of the remaining futex and context-switch cost and keeps each outbox on one core, as Redis's single thread does. It needs the write half of a subscribed connection shared under a lock, so replies and messages keep their order, and it adds the write syscalls to the publisher's latency.
2. **Take pub/sub off the db lock** (its own lock, or sharded by channel). Pure pub/sub barely contends today, but the fan-out runs under the lock that every `SET` and `GET` takes. Separately, that lock serializes all commands: SET is at 0.36x Redis on 10 cores, while my-mini-redis on one thread beats Redis.
3. **Benchmark many channels, and SET mixed with PUBLISH.** The current scenario uses one channel, so it can't show lock effects.
4. **Share payloads instead of copying** above a size threshold (see above), if a large-payload sweep shows the copy matters.
5. Minor: no `format!` in `encode_message`, and fewer `Vec`s per command in `Session::execute` (each under 1%).

At 16 KiB payloads, Redis disconnected all 50 benchmark subscribers for going over its 32 MB output-buffer limit, so that point isn't a valid comparison. my-mini-redis has the same limit but kept all 50.

## Reproducing

```bash
docker run -d --name mmr-linux --privileged -v "$PWD":/work -v mmr-target:/work/target -w /work rust:1 sleep infinity
docker exec mmr-linux bash -c 'apt-get update && apt-get install -y redis-server linux-perf'
docker exec mmr-linux cargo bench --bench load -- --quick --only pubsub --subscribers 50 --duration 5
# Syscalls per delivery: start target/release/my-mini-redis-server, run the benchmark with --address, and
# perf stat -p <pid> -e syscalls:sys_enter_sendto,syscalls:sys_enter_futex,task-clock -- sleep 5
```
