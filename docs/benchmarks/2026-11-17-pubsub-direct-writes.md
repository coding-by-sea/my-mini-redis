# Direct subscriber writes (Linux)

Next step 1 of the [pub/sub deep dive](2026-11-16-pubsub-deep-dive.md): the connection that runs PUBLISH writes its subscribers' sockets itself, as Redis's event loop does, instead of waking each subscriber's task. This records what was built, the two problems benchmarking exposed, and the results.

| | |
|---|---|
| Machine | Linux aarch64 (Docker Desktop VM, kernel 7.0.14-linuxkit), 18 CPUs |
| Redis | 8.0.2, fresh process per run, in the same session as the control |
| Load | `benches/load.rs` pub/sub scenario: 4 publishers, pipeline 16, one channel, 64 B payload unless noted |
| Before | `main` at `7d09dd3` (per-subscriber outboxes with batched wakeups, PR #6) |
| Full reports | [before](2026-11-17-direct-writes-before-linux.md), [after](2026-11-17-direct-writes-linux.md) (`--quick`, all scenarios) |

## Result

Publishes per second, each ratio against the Redis run from the same session:

| Fan-out | `main` | this change | `main` / Redis | this change / Redis | CPU per publish, `main` / this change / Redis |
|---|---:|---:|---:|---:|---:|
| 1 subscriber | 1,302,634 | 1,342,864 | 1.27x | 1.27x | 1.12 / 1.15 / 0.56 us |
| 10 subscribers | 726,474 | 807,146 | 0.99x | **1.09x** | 2.99 / 2.66 / 1.28 us |
| 50 subscribers | 214,464 | 254,522 | 0.77x | **0.91x** | 12.15 / 10.45 / 3.57 us |
| 100 subscribers | 130,890 | 143,430 | 0.90x | **0.98x** | 22.51 / 19.54 / 6.84 us |
| 500 subscribers | 35,165 | 34,202 | 1.24x | 1.20x | 88.38 / 77.66 / 35.3 us |
| 50 subscribers, 1 KiB | 108,560 | 109,504 | 0.83x | 0.82x | 23.08 / 23.41 / 7.56 us |

- **Latency.** At 50 subscribers, delivery p50 went from 340 to 291 us (Redis 328 us), p99 from 778 to 705 us (Redis 461 us) and p99.9 from 967 to 893 us (Redis 541 us). The tail is still behind Redis.
- **Large fan-out and payloads.** 500 subscribers is level with `main` within this size's ~4% run-to-run noise, at 12% less CPU per publish. 1 KiB payloads gain nothing.
- **Other scenarios.** No regressions: PING 1.75x -> 1.80x Redis, SET 0.36x -> 0.35x, GET 0.56x -> 0.57x, SET and GET via Client 1.25x/1.26x -> 1.24x/1.26x.
- **Memory.** Per idle connection went from 9.6 KiB to 5.6 KiB, because the server no longer keeps a `BufWriter` per connection.
- **Tests.** They passed 10 runs out of 10 on Linux (76 per run).
- **Report headers.** The "before" report shows commit "unknown" because it was built from an archive of `main` (`7d09dd3`). The "after" report was built from `958c7b7` with only these docs uncommitted.

## Design

- **One FIFO per connection.** Everything sent to a client, replies and published messages, is appended to its outbox (`src/outbox.rs`). Nobody writes to a socket directly.
- **One writer per socket.** Several tasks can now write to one socket, so a writer token keeps Redis's rule that a socket has a single writer. Only the token holder writes, from the head of the FIFO, and a partial write leaves the rest at the head, so frames from different writers never interleave. Others only append. The holder sends what they appended before releasing the token, and it only releases it with the FIFO empty, checked under the lock, so nothing is left without a writer.
- **No lock during a syscall.** The holder moves the queued bytes into a buffer only it uses before writing. Publishers append while holding the db lock, so they must never wait behind a `sendto`.
- **Other writers never wait.** When the socket is full, or more arrived during its turn, a writer that isn't the connection's own task hands the token to that task, which waits until the socket is writable.
- **Order.** SUBSCRIBE, UNSUBSCRIBE and RESET append their acknowledgments while still holding the db lock, and messages are appended under the db lock, so a channel's messages stay between its subscribe and unsubscribe acknowledgments.
- **Inline limit.** A publisher writes up to 512 subscribers itself per batch; beyond that, their tasks are woken as before.
- **Reading.** The socket is split: the request reader owns the read half, and the outbox shares the write half.

Two tests in `src/server.rs` check this under concurrency: whole, ordered frames while 4 publishers and the subscriber's own replies write one socket that fills up, and acknowledgment order while a publisher publishes without pause. Injecting a bug proves they catch it: replies written straight to the socket, bypassing the FIFO, fail the first test in 20 of 20 runs (an earlier version of the test, whose socket never filled up, failed 0 of 20); a subscribe acknowledgment appended after releasing the db lock fails the second in 19 of 20.

## Problem 1: stalls of 100 ms to 4 s

The first version was 25% *slower* than `main` at 100 subscribers, although its p50 latency was 2.6x lower. The 0.1% tail explained it: publish p99.9 of 134 to 185 ms and maxima up to 4.5 s, against 1.5 ms on `main`. In a closed-loop benchmark, that tail is where the throughput goes.

How it was found:

| Step | Measurement | Finding |
|---|---|---|
| 1 | Timers on every batch, lock wait and socket wait (log at 20 ms) | None fired: no server code waited that long |
| 2 | TCP retransmit and drop counters (`nstat`) | Zero retransmits in a run with a 3 s stall: not packet loss |
| 3 | Socket receive queues sampled every 5 ms (`ss`) | The server left publishers' requests unread for up to 1.8 s; `main` never did |
| 4 | Thread states during a stall (`/proc`) | One worker running, 17 parked on their own condvars, none in `epoll_wait` |
| 5 | Per-connection phase watchdog | Stalled publishers were waiting in `select!` with unread requests |
| 6 | tokio runtime metrics at the stall | All run queues empty: the publishers were never woken; the I/O driver processed only ~500 events/s |
| 7 | `event_interval(1)`, no LIFO slot, `global_queue_interval(1)` | No effect |
| 8 | Fewer workers | 2 workers: no stalls |

Cause: when a publisher's next batch had already arrived, its reads completed without yielding, so the task served batch after batch in a single poll, until tokio's cooperative budget of about 128 operations ran out. With the subscriber writes in each batch, that poll lasted about 50 ms (on `main`, about 5 ms). Meanwhile the other workers were parked and none was in `epoll_wait`, so nobody collected the other publishers' socket events. Per-batch timers can't see this, because each batch is short.

Fix: after a batch that wrote to other clients, the connection yields, so each client gets one batch at a time, like Redis's event loop. Batches without such writes (SET, GET, PING) don't yield.

A related bound came first: a writer used to keep writing while publishers kept appending, a loop that never awaits. Each turn with the token is now limited to what the writer took.

## Problem 2: a second `recvfrom` per request

With one request in flight per connection, SET was about 5% slower than `main`, at 14% more server CPU per request. Counting syscalls per request showed 1.8 to 2.0 `recvfrom` against 1.0. The first version read with `readable()` and `try_read_buf` on a shared socket. tokio's `AsyncRead` for TCP assumes the socket is drained after a short read and clears its readiness. `try_read_buf` doesn't, so each wait first made another read that returned EWOULDBLOCK. Splitting the socket, so the reader can use `read_buf` on the read half, brought it back to 1.0 per request, with CPU per request level with `main`.

## Inline limit

Measured after the stall fix (deliveries/s, mean of 2 runs; 500-subscriber runs vary about 4%):

| Inline limit | 100 subscribers | 500 subscribers | 500: delivery p99 / p99.9 | 500: server cores |
|---|---:|---:|---:|---:|
| `main` (no inline writes) | 13.26M | 18.01M | 6.9 / 9.7 ms | 3.05 |
| 32 | 14.45M | 16.72M | 7.8 / 15.1 ms | 2.95 |
| 128 | 14.79M | 17.76M | 7.0 / 9.7 ms | 2.8 |
| 512 | 14.69M | 18.05M | 6.2 / 8.6 ms | 2.65 |

At 2,000 subscribers (single runs, load generator likely saturated): 512 gave 16.9M deliveries/s against 15.0M for 128 and 14.9M for `main`. 512 was kept.

## What didn't help

| Experiment | Result |
|---|---|
| Yielding between the owner's write turns | No change in stalls |
| `event_interval(1)`, LIFO slot disabled, `global_queue_interval(1)` | No change in stalls |
| A per-batch inline limit of 32 | Worse at 500 subscribers |

## What is left

Followed up in [pub/sub lock and fairness](2026-11-17-pubsub-lock-and-fairness.md):

- **Done:** the benchmark measures many channels and SET mixed with PUBLISH. The mixed run showed PUBLISH collapsing to 0.15x Redis under SET load.
- **Done:** pub/sub has its own lock, and every connection yields after each batch. Together these fixed the collapse.
- **Tried, not adopted:** merging several publishers' batches into one write per subscriber by yielding before the writes. It saved 8 to 14% of writes, but doubled delivery latency under mixed load.
