# my-mini-redis

**A high-performance Redis clone demonstrating Rust systems programming, async I/O multiplexing, and Redis-style batched pub/sub delivery.**


---

## 🎯 Technical Elevator Pitch

This project implements a **Redis server speaking the RESP2 wire protocol** from scratch in Rust, wire-compatible with Redis for the commands it supports (`GET`, `SET` without options, `PUBLISH`, `SUBSCRIBE`, `UNSUBSCRIBE`, `PING`, `QUIT`, `RESET`):

- **Ordered pub/sub delivery**: one outbox per connection, filled under the pub/sub lock, so messages from every channel arrive in publish order
- **Batched pub/sub writes**: like Redis, the connection that publishes writes to the subscribers' sockets itself, once per batch of pipelined requests, so each subscriber gets the batch in one write without waking its task
- **Slow subscribers never block publishers**: messages queue up to Redis's 32 MB pub/sub limit, then the subscriber is disconnected
- **Redis-compatible request parsing**: RESP arrays and inline commands, with Redis's exact error replies
- **Binary-safe keys, values and channel names** stored as reference-counted `Bytes`
- **Async connection handling** with `tokio::select!` across incoming requests and pending writes

Key Technics:
- Async runtime internals (Tokio)
- Buffer management with `Bytes` and `BytesMut` (shared values, reused I/O buffers)
- Connection-oriented state machines
- Network protocol design and wire-format parsing

---

## 🏗️ Architecture & Data Flow

### System Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                         Client Layer                            │
│   Publisher / Subscriber / Key-Value clients, redis-cli, ...    │
└────────────────────────────────┬────────────────────────────────┘
                                 │ RESP2 or inline commands (TCP)
                                 ▼
┌─────────────────────────────────────────────────────────────────┐
│                       Server Layer (Tokio)                      │
│  ┌───────────────────────────────────────────────────────────┐  │
│  │           Connection Handler (per-connection task)        │  │
│  │                                                           │  │
│  │   tokio::select! {                                        │  │
│  │     reader.read_request()  ─▶ Session::execute()          │  │
│  │     outbox.ready()         ─▶ finish a write the socket   │  │
│  │                               blocked, or disconnect      │  │
│  │                               (>32 MB queued)             │  │
│  │   }                                                       │  │
│  └───────────────────────────────────┬───────────────────────┘  │
│                                      │ lock                     │
│  ┌───────────────────────────────────▼───────────────────────┐  │
│  │                 Shared State (Arc<Shared>)                │  │
│  │   entries:  Mutex<HashMap<Bytes, Bytes>>                  │  │
│  │   registry: Mutex<HashMap<channel, HashMap<id, Outbox>>>  │  │
│  └───────────────────────────────────────────────────────────┘  │
└─────────────────────────────────────────────────────────────────┘
        Every connection's output goes through its Outbox: a FIFO
        of encoded replies and messages with a byte limit. PUBLISH
        encodes the message once and appends it to each subscriber's
        Outbox; after its batch of requests, the publishing
        connection writes those outboxes to their sockets itself.
```

### Key Architectural Decisions

1. **One outbox per connection**: `PUBLISH` appends to every subscriber's outbox while holding the pub/sub lock. Publishes are therefore totally ordered, and each subscriber receives them in that order no matter how many channels it listens on.

2. **Replies and messages stay in order**: replies and messages go through the same outbox, and `SUBSCRIBE`, `UNSUBSCRIBE` and `RESET` append their acknowledgments while still holding the pub/sub lock. A channel's messages therefore come after its subscribe acknowledgment and before its unsubscribe acknowledgment, exactly as in Redis.

3. **Few, large writes**: after running its whole batch of pipelined requests, a publishing connection writes each subscriber's outbox to its socket itself, outside the pub/sub lock, as Redis writes pending clients once per event loop pass. Each subscriber gets the batch in one write, and its task is only woken if its socket is full, or for subscribers beyond the first 512. Sending per message instead gave about 3 messages per write, which made write syscalls the bottleneck (see [the pub/sub deep dive](docs/benchmarks/2026-11-16-pubsub-deep-dive.md)). Reads also grow from 4 KiB up to 64 KiB while they keep filling the buffer, so a pipelined batch runs in few steps.

4. **One writer per socket at a time**: several connections can now write to one socket, so a writer token keeps Redis's rule that a socket has a single writer. Only the token holder writes, from the head of the outbox; everyone else only appends, and the holder sends what they appended before releasing the token. Writers other than the connection's own task never wait: when the socket is full, they hand the token to that task.

5. **Separate locks for keys and pub/sub**: `GET` and `SET` take the keys' lock; `PUBLISH`, `SUBSCRIBE` and `UNSUBSCRIBE` take the pub/sub registry's. A fan-out to many subscribers therefore doesn't hold up key commands, and a burst of `SET`s doesn't starve publishers. Pub/sub keeps one lock rather than one per channel, so every subscriber sees publishes in the same order.

6. **One batch per client at a time**: a connection yields to the scheduler after each batch of pipelined requests, as Redis's event loop serves each client once per pass. Without this, a client whose next batch had already arrived was served batch after batch in one task poll, and other clients' requests waited: publishers stalled for up to seconds, and `PUBLISH` ran at a fifth of its rate next to pipelined `SET`s. A lone request that wrote to nobody else skips the yield, since its client is waiting for the reply anyway.

7. **Bounded memory without backpressure**: outboxes never block publishers. A subscriber with more than 32 MB queued (Redis's `client-output-buffer-limit pubsub` hard limit) is disconnected.

8. **Errors are typed by who caused them**: a bad command gets an error reply and the connection stays open; malformed RESP gets `ERR Protocol error` and the connection closes; I/O failures close the connection and are logged.

9. **Cleanup by RAII**: a connection's `Session` removes its subscriptions when dropped, so channels without subscribers never stay in the registry, even if the connection fails.

---

## 🦀 Rust Design Patterns

### 1. **Ordered, non-blocking fan-out**

```rust
// src/subscription.rs
pub(crate) fn publish(state: &State, channel: &Bytes, payload: &Bytes, wakes: &mut Wakes) -> usize {
    let Some(subscribers) = state.subscribers.get(channel) else {
        return 0;
    };
    let message = encode_message(channel, payload);
    for outbox in subscribers.values() {
        if outbox.deliver(&message) {
            wakes.0.push(outbox.clone());
        }
    }
    subscribers.len()
}
```

The message is encoded once per `PUBLISH`. `deliver` appends it to the subscriber's outbox, so it never waits, and asks to have the outbox written only when it made an idle outbox non-empty. The copy per subscriber is the one that writing needs anyway (before, a shared `Bytes` was copied into each connection's write buffer), and it avoids every subscriber's thread updating one shared reference count. The publisher's connection calls `wakes.write_all()` after its batch.

### 2. **Cancel-safe `select!`**

```rust
// src/server.rs
tokio::select! {
    request = reader.read_request() => { /* execute, then write replies and subscribers' outboxes */ }
    () = session.outbox.ready() => { /* finish a write the socket blocked, or disconnect if it overflowed */ }
}
```

Every branch is cancel safe: `read_request` only awaits socket readiness, and `ready` waits on a `Notify`, which keeps a signal sent while nobody waits. Queued bytes can't be stranded: the writer token is only released with the outbox empty, checked under its lock.

### 3. **Redis-compatible errors**

```rust
// src/cmd/mod.rs
let arity: i32 = match name.as_slice() {
    b"get" => 2,
    b"set" => -3,
    b"publish" => 3,
    b"subscribe" => -2,
    // ...
    _ => return Err(unknown_command(&args)),
};
```

Commands are checked against Redis's arity table, and errors use Redis's exact text (`ERR unknown command 'FOOBAR', with args beginning with: ...`, `ERR wrong number of arguments for 'get' command`), so client libraries behave as they would against Redis.

---

### Running the Server

```bash
# Start the Redis server (default port: 127.0.0.1:6379)
cargo run --bin my-mini-redis-server

# Or with release optimizations
cargo run --release --bin my-mini-redis-server
```

### Using the CLI Client

```bash
# Set a key-value pair
cargo run --bin my-mini-redis-cli -- set greeting "Hello, Rust!"

# Retrieve a value
cargo run --bin my-mini-redis-cli -- get greeting

# Publish to a channel
cargo run --bin my-mini-redis-cli -- publish events "deployment_complete"

# Subscribe to multiple channels; messages print as they arrive
cargo run --bin my-mini-redis-cli -- subscribe events logs alerts
```

While subscribed, type commands to change the subscriptions. Leaving the last channel, or pressing Ctrl-C, exits:

```text
subscribe metrics        # add channels
unsubscribe events       # leave specific channels
unsubscribe              # leave every channel
```

There is no standalone `unsubscribe` command: subscriptions belong to a connection, and each CLI run opens a new one.

### Environment Configuration

```bash
# Override default server address
export MY_MINI_REDIS_ADDRESS="127.0.0.1:6380"
```

### Benchmarking against Redis

```bash
brew install redis              # or: apt install redis-server (needed for the comparison)
scripts/bench.sh                # full run, about 4 minutes
scripts/bench.sh --quick        # summary and memory only
scripts/bench.sh --help         # all options
```

[`benches/load.rs`](benches/load.rs) starts my-mini-redis and `redis-server` itself, with a fresh process for every run. It loads each server as fast as it answers and writes a report to `target/bench/latest.md`, with every number also in `latest.json`. The report has these sections:

- **Summary:** PING, SET, GET and PUBLISH (raw and pipelined), SET and GET through the `Client` library, pub/sub fan-out, and pipelined SET running alongside pub/sub. Each scenario has its throughput relative to Redis, p50/p90/p99/p99.9/max latency, server CPU per operation, cores used and peak memory.
- **Memory:** idle footprint, memory per connection, and memory per key.
- **Sweeps:** pipeline depth, number of connections, payload size, pub/sub fan-out, and the number of channels the subscribers are spread over, each varied on its own.
- **Single-threaded:** my-mini-redis on one worker thread against Redis, which also runs commands on one thread.
- **Load generator CPU and observations:** how much CPU the benchmark itself used, and notes computed from the data that point to where to optimize.

You can also run it from the Actions tab (the `bench` workflow), which adds the report to the job summary.

---

## 🚧 Work in Progress

my-mini-redis currently supports 8 commands (`GET`, `SET`, `PUBLISH`, `SUBSCRIBE`, `UNSUBSCRIBE`, `PING`, `QUIT`, `RESET`) over RESP2. The full comparison with Redis (cluster mode excluded) and the roadmap are in the **[gap analysis](https://claude.ai/artifact/NhemL1P9dvxr7bFKskRZbB#3d6c30db-642c)**.

### Redis wire compatibility

[`tests/redis_compat.rs`](tests/redis_compat.rs) pins down Redis's behavior with 34 raw-RESP tests: error replies, pub/sub semantics, binary safety, protocol errors, inline commands, pipelining (including Nagle stalls), slow subscribers and running out of file descriptors. 5 more tests in `src/server.rs` cover pub/sub channel cleanup and, under concurrent writers, whole frames and the order of messages and acknowledgments. All of them pass.

The test expectations are themselves checked against a real Redis: CI's `redis-compat` job runs them against Redis 7.2.5 on every pull request, and all pass there (the file-descriptor test skips itself in that mode). To run that check locally:

```bash
redis-server --port 47392 --save "" --appendonly no --daemonize yes
REDIS_COMPAT_TARGET=127.0.0.1:47392 cargo test --test redis_compat
redis-cli -p 47392 shutdown nosave
```

> ⚠️ `REDIS_COMPAT_TARGET` mode runs `FLUSHALL` before each test. Only point it at a throwaway server.

### Roadmap

1. **Wire compatibility fixes** — make `tests/redis_compat.rs` pass *(done)*
2. **Basic commands** — `ECHO`, `DEL`, `EXISTS`, key expiration and `SET` options
3. **Typed values** — lists, hashes, sets, sorted sets
4. **Transactions and persistence** — `MULTI`/`EXEC`, RDB/AOF
5. **Protocol and security** — RESP3, `AUTH`/ACL, `INFO`, `CONFIG`
6. **Advanced** — replication, Lua scripting, streams

Design notes and earlier plans are in [`docs/`](docs/).

---

## 🛡️ Safety & Correctness

### Test Suite

```bash
# Run all tests (unit + integration)
cargo test

# Run with output for debugging
cargo test -- --nocapture

# Run specific test suite
cargo test --test cli_integration

# Run only the Redis compatibility tests
cargo test --test redis_compat
```

### Test Coverage

- **23 Unit Tests**: RESP frame encoding/decoding, request parsing (RESP and inline), 3 pub/sub channel-cleanup tests, and 2 stress tests of pub/sub delivery with several writers per socket
- **15 Integration Tests**: Multi-client pub/sub scenarios, idempotent subscriptions, unsubscribe-all semantics, and CLI subscribe mode driven through stdin
- **34 Redis Compatibility Tests**: Raw-RESP checks of Redis wire behavior, run against Redis 7.2.5 in CI
- **4 CLI Parsing Tests**: Clap argument validation


### Memory Safety Guarantees

- **No `unsafe` blocks**: 100% safe Rust (verified with `rg "unsafe"`)
- **No data races**: All shared state guarded by `Arc<Mutex<T>>`, atomics or Tokio sync primitives
- **No use-after-free**: Ownership system enforces lifetime validity at compile time
- **Connection cleanup by RAII**: when a connection's task ends, including by panic, its `Session` is dropped, which removes its subscriptions; its socket closes once the last outbox reference is gone

---

## 📚 Project Structure

```
my-mini-redis/
├── src/
│   ├── bin/
│   │   ├── client.rs          # CLI client binary
│   │   └── server.rs          # Server binary entry point
│   ├── cmd/
│   │   ├── get.rs             # GET command implementation
│   │   ├── set.rs             # SET command implementation
│   │   ├── publish.rs         # PUBLISH command
│   │   ├── subscribe.rs       # SUBSCRIBE command
│   │   ├── unsubscribe.rs     # UNSUBSCRIBE command
│   │   └── mod.rs             # Command dispatch enum
│   ├── client.rs              # Client library API
│   ├── server.rs              # Server connection handler
│   ├── connection.rs          # Client connection (RESP framing) and the server's request reader
│   ├── frame.rs               # RESP frames (encoded by the server, parsed by the client)
│   ├── outbox.rs              # Per-connection output FIFO and single-writer token
│   ├── request.rs             # Request parsing (RESP arrays and inline commands)
│   ├── subscription.rs        # Channel registry, fan-out and batched subscriber writes
│   └── lib.rs                 # Public library interface
├── tests/
│   ├── cli_integration.rs     # End-to-end integration tests
│   └── redis_compat.rs        # Redis wire-compatibility tests (raw RESP)
├── docs/                      # Design notes and implementation plans
├── examples/
│   └── hello-redis.rs         # Example usage
├── Cargo.toml                 # Dependency manifest
└── README.md                  # This file
```