# my-mini-redis

**A high-performance Redis clone demonstrating advanced Rust systems programming, async I/O multiplexing, and lock-free concurrent architectures.**


---

## 🎯 Technical Elevator Pitch

This project implements a **Redis server speaking the RESP2 wire protocol** from scratch in Rust, wire-compatible with Redis for the commands it supports (`GET`, `SET` without options, `PUBLISH`, `SUBSCRIBE`, `UNSUBSCRIBE`, `PING`, `QUIT`, `RESET`):

- **Ordered pub/sub delivery**: one queue per connection, filled under the db lock, so messages from every channel arrive in publish order
- **Slow subscribers never block publishers**: messages queue up to Redis's 32 MB pub/sub limit, then the subscriber is disconnected
- **Redis-compatible request parsing**: RESP arrays and inline commands, with Redis's exact error replies
- **Binary-safe keys, values and channel names** using `Bytes` zero-copy buffers
- **Async connection handling** with `tokio::select!` across commands and pub/sub messages

Key Technics:
- Async runtime internals (Tokio)
- Zero-copy buffer management (`Bytes`, `BytesMut`)
- Connection-oriented state machines
- Network protocol design and wire-format parsing

---

## 🏗️ Architecture & Data Flow

### System Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                         Client Layer                             │
│   Publisher / Subscriber / Key-Value clients, redis-cli, ...     │
└────────────────────────────────┬────────────────────────────────┘
                                 │ RESP2 or inline commands (TCP)
                                 ▼
┌─────────────────────────────────────────────────────────────────┐
│                       Server Layer (Tokio)                       │
│  ┌───────────────────────────────────────────────────────────┐  │
│  │           Connection Handler (per-connection task)        │  │
│  │                                                           │  │
│  │   tokio::select! {                                        │  │
│  │     connection.read_request()   ─▶ Session::execute()     │  │
│  │     subscriptions.next_message() ─▶ write "message" push  │  │
│  │     overflow.wait()              ─▶ disconnect (>32 MB)   │  │
│  │   }                                                       │  │
│  └───────────────────────────────────┬───────────────────────┘  │
│                                      │ lock                     │
│  ┌───────────────────────────────────▼───────────────────────┐  │
│  │              Shared State (Arc<Mutex<State>>)              │  │
│  │   entries:     HashMap<Bytes, Bytes>                       │  │
│  │   subscribers: HashMap<channel, HashMap<conn id, Mailbox>> │  │
│  └───────────────────────────────────────────────────────────┘  │
└─────────────────────────────────────────────────────────────────┘
        PUBLISH appends (channel, payload) to each subscriber's
        Mailbox: an unbounded per-connection queue with a byte limit
```

### Key Architectural Decisions

1. **One queue per connection**: `PUBLISH` appends to every subscriber's queue while holding the db lock. Publishes are therefore totally ordered, and each subscriber receives them in that order no matter how many channels it listens on.

2. **Replies and messages stay in order**: before running a command, the connection takes the messages already queued (under the same lock) and sends them first. A message published before an `UNSUBSCRIBE` arrives before its acknowledgment, exactly as in Redis.

3. **Bounded memory without backpressure**: queues never block publishers. A subscriber with more than 32 MB queued (Redis's `client-output-buffer-limit pubsub` hard limit) is disconnected.

4. **Errors are typed by who caused them**: a bad command gets an error reply and the connection stays open; malformed RESP gets `ERR Protocol error` and the connection closes; I/O failures close the connection and are logged.

5. **Cleanup by RAII**: a connection's `Session` removes its subscriptions when dropped, so channels without subscribers never stay in the registry, even if the connection fails.

---

## 🦀 Rust Design Patterns

### 1. **Ordered, non-blocking fan-out**

```rust
// src/subscription.rs
pub(crate) fn publish(state: &State, channel: &Bytes, payload: &Bytes) -> usize {
    let Some(subscribers) = state.subscribers.get(channel) else {
        return 0;
    };
    for mailbox in subscribers.values() {
        mailbox.deliver(channel, payload);
    }
    subscribers.len()
}
```

`deliver` pushes onto an unbounded `mpsc` queue, so it never waits. `Bytes` clones are reference-count increments, so a payload is never copied per subscriber.

### 2. **Cancel-safe `select!`**

```rust
// src/server.rs
tokio::select! {
    request = connection.read_request() => { /* execute, write replies */ }
    message = session.subscriptions.next_message() => { /* write message */ }
    () = overflow.wait() => { /* disconnect */ }
}
```

Every branch is cancel safe: `read_request` only awaits `read_buf`, `mpsc::recv` loses nothing when dropped, and `Notify` keeps a signal sent while nobody waits.

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

---

## 🚧 Work in Progress

my-mini-redis currently supports 8 commands (`GET`, `SET`, `PUBLISH`, `SUBSCRIBE`, `UNSUBSCRIBE`, `PING`, `QUIT`, `RESET`) over RESP2. The full comparison with Redis (cluster mode excluded) and the roadmap are in the **[gap analysis](https://claude.ai/artifact/NhemL1P9dvxr7bFKskRZbB#3d6c30db-642c)**.

### Redis wire compatibility

[`tests/redis_compat.rs`](tests/redis_compat.rs) pins down Redis's behavior with 33 raw-RESP tests: error replies, pub/sub semantics, binary safety, protocol errors, inline commands, pipelining, slow subscribers and running out of file descriptors. 3 more tests in `src/server.rs` cover pub/sub channel cleanup. All of them pass.

The test expectations are themselves checked against a real Redis. The first 30 pass against Redis 7.2.5; the three newest (cross-channel message order, lagging subscribers, and the output buffer limit) haven't been checked yet:

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

- **20 Unit Tests**: RESP frame encoding/decoding, request parsing (RESP and inline), and 3 pub/sub channel-cleanup tests
- **15 Integration Tests**: Multi-client pub/sub scenarios, idempotent subscriptions, unsubscribe-all semantics, and CLI subscribe mode driven through stdin
- **33 Redis Compatibility Tests**: Raw-RESP checks of Redis wire behavior, 30 of them verified against Redis 7.2.5
- **4 CLI Parsing Tests**: Clap argument validation


### Memory Safety Guarantees

- **No `unsafe` blocks**: 100% safe Rust (verified with `rg "unsafe"`)
- **No data races**: All shared state guarded by `Arc<Mutex<T>>`, atomics or Tokio sync primitives
- **No use-after-free**: Ownership system enforces lifetime validity at compile time
- **No memory leaks**: RAII ensures automatic cleanup on task panic (Tokio's `JoinHandle` drops clean up resources)

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
│   ├── connection.rs          # TCP stream wrapper (RESP framing)
│   ├── frame.rs               # RESP frames (replies, parsed by the client)
│   ├── request.rs             # Request parsing (RESP arrays and inline commands)
│   ├── subscription.rs        # Per-connection message queues and channel registry
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