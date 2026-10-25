# Plan: Adding Synchronous Client API

## Overview
Add a synchronous (blocking) client API alongside the existing async client to support use cases where async is not needed or desirable (simple scripts, REPL tools, testing utilities, compatibility with sync codegen).

## Goals
1. Provide ergonomic blocking API for simple use cases
2. Maintain code reuse with async implementation where possible
3. Zero overhead - compile away unused code paths
4. Support all operations: GET, SET, PUBLISH, SUBSCRIBE, UNSUBSCRIBE

## Design Options

### Option 1: Wrapper Around Async Client (Simplest)
Use `tokio::runtime::Runtime` to block on async operations.

**Pros**:
- Minimal code duplication
- Automatically gets all async features
- Easy to maintain (one source of truth)

**Cons**:
- Must bundle Tokio runtime overhead
- Slight performance penalty from runtime creation/destruction
- Not truly "sync" - still async underneath

**Implementation**:
```rust
pub struct BlockingClient {
    runtime: tokio::runtime::Runtime,
    async_client: crate::client::Client,
}

impl BlockingClient {
    pub fn new() -> anyhow::Result<Self> {
        let runtime = tokio::runtime::Runtime::new()?;
        let async_client = runtime.block_on(crate::client::Client::new())?;
        Ok(Self { runtime, async_client })
    }
    
    pub fn get(&mut self, key: &str) -> anyhow::Result<Option<Bytes>> {
        self.runtime.block_on(self.async_client.get(key))
    }
}
```

### Option 2: Separate Sync Implementation (Most Control)
Implement blocking I/O using `std::net::TcpStream`.

**Pros**:
- No Tokio dependency for sync client
- True blocking semantics
- Optimal performance for blocking workloads
- Clear separation of concerns

**Cons**:
- Code duplication (connection, framing, protocol logic)
- Two implementations to maintain
- Risk of divergence between sync/async

**Implementation**:
```rust
pub struct BlockingClient {
    stream: std::net::TcpStream,
    connection: BlockingConnection, // sync version of Connection
}

struct BlockingConnection {
    stream: std::io::BufReader<std::io::BufWriter<std::net::TcpStream>>,
    buffer: BytesMut,
}
```

### Option 3: Shared Core with Dual Interface (Best Balance)
Extract protocol logic into sync core, wrap with async/blocking adapters.

**Pros**:
- Code reuse for protocol logic (framing, parsing)
- Both implementations stay in sync
- Can optimize each I/O layer independently

**Cons**:
- Requires refactoring existing code
- More complex architecture
- Trait abstraction overhead

**Implementation**:
```rust
// Shared protocol core
trait ConnectionTrait {
    fn read_frame(&mut self) -> anyhow::Result<Option<Frame>>;
    fn write_frame(&mut self, frame: Frame) -> anyhow::Result<()>;
}

struct AsyncConnection { /* tokio */ }
struct BlockingConnection { /* std::net */ }

struct Client<C: ConnectionTrait> {
    connection: C,
}
```

## Recommended Approach: Option 1 (Wrapper)

**Rationale**:
- Fastest to implement (1-2 hours)
- Zero risk of protocol divergence
- Users who need sync likely already have Tokio in their dependency tree
- Can always refactor to Option 3 later if performance profiling shows bottleneck

## Implementation Plan

### Phase 1: Core Blocking Client (2-3 hours)

#### File: `src/blocking_client.rs` (new)
```rust
use crate::client::{Client as AsyncClient, Message};
use bytes::Bytes;
use tokio::runtime::Runtime;

pub struct Client {
    runtime: Runtime,
    async_client: AsyncClient,
}

impl Client {
    pub fn new() -> anyhow::Result<Self> {
        Self::new_with_address(crate::DEFAULT_ADDRESS)
    }

    pub fn new_with_address(address: &str) -> anyhow::Result<Self> {
        let runtime = Runtime::new()?;
        let async_client = runtime.block_on(AsyncClient::new_with_address(address))?;
        Ok(Self { runtime, async_client })
    }

    pub fn get(&mut self, key: &str) -> anyhow::Result<Option<Bytes>> {
        self.runtime.block_on(self.async_client.get(key))
    }

    pub fn set(&mut self, key: &str, value: Bytes) -> anyhow::Result<()> {
        self.runtime.block_on(self.async_client.set(key, value))
    }

    pub fn publish(&mut self, channel: &str, message: Bytes) -> anyhow::Result<u64> {
        self.runtime.block_on(self.async_client.publish(channel, message))
    }

    pub fn subscribe(&mut self, channel: &str) -> anyhow::Result<()> {
        self.runtime.block_on(self.async_client.subscribe(channel))
    }

    pub fn subscribe_multiple(&mut self, channels: &[&str]) -> anyhow::Result<()> {
        self.runtime.block_on(self.async_client.subscribe_multiple(channels))
    }

    pub fn unsubscribe(&mut self, channels: &[&str]) -> anyhow::Result<()> {
        self.runtime.block_on(self.async_client.unsubscribe(channels))
    }

    pub fn unsubscribe_all(&mut self) -> anyhow::Result<()> {
        self.runtime.block_on(self.async_client.unsubscribe_all())
    }

    pub fn next_message(&mut self) -> anyhow::Result<Option<Message>> {
        self.runtime.block_on(self.async_client.next_message())
    }
}
```

#### Update `src/lib.rs`:
```rust
pub mod blocking_client;
pub use blocking_client::Client as BlockingClient;
```

### Phase 2: Subscription Iterator (2 hours)

Add blocking iterator for subscription messages:

```rust
// src/blocking_client.rs

pub struct Subscription<'a> {
    client: &'a mut Client,
}

impl<'a> Iterator for Subscription<'a> {
    type Item = anyhow::Result<Message>;

    fn next(&mut self) -> Option<Self::Item> {
        match self.client.next_message() {
            Ok(Some(msg)) => Some(Ok(msg)),
            Ok(None) => None,
            Err(e) => Some(Err(e)),
        }
    }
}

impl Client {
    pub fn into_subscription(self) -> SubscriptionOwned {
        SubscriptionOwned { client: self }
    }
}

pub struct SubscriptionOwned {
    client: Client,
}

impl Iterator for SubscriptionOwned {
    type Item = anyhow::Result<Message>;

    fn next(&mut self) -> Option<Self::Item> {
        match self.client.next_message() {
            Ok(Some(msg)) => Some(Ok(msg)),
            Ok(None) => None,
            Err(e) => Some(Err(e)),
        }
    }
}
```

**Usage**:
```rust
let mut client = BlockingClient::new()?;
client.subscribe_multiple(&["events", "logs"])?;

for msg in client.into_subscription() {
    let msg = msg?;
    println!("{}: {}", msg.channel, String::from_utf8_lossy(&msg.payload));
}
```

### Phase 3: Testing (2 hours)

#### Unit Tests (`src/blocking_client.rs`):
```rust
#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;

    #[test]
    fn test_blocking_get_set() {
        let mut client = Client::new().unwrap();
        client.set("key1", Bytes::from("value1")).unwrap();
        let value = client.get("key1").unwrap();
        assert_eq!(value, Some(Bytes::from("value1")));
    }

    #[test]
    fn test_blocking_publish_subscribe() {
        let mut subscriber = Client::new().unwrap();
        subscriber.subscribe("test_channel").unwrap();

        let mut publisher = Client::new().unwrap();
        let count = publisher.publish("test_channel", Bytes::from("hello")).unwrap();
        assert_eq!(count, 1);

        let msg = subscriber.next_message().unwrap().unwrap();
        assert_eq!(msg.channel, "test_channel");
        assert_eq!(msg.payload, Bytes::from("hello"));
    }
}
```

#### Integration Tests (`tests/blocking_integration.rs` - new):
```rust
use bytes::Bytes;
use my_redis::BlockingClient;
use my_redis::server::Server;
use tokio::net::TcpListener;
use std::thread;
use std::time::Duration;

#[test]
fn blocking_client_basic_operations() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap().to_string();
    
    // Spawn server in background thread
    thread::spawn(move || {
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            Server::new().run_with_listener(listener).await;
        });
    });
    
    thread::sleep(Duration::from_millis(100)); // Let server start

    let mut client = BlockingClient::new_with_address(&address).unwrap();
    
    // Test SET/GET
    client.set("greeting", Bytes::from("hello")).unwrap();
    let value = client.get("greeting").unwrap();
    assert_eq!(value, Some(Bytes::from("hello")));
    
    // Test pub/sub
    let mut subscriber = BlockingClient::new_with_address(&address).unwrap();
    subscriber.subscribe("events").unwrap();
    
    client.publish("events", Bytes::from("test")).unwrap();
    let msg = subscriber.next_message().unwrap().unwrap();
    assert_eq!(msg.payload, Bytes::from("test"));
}
```

### Phase 4: Documentation & Examples (1 hour)

#### Example: `examples/blocking_hello.rs` (new)
```rust
use bytes::Bytes;
use my_redis::BlockingClient;

fn main() -> anyhow::Result<()> {
    // Connect to server
    let mut client = BlockingClient::new()?;

    // Set a key
    client.set("greeting", Bytes::from("Hello, blocking world!"))?;

    // Get the key
    if let Some(value) = client.get("greeting")? {
        println!("Got: {}", String::from_utf8_lossy(&value));
    }

    // Publish to a channel
    let subscriber_count = client.publish("events", Bytes::from("startup"))?;
    println!("Published to {} subscribers", subscriber_count);

    Ok(())
}
```

#### Example: `examples/blocking_pubsub.rs` (new)
```rust
use bytes::Bytes;
use my_redis::BlockingClient;
use std::thread;
use std::time::Duration;

fn main() -> anyhow::Result<()> {
    // Spawn subscriber thread
    let subscriber_thread = thread::spawn(|| {
        let mut client = BlockingClient::new().unwrap();
        client.subscribe_multiple(&["events", "logs"]).unwrap();
        
        println!("Subscribed to events and logs");
        
        for msg in client.into_subscription().take(5) {
            let msg = msg.unwrap();
            println!("[{}] {}", msg.channel, String::from_utf8_lossy(&msg.payload));
        }
    });

    // Give subscriber time to connect
    thread::sleep(Duration::from_millis(100));

    // Publish messages
    let mut publisher = BlockingClient::new()?;
    for i in 0..5 {
        publisher.publish("events", Bytes::from(format!("event_{}", i)))?;
        thread::sleep(Duration::from_millis(50));
    }

    subscriber_thread.join().unwrap();
    Ok(())
}
```

#### Update README.md:
```markdown
### Blocking Client (for non-async code)

```rust
use my_redis::BlockingClient;
use bytes::Bytes;

fn main() -> anyhow::Result<()> {
    let mut client = BlockingClient::new()?;
    
    client.set("key", Bytes::from("value"))?;
    let value = client.get("key")?;
    
    Ok(())
}
```

**When to use**:
- Simple scripts without async runtime
- Testing utilities
- Integration with sync code generators
- REPL or CLI tools

**Performance note**: The blocking client wraps the async client with `tokio::runtime::Runtime::block_on()`. For high-performance applications, prefer the async client.
```

## API Design Considerations

### Error Handling
Use same `anyhow::Result` as async client for consistency:
```rust
pub fn get(&mut self, key: &str) -> anyhow::Result<Option<Bytes>>
```

### Ownership
- Take `&mut self` for all operations (matches async client)
- `into_subscription()` consumes client (cannot do other operations while subscribed)
- Alternative: `subscription(&mut self)` returns `Subscription<'_>` with lifetime

### Thread Safety
```rust
// NOT thread-safe (matches async client)
// Users must wrap in Arc<Mutex<_>> if needed
let client = BlockingClient::new()?;

// Usage across threads:
let client = Arc::new(Mutex::new(BlockingClient::new()?));
let client_clone = Arc::clone(&client);
thread::spawn(move || {
    let mut client = client_clone.lock().unwrap();
    client.get("key").unwrap();
});
```

## Performance Considerations

### Runtime Creation
**Current approach**: Create runtime once in constructor, reuse for all operations.

**Alternative**: Create runtime per operation
```rust
pub fn get(&mut self, key: &str) -> anyhow::Result<Option<Bytes>> {
    let rt = Runtime::new()?;  // ~100µs overhead
    rt.block_on(self.async_client.get(key))
}
```
❌ Too expensive - runtime creation is ~100µs

### Runtime Configuration
Use minimal runtime:
```rust
Runtime::builder()
    .enable_io()
    .enable_time()
    .build()?
```

### Connection Pooling
Single connection per client (matches async client). Users can pool:
```rust
use deadpool::managed::Pool;

struct ClientManager;
impl deadpool::managed::Manager for ClientManager {
    type Type = BlockingClient;
    // ... implement pool
}
```

## Migration Path for Existing Users

### Before (async only):
```rust
#[tokio::main]
async fn main() {
    let mut client = Client::new().await.unwrap();
    let value = client.get("key").await.unwrap();
}
```

### After (blocking option):
```rust
fn main() {
    let mut client = BlockingClient::new().unwrap();
    let value = client.get("key").unwrap();
}
```

## Testing Strategy

1. **Unit tests**: Test each method independently
2. **Integration tests**: Test against running server
3. **Compatibility tests**: Ensure sync and async clients interoperate correctly
4. **Performance tests**: Benchmark overhead of `block_on()` wrapper

## Timeline Estimate

- Phase 1 (Core client): 2-3 hours
- Phase 2 (Iterator API): 2 hours
- Phase 3 (Testing): 2 hours
- Phase 4 (Docs/Examples): 1 hour
- **Total**: 7-8 hours

## Future Enhancements

1. **Connection pooling integration** (deadpool, r2d2)
2. **Timeout support**: `get_timeout(key, duration)`
3. **Retry logic**: Automatic reconnection on network failures
4. **Builder pattern**: `BlockingClient::builder().timeout(...).build()`
5. **Feature flag**: Make blocking client opt-in via `blocking` Cargo feature

## Alternative: Feature-Flag Architecture

If binary size is a concern, make blocking client optional:

**Cargo.toml**:
```toml
[features]
default = ["async"]
async = []
blocking = ["tokio/rt"]
```

**lib.rs**:
```rust
#[cfg(feature = "blocking")]
pub mod blocking_client;

#[cfg(feature = "blocking")]
pub use blocking_client::Client as BlockingClient;
```

**Usage**:
```toml
# Async only (default)
my-redis = "0.1"

# Blocking only
my-redis = { version = "0.1", default-features = false, features = ["blocking"] }

# Both
my-redis = { version = "0.1", features = ["blocking"] }
```

## Summary

**Recommended**: Option 1 wrapper approach
- Fast to implement (7-8 hours total)
- Low maintenance burden
- Zero protocol divergence risk
- Good enough performance for blocking use cases
- Can optimize later if profiling shows bottleneck

This gives users the ergonomics they need without massive engineering investment.
