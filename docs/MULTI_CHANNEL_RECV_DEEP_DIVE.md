# Deep Dive: recv_from_any_channel Implementation

## The Challenge

We need to simultaneously listen to messages from multiple broadcast channels and forward them to the client. The core problem: **how do you wait on N receivers at once and know which one received a message?**

```rust
// We have multiple receivers
let mut receivers: HashMap<String, broadcast::Receiver<Bytes>> = HashMap::new();
receivers.insert("events".to_string(), events_rx);
receivers.insert("logs".to_string(), logs_rx);
receivers.insert("alerts".to_string(), alerts_rx);

// Need to wait on all of them and get: (channel_name, message)
let (channel, message) = recv_from_any_channel(&mut receivers).await;
```

## Solution Options

### Option 1: tokio::select! with Explicit Branches (Simple, Limited)

**Pros**: Clearest syntax, best performance
**Cons**: Requires compile-time known number of channels

```rust
use tokio::sync::broadcast::Receiver;
use bytes::Bytes;

// Works for a fixed number of channels
async fn recv_from_three_channels(
    events_rx: &mut Receiver<Bytes>,
    logs_rx: &mut Receiver<Bytes>,
    alerts_rx: &mut Receiver<Bytes>,
) -> Option<(String, Bytes)> {
    tokio::select! {
        result = events_rx.recv() => {
            match result {
                Ok(msg) => Some(("events".to_string(), msg)),
                Err(_) => None,
            }
        }
        result = logs_rx.recv() => {
            match result {
                Ok(msg) => Some(("logs".to_string(), msg)),
                Err(_) => None,
            }
        }
        result = alerts_rx.recv() => {
            match result {
                Ok(msg) => Some(("alerts".to_string(), msg)),
                Err(_) => None,
            }
        }
    }
}
```

**Why this doesn't work for us**:
- Channel count is dynamic (user can SUBSCRIBE/UNSUBSCRIBE at runtime)
- Can't generate code at compile time for runtime data
- Would need to limit to a fixed max (e.g., 10 channels)

### Option 2: FuturesUnordered (Dynamic, Flexible)

**Pros**: Handles dynamic number of futures, idiomatic async
**Cons**: Slightly more complex, requires managing future stream

```rust
use tokio::sync::broadcast::{Receiver, error::RecvError};
use bytes::Bytes;
use futures::stream::{FuturesUnordered, StreamExt};
use std::collections::HashMap;

async fn recv_from_any_channel(
    receivers: &mut HashMap<String, Receiver<Bytes>>
) -> Option<(String, Bytes)> {
    if receivers.is_empty() {
        return None;
    }

    // Create a stream of futures, one per receiver
    let mut futures = FuturesUnordered::new();
    
    for (channel, receiver) in receivers.iter_mut() {
        let channel = channel.clone();
        // Note: We need to be careful here - we're creating a future
        // that borrows the receiver, but we need to handle ownership
        futures.push(async move {
            let result = receiver.recv().await;
            (channel, result)
        });
    }
    
    // Wait for the first future to complete
    if let Some((channel, result)) = futures.next().await {
        match result {
            Ok(msg) => Some((channel, msg)),
            Err(_) => None,
        }
    } else {
        None
    }
}
```

**Problem with this approach**: The borrow checker fight!
- `receiver.recv()` needs `&mut Receiver`
- We're iterating with `receivers.iter_mut()` which gives us `&mut Receiver`
- But the future captures that borrow and would outlive the loop iteration
- **This doesn't compile without additional complexity**

### Option 3: Task-Per-Channel with mpsc (Heavyweight, Robust)

**Pros**: Clean separation, each channel gets its own task
**Cons**: More overhead (N tasks + N channels), more memory

```rust
use tokio::sync::{broadcast, mpsc};
use bytes::Bytes;
use std::collections::HashMap;

struct ChannelMessage {
    channel: String,
    payload: Bytes,
}

struct SubscriptionManager {
    // Unified receiver for all subscribed channels
    unified_rx: mpsc::Receiver<ChannelMessage>,
    // One sender per task (to shut them down)
    tasks: HashMap<String, tokio::task::JoinHandle<()>>,
}

impl SubscriptionManager {
    fn new() -> Self {
        let (unified_tx, unified_rx) = mpsc::channel(32);
        Self {
            unified_rx,
            tasks: HashMap::new(),
        }
    }
    
    fn subscribe(&mut self, channel: String, mut broadcast_rx: broadcast::Receiver<Bytes>) {
        let unified_tx = self.unified_tx.clone();
        let channel_clone = channel.clone();
        
        // Spawn a task dedicated to this channel
        let task = tokio::spawn(async move {
            loop {
                match broadcast_rx.recv().await {
                    Ok(payload) => {
                        let msg = ChannelMessage {
                            channel: channel_clone.clone(),
                            payload,
                        };
                        // If send fails, the manager was dropped, so exit
                        if unified_tx.send(msg).await.is_err() {
                            break;
                        }
                    }
                    Err(_) => break, // Channel closed
                }
            }
        });
        
        self.tasks.insert(channel, task);
    }
    
    fn unsubscribe(&mut self, channel: &str) {
        if let Some(task) = self.tasks.remove(channel) {
            task.abort();
        }
    }
    
    async fn recv(&mut self) -> Option<ChannelMessage> {
        self.unified_rx.recv().await
    }
}

// Usage in server handler:
async fn handle_subscriptions(
    mut connection: Connection,
    db: Db,
) -> anyhow::Result<()> {
    let mut manager = SubscriptionManager::new();
    
    loop {
        tokio::select! {
            // Read commands from client
            frame = connection.read_frame() => {
                match frame? {
                    Some(Frame::Array(parts)) => {
                        // Parse SUBSCRIBE/UNSUBSCRIBE
                        if let Some(Frame::Bulk(cmd)) = parts.first() {
                            match cmd.as_ref() {
                                b"SUBSCRIBE" => {
                                    let channel = parse_channel(&parts)?;
                                    let rx = get_or_create_receiver(db.clone(), &channel);
                                    manager.subscribe(channel, rx);
                                    // Send acknowledgment
                                }
                                b"UNSUBSCRIBE" => {
                                    let channel = parse_channel(&parts)?;
                                    manager.unsubscribe(&channel);
                                    // Send acknowledgment
                                }
                                _ => {}
                            }
                        }
                    }
                    None => break,
                }
            }
            
            // Receive messages from any subscribed channel
            msg = manager.recv() => {
                match msg {
                    Some(msg) => {
                        connection.write_frame(Frame::Array(vec![
                            Frame::Bulk(Bytes::from_static(b"message")),
                            Frame::Bulk(Bytes::from(msg.channel)),
                            Frame::Bulk(msg.payload),
                        ])).await?;
                    }
                    None => break, // Manager dropped
                }
            }
        }
    }
    
    Ok(())
}
```

**Analysis**:
- Each channel gets its own `tokio::task` that forwards messages to a unified `mpsc` channel
- The main loop uses `select!` with just two branches (simple!)
- Easy to add/remove channels dynamically
- Clean shutdown via task abort
- **Overhead**: N tasks + 1 mpsc channel (32 message buffer each)

### Option 4: Manual Polling with select_biased! (Efficient, Complex)

**Pros**: No extra tasks, direct control
**Cons**: Complex, requires unsafe or pin projections

```rust
use tokio::sync::broadcast::Receiver;
use bytes::Bytes;
use std::collections::HashMap;
use std::pin::Pin;
use futures::Future;

// Store the receivers and their pending futures
struct ReceiverPool {
    receivers: HashMap<String, Receiver<Bytes>>,
    // We'd need to store pinned futures here and poll them manually
    // This gets very complex with ownership and lifetimes
}
```

**Why this is hard**:
- Futures need to be pinned and stored
- Polling API is complex and easy to misuse
- Not much better than Option 3 for our use case
- **Skip this unless performance profiling shows it's needed**

### Option 5: StreamMap (Tokio Utility, Clean)

**Pros**: Built for exactly this use case, clean API
**Cons**: Requires wrapping receivers in streams

```rust
use tokio::sync::broadcast::{Receiver, error::RecvError};
use tokio_stream::wrappers::BroadcastStream;
use tokio_stream::StreamMap;
use tokio_stream::StreamExt;
use bytes::Bytes;

struct ChannelManager {
    streams: StreamMap<String, BroadcastStream<Bytes>>,
}

impl ChannelManager {
    fn new() -> Self {
        Self {
            streams: StreamMap::new(),
        }
    }
    
    fn subscribe(&mut self, channel: String, receiver: Receiver<Bytes>) {
        // Wrap the broadcast receiver in a stream
        let stream = BroadcastStream::new(receiver);
        self.streams.insert(channel, stream);
    }
    
    fn unsubscribe(&mut self, channel: &str) {
        self.streams.remove(channel);
    }
    
    async fn recv(&mut self) -> Option<(String, Bytes)> {
        // StreamMap yields (key, value) pairs
        while let Some((channel, result)) = self.streams.next().await {
            match result {
                Ok(msg) => return Some((channel, msg)),
                Err(RecvError::Lagged(n)) => {
                    // Subscriber is too slow, messages were dropped
                    eprintln!("Channel {} lagged by {} messages", channel, n);
                    continue;
                }
                Err(RecvError::Closed) => {
                    // Channel closed, remove it
                    self.streams.remove(&channel);
                    continue;
                }
            }
        }
        None
    }
}

// Usage in server handler:
async fn handle_subscriptions(
    mut connection: Connection,
    db: Db,
) -> anyhow::Result<()> {
    let mut manager = ChannelManager::new();
    
    loop {
        tokio::select! {
            // Read commands from client
            frame = connection.read_frame() => {
                match frame? {
                    Some(frame) => {
                        match parse_command(frame)? {
                            Command::Subscribe(channel) => {
                                let receiver = get_or_create_receiver(db.clone(), &channel);
                                manager.subscribe(channel.clone(), receiver);
                                send_subscribe_ack(&mut connection, &channel, manager.streams.len()).await?;
                            }
                            Command::Unsubscribe(channel) => {
                                manager.unsubscribe(&channel);
                                send_unsubscribe_ack(&mut connection, &channel, manager.streams.len()).await?;
                            }
                            _ => {}
                        }
                    }
                    None => break,
                }
            }
            
            // Receive from any channel
            msg = manager.recv() => {
                match msg {
                    Some((channel, payload)) => {
                        connection.write_frame(Frame::Array(vec![
                            Frame::Bulk(Bytes::from_static(b"message")),
                            Frame::Bulk(Bytes::from(channel)),
                            Frame::Bulk(payload),
                        ])).await?;
                    }
                    None => break, // All channels closed
                }
            }
        }
    }
    
    Ok(())
}
```

**Analysis**:
- `StreamMap` is designed exactly for this: multiplex multiple streams by key
- Automatically handles stream removal on close
- Clean error handling for lagged subscribers
- No extra tasks needed
- **This is likely the best balance of simplicity and efficiency**

## Comparison Matrix

| Approach | Dynamic Channels | Complexity | Overhead | Error Handling |
|----------|-----------------|------------|----------|----------------|
| select! explicit | ❌ Fixed | ⭐ Simple | ⭐⭐⭐ Lowest | ⭐⭐ Good |
| FuturesUnordered | ✅ Yes | ⭐⭐ Medium | ⭐⭐ Low | ⭐⭐ Good |
| Task-per-channel | ✅ Yes | ⭐⭐⭐ High | ⭐ High | ⭐⭐⭐ Excellent |
| Manual polling | ✅ Yes | ⭐⭐⭐⭐ Very High | ⭐⭐⭐ Lowest | ⭐ Complex |
| StreamMap | ✅ Yes | ⭐⭐ Medium | ⭐⭐ Low | ⭐⭐⭐ Excellent |

## Recommended Approach: StreamMap (Option 5)

### Why StreamMap wins:

1. **Purpose-built**: Tokio provides `StreamMap` specifically for multiplexing keyed streams
2. **No extra tasks**: Everything runs in the same task context
3. **Clean API**: Subscribe/unsubscribe is just insert/remove
4. **Good error handling**: Built-in handling for lagged/closed channels
5. **Memory efficient**: Only stores the receivers and a small index
6. **Proven**: Used widely in production Tokio applications

### Dependency Required

```toml
[dependencies]
tokio-stream = "0.1"
```

### Full Implementation Example

```rust
use tokio::sync::broadcast::{Receiver, error::RecvError};
use tokio_stream::wrappers::BroadcastStream;
use tokio_stream::{StreamMap, StreamExt};
use bytes::Bytes;
use crate::connection::Connection;
use crate::frame::Frame;
use crate::Db;

pub struct SubscriptionState {
    streams: StreamMap<String, BroadcastStream<Bytes>>,
    subscription_count: usize,
}

impl SubscriptionState {
    pub fn new() -> Self {
        Self {
            streams: StreamMap::new(),
            subscription_count: 0,
        }
    }
    
    pub fn subscribe(&mut self, channel: String, receiver: Receiver<Bytes>) {
        if !self.streams.contains_key(&channel) {
            let stream = BroadcastStream::new(receiver);
            self.streams.insert(channel, stream);
            self.subscription_count += 1;
        }
    }
    
    pub fn unsubscribe(&mut self, channel: &str) -> bool {
        if self.streams.remove(channel).is_some() {
            self.subscription_count -= 1;
            true
        } else {
            false
        }
    }
    
    pub fn unsubscribe_all(&mut self) {
        self.streams.clear();
        self.subscription_count = 0;
    }
    
    pub fn count(&self) -> usize {
        self.subscription_count
    }
    
    pub fn is_empty(&self) -> bool {
        self.subscription_count == 0
    }
    
    /// Receive the next message from any subscribed channel
    pub async fn next_message(&mut self) -> Option<(String, Bytes)> {
        loop {
            match self.streams.next().await {
                Some((channel, Ok(msg))) => {
                    return Some((channel, msg));
                }
                Some((channel, Err(RecvError::Lagged(n)))) => {
                    // Log but continue - subscriber caught up
                    eprintln!("Subscriber lagged on channel '{}' by {} messages", channel, n);
                    continue;
                }
                Some((channel, Err(RecvError::Closed))) => {
                    // Channel was closed, remove it
                    self.streams.remove(&channel);
                    self.subscription_count -= 1;
                    continue;
                }
                None => {
                    // All streams exhausted
                    return None;
                }
            }
        }
    }
}

// Usage in server.rs handler:
pub async fn handle_subscriber(
    mut connection: Connection,
    db: Db,
) -> anyhow::Result<()> {
    let mut state = SubscriptionState::new();
    
    loop {
        tokio::select! {
            // Handle incoming commands
            frame_result = connection.read_frame() => {
                match frame_result? {
                    Some(frame) => {
                        if !handle_pubsub_command(frame, &mut connection, &mut state, db.clone()).await? {
                            break; // Exit subscription mode
                        }
                    }
                    None => break, // Connection closed
                }
            }
            
            // Forward messages from any subscribed channel
            msg = state.next_message(), if !state.is_empty() => {
                match msg {
                    Some((channel, payload)) => {
                        connection.write_frame(Frame::Array(vec![
                            Frame::Bulk(Bytes::from_static(b"message")),
                            Frame::Bulk(Bytes::from(channel)),
                            Frame::Bulk(payload),
                        ])).await?;
                    }
                    None => {
                        // All channels closed
                        break;
                    }
                }
            }
        }
    }
    
    Ok(())
}
```

## Performance Characteristics

### Memory Usage per Subscription
- `BroadcastStream`: ~48 bytes wrapper
- `broadcast::Receiver`: ~104 bytes
- `StreamMap` entry: ~32 bytes
- **Total**: ~184 bytes per subscribed channel

### CPU Overhead
- `StreamMap::next()`: O(n) where n = number of active streams
  - Uses round-robin polling internally
  - Fair scheduling across all channels
- For most use cases (<100 channels), overhead is negligible
- For high channel counts (>1000), consider task-per-channel

### Compared to Task-Per-Channel
- **Memory**: 10-20x less (no task stack per channel)
- **CPU**: Slightly higher due to polling overhead
- **Latency**: Similar (both are bounded by tokio scheduler)

## Edge Cases & Error Handling

### 1. Lagged Subscriber
```rust
Err(RecvError::Lagged(n)) => {
    // Messages were dropped because subscriber is too slow
    // Options:
    // a) Log and continue (best for resilience)
    // b) Disconnect slow clients (best for resource management)
    // c) Send error to client (best for observability)
}
```

### 2. Channel Closed
```rust
Err(RecvError::Closed) => {
    // No more senders exist for this channel
    // Remove from active subscriptions
    state.unsubscribe(&channel);
}
```

### 3. Duplicate Subscriptions
```rust
pub fn subscribe(&mut self, channel: String, receiver: Receiver<Bytes>) {
    if !self.streams.contains_key(&channel) {
        // Only add if not already subscribed (idempotent)
        self.streams.insert(channel, BroadcastStream::new(receiver));
    }
}
```

### 4. Empty Subscription Set
```rust
msg = state.next_message(), if !state.is_empty() => {
    // The condition prevents polling when no channels are subscribed
    // Without this, next_message() would return None immediately
}
```

## Alternative: Task-Per-Channel for High Scale

If profiling shows that `StreamMap` polling overhead is significant (>1000 channels), use the task-per-channel approach:

**When to switch**:
- More than 500-1000 concurrent subscriptions per connection
- CPU profiling shows `StreamMap::next()` is a bottleneck
- Need more precise per-channel backpressure control

**Trade-off**:
- Higher memory usage (task stack per channel)
- Better CPU efficiency at high channel counts
- More complex lifecycle management

## Conclusion

**For this project, use StreamMap (Option 5)**:
- Clean, maintainable code
- Low memory overhead
- Good performance for typical use cases (<100 channels per client)
- Excellent error handling
- No complex task management

The implementation can be upgraded to task-per-channel later if profiling shows the need, but start with the simpler approach.
