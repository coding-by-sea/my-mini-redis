# Pub/Sub Enhancement Plan: Multi-Channel Support & Unsubscribe

## Current State Analysis

### Architecture
- **Single channel per client**: Each client can only subscribe to one channel at a time
- **No unsubscribe**: Once subscribed, clients enter an infinite loop receiving messages
- **Server-side**: Uses `HashMap<String, broadcast::Sender<Bytes>>` to store channel senders
- **Client-side**: `enter_subscribe_mode()` loops forever reading messages

### Current Flow
1. Client calls `subscribe(channel)` → sends SUBSCRIBE command
2. Server responds with `["subscribe", channel, 1]`
3. Client enters blocking loop calling `next_message()` until connection closes
4. Server's `Subscribe::apply()` enters infinite loop forwarding messages

### Limitations
1. **Cannot subscribe to multiple channels** - the current design locks the connection into one channel's receiver loop
2. **Cannot unsubscribe** - no command to stop receiving from a channel
3. **No subscription tracking** - client doesn't maintain state about active subscriptions
4. **Blocking design** - server handler blocks on one channel's receiver

## Enhancement Goals

### 1. Multiple Channels Per Client
Allow a single client connection to:
- Subscribe to multiple channels dynamically
- Receive messages from all subscribed channels
- Track which channels are currently subscribed

### 2. Unsubscribe Support
Allow clients to:
- Unsubscribe from specific channels while staying connected
- Unsubscribe from all channels
- Continue using the same connection for other operations

## Technical Design

### Phase 1: Server-Side Multi-Channel Support

#### 1.1 Add Unsubscribe Command
**File**: `src/cmd/unsubscribe.rs` (new)

```rust
pub struct Unsubscribe {
    channels: Vec<String>,  // Empty vec means unsubscribe from all
}
```

**Protocol**:
- Request: `["UNSUBSCRIBE", channel1, channel2, ...]`
- Response (per channel): `["unsubscribe", channel, remaining_count]`

#### 1.2 Refactor Subscribe Handler
**File**: `src/cmd/subscribe.rs`

**Current Issue**: The `Subscribe::apply()` method enters an infinite loop, blocking the handler from processing other commands.

**Solution**: Change the subscription model from blocking to message-driven:

1. **Track subscriptions per connection**:
   - Add subscription state to the connection handler
   - Store multiple receivers: `HashMap<String, broadcast::Receiver<Bytes>>`

2. **Use tokio::select! for multiplexing**:
   ```rust
   loop {
       tokio::select! {
           // Read incoming commands from client
           frame = connection.read_frame() => {
               // Handle SUBSCRIBE, UNSUBSCRIBE, or other commands
           }
           // Receive messages from subscribed channels
           msg = recv_from_any_channel(&mut receivers) => {
               // Forward message to client
           }
       }
   }
   ```

3. **Subscription count tracking**:
   - Return actual subscription count (not hardcoded `1`)
   - Track per-connection: how many channels this client is subscribed to

#### 1.3 Update Server Handler
**File**: `src/server.rs`

**Current**: Simple command-response loop
```rust
while let Some(frame) = connection.read_frame().await {
    match Command::from_frame(frame) {
        Subscribe(cmd) => cmd.apply(...).await, // Blocks forever
        ...
    }
}
```

**New**: Subscription-aware handler
- Maintain connection state with active subscriptions
- Use `tokio::select!` to multiplex between:
  - Reading new commands from the client
  - Receiving messages from all subscribed channels
- Exit subscribe mode when all channels are unsubscribed

### Phase 2: Client-Side Multi-Channel Support

#### 2.1 Add Subscription State Tracking
**File**: `src/client.rs`

Add field to track subscriptions:
```rust
pub struct Client {
    connection: Connection,
    subscriptions: HashSet<String>,  // Track active subscriptions
}
```

#### 2.2 New Client Methods

**`subscribe_multiple(&mut self, channels: &[&str])`**:
- Send SUBSCRIBE command for each channel
- Wait for acknowledgment for each
- Update local subscription state

**`unsubscribe(&mut self, channels: &[&str])`**:
- Send UNSUBSCRIBE command
- Wait for acknowledgments
- Update local subscription state

**`unsubscribe_all(&mut self)`**:
- Send UNSUBSCRIBE with no channels
- Clear local subscription state

**Update `enter_subscribe_mode()`**:
- Keep existing behavior for backward compatibility
- Continue reading messages until all subscriptions are removed

#### 2.3 Enhanced Message Structure
**File**: `src/client.rs`

The `Message` struct already includes the channel, which is good for multi-channel scenarios:
```rust
pub struct Message {
    pub channel: String,  // Already present - no change needed
    pub payload: Bytes,
}
```

### Phase 3: CLI Enhancement

#### 3.1 Update CLI Commands
**File**: `src/bin/client.rs`

**Current**:
```rust
Subscribe { name: String }
```

**New**:
```rust
Subscribe { 
    channels: Vec<String>  // Accept multiple channels
}
Unsubscribe {
    channels: Vec<String>  // Accept multiple channels; empty = all
}
```

**Usage**:
```bash
my-mini-redis subscribe channel1 channel2 channel3
my-mini-redis unsubscribe channel1
my-mini-redis unsubscribe  # Unsubscribe from all
```

#### 3.2 Interactive Subscribe Mode (Optional Enhancement)
For better UX, consider making subscribe mode interactive:
- Allow sending commands while subscribed
- Commands like `sub channel`, `unsub channel`, `quit`
- This would require reading from both stdin and the connection

### Phase 4: Protocol Considerations

#### 4.1 Frame Format Consistency
Follow Redis protocol conventions:

**SUBSCRIBE**:
- Request: `["SUBSCRIBE", "channel1", "channel2", ...]`
- Response (per channel): `["subscribe", channel, total_subscriptions]`

**UNSUBSCRIBE**:
- Request: `["UNSUBSCRIBE", "channel1", "channel2", ...]` or `["UNSUBSCRIBE"]` for all
- Response (per channel): `["unsubscribe", channel, remaining_subscriptions]`

**MESSAGE** (no change):
- `["message", channel, payload]`

#### 4.2 Subscription Count
Return the **total number of subscriptions for this connection**, not a hardcoded `1`:
- After SUBSCRIBE: increment and return count
- After UNSUBSCRIBE: decrement and return count
- When count reaches 0, connection can exit subscribe mode

## Implementation Order

### Step 1: Server-Side Foundation (High Priority)
1. Create `Unsubscribe` command struct
2. Refactor server handler to support subscription state
3. Implement channel multiplexing with `tokio::select!`
4. Update subscription count tracking

### Step 2: Basic Client Support (High Priority)
1. Add `unsubscribe()` method to client
2. Update existing `subscribe()` to work with new server behavior
3. Test single-channel subscribe/unsubscribe flow

### Step 3: Multi-Channel Support (Medium Priority)
1. Add `subscribe_multiple()` client method
2. Update CLI to accept multiple channels
3. Test multi-channel scenarios

### Step 4: Enhanced UX (Low Priority)
1. Interactive subscribe mode
2. Better error messages
3. Subscription status queries

## Testing Strategy

### Unit Tests
- `Unsubscribe` command parsing and frame generation
- Subscription count tracking logic
- Message routing to correct channels

### Integration Tests
**File**: `tests/cli_integration.rs`

Add tests for:
1. **Multi-channel subscription**:
   ```rust
   subscriber.subscribe("events").await;
   subscriber.subscribe("logs").await;
   // Publish to both, verify both received
   ```

2. **Unsubscribe from one channel**:
   ```rust
   subscriber.subscribe("events").await;
   subscriber.subscribe("logs").await;
   subscriber.unsubscribe(&["events"]).await;
   // Publish to events → not received
   // Publish to logs → received
   ```

3. **Unsubscribe from all**:
   ```rust
   subscriber.subscribe("events").await;
   subscriber.subscribe("logs").await;
   subscriber.unsubscribe_all().await;
   // Connection should exit subscribe mode
   ```

4. **Concurrent subscribers on same channels**:
   ```rust
   // Multiple clients subscribe to same channel
   // Verify all receive the published message
   ```

## Backward Compatibility

### Maintaining Existing Behavior
- Keep `subscribe(channel)` single-channel method
- Keep `enter_subscribe_mode()` blocking behavior
- Existing CLI usage should continue to work

### Migration Path
- New methods are additions, not replacements
- Old code continues to work unchanged
- Users can gradually adopt multi-channel features

## Edge Cases to Handle

1. **Subscribe to already-subscribed channel**: Should be idempotent (no error, just ack)
2. **Unsubscribe from non-subscribed channel**: Should be idempotent (return count)
3. **Connection drops while subscribed**: Server should clean up receivers
4. **No active publishers**: Subscriptions should still be tracked, just no messages
5. **Unsubscribe with empty channel list**: Unsubscribe from all channels

## Performance Considerations

### Memory
- Each subscription adds one `broadcast::Receiver` (~104 bytes)
- Limit maximum subscriptions per connection (e.g., 1000 channels)

### CPU
- `tokio::select!` with many receivers has O(n) overhead
- For very high channel counts, consider alternative designs:
  - Single receiver with channel tagging
  - Separate subscriber task per channel with message forwarding

### Concurrency
- `broadcast` channel already handles multiple subscribers efficiently
- No lock contention on per-channel basis

## Alternative Designs Considered

### 1. Pattern-Based Subscriptions (Future Enhancement)
Redis supports `PSUBSCRIBE` for pattern matching:
- `PSUBSCRIBE events.*` subscribes to `events.user`, `events.order`, etc.
- Would require pattern matching on publish
- Defer to future work

### 2. Separate Connection Per Subscription
Simpler implementation but:
- Higher resource usage (N connections × N channels)
- More complex client management
- Not how Redis does it
- **Rejected**

### 3. Non-Blocking Subscribe Mode
Allow GET/SET commands while subscribed:
- Redis doesn't allow this (subscribed connections only accept pub/sub commands)
- Adds complexity
- **Rejected for now**, keep Redis-compatible behavior

## Summary

This plan transforms the pub/sub system from single-channel/blocking to multi-channel/multiplexed while maintaining backward compatibility. The key architectural change is moving from a blocking `loop { receiver.recv() }` to a `select!`-based handler that can manage multiple subscriptions and respond to unsubscribe commands.

**Estimated Implementation Time**:
- Phase 1 (Server multi-channel): 4-6 hours
- Phase 2 (Client methods): 2-3 hours
- Phase 3 (CLI updates): 1-2 hours
- Phase 4 (Tests & polish): 2-3 hours
- **Total**: ~10-15 hours

**Risk Areas**:
- Server handler refactoring (most complex change)
- Correct subscription count tracking
- Race conditions in subscribe/unsubscribe
- Message ordering guarantees
