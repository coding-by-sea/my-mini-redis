//! Pub/sub delivery. Each connection has one queue that PUBLISH appends to while
//! holding the db lock, so messages from every channel reach a subscriber in the
//! order they were published, and a slow subscriber never blocks a publisher.

use crate::State;
use bytes::{BufMut, Bytes, BytesMut};
use std::collections::HashSet;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use tokio::sync::{Notify, mpsc};

/// Redis's default `client-output-buffer-limit pubsub` hard limit. A subscriber with
/// more than this queued (counted in encoded bytes, as Redis counts its output
/// buffer) is disconnected rather than buffered without bound.
pub(crate) const QUEUE_LIMIT_BYTES: usize = 32 * 1024 * 1024;

/// A published message, already encoded as the RESP `[message, channel, payload]` push
/// a subscriber receives. It is encoded once per PUBLISH and shared by every
/// subscriber's queue.
pub(crate) type Message = Bytes;

/// The sending side of a connection's message queue, stored in the channel registry.
#[derive(Clone)]
pub(crate) struct Mailbox {
    sender: mpsc::UnboundedSender<Message>,
    shared: Arc<Shared>,
}

struct Shared {
    queued_bytes: AtomicUsize,
    overflowed: AtomicBool,
    overflow: Notify,
}

impl Mailbox {
    /// Queues a message. Over the limit, the message is dropped and the connection is
    /// told to close instead.
    fn deliver(&self, message: &Message) {
        if self.shared.overflowed.load(Ordering::Relaxed) {
            return;
        }
        let size = message.len();
        if self.shared.queued_bytes.fetch_add(size, Ordering::Relaxed) + size > QUEUE_LIMIT_BYTES {
            self.shared.overflowed.store(true, Ordering::Relaxed);
            self.shared.overflow.notify_one();
            return;
        }
        // Fails only once the connection is gone; its cleanup removes this mailbox.
        let _ = self.sender.send(message.clone());
    }
}

/// Sends `payload` to every subscriber of `channel` and returns how many there were.
/// Never creates a registry entry for a channel without subscribers.
pub(crate) fn publish(state: &State, channel: &Bytes, payload: &Bytes) -> usize {
    let Some(subscribers) = state.subscribers.get(channel) else {
        return 0;
    };
    let message = encode_message(channel, payload);
    for mailbox in subscribers.values() {
        mailbox.deliver(&message);
    }
    subscribers.len()
}

/// Encodes `*3\r\n$7\r\nmessage\r\n$<len>\r\n<channel>\r\n$<len>\r\n<payload>\r\n`.
fn encode_message(channel: &[u8], payload: &[u8]) -> Message {
    let mut out = BytesMut::with_capacity(channel.len() + payload.len() + 48);
    out.put_slice(b"*3\r\n$7\r\nmessage\r\n");
    for part in [channel, payload] {
        out.put_slice(format!("${}\r\n", part.len()).as_bytes());
        out.put_slice(part);
        out.put_slice(b"\r\n");
    }
    out.freeze()
}

/// One connection's subscriptions and the receiving side of its message queue.
pub(crate) struct Subscriptions {
    id: u64,
    channels: HashSet<Bytes>,
    mailbox: Mailbox,
    receiver: mpsc::UnboundedReceiver<Message>,
}

impl Subscriptions {
    pub(crate) fn new() -> Self {
        static NEXT_ID: AtomicU64 = AtomicU64::new(0);
        let (sender, receiver) = mpsc::unbounded_channel();
        Self {
            id: NEXT_ID.fetch_add(1, Ordering::Relaxed),
            channels: HashSet::new(),
            mailbox: Mailbox {
                sender,
                shared: Arc::new(Shared {
                    queued_bytes: AtomicUsize::new(0),
                    overflowed: AtomicBool::new(false),
                    overflow: Notify::new(),
                }),
            },
            receiver,
        }
    }

    pub(crate) fn count(&self) -> usize {
        self.channels.len()
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.channels.is_empty()
    }

    /// The subscribed channels, in no particular order.
    pub(crate) fn channels(&self) -> Vec<Bytes> {
        self.channels.iter().cloned().collect()
    }

    /// Subscribes to `channel`; subscribing twice has no extra effect.
    pub(crate) fn subscribe(&mut self, state: &mut State, channel: Bytes) {
        if self.channels.insert(channel.clone()) {
            state
                .subscribers
                .entry(channel)
                .or_default()
                .insert(self.id, self.mailbox.clone());
        }
    }

    /// Unsubscribes from `channel`, removing the registry entry once it has no
    /// subscribers left. Unsubscribing from a channel never joined is a no-op.
    pub(crate) fn unsubscribe(&mut self, state: &mut State, channel: &Bytes) {
        if self.channels.remove(channel) {
            remove_from_registry(state, self.id, channel);
        }
    }

    /// Unsubscribes from every channel and returns them, in no particular order.
    pub(crate) fn unsubscribe_all(&mut self, state: &mut State) -> Vec<Bytes> {
        let channels: Vec<Bytes> = self.channels.drain().collect();
        for channel in &channels {
            remove_from_registry(state, self.id, channel);
        }
        channels
    }

    /// Takes the messages already queued, without waiting. Called while holding the db
    /// lock, the result is exactly the messages published before this point, which
    /// matters when the caller also changes subscriptions.
    pub(crate) fn take_queued(&mut self) -> Vec<Message> {
        let mut messages = Vec::new();
        while let Ok(message) = self.receiver.try_recv() {
            self.dequeued(&message);
            messages.push(message);
        }
        messages
    }

    /// Waits for the next queued message. Cancel safe.
    pub(crate) async fn next_message(&mut self) -> Message {
        // The queue can't close: `self.mailbox` holds a sender for as long as `self` lives.
        let message = self.receiver.recv().await.expect("own sender is alive");
        self.dequeued(&message);
        message
    }

    /// A handle that completes once this connection's queue goes over its limit. It is
    /// separate so it can be awaited alongside `next_message`.
    pub(crate) fn overflow_signal(&self) -> OverflowSignal {
        OverflowSignal(self.mailbox.shared.clone())
    }

    fn dequeued(&self, message: &Message) {
        self.mailbox
            .shared
            .queued_bytes
            .fetch_sub(message.len(), Ordering::Relaxed);
    }
}

pub(crate) struct OverflowSignal(Arc<Shared>);

impl OverflowSignal {
    /// Cancel safe: a signal sent while nobody waits is kept for the next call.
    pub(crate) async fn wait(&self) {
        self.0.overflow.notified().await;
    }
}

fn remove_from_registry(state: &mut State, id: u64, channel: &Bytes) {
    if let Some(subscribers) = state.subscribers.get_mut(channel) {
        subscribers.remove(&id);
        if subscribers.is_empty() {
            state.subscribers.remove(channel);
        }
    }
}
