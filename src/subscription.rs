//! Pub/sub delivery. Each connection has one outbox of encoded messages that PUBLISH
//! appends to while holding the db lock, so messages from every channel reach a
//! subscriber in the order they were published, and a slow subscriber never blocks a
//! publisher.
//!
//! Like Redis's "clients pending write", a publisher doesn't wake a subscriber per
//! message: it notes the subscribers whose outbox it made non-empty and wakes them once
//! its whole batch of pipelined requests has run (`Wakes`). Each subscriber then sends
//! everything queued with one write, instead of racing the publisher and sending a
//! few messages per write.

use crate::State;
use bytes::{BufMut, Bytes, BytesMut};
use std::collections::HashSet;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, MutexGuard};
use tokio::sync::Notify;

/// Redis's default `client-output-buffer-limit pubsub` hard limit. A subscriber with
/// more than this queued (counted in encoded bytes, as Redis counts its output
/// buffer) is disconnected rather than buffered without bound.
pub(crate) const QUEUE_LIMIT_BYTES: usize = 32 * 1024 * 1024;

/// A spare outbox buffer larger than this is freed rather than kept for reuse, so one
/// burst doesn't pin its memory for the life of the connection.
const SPARE_CAPACITY_LIMIT: usize = 1024 * 1024;

/// The publishing side of a connection's outbox, stored in the channel registry.
#[derive(Clone)]
pub(crate) struct Mailbox(Arc<Shared>);

struct Shared {
    outbox: Mutex<Outbox>,
    /// Notified when the outbox goes from empty to non-empty, or overflows.
    ready: Notify,
}

#[derive(Default)]
struct Outbox {
    bytes: BytesMut,
    overflowed: bool,
}

impl Mailbox {
    /// Appends an encoded message. Returns whether the subscriber needs waking: the
    /// outbox was empty, so no wake is pending yet, or it just overflowed. Over the
    /// limit, what is queued is dropped and the connection is told to close instead.
    fn deliver(&self, message: &[u8]) -> bool {
        let mut outbox = self.outbox();
        if outbox.overflowed {
            return false;
        }
        if outbox.bytes.len() + message.len() > QUEUE_LIMIT_BYTES {
            outbox.overflowed = true;
            outbox.bytes = BytesMut::new();
            return true;
        }
        let was_empty = outbox.bytes.is_empty();
        outbox.bytes.extend_from_slice(message);
        was_empty
    }

    /// The outbox lock is never held across anything that can panic, so poisoning is
    /// ignored as for the db lock.
    fn outbox(&self) -> MutexGuard<'_, Outbox> {
        self.0
            .outbox
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }
}

/// Subscribers a publisher has queued messages for and still has to wake. Waking them
/// after the publisher's batch, instead of per message, lets each subscriber send the
/// whole batch with one write, and keeps the wakeups outside the db lock.
#[derive(Default)]
pub(crate) struct Wakes(Vec<Mailbox>);

impl Wakes {
    pub(crate) fn wake_all(&mut self) {
        for mailbox in self.0.drain(..) {
            mailbox.0.ready.notify_one();
        }
    }
}

impl Drop for Wakes {
    /// However the publisher's connection ends, its subscribers still get their messages.
    fn drop(&mut self) {
        self.wake_all();
    }
}

/// Queues `payload` for every subscriber of `channel` and returns how many there were.
/// The subscribers to wake are added to `wakes`. Never creates a registry entry for a
/// channel without subscribers.
pub(crate) fn publish(state: &State, channel: &Bytes, payload: &Bytes, wakes: &mut Wakes) -> usize {
    let Some(subscribers) = state.subscribers.get(channel) else {
        return 0;
    };
    let message = encode_message(channel, payload);
    for mailbox in subscribers.values() {
        if mailbox.deliver(&message) {
            wakes.0.push(mailbox.clone());
        }
    }
    subscribers.len()
}

/// Encodes `*3\r\n$7\r\nmessage\r\n$<len>\r\n<channel>\r\n$<len>\r\n<payload>\r\n`.
fn encode_message(channel: &[u8], payload: &[u8]) -> BytesMut {
    let mut out = BytesMut::with_capacity(channel.len() + payload.len() + 48);
    out.put_slice(b"*3\r\n$7\r\nmessage\r\n");
    for part in [channel, payload] {
        out.put_slice(format!("${}\r\n", part.len()).as_bytes());
        out.put_slice(part);
        out.put_slice(b"\r\n");
    }
    out
}

/// One connection's subscriptions and the receiving side of its outbox.
pub(crate) struct Subscriptions {
    id: u64,
    channels: HashSet<Bytes>,
    mailbox: Mailbox,
    /// An empty buffer swapped in for the outbox's when taking messages, so steady
    /// state delivery allocates nothing.
    spare: BytesMut,
}

impl Subscriptions {
    pub(crate) fn new() -> Self {
        static NEXT_ID: AtomicU64 = AtomicU64::new(0);
        Self {
            id: NEXT_ID.fetch_add(1, Ordering::Relaxed),
            channels: HashSet::new(),
            mailbox: Mailbox(Arc::new(Shared {
                outbox: Mutex::default(),
                ready: Notify::new(),
            })),
            spare: BytesMut::new(),
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

    /// Takes the encoded messages already queued, without waiting. Called while holding
    /// the db lock, the result is exactly the messages published before this point,
    /// which matters when the caller also changes subscriptions. Hand the buffer back
    /// with `recycle` once it is written.
    pub(crate) fn take_queued(&mut self) -> BytesMut {
        let spare = std::mem::take(&mut self.spare);
        std::mem::replace(&mut self.mailbox.outbox().bytes, spare)
    }

    /// Keeps a buffer from `take_queued` for reuse.
    pub(crate) fn recycle(&mut self, mut buffer: BytesMut) {
        if buffer.capacity() <= SPARE_CAPACITY_LIMIT && buffer.capacity() > self.spare.capacity() {
            buffer.clear();
            self.spare = buffer;
        }
    }

    /// Waits until messages may be queued, or the outbox overflowed. It can return with
    /// nothing queued. Cancel safe: a wakeup sent while nobody waits is kept for the
    /// next call.
    pub(crate) async fn ready(&self) {
        self.mailbox.0.ready.notified().await;
    }

    /// Whether the outbox went over its limit; the connection must then close.
    pub(crate) fn overflowed(&self) -> bool {
        self.mailbox.outbox().overflowed
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
