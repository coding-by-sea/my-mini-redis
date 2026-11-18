//! Pub/sub delivery. PUBLISH appends each message to the outbox of every subscriber
//! while holding the db lock, so messages from every channel reach a subscriber in the
//! order they were published, and a slow subscriber never blocks a publisher.
//!
//! Like Redis's "clients pending write", a publisher doesn't send per message: it notes
//! the subscribers whose outbox it made non-empty, and once its whole batch of
//! pipelined requests has run, writes their outboxes itself (`Wakes`). Each subscriber
//! then gets the batch with one write, and its task isn't woken unless its socket is
//! full.

use crate::State;
use crate::outbox::Outbox;
use bytes::{BufMut, Bytes, BytesMut};
use std::collections::HashSet;
use std::sync::atomic::{AtomicU64, Ordering};

/// How many subscribers a publisher writes to itself after a batch. The rest are
/// written by their own tasks, so a channel with thousands of subscribers doesn't hold
/// up one publisher's next batch for thousands of writes. Measured on Linux with 4
/// publishers: writing all of 500 subscribers inline gave the best tail latency and
/// 13% less CPU than waking their tasks, and 512 beat 128 at 2000 subscribers; a cap
/// of 32 was clearly worse.
const INLINE_WRITE_LIMIT: usize = 512;

/// Subscribers a publisher has queued messages for and still has to send them to.
/// Sending after the publisher's batch, instead of per message, lets each subscriber
/// get the whole batch with one write, and keeps the writes outside the db lock.
#[derive(Default)]
pub(crate) struct Wakes(Vec<Outbox>);

impl Wakes {
    /// Writes the first subscribers' outboxes without waiting, and wakes the tasks of
    /// the rest, and of any whose socket is full. Returns whether there were any.
    pub(crate) fn write_all(&mut self) -> bool {
        let any = !self.0.is_empty();
        for (index, outbox) in self.0.drain(..).enumerate() {
            if index < INLINE_WRITE_LIMIT {
                outbox.write_now();
            } else {
                outbox.wake();
            }
        }
        any
    }
}

impl Drop for Wakes {
    /// However the publisher's connection ends, its subscribers still get their messages.
    fn drop(&mut self) {
        self.write_all();
    }
}

/// Queues `payload` for every subscriber of `channel` and returns how many there were.
/// The subscribers to send to are added to `wakes`. Never creates a registry entry for
/// a channel without subscribers.
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

/// One connection's subscriptions.
pub(crate) struct Subscriptions {
    id: u64,
    channels: HashSet<Bytes>,
    /// The connection's outbox, registered for every subscribed channel.
    outbox: Outbox,
}

impl Subscriptions {
    pub(crate) fn new(outbox: Outbox) -> Self {
        static NEXT_ID: AtomicU64 = AtomicU64::new(0);
        Self {
            id: NEXT_ID.fetch_add(1, Ordering::Relaxed),
            channels: HashSet::new(),
            outbox,
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
                .insert(self.id, self.outbox.clone());
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
}

fn remove_from_registry(state: &mut State, id: u64, channel: &Bytes) {
    if let Some(subscribers) = state.subscribers.get_mut(channel) {
        subscribers.remove(&id);
        if subscribers.is_empty() {
            state.subscribers.remove(channel);
        }
    }
}
