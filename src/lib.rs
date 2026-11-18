use bytes::Bytes;
use dashmap::DashMap;
use std::collections::HashMap;
use std::sync::{Arc, Mutex, MutexGuard};

pub mod connection;
pub mod frame;
mod outbox;

pub mod client;
pub mod cmd;
mod request;
pub mod server;
pub mod subscription;

pub const DEFAULT_ADDRESS: &str = "127.0.0.1:6379";

/// State shared by every connection. Keys and the pub/sub registry have separate
/// locks, so a PUBLISH fanning out to many subscribers doesn't hold up SET and GET,
/// and the other way around.
#[derive(Default)]
struct Shared {
    /// Sharded, each shard with its own read-write lock: with one lock for all keys,
    /// pipelined SETs from 50 connections spent about 60% of the server's CPU waking
    /// each other up on unlock (`futex_wake`), and under 1% inserting.
    entries: DashMap<Bytes, Bytes>,
    /// One lock for all of pub/sub, so publishes are totally ordered: every subscriber
    /// sees them in the same order, as in Redis.
    registry: Mutex<Registry>,
}
type Db = Arc<Shared>;

/// Channel -> (connection id -> that connection's outbox). A channel is only present
/// while it has subscribers.
type Registry = HashMap<Bytes, HashMap<u64, outbox::Outbox>>;

/// Locks part of the db. A panic while holding a lock leaves the data intact (every
/// update is a single map operation), so poisoning is ignored instead of failing every
/// connection that comes after.
fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
}
