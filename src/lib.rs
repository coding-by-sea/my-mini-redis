use bytes::Bytes;
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

#[derive(Default)]
struct State {
    entries: HashMap<Bytes, Bytes>,
    /// Channel -> (connection id -> that connection's outbox). A channel is only present
    /// while it has subscribers.
    subscribers: HashMap<Bytes, HashMap<u64, outbox::Outbox>>,
}
type Db = Arc<Mutex<State>>;

/// Locks the db. A panic while holding the lock leaves `State` intact (every update is
/// a single map operation), so poisoning is ignored instead of failing every
/// connection that comes after.
fn lock(db: &Db) -> MutexGuard<'_, State> {
    db.lock().unwrap_or_else(|poisoned| poisoned.into_inner())
}
