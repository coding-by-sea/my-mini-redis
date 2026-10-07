use bytes::Bytes;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use tokio::sync::broadcast;

pub mod connection;
pub mod frame;

pub mod client;
pub mod cmd;
mod parse;
pub mod server;

pub const DEFAULT_ADDRESS: &str = "127.0.0.1:6379";

#[derive(Debug, Default)]
struct State {
    entries: HashMap<String, Bytes>,
    subscribers: HashMap<String, broadcast::Sender<Bytes>>,
}
type Db = Arc<Mutex<State>>;
