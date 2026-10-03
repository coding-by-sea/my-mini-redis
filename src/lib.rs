use bytes::Bytes;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};

pub mod connection;
pub mod frame;

pub mod client;
pub mod cmd;
mod parse;
pub mod server;

pub const DEFAULT_ADDRESS: &str = "127.0.0.1:6379";
type Db = Arc<Mutex<HashMap<String, Bytes>>>;
