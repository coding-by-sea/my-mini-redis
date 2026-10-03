use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use bytes::Bytes;

pub mod connection;
pub mod frame;

pub mod cmd;
mod parse;
pub mod client;
pub mod server;

const ADDRESS: &str = "127.0.0.1:6379";
type Db = Arc<Mutex<HashMap<String, Bytes>>>;
