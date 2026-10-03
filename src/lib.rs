pub mod connection;
pub mod frame;

pub mod cmd;
mod parse;
pub mod client;
pub mod server;

const ADDRESS: &str = "127.0.0.1:6379";
