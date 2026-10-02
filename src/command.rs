use anyhow::{anyhow};
use bytes::Bytes;
use crate::frame::Frame;
use crate::parse::Parse;

pub struct Get {
    key: String,
}

pub struct Set {
    key: String,
    value: Bytes,
}

pub enum Command {
    Get(Get),
    Set(Set),
}

impl Command {
    pub fn from_frame(frame: Frame) -> anyhow::Result<Command> {
        let mut parse = Parse::new(frame)?;
        let string = parse.next_string()?;
        match string.to_lowercase().as_str() {
            "get" => {
                Ok(Command::Get(
                    Get { key:  parse.next_string()?},
                ))
            }
            "set" => {
                Ok(Command::Set(
                    Set {key: parse.next_string()?, value: parse.next_bytes()?}
                ))
            }
            _ => Err(anyhow!("cannot execute command {:?}", string)),
        }
    }
}

impl Get {
    pub fn new(key: String) -> Self {
        Self { key }
    }
    pub fn key(&self) -> &str {
        &self.key
    }
}

impl Set {
    pub fn new(key: String, value: Bytes) -> Self {
        Self { key, value }
    }
    pub fn key(&self) -> &str {
        &self.key
    }

    pub fn value(&self) -> Bytes {
        self.value.clone()
    }
}