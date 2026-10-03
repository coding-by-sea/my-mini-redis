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
    pub fn new(key: impl ToString) -> Self {
        Self { key: key.to_string() }
    }
    pub fn key(&self) -> &str {
        &self.key
    }
    pub fn into_frame(self) -> Frame {
        Frame::Array(
            vec![
                Frame::Bulk(Bytes::from_static(b"GET")),
                Frame::Bulk(Bytes::from(self.key.into_bytes())),
            ]
        )
    }
}

impl Set {
    pub fn new(key: impl ToString, value: Bytes) -> Self {
        Self { key: key.to_string(), value }
    }
    pub fn key(&self) -> &str {
        &self.key
    }

    pub fn value(&self) -> Bytes {
        self.value.clone()
    }

    pub fn into_frame(self) -> Frame {
        Frame::Array(
            vec![
                Frame::Bulk(Bytes::from_static(b"SET")),
                Frame::Bulk(Bytes::from(self.key.into_bytes())),
                Frame::Bulk(self.value),
            ]
        )
    }
}