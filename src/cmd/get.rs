use super::to_bytes;
use crate::frame::Frame;
use bytes::Bytes;

pub struct Get {
    key: Bytes,
}

impl Get {
    pub fn new(key: impl AsRef<[u8]>) -> Self {
        Self { key: to_bytes(key) }
    }

    /// Parses `GET key`; the arity was already checked.
    pub(crate) fn from_args(mut args: impl Iterator<Item = Bytes>) -> Self {
        Self {
            key: args.next().unwrap(),
        }
    }

    pub fn key(&self) -> &Bytes {
        &self.key
    }

    pub fn into_frame(self) -> Frame {
        Frame::Array(vec![
            Frame::Bulk(Bytes::from_static(b"GET")),
            Frame::Bulk(self.key),
        ])
    }
}
