use bytes::Bytes;
use crate::frame::Frame;

pub struct Get {
    key: String,
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

