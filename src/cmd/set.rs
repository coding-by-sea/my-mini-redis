use bytes::Bytes;
use crate::frame::Frame;

pub struct Set {
    key: String,
    value: Bytes,
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