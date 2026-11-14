use crate::frame::Frame;
use bytes::Bytes;

pub struct Set {
    key: Bytes,
    value: Bytes,
}

impl Set {
    pub fn new(key: impl ToString, value: Bytes) -> Self {
        Self {
            key: Bytes::from(key.to_string()),
            value,
        }
    }

    /// Parses `SET key value`; the arity was already checked. No options are
    /// supported, so any extra argument is a syntax error, as Redis reports for
    /// an unknown option.
    pub(crate) fn from_args(mut args: impl Iterator<Item = Bytes>) -> Result<Self, String> {
        let key = args.next().unwrap();
        let value = args.next().unwrap();
        if args.next().is_some() {
            return Err("ERR syntax error".into());
        }
        Ok(Self { key, value })
    }

    pub fn key(&self) -> &Bytes {
        &self.key
    }

    pub fn value(&self) -> Bytes {
        self.value.clone()
    }

    pub fn into_frame(self) -> Frame {
        Frame::Array(vec![
            Frame::Bulk(Bytes::from_static(b"SET")),
            Frame::Bulk(self.key),
            Frame::Bulk(self.value),
        ])
    }
}
