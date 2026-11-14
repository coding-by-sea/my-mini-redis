use crate::frame::Frame;
use bytes::Bytes;

pub struct Publish {
    channel: Bytes,
    message: Bytes,
}

impl Publish {
    pub fn new(channel: impl ToString, message: Bytes) -> Self {
        Self {
            channel: Bytes::from(channel.to_string()),
            message,
        }
    }

    /// Parses `PUBLISH channel message`; the arity was already checked.
    pub(crate) fn from_args(mut args: impl Iterator<Item = Bytes>) -> Self {
        Self {
            channel: args.next().unwrap(),
            message: args.next().unwrap(),
        }
    }

    pub fn channel(&self) -> &Bytes {
        &self.channel
    }

    pub fn message(&self) -> &Bytes {
        &self.message
    }

    pub fn into_frame(self) -> Frame {
        Frame::Array(vec![
            Frame::Bulk(Bytes::from_static(b"PUBLISH")),
            Frame::Bulk(self.channel),
            Frame::Bulk(self.message),
        ])
    }
}
