use super::to_bytes;
use crate::frame::Frame;
use bytes::Bytes;

pub struct Unsubscribe {
    channels: Vec<Bytes>,
}

impl Unsubscribe {
    /// Unsubscribes from `channels`; an empty list unsubscribes from every channel.
    pub fn new(channels: impl IntoIterator<Item = impl AsRef<[u8]>>) -> Self {
        Self {
            channels: channels.into_iter().map(to_bytes).collect(),
        }
    }

    /// Unsubscribes from every channel.
    pub fn all() -> Self {
        Self {
            channels: Vec::new(),
        }
    }

    /// Parses `UNSUBSCRIBE [channel ...]`.
    pub(crate) fn from_args(args: impl Iterator<Item = Bytes>) -> Self {
        Self {
            channels: args.collect(),
        }
    }

    pub fn channels(&self) -> &[Bytes] {
        &self.channels
    }

    pub fn into_frame(self) -> Frame {
        let mut frames = vec![Frame::Bulk(Bytes::from_static(b"UNSUBSCRIBE"))];
        frames.extend(self.channels.into_iter().map(Frame::Bulk));
        Frame::Array(frames)
    }
}
