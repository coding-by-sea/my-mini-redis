use crate::frame::Frame;
use bytes::Bytes;

pub struct Unsubscribe {
    channels: Vec<Bytes>,
}

impl Unsubscribe {
    /// An empty list unsubscribes from every channel.
    pub fn new(channels: Vec<String>) -> Self {
        Self {
            channels: channels.into_iter().map(Bytes::from).collect(),
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
