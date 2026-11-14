use crate::frame::Frame;
use bytes::Bytes;

pub struct Subscribe {
    channels: Vec<Bytes>,
}

impl Subscribe {
    pub fn new(channel: impl ToString) -> Self {
        Self::new_multiple([channel])
    }

    pub fn new_multiple(channels: impl IntoIterator<Item = impl ToString>) -> Self {
        Self {
            channels: channels
                .into_iter()
                .map(|channel| Bytes::from(channel.to_string()))
                .collect(),
        }
    }

    /// Parses `SUBSCRIBE channel [channel ...]`; the arity was already checked.
    pub(crate) fn from_args(args: impl Iterator<Item = Bytes>) -> Self {
        Self {
            channels: args.collect(),
        }
    }

    pub fn channels(&self) -> &[Bytes] {
        &self.channels
    }

    pub fn into_frame(self) -> Frame {
        let mut frames = vec![Frame::Bulk(Bytes::from_static(b"SUBSCRIBE"))];
        frames.extend(self.channels.into_iter().map(Frame::Bulk));
        Frame::Array(frames)
    }
}
