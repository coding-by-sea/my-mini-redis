use crate::connection::Connection;
use crate::frame::Frame;
use crate::subscription::SubscriptionState;
use bytes::Bytes;

pub struct Unsubscribe {
    channels: Vec<String>,
}

impl Unsubscribe {
    pub fn new(channels: Vec<String>) -> Self {
        Self { channels }
    }

    pub fn channels(&self) -> &[String] {
        &self.channels
    }

    pub fn into_frame(self) -> Frame {
        let mut frames = vec![Frame::Bulk(Bytes::from_static(b"UNSUBSCRIBE"))];
        for channel in self.channels {
            frames.push(Frame::Bulk(Bytes::from(channel.into_bytes())));
        }
        Frame::Array(frames)
    }

    pub(crate) async fn apply(
        self,
        _connection: &mut Connection,
        subscription_state: &mut Option<SubscriptionState>,
    ) -> anyhow::Result<Vec<Frame>> {
        let mut responses = Vec::new();

        if let Some(state) = subscription_state.as_mut() {
            if self.channels.is_empty() {
                // Unsubscribe from all channels
                let channels = state.unsubscribe_all();
                for channel in channels {
                    responses.push(Frame::Array(vec![
                        Frame::Bulk(Bytes::from_static(b"unsubscribe")),
                        Frame::Bulk(Bytes::from(channel)),
                        Frame::Integer(state.count() as i64),
                    ]));
                }
            } else {
                // Unsubscribe from specific channels
                for channel in &self.channels {
                    state.unsubscribe(channel);

                    responses.push(Frame::Array(vec![
                        Frame::Bulk(Bytes::from_static(b"unsubscribe")),
                        Frame::Bulk(Bytes::from(channel.clone())),
                        Frame::Integer(state.count() as i64),
                    ]));
                }
            }
        } else {
            // Not in subscription mode, send 0 count for each channel
            let channels = if self.channels.is_empty() {
                vec![]
            } else {
                self.channels.clone()
            };

            for channel in channels {
                responses.push(Frame::Array(vec![
                    Frame::Bulk(Bytes::from_static(b"unsubscribe")),
                    Frame::Bulk(Bytes::from(channel)),
                    Frame::Integer(0),
                ]));
            }
        }

        Ok(responses)
    }
}
