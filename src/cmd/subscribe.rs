use crate::Db;
use crate::connection::Connection;
use crate::frame::Frame;
use crate::subscription::SubscriptionState;
use bytes::Bytes;

pub struct Subscribe {
    channel: String,
}

impl Subscribe {
    pub fn new(channel: impl ToString) -> Self {
        Self {
            channel: channel.to_string(),
        }
    }
    pub fn channel(&self) -> &str {
        &self.channel
    }
    pub fn into_frame(self) -> Frame {
        Frame::Array(vec![
            Frame::Bulk(Bytes::from_static(b"SUBSCRIBE")),
            Frame::Bulk(Bytes::from(self.channel.into_bytes())),
        ])
    }

    pub(crate) async fn apply(
        self,
        db: Db,
        _connection: &mut Connection,
        subscription_state: &mut Option<SubscriptionState>,
    ) -> anyhow::Result<Frame> {
        // Get or create the broadcast receiver for this channel
        let receiver = {
            let mut db = db.lock().unwrap();
            db.subscribers
                .entry(self.channel.to_string())
                .or_insert_with(|| tokio::sync::broadcast::channel(8).0)
                .subscribe()
        };

        // Initialize subscription state if needed
        if subscription_state.is_none() {
            *subscription_state = Some(SubscriptionState::new());
        }

        // Subscribe to the channel
        let state = subscription_state.as_mut().unwrap();
        state.subscribe(self.channel.to_string(), receiver);

        // Return acknowledgment frame
        Ok(Frame::Array(vec![
            Frame::Bulk(Bytes::from_static(b"subscribe")),
            Frame::Bulk(Bytes::copy_from_slice(self.channel.as_bytes())),
            Frame::Integer(state.count() as i64),
        ]))
    }
}
