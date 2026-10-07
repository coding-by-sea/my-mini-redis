use bytes::Bytes;

use crate::Db;
use crate::connection::Connection;
use crate::frame::Frame;
use tokio::sync::broadcast;

pub struct Publish {
    channel: String,
    message: Bytes,
}

impl Publish {
    pub fn new(channel: impl ToString, message: Bytes) -> Self {
        Self {
            channel: channel.to_string(),
            message,
        }
    }

    pub fn channel(&self) -> &str {
        &self.channel
    }

    pub fn into_frame(self) -> Frame {
        Frame::Array(vec![
            Frame::Bulk(Bytes::from_static(b"PUBLISH")),
            Frame::Bulk(Bytes::from(self.channel.into_bytes())),
            Frame::Bulk(self.message),
        ])
    }

    pub(crate) async fn apply(self, db: Db, connection: &mut Connection) -> anyhow::Result<()> {
        let sender = {
            let mut db = db.lock().unwrap();
            db.subscribers
                .entry(self.channel.clone())
                .or_insert_with(|| broadcast::channel(8).0)
                .clone()
        };
        let subscriber_count = sender.send(self.message).unwrap_or(0);

        connection
            .write_frame(Frame::Bulk(Bytes::from(format!(
                "publish {subscriber_count}"
            ))))
            .await
    }
}
