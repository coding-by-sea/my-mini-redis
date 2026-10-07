use crate::{Db};
use crate::connection::Connection;
use crate::frame::Frame;
use bytes::Bytes;
use tokio::sync::broadcast;
use tokio::sync::broadcast::Receiver;

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

    pub(crate) async fn apply(self, db: Db, connection: &mut Connection) -> anyhow::Result<()> {
        let mut receiver: Receiver<Bytes>;
        {
            let sub = &mut db.lock().unwrap().subscribers;
            if let Some(sender) = sub.get(&self.channel().to_string()) {
                receiver = sender.subscribe();
            } else {
                let (tx, rx) = broadcast::channel::<Bytes>(8);
                sub.insert(self.channel().to_string(), tx.clone());
                receiver = rx;
            }
        }
        connection.write_frame(Frame::Bulk(Bytes::from(format!("subscribe {}", self.channel)))).await?;
        loop {
            let bytes = receiver.recv().await?;
            connection.write_frame(Frame::Bulk(bytes)).await?;
        }
    }
}
