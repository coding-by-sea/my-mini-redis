use crate::Db;
use crate::connection::Connection;
use crate::frame::Frame;
use bytes::Bytes;

pub struct Get {
    key: String,
}

impl Get {
    pub fn new(key: impl ToString) -> Self {
        Self {
            key: key.to_string(),
        }
    }
    pub fn key(&self) -> &str {
        &self.key
    }
    pub fn into_frame(self) -> Frame {
        Frame::Array(vec![
            Frame::Bulk(Bytes::from_static(b"GET")),
            Frame::Bulk(Bytes::from(self.key.into_bytes())),
        ])
    }

    pub(crate) async fn apply(self, db: Db, connection: &mut Connection) -> anyhow::Result<()> {
        let response = {
            let db = db.lock().unwrap();
            if let Some(value) = db.entries.get(&self.key().to_string()) {
                Frame::Bulk(value.clone())
            } else {
                Frame::Null
            }
        };
        connection.write_frame(response).await
    }
}
