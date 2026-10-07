use crate::Db;
use crate::connection::Connection;
use crate::frame::Frame;
use bytes::Bytes;

pub struct Set {
    key: String,
    value: Bytes,
}
impl Set {
    pub fn new(key: impl ToString, value: Bytes) -> Self {
        Self {
            key: key.to_string(),
            value,
        }
    }
    pub fn key(&self) -> &str {
        &self.key
    }

    pub fn value(&self) -> Bytes {
        self.value.clone()
    }

    pub fn into_frame(self) -> Frame {
        Frame::Array(vec![
            Frame::Bulk(Bytes::from_static(b"SET")),
            Frame::Bulk(Bytes::from(self.key.into_bytes())),
            Frame::Bulk(self.value),
        ])
    }

    pub(crate) async fn apply(self, db: Db, connection: &mut Connection) -> anyhow::Result<()> {
        {
            let mut db = db.lock().unwrap();
            db.entries.insert(self.key().to_string(), self.value());
        }
        let response = Frame::Simple("OK".into());
        connection.write_frame(response).await
    }
}
