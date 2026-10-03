use crate::DEFAULT_ADDRESS;
use crate::cmd::{Get, Set};
use crate::connection::Connection;
use crate::frame::Frame;
use bytes::Bytes;
use tokio::net::TcpStream;

pub struct Client {
    connection: Connection,
}

impl Client {
    pub async fn new() -> anyhow::Result<Self> {
        Self::new_with_address(DEFAULT_ADDRESS).await
    }

    pub async fn new_with_address(address: &str) -> anyhow::Result<Self> {
        let connection = TcpStream::connect(address).await?;
        Ok(Client {
            connection: Connection::new(connection),
        })
    }

    pub async fn set(&mut self, key: &str, value: Bytes) -> anyhow::Result<()> {
        let set_frame = Set::new(key, value).into_frame();
        self.connection.write_frame(set_frame).await?;
        let response = self.connection.read_frame().await?;
        match response {
            Some(Frame::Simple(_)) => Ok(()),
            Some(Frame::Error(err)) => anyhow::bail!(err),
            _ => unreachable!(),
        }
    }

    pub async fn get(&mut self, key: &str) -> anyhow::Result<Option<Bytes>> {
        let get_frame = Get::new(key).into_frame();
        self.connection.write_frame(get_frame).await?;
        let response = self.connection.read_frame().await?;
        match response {
            Some(Frame::Bulk(bytes)) => Ok(Some(bytes)),
            Some(Frame::Null) => Ok(None),
            None => Ok(None),
            _ => unreachable!(),
        }
    }
}
