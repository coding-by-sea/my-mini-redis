use std::io::{Cursor, Error, ErrorKind};
use bytes::{Buf, BytesMut};
use tokio::io::{AsyncReadExt, AsyncWriteExt, BufWriter};
use tokio::net::TcpStream;
use super::frame::Frame;
use super::frame::Error as FrameError;
use anyhow;

pub struct Connection {
    stream: BufWriter<TcpStream>,
    buffer: BytesMut,
}

impl Connection {
    pub fn new(stream: TcpStream) -> Connection {
        Connection {
            stream: BufWriter::new(stream),
            buffer: BytesMut::with_capacity(4096),
        }
    }

    pub async fn read_frame(&mut self) -> Result<Option<Frame>, anyhow::Error> {
        loop {
            if let Some(frame) = self.parse_frame().await? {
                return Ok(Some(frame));
            }
            if self.stream.read_buf(&mut self.buffer).await? == 0 {
                // the read side of the stream is closed
                return if self.buffer.is_empty() {
                    Ok(None)
                } else {
                    Err(Error::from(ErrorKind::ConnectionReset).into())
                }
            }
        }
    }

    async fn parse_frame(&mut self) -> Result<Option<Frame>, anyhow::Error> {
        let mut cursor = Cursor::new(&self.buffer[..]);
        match Frame::check(&mut cursor) {
            Ok(_) => {
                let len = cursor.position() as usize;
                cursor.set_position(0);
                let frame = Frame::read_frame(&mut cursor)?;
                self.buffer.advance(len);
                Ok(Some(frame))
            }
            Err(FrameError::IncompleteFrame) => {
                Ok(None)
            }
            Err(e) => {
                Err(e.into())
            }
        }
    }

    pub async fn write_frame(&mut self, frame: Frame) -> Result<(), anyhow::Error> {
        match frame {
            Frame::Array(array) => {
                self.stream.write_u8(b'*').await?;
                self.write_decimal(array.len() as i64).await?;
                for val in array {
                    self.write_value(val).await?;
                }
            }
            other => self.write_value(other).await?,
        }
        let res = self.stream.flush().await;
        if let Err(e) = res {
            Err(e.into())
        } else {
            Ok(())
        }
    }

    async fn write_value(&mut self, frame: Frame) -> Result<(), anyhow::Error> {
        match frame {
            Frame::Simple(response) => {
                self.stream.write_u8(b'+').await?;
                self.stream.write_all(response.as_ref()).await?;
                self.stream.write_all(b"\r\n").await?;
                Ok(())
            }
            Frame::Error(response) => {
                self.stream.write_u8(b'-').await?;
                self.stream.write_all(response.as_ref()).await?;
                self.stream.write_all(b"\r\n").await?;
                Ok(())
            }
            Frame::Integer(response) => {
                self.stream.write_u8(b':').await?;
                self.write_decimal(response).await
            }
            Frame::Bulk(response) => {
                self.stream.write_u8(b'$').await?;
                self.write_decimal(response.len() as i64).await?;

                self.stream.write_all(response.as_ref()).await?;
                self.stream.write_all(b"\r\n").await?;
                Ok(())
            }
            Frame::Array(_) => {
                // add recursive support for encoding nested arrays
                todo!("nested arrays not supported yet");
            }
            Frame::Null => {
                self.stream.write_all(b"-1\r\n").await?;
                Ok(())
            },
        }
    }

    async fn write_decimal(&mut self, response: i64) -> Result<(), anyhow::Error> {
        use std::io::Write;
        let mut string = [0u8; 20];
        let mut buf = Cursor::new(&mut string[..]);
        write!(&mut buf, "{}", response)?;

        let pos = buf.position() as usize;
        self.stream.write_all(&buf.get_ref()[..pos]).await?;
        self.stream.write_all(b"\r\n").await?;

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::Connection;
    use crate::frame::Frame;
    use bytes::Bytes;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::{TcpListener, TcpStream};

    async fn connection_pair() -> (Connection, TcpStream) {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let client = TcpStream::connect(address).await.unwrap();
        let (server, _) = listener.accept().await.unwrap();
        (Connection::new(client), server)
    }

    #[tokio::test]
    async fn write_frame_writes_a_resp_array() {
        let (mut connection, mut peer) = connection_pair().await;
        let expected = b"*3\r\n+OK\r\n$5\r\nhello\r\n:7\r\n";

        connection
            .write_frame(Frame::Array(vec![
                Frame::Simple("OK".into()),
                Frame::Bulk(Bytes::from_static(b"hello")),
                Frame::Integer(7),
            ]))
            .await
            .unwrap();

        let mut actual = vec![0; expected.len()];
        peer.read_exact(&mut actual).await.unwrap();
        assert_eq!(actual, expected);
    }

    #[tokio::test]
    async fn write_frame_writes_a_resp_integer() {
        let (mut connection, mut peer) = connection_pair().await;
        let expected = b":42\r\n";

        connection.write_frame(Frame::Integer(42)).await.unwrap();

        let mut actual = vec![0; expected.len()];
        peer.read_exact(&mut actual).await.unwrap();
        assert_eq!(actual, expected);
    }

    #[tokio::test]
    async fn read_frame_reads_consecutive_frames_and_end_of_stream() {
        let (mut connection, mut peer) = connection_pair().await;
        peer.write_all(b"+OK\r\n:7\r\n").await.unwrap();
        peer.shutdown().await.unwrap();

        assert_simple_frame(connection.read_frame().await.unwrap().unwrap(), "OK");
        assert_integer_frame(connection.read_frame().await.unwrap().unwrap(), 7);
        assert!(connection.read_frame().await.unwrap().is_none());
    }

    fn assert_simple_frame(frame: Frame, expected: &str) {
        match frame {
            Frame::Simple(actual) => assert_eq!(actual, expected),
            _ => panic!("expected a simple frame"),
        }
    }

    fn assert_integer_frame(frame: Frame, expected: i64) {
        match frame {
            Frame::Integer(actual) => assert_eq!(actual, expected),
            _ => panic!("expected an integer frame"),
        }
    }
}
