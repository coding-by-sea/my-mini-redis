use super::frame::Error as FrameError;
use super::frame::Frame;
use super::request::{self, Parsed, ProtocolError};
use anyhow;
use bytes::{Buf, Bytes, BytesMut};
use std::io::{Cursor, Error, ErrorKind};
use tokio::io::{AsyncReadExt, AsyncWriteExt, BufWriter};
use tokio::net::TcpStream;
use tokio::net::tcp::OwnedReadHalf;

/// Space for the first read of a request batch.
const READ_SIZE: usize = 4096;
/// How large reads grow while each one fills the space it was given. A pipelined batch
/// is then run, and its replies and published messages sent, in a few large steps
/// instead of one per 4 KiB, as Redis reads up to 16 KiB at a time.
const MAX_READ_SIZE: usize = 64 * 1024;

pub struct Connection {
    stream: BufWriter<TcpStream>,
    buffer: BytesMut,
}

/// The server's reading side of a connection; the connection's outbox has the
/// writing side.
pub(crate) struct RequestReader {
    // Read through `AsyncRead`, which assumes the socket is drained after a short read,
    // instead of `readable` and `try_read`, which cost a second `recvfrom` per request.
    socket: OwnedReadHalf,
    buffer: BytesMut,
    /// Space reserved for the next read, between `READ_SIZE` and `MAX_READ_SIZE`.
    read_size: usize,
}

pub(crate) enum ReadRequestError {
    /// The client sent bytes that aren't a valid request. Reply, then close.
    Protocol(ProtocolError),
    /// The connection itself failed.
    Io(anyhow::Error),
}

impl Connection {
    pub fn new(stream: TcpStream) -> Connection {
        Connection {
            // Large enough to hold a typical pipelined batch, so it goes out in one
            // write.
            stream: BufWriter::with_capacity(64 * 1024, stream),
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
                };
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
            Err(FrameError::IncompleteFrame) => Ok(None),
            Err(e) => Err(e.into()),
        }
    }

    pub async fn write_frame(&mut self, frame: Frame) -> Result<(), anyhow::Error> {
        self.write_value(frame).await?;
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
            Frame::Array(response) => {
                self.stream.write_u8(b'*').await?;
                self.write_decimal(response.len() as i64).await?;
                for val in response {
                    Box::pin(self.write_value(val)).await?;
                }
                Ok(())
            }
            Frame::Null => {
                self.stream.write_all(b"$-1\r\n").await?;
                Ok(())
            }
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

impl RequestReader {
    pub(crate) fn new(socket: OwnedReadHalf) -> Self {
        Self {
            socket,
            buffer: BytesMut::with_capacity(READ_SIZE),
            read_size: READ_SIZE,
        }
    }

    /// Reads the next client request. Returns `Ok(None)` once the client closes the
    /// connection. Requests Redis ignores, such as empty arrays, are skipped.
    ///
    /// Cancel safe: it only awaits `read_buf`, so dropping it loses no data.
    pub(crate) async fn read_request(&mut self) -> Result<Option<Vec<Bytes>>, ReadRequestError> {
        let io_error = |error: std::io::Error| ReadRequestError::Io(error.into());
        loop {
            if let Some(args) = self
                .buffered_request()
                .map_err(ReadRequestError::Protocol)?
            {
                return Ok(Some(args));
            }
            // Parsed arguments are copied out, so this reuses the consumed space.
            self.buffer.reserve(self.read_size);
            let space = self.buffer.capacity() - self.buffer.len();
            let read = self
                .socket
                .read_buf(&mut self.buffer)
                .await
                .map_err(io_error)?;
            if read == 0 {
                // A request cut off by the client closing has nobody left to answer.
                return Ok(None);
            }
            self.read_size = if read == space {
                (self.read_size * 2).min(MAX_READ_SIZE)
            } else {
                READ_SIZE
            };
        }
    }

    /// Returns the next request that has already been received in full, without
    /// reading from the socket. Requests Redis ignores, such as empty arrays, are
    /// skipped.
    pub(crate) fn buffered_request(&mut self) -> Result<Option<Vec<Bytes>>, ProtocolError> {
        loop {
            match request::parse(&self.buffer)? {
                Parsed::Request { args, consumed } => {
                    self.buffer.advance(consumed);
                    if !args.is_empty() {
                        return Ok(Some(args));
                    }
                }
                Parsed::Incomplete => return Ok(None),
            }
        }
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
    async fn write_frame_writes_nested_resp_arrays() {
        let (mut connection, mut peer) = connection_pair().await;
        let expected = b"*2\r\n+outer\r\n*2\r\n:1\r\n:2\r\n";

        connection
            .write_frame(Frame::Array(vec![
                Frame::Simple("outer".into()),
                Frame::Array(vec![Frame::Integer(1), Frame::Integer(2)]),
            ]))
            .await
            .unwrap();

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
