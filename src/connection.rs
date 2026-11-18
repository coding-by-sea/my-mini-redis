use super::frame::{self, Frame};
use super::request::{Parser, ProtocolError};
use bytes::{Buf, Bytes, BytesMut};
use std::io::{Cursor, Error, ErrorKind};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::net::tcp::OwnedReadHalf;

/// Space for the first read of a request batch.
const READ_SIZE: usize = 4096;
/// How large reads grow while each one fills the space it was given. A pipelined batch
/// is then run, and its replies and published messages sent, in a few large steps
/// instead of one per 4 KiB, as Redis reads up to 16 KiB at a time.
const MAX_READ_SIZE: usize = 64 * 1024;
/// A client's write buffer larger than this, grown by one large frame, is freed after
/// the write rather than kept for the life of the connection.
const WRITE_BUFFER_KEEP_LIMIT: usize = 64 * 1024;

/// The client's side of a connection: writes frames and reads replies.
pub struct Connection {
    stream: TcpStream,
    /// Bytes received but not yet parsed into a frame.
    buffer: BytesMut,
    /// Reused to encode each outgoing frame, so it goes out in one write.
    write_buffer: BytesMut,
}

/// The server's reading side of a connection; the connection's outbox has the
/// writing side.
pub(crate) struct RequestReader {
    // Read through `AsyncRead`, which assumes the socket is drained after a short read,
    // instead of `readable` and `try_read`, which cost a second `recvfrom` per request.
    socket: OwnedReadHalf,
    buffer: BytesMut,
    /// Keeps a partly received request between reads.
    parser: Parser,
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
            stream,
            buffer: BytesMut::with_capacity(READ_SIZE),
            write_buffer: BytesMut::with_capacity(READ_SIZE),
        }
    }

    /// Reads the next frame. Returns `Ok(None)` if the server closed the connection
    /// between frames; closing in the middle of one is an error.
    ///
    /// Cancel safe: it only awaits `read_buf`, so dropping it loses no data.
    pub async fn read_frame(&mut self) -> anyhow::Result<Option<Frame>> {
        loop {
            if let Some(frame) = self.parse_frame()? {
                return Ok(Some(frame));
            }
            if self.stream.read_buf(&mut self.buffer).await? == 0 {
                return if self.buffer.is_empty() {
                    Ok(None)
                } else {
                    Err(Error::from(ErrorKind::ConnectionReset).into())
                };
            }
        }
    }

    /// Takes the first frame out of `buffer` if it has arrived in full.
    fn parse_frame(&mut self) -> Result<Option<Frame>, frame::Error> {
        let mut cursor = Cursor::new(&self.buffer[..]);
        match Frame::check(&mut cursor) {
            Ok(()) => {
                let len = cursor.position() as usize;
                cursor.set_position(0);
                let frame = Frame::parse(&mut cursor)?;
                self.buffer.advance(len);
                Ok(Some(frame))
            }
            Err(frame::Error::IncompleteFrame) => Ok(None),
            Err(error) => Err(error),
        }
    }

    pub async fn write_frame(&mut self, frame: Frame) -> anyhow::Result<()> {
        self.write_buffer.clear();
        frame.encode(&mut self.write_buffer);
        self.stream.write_all(&self.write_buffer).await?;
        if self.write_buffer.capacity() > WRITE_BUFFER_KEEP_LIMIT {
            self.write_buffer = BytesMut::with_capacity(READ_SIZE);
        }
        Ok(())
    }
}

impl RequestReader {
    pub(crate) fn new(socket: OwnedReadHalf) -> Self {
        Self {
            socket,
            buffer: BytesMut::with_capacity(READ_SIZE),
            parser: Parser::default(),
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
            match self.parser.parse(&mut self.buffer)? {
                Some(args) if args.is_empty() => {}
                request => return Ok(request),
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
