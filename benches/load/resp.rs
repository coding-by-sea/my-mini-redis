//! A minimal RESP encoder, reply parser and connection for the raw scenarios.

use bytes::{Buf, BytesMut};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

pub(crate) fn encode(args: &[&[u8]], out: &mut Vec<u8>) {
    out.extend_from_slice(format!("*{}\r\n", args.len()).as_bytes());
    for arg in args {
        out.extend_from_slice(format!("${}\r\n", arg.len()).as_bytes());
        out.extend_from_slice(arg);
        out.extend_from_slice(b"\r\n");
    }
}

/// A parsed reply that borrows from the read buffer.
pub(crate) enum Reply<'a> {
    Simple,
    Error(&'a [u8]),
    Integer,
    Bulk(Option<&'a [u8]>),
    Array(Vec<Reply<'a>>),
}

impl Reply<'_> {
    pub(crate) fn ok(self) -> anyhow::Result<()> {
        match self {
            Reply::Error(message) => {
                anyhow::bail!("server error: {}", String::from_utf8_lossy(message))
            }
            _ => Ok(()),
        }
    }

    /// The send timestamp in a `[message, channel, payload]` push.
    pub(crate) fn message_timestamp(self) -> anyhow::Result<u64> {
        match self {
            Reply::Array(parts) => match parts.as_slice() {
                [Reply::Bulk(Some(b"message")), _, Reply::Bulk(Some(payload))]
                    if payload.len() >= 8 =>
                {
                    Ok(u64::from_le_bytes(payload[..8].try_into().unwrap()))
                }
                _ => anyhow::bail!("expected a pub/sub message"),
            },
            other => other
                .ok()
                .and_then(|()| anyhow::bail!("expected a pub/sub message")),
        }
    }
}

/// Parses one reply at the start of `buf`; `None` if it isn't complete yet.
fn parse(buf: &[u8]) -> anyhow::Result<Option<(Reply<'_>, usize)>> {
    let Some(line_end) = buf.windows(2).position(|w| w == b"\r\n") else {
        return Ok(None);
    };
    let line = &buf[1..line_end];
    let after = line_end + 2;
    let number = || -> anyhow::Result<i64> { Ok(std::str::from_utf8(line)?.parse()?) };
    Ok(Some(match buf[0] {
        b'+' => (Reply::Simple, after),
        b'-' => (Reply::Error(line), after),
        b':' => (Reply::Integer, after),
        b'$' => match number()? {
            len if len < 0 => (Reply::Bulk(None), after),
            len => {
                let end = after + len as usize;
                if buf.len() < end + 2 {
                    return Ok(None);
                }
                (Reply::Bulk(Some(&buf[after..end])), end + 2)
            }
        },
        b'*' => {
            let count = number()?.max(0) as usize;
            let mut items = Vec::with_capacity(count);
            let mut position = after;
            for _ in 0..count {
                let Some((item, len)) = parse(&buf[position..])? else {
                    return Ok(None);
                };
                items.push(item);
                position += len;
            }
            (Reply::Array(items), position)
        }
        other => anyhow::bail!("unexpected reply type byte {:?}", other as char),
    }))
}

pub(crate) struct RawConnection {
    stream: TcpStream,
    buffer: BytesMut,
}

impl RawConnection {
    pub(crate) async fn connect(address: &str) -> anyhow::Result<Self> {
        let stream = TcpStream::connect(address).await?;
        stream.set_nodelay(true)?;
        Ok(Self {
            stream,
            buffer: BytesMut::with_capacity(64 * 1024),
        })
    }

    pub(crate) async fn send(&mut self, bytes: &[u8]) -> anyhow::Result<()> {
        self.stream.write_all(bytes).await?;
        Ok(())
    }

    /// Reads the next reply and passes it to `handle`; `None` once the server closes
    /// the connection. Cancel safe: it only awaits `read_buf`.
    pub(crate) async fn reply<T>(
        &mut self,
        handle: impl FnOnce(Reply<'_>) -> T,
    ) -> anyhow::Result<Option<T>> {
        loop {
            if let Some((reply, len)) = parse(&self.buffer)? {
                let result = handle(reply);
                self.buffer.advance(len);
                return Ok(Some(result));
            }
            if self.stream.read_buf(&mut self.buffer).await? == 0 {
                return Ok(None);
            }
        }
    }

    pub(crate) async fn expect_reply<T>(
        &mut self,
        handle: impl FnOnce(Reply<'_>) -> anyhow::Result<T>,
    ) -> anyhow::Result<T> {
        match self.reply(handle).await? {
            Some(result) => result,
            None => anyhow::bail!("server closed the connection"),
        }
    }
}
