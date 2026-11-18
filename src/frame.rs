use bytes::{Buf, BufMut, Bytes, BytesMut};
use std::fmt;
use std::fmt::Formatter;
use std::io::Cursor;

#[derive(Debug)]
pub enum Frame {
    Simple(String),
    Error(String),
    Integer(i64),
    Bulk(Bytes),
    Null,
    Array(Vec<Frame>),
}

#[derive(Debug)]
pub enum Error {
    /// Not enough data is available to parse a message
    IncompleteFrame,
    /// Invalid message encoding
    InvalidEncoding,
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Error::IncompleteFrame => "incomplete RESP frame",
            Error::InvalidEncoding => "invalid RESP encoding",
        })
    }
}
impl std::error::Error for Error {}

impl Frame {
    /// Advances `bytes` past one frame without decoding it, so a frame still being
    /// received costs no allocation. Fails with `IncompleteFrame` until all of it has
    /// arrived; `parse` then decodes it from the same start.
    pub(crate) fn check(bytes: &mut Cursor<&[u8]>) -> Result<(), Error> {
        match get_u8(bytes)? {
            b'+' | b'-' => {
                get_line(bytes)?;
            }
            b':' => {
                get_decimal(bytes)?;
            }
            b'$' => {
                if let Some(len) = get_length(bytes)? {
                    skip(bytes, len + 2)?;
                }
            }
            b'*' => {
                if let Some(len) = get_length(bytes)? {
                    for _ in 0..len {
                        Self::check(bytes)?;
                    }
                }
            }
            _ => return Err(Error::InvalidEncoding),
        }
        Ok(())
    }

    /// Decodes one frame, advancing `bytes` past it. Call it after `check` succeeds.
    pub(crate) fn parse(bytes: &mut Cursor<&[u8]>) -> Result<Frame, Error> {
        Ok(match get_u8(bytes)? {
            b'+' => Frame::Simple(get_string(bytes)?),
            b'-' => Frame::Error(get_string(bytes)?),
            b':' => Frame::Integer(get_decimal(bytes)?),
            b'$' => match get_length(bytes)? {
                None => Frame::Null,
                Some(len) => {
                    if bytes.remaining() < len + 2 {
                        return Err(Error::IncompleteFrame);
                    }
                    let data = bytes.copy_to_bytes(len);
                    skip(bytes, 2)?; // CRLF
                    Frame::Bulk(data)
                }
            },
            b'*' => match get_length(bytes)? {
                None => Frame::Null,
                Some(len) => Frame::Array(
                    (0..len)
                        .map(|_| Self::parse(bytes))
                        .collect::<Result<_, _>>()?,
                ),
            },
            _ => return Err(Error::InvalidEncoding),
        })
    }

    /// Appends the RESP encoding of this frame to `out`.
    pub(crate) fn encode(&self, out: &mut BytesMut) {
        match self {
            Frame::Simple(text) => {
                out.put_u8(b'+');
                out.put_slice(text.as_bytes());
                out.put_slice(b"\r\n");
            }
            Frame::Error(text) => {
                out.put_u8(b'-');
                out.put_slice(text.as_bytes());
                out.put_slice(b"\r\n");
            }
            Frame::Integer(value) => {
                out.put_u8(b':');
                put_decimal(out, *value);
            }
            Frame::Bulk(bytes) => {
                out.put_u8(b'$');
                put_decimal(out, bytes.len() as i64);
                out.put_slice(bytes);
                out.put_slice(b"\r\n");
            }
            Frame::Null => out.put_slice(b"$-1\r\n"),
            Frame::Array(frames) => {
                out.put_u8(b'*');
                put_decimal(out, frames.len() as i64);
                for frame in frames {
                    frame.encode(out);
                }
            }
        }
    }
}

/// Reads up to the next CRLF and returns the line without it.
fn get_line<'a>(bytes: &mut Cursor<&'a [u8]>) -> Result<&'a [u8], Error> {
    let start = bytes.position() as usize;
    let buf = *bytes.get_ref();
    let Some(len) = buf[start..].windows(2).position(|pair| pair == b"\r\n") else {
        return Err(Error::IncompleteFrame);
    };
    bytes.set_position((start + len + 2) as u64);
    Ok(&buf[start..start + len])
}

/// Reads a line of UTF-8 text, as in simple strings and errors.
fn get_string(bytes: &mut Cursor<&[u8]>) -> Result<String, Error> {
    let line = get_line(bytes)?;
    String::from_utf8(line.to_vec()).map_err(|_| Error::InvalidEncoding)
}

/// Reads a line holding a signed decimal integer.
fn get_decimal(bytes: &mut Cursor<&[u8]>) -> Result<i64, Error> {
    atoi::atoi(get_line(bytes)?).ok_or(Error::InvalidEncoding)
}

/// Reads a bulk string's or array's length: `None` for -1, which RESP2 uses for null.
fn get_length(bytes: &mut Cursor<&[u8]>) -> Result<Option<usize>, Error> {
    match get_decimal(bytes)? {
        -1 => Ok(None),
        len => usize::try_from(len)
            .map(Some)
            .map_err(|_| Error::InvalidEncoding),
    }
}

fn get_u8(bytes: &mut Cursor<&[u8]>) -> Result<u8, Error> {
    if !bytes.has_remaining() {
        return Err(Error::IncompleteFrame);
    }
    Ok(bytes.get_u8())
}

fn skip(bytes: &mut Cursor<&[u8]>, n: usize) -> Result<(), Error> {
    if bytes.remaining() < n {
        return Err(Error::IncompleteFrame);
    }
    bytes.advance(n);
    Ok(())
}

/// Appends `value` in decimal, then CRLF.
fn put_decimal(out: &mut BytesMut, value: i64) {
    use std::fmt::Write;
    // Writing to a `BytesMut` can't fail.
    let _ = write!(out, "{value}\r\n");
}

#[cfg(test)]
mod tests {
    use super::{Error, Frame};
    use bytes::Bytes;
    use std::io::Cursor;

    #[test]
    fn check_accepts_complete_frames() {
        for input in [
            b"+OK\r\n".as_slice(),
            b"-ERR unknown command\r\n",
            b":42\r\n",
            b"$5\r\nhello\r\n",
            b"$-1\r\n",
            b"*-1\r\n",
            b"*2\r\n+OK\r\n:1\r\n",
        ] {
            let mut cursor = Cursor::new(input);
            assert!(Frame::check(&mut cursor).is_ok(), "{input:?}");
            assert_eq!(cursor.position(), input.len() as u64);
        }
    }

    #[test]
    fn check_rejects_incomplete_frames() {
        for input in [
            b"+OK\r".as_slice(),
            b":42",
            b"$5\r\nhell",
            b"$-1\r",
            b"*2\r\n+OK\r\n:1",
        ] {
            let mut cursor = Cursor::new(input);
            assert!(
                matches!(Frame::check(&mut cursor), Err(Error::IncompleteFrame)),
                "{input:?}"
            );
        }
    }

    #[test]
    fn decode_decodes_scalar_frames() {
        let cases = [
            (b"+OK\r\n".as_slice(), Frame::Simple("OK".into())),
            (
                b"-ERR unknown command\r\n",
                Frame::Error("ERR unknown command".into()),
            ),
            (b":-42\r\n", Frame::Integer(-42)),
            (
                b"$5\r\nhello\r\n",
                Frame::Bulk(Bytes::from_static(b"hello")),
            ),
            (b"$-1\r\n", Frame::Null),
        ];

        for (input, expected) in cases {
            let mut cursor = Cursor::new(input);
            let actual = Frame::parse(&mut cursor).unwrap();
            assert_frame_eq(&actual, &expected);
            assert_eq!(cursor.position(), input.len() as u64);
        }
    }

    #[test]
    fn encode_writes_resp() {
        let frame = Frame::Array(vec![
            Frame::Simple("OK".into()),
            Frame::Error("ERR bad".into()),
            Frame::Integer(-7),
            Frame::Bulk(Bytes::from_static(b"hi")),
            Frame::Null,
            Frame::Array(vec![]),
        ]);
        let mut out = bytes::BytesMut::new();
        frame.encode(&mut out);
        assert_eq!(
            &out[..],
            b"*6\r\n+OK\r\n-ERR bad\r\n:-7\r\n$2\r\nhi\r\n$-1\r\n*0\r\n"
        );
    }

    #[test]
    fn decode_decodes_arrays_and_null_arrays() {
        let input = b"*3\r\n+OK\r\n$5\r\nhello\r\n:7\r\n";
        let mut cursor = Cursor::new(input.as_slice());

        let actual = Frame::parse(&mut cursor).unwrap();
        assert_frame_eq(
            &actual,
            &Frame::Array(vec![
                Frame::Simple("OK".into()),
                Frame::Bulk(Bytes::from_static(b"hello")),
                Frame::Integer(7),
            ]),
        );
        assert_eq!(cursor.position(), input.len() as u64);

        let mut cursor = Cursor::new(b"*-1\r\n".as_slice());
        assert_frame_eq(&Frame::parse(&mut cursor).unwrap(), &Frame::Null);
    }

    #[test]
    fn rejects_unknown_type_bytes_and_negative_lengths_other_than_minus_one() {
        for input in [b"?OK\r\n".as_slice(), b"$-2\r\n", b"*-2\r\n", b":abc\r\n"] {
            assert!(
                matches!(
                    Frame::check(&mut Cursor::new(input)),
                    Err(Error::InvalidEncoding)
                ),
                "check {input:?}"
            );
            assert!(
                matches!(
                    Frame::parse(&mut Cursor::new(input)),
                    Err(Error::InvalidEncoding)
                ),
                "parse {input:?}"
            );
        }
    }

    #[test]
    fn parse_reports_an_incomplete_bulk_string_instead_of_panicking() {
        let mut cursor = Cursor::new(b"$5\r\nhel".as_slice());
        assert!(matches!(
            Frame::parse(&mut cursor),
            Err(Error::IncompleteFrame)
        ));
    }

    fn assert_frame_eq(actual: &Frame, expected: &Frame) {
        match (actual, expected) {
            (Frame::Simple(actual), Frame::Simple(expected))
            | (Frame::Error(actual), Frame::Error(expected)) => assert_eq!(actual, expected),
            (Frame::Integer(actual), Frame::Integer(expected)) => assert_eq!(actual, expected),
            (Frame::Bulk(actual), Frame::Bulk(expected)) => assert_eq!(actual, expected),
            (Frame::Null, Frame::Null) => {}
            (Frame::Array(actual), Frame::Array(expected)) => {
                assert_eq!(actual.len(), expected.len());
                for (actual, expected) in actual.iter().zip(expected) {
                    assert_frame_eq(actual, expected);
                }
            }
            _ => panic!("frame variants differ"),
        }
    }
}
