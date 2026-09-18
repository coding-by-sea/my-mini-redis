use bytes::{Buf, Bytes};
use std::fmt;
use std::fmt::Formatter;
use std::io::Cursor;

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
        write!(f, "{:?}", self)
    }
}
impl std::error::Error for Error {}

impl Frame {
    // return whether there is at least a complete frame in bytes
    pub(crate) fn check(bytes: &mut Cursor<&[u8]>) -> Result<(), Error> {
        match Self::get_u8(bytes)? {
            // simple string
            b'+' => {
                Self::get_line(bytes)?;
                Ok(())
            }
            // simple error
            b'-' => {
                Self::get_line(bytes)?;
                Ok(())
            }
            // integer
            b':' => {
                Self::get_decimal(bytes)?;
                Ok(())
            }
            // bulk string (binary safe)
            b'$' => {
                if Self::peek_u8(bytes)? == b'-' {
                    Self::skip(bytes, 4)?;
                } else {
                    let length = Self::get_decimal(bytes)?;
                    Self::skip(bytes, length as usize + 2)?;
                }
                Ok(())
            }
            b'*' => {
                if Self::peek_u8(bytes)? == b'-' {
                    Self::skip(bytes, 4)?;
                } else {
                    let length = Self::get_decimal(bytes)?;
                    for _ in 0..length {
                        Self::check(bytes)?;
                    }
                }
                Ok(())
            }
            actual => unimplemented!(
                "the protocol is not implemented for first byte {:?}",
                actual
            ),
        }
    }

    // assuming the bytes has a complete frame
    pub(crate) fn read_frame(bytes: &mut Cursor<&[u8]>) -> Result<Frame, Error> {
        match Self::get_u8(bytes)? {
            // simple string
            b'+' => {
                let result = Self::get_line(bytes)?.to_vec();
                Ok(Frame::Simple(String::from_utf8(result).unwrap()))
            }
            // simple error
            b'-' => {
                let result = Self::get_line(bytes)?.to_vec();
                Ok(Frame::Error(String::from_utf8(result).unwrap()))
            }
            // integer
            b':' => Ok(Frame::Integer(Self::get_decimal(bytes)?)),
            // bulk string (binary safe)
            b'$' => {
                let length = Self::get_decimal(bytes)?;
                if length < 0 {
                    if length != -1 {
                        Err(Error::InvalidEncoding)
                    } else {
                        Ok(Frame::Null)
                    }
                } else {
                    let result = bytes.copy_to_bytes(length as usize);
                    Self::skip(bytes, 2)?; // skip /r/n
                    Ok(Frame::Bulk(result))
                }
            }
            b'*' => {
                let length = Self::get_decimal(bytes)?;
                if length < 0 {
                    if length != -1 {
                        Err(Error::InvalidEncoding)
                    } else {
                        Ok(Frame::Null)
                    }
                } else {
                    let mut result = Vec::with_capacity(length as usize);
                    for _ in 0..length {
                        result.push(Self::read_frame(bytes)?);
                    }
                    Ok(Frame::Array(result))
                }
            }
            actual => unimplemented!(
                "the protocol is not implemented for first byte {:?}",
                actual
            ),
        }
    }

    fn get_line<'a>(bytes: &mut Cursor<&'a [u8]>) -> Result<&'a [u8], Error> {
        let start = bytes.position() as usize;
        let array = *bytes.get_ref();
        let end = array.len() - 1;
        for i in start..end {
            if array[i] == b'\r' && array[i + 1] == b'\n' {
                bytes.set_position(i as u64 + 2);
                return Ok(&array[start..i]);
            }
        }
        Err(Error::IncompleteFrame)
    }

    // get_decimal is safe to use for both length and signed integer because the length case is always in a smaller range
    fn get_decimal(bytes: &mut Cursor<&[u8]>) -> Result<i64, Error> {
        use atoi::atoi;
        let slice = Self::get_line(bytes)?;
        atoi::<i64>(slice).ok_or(Error::InvalidEncoding)
    }

    fn skip(bytes: &mut Cursor<&[u8]>, n: usize) -> Result<(), Error> {
        if n > bytes.remaining() {
            return Err(Error::IncompleteFrame);
        }
        bytes.advance(n);
        Ok(())
    }

    fn get_u8(bytes: &mut Cursor<&[u8]>) -> Result<u8, Error> {
        if bytes.remaining() < 1 {
            return Err(Error::IncompleteFrame);
        }
        Ok(bytes.get_u8())
    }

    fn peek_u8(bytes: &mut Cursor<&[u8]>) -> Result<u8, Error> {
        if bytes.remaining() < 1 {
            return Err(Error::IncompleteFrame);
        }
        Ok(bytes.get_ref()[bytes.position() as usize])
    }
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
            let actual = Frame::read_frame(&mut cursor).unwrap();
            assert_frame_eq(&actual, &expected);
            assert_eq!(cursor.position(), input.len() as u64);
        }
    }

    #[test]
    fn decode_decodes_arrays_and_null_arrays() {
        let input = b"*3\r\n+OK\r\n$5\r\nhello\r\n:7\r\n";
        let mut cursor = Cursor::new(input.as_slice());

        let actual = Frame::read_frame(&mut cursor).unwrap();
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
        assert_frame_eq(&Frame::read_frame(&mut cursor).unwrap(), &Frame::Null);
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
                for (actual, expected) in actual.into_iter().zip(expected) {
                    assert_frame_eq(&actual, &expected);
                }
            }
            _ => panic!("frame variants differ"),
        }
    }
}
