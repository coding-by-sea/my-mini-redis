use crate::frame::Frame;
use bytes::Bytes;
use std::vec;
use thiserror::Error;

pub(crate) struct Parse {
    parts: vec::IntoIter<Frame>,
}

#[derive(Debug, Error)]
pub(crate) enum ParseError {
    #[error("invalid command")]
    InvalidCommand,
    #[error("end of input while parsing")]
    EndOfInput,
}

pub(crate) type ParseResult<T> = Result<T, ParseError>;

impl Parse {
    pub(crate) fn new(frame: Frame) -> anyhow::Result<Self> {
        if let Frame::Array(array) = frame {
            Ok(Parse {
                parts: array.into_iter(),
            })
        } else {
            anyhow::bail!("parse can only be applied to an array frame");
        }
    }

    pub(crate) fn next(&mut self) -> ParseResult<Frame> {
        match self.parts.next() {
            Some(frame) => Ok(frame),
            None => Err(ParseError::EndOfInput.into()),
        }
    }

    pub(crate) fn next_string(&mut self) -> ParseResult<String> {
        match self.parts.next() {
            Some(frame) => match frame {
                Frame::Simple(s) => Ok(s),
                Frame::Bulk(bytes) => Ok(String::from_utf8(bytes.to_vec()).unwrap()),
                _ => Err(ParseError::InvalidCommand.into()),
            },
            None => Err(ParseError::EndOfInput.into()),
        }
    }

    pub(crate) fn next_bytes(&mut self) -> ParseResult<Bytes> {
        match self.parts.next() {
            Some(frame) => match frame {
                Frame::Bulk(bytes) => Ok(bytes),
                _ => Err(ParseError::InvalidCommand.into()),
            },
            None => Err(ParseError::EndOfInput.into()),
        }
    }
}
