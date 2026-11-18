//! Parses client requests the way Redis does: a RESP array of bulk strings, or an
//! inline command (a plain text line). Replies are parsed by `Frame` instead.

use bytes::Bytes;

/// Longest inline request, or `*`/`$` header line, Redis accepts before a newline.
const MAX_INLINE_LEN: usize = 64 * 1024;
/// Largest bulk string Redis accepts in a request (`proto-max-bulk-len`).
const MAX_BULK_LEN: i64 = 512 * 1024 * 1024;

#[derive(Debug, PartialEq, Eq)]
pub(crate) enum Parsed {
    /// Not enough bytes yet.
    Incomplete,
    /// `consumed` bytes held a request. `args` is empty for requests Redis ignores
    /// without replying: empty or null arrays, and blank inline lines.
    Request { args: Vec<Bytes>, consumed: usize },
}

/// The text after `ERR Protocol error: `. The connection must be closed after replying.
#[derive(Debug, PartialEq, Eq)]
pub(crate) struct ProtocolError(pub(crate) String);

pub(crate) fn parse(buf: &[u8]) -> Result<Parsed, ProtocolError> {
    match buf.first() {
        None => Ok(Parsed::Incomplete),
        Some(b'*') => parse_multibulk(buf),
        Some(_) => parse_inline(buf),
    }
}

fn parse_multibulk(buf: &[u8]) -> Result<Parsed, ProtocolError> {
    let Some((line, mut pos)) = header_line(buf, 0, "too big mbulk count string")? else {
        return Ok(Parsed::Incomplete);
    };
    let count = parse_int(line)
        .filter(|&count| count <= i32::MAX as i64)
        .ok_or_else(|| ProtocolError("invalid multibulk length".into()))?;
    if count <= 0 {
        return Ok(Parsed::Request {
            args: Vec::new(),
            consumed: pos,
        });
    }

    // Don't trust the count for preallocation: it comes from the client.
    let mut args = Vec::with_capacity((count as usize).min(1024));
    for _ in 0..count {
        match buf.get(pos) {
            None => return Ok(Parsed::Incomplete),
            Some(b'$') => {}
            Some(&other) => {
                return Err(ProtocolError(format!(
                    "expected '$', got '{}'",
                    other as char
                )));
            }
        }
        let Some((line, start)) = header_line(buf, pos, "too big bulk count string")? else {
            return Ok(Parsed::Incomplete);
        };
        let len = parse_int(line)
            .filter(|&len| (0..=MAX_BULK_LEN).contains(&len))
            .ok_or_else(|| ProtocolError("invalid bulk length".into()))? as usize;
        // Like Redis, the two bytes after the data are skipped without checking for CRLF.
        if buf.len() < start + len + 2 {
            return Ok(Parsed::Incomplete);
        }
        args.push(Bytes::copy_from_slice(&buf[start..start + len]));
        pos = start + len + 2;
    }
    Ok(Parsed::Request {
        args,
        consumed: pos,
    })
}

/// Returns the text of the `*`/`$` line starting at `start` (without its type byte or
/// CRLF) and the position after it, or `None` if the line isn't complete yet.
fn header_line<'a>(
    buf: &'a [u8],
    start: usize,
    too_big: &str,
) -> Result<Option<(&'a [u8], usize)>, ProtocolError> {
    match find_crlf(&buf[start..]) {
        Some(end) => Ok(Some((&buf[start + 1..start + end], start + end + 2))),
        None if buf.len() - start > MAX_INLINE_LEN => Err(ProtocolError(too_big.into())),
        None => Ok(None),
    }
}

fn find_crlf(buf: &[u8]) -> Option<usize> {
    buf.windows(2).position(|window| window == b"\r\n")
}

/// Strict decimal parsing: an optional `-` and digits only, like Redis's `string2ll`.
fn parse_int(text: &[u8]) -> Option<i64> {
    let digits = text.strip_prefix(b"-").unwrap_or(text);
    if digits.is_empty() || !digits.iter().all(u8::is_ascii_digit) {
        return None;
    }
    std::str::from_utf8(text).ok()?.parse().ok()
}

fn parse_inline(buf: &[u8]) -> Result<Parsed, ProtocolError> {
    let Some(newline) = buf.iter().position(|&b| b == b'\n') else {
        if buf.len() > MAX_INLINE_LEN {
            return Err(ProtocolError("too big inline request".into()));
        }
        return Ok(Parsed::Incomplete);
    };
    let line = buf[..newline]
        .strip_suffix(b"\r")
        .unwrap_or(&buf[..newline]);
    let args =
        split_args(line).ok_or_else(|| ProtocolError("unbalanced quotes in request".into()))?;
    Ok(Parsed::Request {
        args,
        consumed: newline + 1,
    })
}

/// Splits an inline command into arguments like Redis's `sdssplitargs`: whitespace
/// separates arguments, and double or single quotes group them. Double quotes support
/// `\n`, `\r`, `\t`, `\b`, `\a` and `\xHH` escapes; single quotes only `\'`. A closing
/// quote must be followed by whitespace or the end of the line. Returns `None` for
/// unbalanced quotes.
fn split_args(line: &[u8]) -> Option<Vec<Bytes>> {
    let mut args = Vec::new();
    let mut i = 0;
    loop {
        while i < line.len() && line[i].is_ascii_whitespace() {
            i += 1;
        }
        if i == line.len() {
            return Some(args);
        }

        let mut arg = Vec::new();
        let mut in_double = false;
        let mut in_single = false;
        loop {
            let Some(&c) = line.get(i) else {
                if in_double || in_single {
                    return None;
                }
                break;
            };
            if in_double {
                match c {
                    b'\\'
                        if i + 3 < line.len()
                            && line[i + 1] == b'x'
                            && line[i + 2].is_ascii_hexdigit()
                            && line[i + 3].is_ascii_hexdigit() =>
                    {
                        arg.push(hex_value(line[i + 2]) * 16 + hex_value(line[i + 3]));
                        i += 3;
                    }
                    b'\\' if i + 1 < line.len() => {
                        i += 1;
                        arg.push(match line[i] {
                            b'n' => b'\n',
                            b'r' => b'\r',
                            b't' => b'\t',
                            b'b' => 0x08,
                            b'a' => 0x07,
                            other => other,
                        });
                    }
                    b'"' => {
                        if line
                            .get(i + 1)
                            .is_some_and(|next| !next.is_ascii_whitespace())
                        {
                            return None;
                        }
                        i += 1;
                        break;
                    }
                    _ => arg.push(c),
                }
            } else if in_single {
                match c {
                    b'\\' if line.get(i + 1) == Some(&b'\'') => {
                        i += 1;
                        arg.push(b'\'');
                    }
                    b'\'' => {
                        if line
                            .get(i + 1)
                            .is_some_and(|next| !next.is_ascii_whitespace())
                        {
                            return None;
                        }
                        i += 1;
                        break;
                    }
                    _ => arg.push(c),
                }
            } else {
                match c {
                    c if c.is_ascii_whitespace() => break,
                    b'"' => in_double = true,
                    b'\'' => in_single = true,
                    _ => arg.push(c),
                }
            }
            i += 1;
        }
        args.push(Bytes::from(arg));
    }
}

fn hex_value(digit: u8) -> u8 {
    match digit {
        b'0'..=b'9' => digit - b'0',
        b'a'..=b'f' => digit - b'a' + 10,
        _ => digit - b'A' + 10,
    }
}

#[cfg(test)]
mod tests {
    use super::{Parsed, ProtocolError, parse};

    fn request(args: &[&str], consumed: usize) -> Result<Parsed, ProtocolError> {
        Ok(Parsed::Request {
            args: args
                .iter()
                .map(|arg| arg.as_bytes().to_vec().into())
                .collect(),
            consumed,
        })
    }

    fn protocol_error(message: &str) -> Result<Parsed, ProtocolError> {
        Err(ProtocolError(message.into()))
    }

    #[test]
    fn parses_a_multibulk_request() {
        let input = b"*2\r\n$3\r\nGET\r\n$1\r\nk\r\nleftover";
        assert_eq!(parse(input), request(&["GET", "k"], input.len() - 8));
    }

    #[test]
    fn waits_for_the_rest_of_a_multibulk_request() {
        let input = b"*2\r\n$3\r\nGET\r\n$1\r\nk\r\n";
        for end in 0..input.len() {
            assert_eq!(
                parse(&input[..end]),
                Ok(Parsed::Incomplete),
                "prefix of {end} bytes"
            );
        }
    }

    #[test]
    fn empty_and_null_arrays_are_requests_without_arguments() {
        assert_eq!(parse(b"*0\r\n"), request(&[], 4));
        assert_eq!(parse(b"*-1\r\n"), request(&[], 5));
    }

    #[test]
    fn rejects_malformed_multibulk_requests() {
        assert_eq!(
            parse(b"*abc\r\n"),
            protocol_error("invalid multibulk length")
        );
        assert_eq!(
            parse(b"*1\r\n$abc\r\n"),
            protocol_error("invalid bulk length")
        );
        assert_eq!(
            parse(b"*1\r\n$-5\r\n"),
            protocol_error("invalid bulk length")
        );
        assert_eq!(
            parse(b"*1\r\n!3\r\nfoo\r\n"),
            protocol_error("expected '$', got '!'")
        );
        assert_eq!(
            parse(b"*1\r\n:1\r\n"),
            protocol_error("expected '$', got ':'")
        );
    }

    #[test]
    fn rejects_an_oversized_header_line() {
        let mut input = b"*".to_vec();
        input.resize(70 * 1024, b'1');
        assert_eq!(parse(&input), protocol_error("too big mbulk count string"));
    }

    #[test]
    fn parses_inline_requests() {
        assert_eq!(parse(b"GET k\r\n"), request(&["GET", "k"], 7));
        assert_eq!(parse(b"  GET   k  \n"), request(&["GET", "k"], 12));
        assert_eq!(parse(b"\r\n"), request(&[], 2));
        assert_eq!(parse(b"GET k"), Ok(Parsed::Incomplete));
    }

    #[test]
    fn inline_quotes_and_escapes() {
        assert_eq!(
            parse(b"SET k \"hello world\\n\"\r\n"),
            request(&["SET", "k", "hello world\n"], 23)
        );
        assert_eq!(
            parse(b"SET k \"\\x41\\tB\"\n"),
            request(&["SET", "k", "A\tB"], 16)
        );
        assert_eq!(
            parse(b"SET k 'it\\'s'\n"),
            request(&["SET", "k", "it's"], 14)
        );
        assert_eq!(
            parse(b"SET k a\"b c\"\n"),
            request(&["SET", "k", "ab c"], 13)
        );
        assert_eq!(
            parse(b"SET k \"\\r\\b\\a\\q\\x4a\\x4A\"\n"),
            request(&["SET", "k", "\r\x08\x07qJJ"], 25)
        );
    }

    #[test]
    fn rejects_unbalanced_inline_quotes() {
        let unbalanced = protocol_error("unbalanced quotes in request");
        assert_eq!(parse(b"SET k \"abc\"def\r\n"), unbalanced);
        assert_eq!(parse(b"SET k \"unterminated\r\n"), unbalanced);
        assert_eq!(parse(b"SET k 'unterminated\n"), unbalanced);
        assert_eq!(parse(b"SET k 'abc'def\n"), unbalanced);
    }

    #[test]
    fn rejects_an_oversized_inline_request() {
        assert_eq!(
            parse(&vec![b'a'; 70 * 1024]),
            protocol_error("too big inline request")
        );
    }
}
