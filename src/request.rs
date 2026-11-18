//! Parses client requests the way Redis does: a RESP array of bulk strings, or an
//! inline command (a plain text line). Replies are parsed by `Frame` instead.

use bytes::{Buf, Bytes, BytesMut};

/// Longest inline request, or `*`/`$` header line, Redis accepts before a newline.
const MAX_INLINE_LEN: usize = 64 * 1024;
/// Largest bulk string Redis accepts in a request (`proto-max-bulk-len`).
const MAX_BULK_LEN: i64 = 512 * 1024 * 1024;

/// The text after `ERR Protocol error: `. The connection must be closed after replying.
#[derive(Debug, PartialEq, Eq)]
pub(crate) struct ProtocolError(pub(crate) String);

/// Parses a connection's requests out of its read buffer.
///
/// Resumable, like Redis's: it consumes from the buffer whatever it has parsed and
/// keeps a partly received request's arguments, so a request that arrives over many
/// reads is parsed once in total. Re-parsing from the start on every read made a
/// request with 300,000 arguments cost 33x as much as parsing it once.
#[derive(Default)]
pub(crate) struct Parser {
    /// A multibulk request whose header has been read but not all its arguments.
    multibulk: Option<Multibulk>,
}

struct Multibulk {
    args: Vec<Bytes>,
    /// Arguments still to come.
    remaining: usize,
    /// The length of the next argument, once its `$` line has been read.
    bulk_len: Option<usize>,
}

impl Parser {
    /// Parses the next request from the front of `buf`, removing the bytes it uses.
    /// Returns `None` until a request is complete; what was parsed is kept for the next
    /// call, which must get the same buffer with more bytes appended. A request Redis
    /// ignores without replying (an empty or null array, a blank inline line) comes back
    /// with no arguments.
    pub(crate) fn parse(
        &mut self,
        buf: &mut BytesMut,
    ) -> Result<Option<Vec<Bytes>>, ProtocolError> {
        if self.multibulk.is_none() {
            match buf.first() {
                None => return Ok(None),
                Some(b'*') => {
                    let Some((line, end)) = header_line(buf, 0, "too big mbulk count string")?
                    else {
                        return Ok(None);
                    };
                    let count = parse_int(line)
                        .filter(|&count| count <= i32::MAX as i64)
                        .ok_or_else(|| ProtocolError("invalid multibulk length".into()))?;
                    buf.advance(end);
                    if count <= 0 {
                        return Ok(Some(Vec::new()));
                    }
                    self.multibulk = Some(Multibulk {
                        // Don't trust the count for preallocation: it comes from the client.
                        args: Vec::with_capacity((count as usize).min(1024)),
                        remaining: count as usize,
                        bulk_len: None,
                    });
                }
                Some(_) => return parse_inline(buf),
            }
        }
        let request = self.multibulk.as_mut().expect("set above");
        while request.remaining > 0 {
            let len = match request.bulk_len {
                Some(len) => len,
                None => {
                    match buf.first() {
                        None => return Ok(None),
                        Some(b'$') => {}
                        Some(&other) => {
                            return Err(ProtocolError(format!(
                                "expected '$', got '{}'",
                                other as char
                            )));
                        }
                    }
                    let Some((line, end)) = header_line(buf, 0, "too big bulk count string")?
                    else {
                        return Ok(None);
                    };
                    let len = parse_int(line)
                        .filter(|&len| (0..=MAX_BULK_LEN).contains(&len))
                        .ok_or_else(|| ProtocolError("invalid bulk length".into()))?
                        as usize;
                    buf.advance(end);
                    request.bulk_len = Some(len);
                    len
                }
            };
            // Like Redis, the two bytes after the data are skipped without checking for CRLF.
            if buf.len() < len + 2 {
                return Ok(None);
            }
            // Copied, even when large: sharing the read buffer's allocation instead kept
            // the whole buffer alive for as long as the value was stored, and made the
            // next read allocate a new one (SET with 64 KiB values: 8% slower).
            request.args.push(Bytes::copy_from_slice(&buf[..len]));
            buf.advance(len + 2);
            request.remaining -= 1;
            request.bulk_len = None;
        }
        Ok(self.multibulk.take().map(|request| request.args))
    }
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

fn parse_inline(buf: &mut BytesMut) -> Result<Option<Vec<Bytes>>, ProtocolError> {
    let Some(newline) = buf.iter().position(|&b| b == b'\n') else {
        if buf.len() > MAX_INLINE_LEN {
            return Err(ProtocolError("too big inline request".into()));
        }
        return Ok(None);
    };
    let line = buf[..newline]
        .strip_suffix(b"\r")
        .unwrap_or(&buf[..newline]);
    let args =
        split_args(line).ok_or_else(|| ProtocolError("unbalanced quotes in request".into()))?;
    buf.advance(newline + 1);
    Ok(Some(args))
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
        while line.get(i).is_some_and(u8::is_ascii_whitespace) {
            i += 1;
        }
        if i == line.len() {
            return Some(args);
        }

        let mut arg = Vec::new();
        let mut quote = Quote::None;
        loop {
            let Some(&c) = line.get(i) else {
                if quote != Quote::None {
                    return None;
                }
                break;
            };
            match quote {
                Quote::None => match c {
                    c if c.is_ascii_whitespace() => break,
                    b'"' => quote = Quote::Double,
                    b'\'' => quote = Quote::Single,
                    _ => arg.push(c),
                },
                Quote::Double => match c {
                    b'\\' => match &line[i + 1..] {
                        [b'x', high, low, ..]
                            if high.is_ascii_hexdigit() && low.is_ascii_hexdigit() =>
                        {
                            arg.push(hex_value(*high) * 16 + hex_value(*low));
                            i += 3;
                        }
                        [escaped, ..] => {
                            arg.push(match escaped {
                                b'n' => b'\n',
                                b'r' => b'\r',
                                b't' => b'\t',
                                b'b' => 0x08,
                                b'a' => 0x07,
                                other => *other,
                            });
                            i += 1;
                        }
                        // A trailing backslash; the quote is left open.
                        [] => arg.push(c),
                    },
                    b'"' => {
                        i = after_closing_quote(line, i)?;
                        break;
                    }
                    _ => arg.push(c),
                },
                Quote::Single => match c {
                    b'\\' if line.get(i + 1) == Some(&b'\'') => {
                        arg.push(b'\'');
                        i += 1;
                    }
                    b'\'' => {
                        i = after_closing_quote(line, i)?;
                        break;
                    }
                    _ => arg.push(c),
                },
            }
            i += 1;
        }
        args.push(Bytes::from(arg));
    }
}

#[derive(PartialEq, Eq)]
enum Quote {
    None,
    Double,
    Single,
}

/// Returns the position after the closing quote at `i`, or `None` if something other
/// than whitespace follows it.
fn after_closing_quote(line: &[u8], i: usize) -> Option<usize> {
    match line.get(i + 1) {
        Some(next) if !next.is_ascii_whitespace() => None,
        _ => Some(i + 1),
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
    use super::{Parser, ProtocolError};
    use bytes::{Bytes, BytesMut};

    #[derive(Debug, PartialEq, Eq)]
    enum Parsed {
        Incomplete,
        Request { args: Vec<Bytes>, consumed: usize },
    }

    /// Parses `input` with a fresh parser, as if it had arrived in one read.
    fn parse(input: &[u8]) -> Result<Parsed, ProtocolError> {
        let mut buf = BytesMut::from(input);
        Ok(match Parser::default().parse(&mut buf)? {
            Some(args) => Parsed::Request {
                args,
                consumed: input.len() - buf.len(),
            },
            None => Parsed::Incomplete,
        })
    }

    /// Feeds `input` to one parser `chunk` bytes at a time, as reads would deliver it,
    /// and returns every request, in order.
    fn parse_in_chunks(input: &[u8], chunk: usize) -> Result<Vec<Vec<Bytes>>, ProtocolError> {
        let (mut parser, mut buf, mut requests) = (Parser::default(), BytesMut::new(), Vec::new());
        for piece in input.chunks(chunk) {
            buf.extend_from_slice(piece);
            while let Some(args) = parser.parse(&mut buf)? {
                requests.push(args);
            }
        }
        assert!(buf.is_empty(), "{} bytes left unparsed", buf.len());
        Ok(requests)
    }

    fn args(args: &[&[u8]]) -> Vec<Bytes> {
        args.iter().map(|arg| Bytes::copy_from_slice(arg)).collect()
    }

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
    fn resumes_a_request_split_at_every_byte() {
        let input =
            b"*3\r\n$3\r\nSET\r\n$1\r\nk\r\n$5\r\nhello\r\nPING\r\n*0\r\n*1\r\n$4\r\nPING\r\n";
        let expected = vec![
            args(&[b"SET", b"k", b"hello"]),
            args(&[b"PING"]),
            Vec::new(),
            args(&[b"PING"]),
        ];
        for chunk in 1..=input.len() {
            assert_eq!(
                parse_in_chunks(input, chunk),
                Ok(expected.clone()),
                "chunk {chunk}"
            );
        }
    }

    #[test]
    fn consumes_complete_arguments_while_waiting_for_the_rest() {
        let mut parser = Parser::default();
        let mut buf = BytesMut::from(&b"*3\r\n$3\r\nSET\r\n$1\r\nk\r\n$5\r\nhel"[..]);
        assert_eq!(parser.parse(&mut buf), Ok(None));
        // Only the unfinished argument's data is left, so nothing is parsed twice.
        assert_eq!(&buf[..], b"hel");
        buf.extend_from_slice(b"lo\r\n");
        assert_eq!(
            parser.parse(&mut buf),
            Ok(Some(args(&[b"SET", b"k", b"hello"])))
        );
    }

    #[test]
    fn large_arguments_arrive_intact() {
        let value: Vec<u8> = (0..100_000).map(|i| (i % 251) as u8).collect();
        let mut input = format!("*3\r\n$3\r\nSET\r\n$1\r\nk\r\n${}\r\n", value.len()).into_bytes();
        input.extend_from_slice(&value);
        input.extend_from_slice(b"\r\n");
        for chunk in [1000, 4096, input.len()] {
            assert_eq!(
                parse_in_chunks(&input, chunk),
                Ok(vec![args(&[b"SET", b"k", &value])]),
                "chunk {chunk}"
            );
        }
    }

    #[test]
    fn many_arguments_split_over_reads() {
        let mut input = b"*20001\r\n$9\r\nSUBSCRIBE\r\n".to_vec();
        let mut expected = vec![Bytes::from_static(b"SUBSCRIBE")];
        for i in 0..20_000 {
            let channel = format!("ch:{i}");
            input.extend_from_slice(format!("${}\r\n{channel}\r\n", channel.len()).as_bytes());
            expected.push(Bytes::from(channel));
        }
        assert_eq!(parse_in_chunks(&input, 4096), Ok(vec![expected]));
    }

    #[test]
    fn rejects_a_bad_argument_that_arrives_later() {
        let (mut parser, mut buf) = (
            Parser::default(),
            BytesMut::from(&b"*2\r\n$3\r\nGET\r\n"[..]),
        );
        assert_eq!(parser.parse(&mut buf), Ok(None));
        buf.extend_from_slice(b":1\r\n");
        assert_eq!(
            parser.parse(&mut buf),
            Err(ProtocolError("expected '$', got ':'".into()))
        );
    }

    #[test]
    fn rejects_an_oversized_inline_request() {
        assert_eq!(
            parse(&vec![b'a'; 70 * 1024]),
            protocol_error("too big inline request")
        );
    }
}
