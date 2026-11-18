//! Compatibility tests for the "Correctness and compatibility bugs" list (section 1 of the
//! gap analysis). Each test talks raw RESP over TCP so it checks the exact bytes a real Redis
//! client would see, independent of this crate's own `Client`.
//!
//! These tests describe Redis's behavior, and this project's server passes all of them.
//!
//! # Two ways to run
//!
//! **Against this project (default).** Each test starts its own in-process server on a
//! random port, so tests run in parallel and share nothing:
//!
//! ```text
//! cargo test --test redis_compat
//! ```
//!
//! **Against a real Redis (checks the tests themselves).** Set `REDIS_COMPAT_TARGET` to a
//! running server's `host:port`. Every test should pass there. If one fails, its
//! expectation is wrong, not the server.
//!
//! ```text
//! redis-server --port 47392 --save "" --appendonly no --daemonize yes
//! REDIS_COMPAT_TARGET=127.0.0.1:47392 cargo test --test redis_compat
//! redis-cli -p 47392 shutdown nosave
//! ```
//!
//! In this mode:
//! - Each test runs `FLUSHALL` first, which **deletes all data** on the target. Only point
//!   it at a throwaway server, never one whose data you need.
//! - Tests run one at a time (a shared lock in `start_server`), because they reuse key and
//!   channel names on the same server.
//! - `server_survives_running_out_of_file_descriptors` (1.8) skips itself, since it starts
//!   this project's server binary. It is also Unix only in either mode, since it limits
//!   file descriptors with `ulimit`.
//! - The in-crate 1.9 tests in `src/server.rs` are not covered; they inspect this
//!   project's internal state.
//!
//! CI's `redis-compat` job runs this file against Redis 7.2.5 on every pull request; all
//! tests pass there.

use std::future::Future;
use std::io;
use std::pin::Pin;
use std::sync::{Mutex, MutexGuard};

use my_redis::server::Server;
use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader};
use tokio::net::tcp::{OwnedReadHalf, OwnedWriteHalf};
use tokio::net::{TcpListener, TcpStream};
use tokio::task::JoinHandle;
use tokio::time::{Duration, Instant, sleep, timeout};

const REPLY_TIMEOUT: Duration = Duration::from_secs(5);

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

#[derive(Debug, PartialEq, Eq)]
enum Reply {
    Simple(String),
    Error(String),
    Integer(i64),
    Bulk(Vec<u8>),
    Null,
    NullArray,
    Array(Vec<Reply>),
}

fn bulk(bytes: impl AsRef<[u8]>) -> Reply {
    Reply::Bulk(bytes.as_ref().to_vec())
}

fn ok() -> Reply {
    Reply::Simple("OK".into())
}

/// A pub/sub push such as `[subscribe, channel, count]` or `[message, channel, payload]`.
fn push(kind: &str, channel: impl AsRef<[u8]>, last: Reply) -> Reply {
    Reply::Array(vec![bulk(kind), bulk(channel), last])
}

/// `host:port` of an existing server to test instead of this project's.
/// See "Two ways to run" at the top of this file.
const TARGET_ENV: &str = "REDIS_COMPAT_TARGET";

fn external_target() -> Option<String> {
    std::env::var(TARGET_ENV)
        .ok()
        .filter(|address| !address.is_empty())
}

/// The server a test talks to. Dropping it (or calling `abort`) stops our own server.
struct TestServer {
    task: Option<JoinHandle<()>>,
    _serial: Option<MutexGuard<'static, ()>>,
}

impl TestServer {
    fn abort(&self) {
        if let Some(task) = &self.task {
            task.abort();
        }
    }
}

impl Drop for TestServer {
    fn drop(&mut self) {
        self.abort();
    }
}

// Against a real Redis, the returned guard is held for the whole test on purpose, so
// tests that share key and channel names run one at a time.
#[allow(clippy::await_holding_lock)]
async fn start_server() -> (String, TestServer) {
    if let Some(address) = external_target() {
        static SERIAL: Mutex<()> = Mutex::new(());
        let guard = SERIAL
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let mut client = RawClient::connect(&address).await;
        assert_eq!(client.cmd(&[b"FLUSHALL"]).await, ok());
        let server = TestServer {
            task: None,
            _serial: Some(guard),
        };
        return (address, server);
    }

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap().to_string();
    let task = tokio::spawn(async move {
        Server::new().run_with_listener(listener).await;
    });
    let server = TestServer {
        task: Some(task),
        _serial: None,
    };
    (address, server)
}

struct RawClient {
    reader: BufReader<OwnedReadHalf>,
    writer: OwnedWriteHalf,
}

impl RawClient {
    async fn connect(address: &str) -> Self {
        Self::from_stream(TcpStream::connect(address).await.unwrap())
    }

    fn from_stream(stream: TcpStream) -> Self {
        let (reader, writer) = stream.into_split();
        Self {
            reader: BufReader::new(reader),
            writer,
        }
    }

    async fn send_raw(&mut self, bytes: &[u8]) {
        self.writer.write_all(bytes).await.unwrap();
        self.writer.flush().await.unwrap();
    }

    /// Sends a command as a RESP array of bulk strings, without waiting for a reply.
    async fn send(&mut self, args: &[&[u8]]) {
        self.send_raw(&encode(args)).await;
    }

    /// Sends a command and returns its reply.
    async fn cmd(&mut self, args: &[&[u8]]) -> Reply {
        self.send(args).await;
        self.read().await
    }

    /// Publishes and discards the reply, for tests that are not about PUBLISH's reply format (1.2).
    async fn publish(&mut self, channel: &[u8], message: &[u8]) {
        self.cmd(&[b"PUBLISH", channel, message]).await;
    }

    async fn read(&mut self) -> Reply {
        timeout(REPLY_TIMEOUT, read_reply(&mut self.reader))
            .await
            .expect("server did not reply in time")
            .expect("failed to read reply")
    }

    async fn assert_error_starting_with(&mut self, prefix: &str) {
        match self.read().await {
            Reply::Error(message) => assert!(
                message.starts_with(prefix),
                "expected error starting with {prefix:?}, got {message:?}"
            ),
            other => panic!("expected error starting with {prefix:?}, got {other:?}"),
        }
    }

    /// Asserts nothing arrives within `wait` (the connection may also be closed).
    async fn assert_no_reply_within(&mut self, wait: Duration) {
        let mut byte = [0u8; 1];
        if let Ok(Ok(n)) = timeout(wait, self.reader.read(&mut byte)).await {
            assert_eq!(n, 0, "unexpected reply byte {:?}", byte[0] as char);
        }
    }

    /// Asserts the server closes the connection (EOF or reset) without sending more data.
    async fn assert_closed(&mut self) {
        let mut buf = Vec::new();
        match timeout(REPLY_TIMEOUT, self.reader.read_to_end(&mut buf)).await {
            Ok(Ok(_)) => assert!(buf.is_empty(), "unexpected data before close: {buf:?}"),
            Ok(Err(_)) => {} // connection reset also counts as closed
            Err(_) => panic!("server did not close the connection"),
        }
    }

    /// Round-trips a SET and GET to prove the connection (or server) still works.
    async fn assert_usable(&mut self) {
        assert_eq!(self.cmd(&[b"SET", b"health", b"check"]).await, ok());
        assert_eq!(self.cmd(&[b"GET", b"health"]).await, bulk("check"));
    }
}

fn encode(args: &[&[u8]]) -> Vec<u8> {
    let mut out = format!("*{}\r\n", args.len()).into_bytes();
    for arg in args {
        out.extend_from_slice(format!("${}\r\n", arg.len()).as_bytes());
        out.extend_from_slice(arg);
        out.extend_from_slice(b"\r\n");
    }
    out
}

fn invalid(message: String) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, message)
}

fn parse_number(text: &str, what: &str) -> io::Result<i64> {
    text.parse()
        .map_err(|_| invalid(format!("server sent invalid {what}: {text:?}")))
}

/// Reads one RESP2 reply. Malformed server output is returned as an `InvalidData`
/// error (not a panic) so the failure message shows what the server sent.
fn read_reply<'a>(
    reader: &'a mut BufReader<OwnedReadHalf>,
) -> Pin<Box<dyn Future<Output = io::Result<Reply>> + Send + 'a>> {
    Box::pin(async move {
        let mut line = Vec::new();
        if reader.read_until(b'\n', &mut line).await? == 0 {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "connection closed",
            ));
        }
        if line.len() < 3 || !line.ends_with(b"\r\n") {
            return Err(invalid(format!(
                "reply line must be a type byte plus CRLF: {:?}",
                String::from_utf8_lossy(&line)
            )));
        }
        let text = String::from_utf8_lossy(&line[1..line.len() - 2]).into_owned();
        Ok(match line[0] {
            b'+' => Reply::Simple(text),
            b'-' => Reply::Error(text),
            b':' => Reply::Integer(parse_number(&text, "integer reply")?),
            b'$' => {
                let len = parse_number(&text, "bulk length")?;
                if len < 0 {
                    Reply::Null
                } else {
                    let mut data = vec![0; len as usize + 2];
                    reader.read_exact(&mut data).await?;
                    if &data[len as usize..] != b"\r\n" {
                        return Err(invalid("bulk string must end with CRLF".into()));
                    }
                    data.truncate(len as usize);
                    Reply::Bulk(data)
                }
            }
            b'*' => {
                let len = parse_number(&text, "array length")?;
                if len < 0 {
                    Reply::NullArray
                } else {
                    let mut items = Vec::with_capacity(len as usize);
                    for _ in 0..len {
                        items.push(read_reply(&mut *reader).await?);
                    }
                    Reply::Array(items)
                }
            }
            other => {
                return Err(invalid(format!(
                    "unknown reply type byte {:?} in {:?}",
                    other as char,
                    String::from_utf8_lossy(&line)
                )));
            }
        })
    })
}

// ---------------------------------------------------------------------------
// 1.1 Bad or unknown commands get an error reply
// ---------------------------------------------------------------------------

#[tokio::test]
async fn unknown_command_replies_with_error_and_keeps_connection_open() {
    let (address, server) = start_server().await;
    let mut client = RawClient::connect(&address).await;

    // Redis keeps the name's case and lists the first arguments (note the trailing space).
    assert_eq!(
        client.cmd(&[b"FOOBAR", b"arg"]).await,
        Reply::Error("ERR unknown command 'FOOBAR', with args beginning with: 'arg' ".into())
    );
    assert_eq!(
        client.cmd(&[b"fooBar"]).await,
        Reply::Error("ERR unknown command 'fooBar', with args beginning with: ".into())
    );

    client.assert_usable().await;
    server.abort();
}

#[tokio::test]
async fn errors_inside_a_pipeline_do_not_shift_later_replies() {
    let (address, server) = start_server().await;
    let mut client = RawClient::connect(&address).await;

    let mut batch = Vec::new();
    batch.extend(encode(&[b"SET", b"a", b"1"]));
    batch.extend(encode(&[b"FOOBAR"]));
    batch.extend(encode(&[b"GET"]));
    batch.extend(encode(&[b"GET", b"a"]));
    client.send_raw(&batch).await;

    assert_eq!(client.read().await, ok());
    client
        .assert_error_starting_with("ERR unknown command 'FOOBAR'")
        .await;
    assert_eq!(
        client.read().await,
        Reply::Error("ERR wrong number of arguments for 'get' command".into())
    );
    assert_eq!(client.read().await, bulk("1"));
    server.abort();
}

#[tokio::test]
async fn command_names_are_case_insensitive() {
    let (address, server) = start_server().await;
    let mut client = RawClient::connect(&address).await;

    assert_eq!(client.cmd(&[b"sEt", b"k", b"v"]).await, ok());
    assert_eq!(client.cmd(&[b"get", b"k"]).await, bulk("v"));
    server.abort();
}

// ---------------------------------------------------------------------------
// 1.2 PUBLISH replies with a plain integer
// ---------------------------------------------------------------------------

#[tokio::test]
async fn publish_replies_with_integer_receiver_count() {
    let (address, server) = start_server().await;
    let mut publisher = RawClient::connect(&address).await;

    assert_eq!(
        publisher.cmd(&[b"PUBLISH", b"news", b"hi"]).await,
        Reply::Integer(0)
    );

    let mut subscriber = RawClient::connect(&address).await;
    assert_eq!(
        subscriber.cmd(&[b"SUBSCRIBE", b"news"]).await,
        push("subscribe", "news", Reply::Integer(1))
    );

    assert_eq!(
        publisher.cmd(&[b"PUBLISH", b"news", b"hi"]).await,
        Reply::Integer(1)
    );
    assert_eq!(subscriber.read().await, push("message", "news", bulk("hi")));

    let mut second = RawClient::connect(&address).await;
    assert_eq!(
        second.cmd(&[b"SUBSCRIBE", b"news"]).await,
        push("subscribe", "news", Reply::Integer(1))
    );
    assert_eq!(
        publisher.cmd(&[b"PUBLISH", b"news", b"both"]).await,
        Reply::Integer(2)
    );
    assert_eq!(
        subscriber.read().await,
        push("message", "news", bulk("both"))
    );
    assert_eq!(second.read().await, push("message", "news", bulk("both")));
    server.abort();
}

// ---------------------------------------------------------------------------
// 1.3 SUBSCRIBE accepts several channels
// ---------------------------------------------------------------------------

#[tokio::test]
async fn subscribe_to_several_channels_in_one_command() {
    let (address, server) = start_server().await;
    let mut subscriber = RawClient::connect(&address).await;

    subscriber.send(&[b"SUBSCRIBE", b"a", b"b", b"c"]).await;
    assert_eq!(
        subscriber.read().await,
        push("subscribe", "a", Reply::Integer(1))
    );
    assert_eq!(
        subscriber.read().await,
        push("subscribe", "b", Reply::Integer(2))
    );
    assert_eq!(
        subscriber.read().await,
        push("subscribe", "c", Reply::Integer(3))
    );

    let mut publisher = RawClient::connect(&address).await;
    for channel in ["a", "b", "c"] {
        publisher.publish(channel.as_bytes(), b"payload").await;
        assert_eq!(
            subscriber.read().await,
            push("message", channel, bulk("payload"))
        );
    }
    server.abort();
}

#[tokio::test]
async fn subscribe_to_the_same_channel_twice_in_one_command_counts_once() {
    let (address, server) = start_server().await;
    let mut subscriber = RawClient::connect(&address).await;

    subscriber.send(&[b"SUBSCRIBE", b"a", b"a"]).await;
    assert_eq!(
        subscriber.read().await,
        push("subscribe", "a", Reply::Integer(1))
    );
    assert_eq!(
        subscriber.read().await,
        push("subscribe", "a", Reply::Integer(1))
    );
    server.abort();
}

// ---------------------------------------------------------------------------
// 1.4 Bare UNSUBSCRIBE with no subscriptions replies [unsubscribe, nil, 0]
// ---------------------------------------------------------------------------

#[tokio::test]
async fn bare_unsubscribe_without_subscriptions_replies_with_nil_channel() {
    let (address, server) = start_server().await;
    let mut client = RawClient::connect(&address).await;

    assert_eq!(
        client.cmd(&[b"UNSUBSCRIBE"]).await,
        Reply::Array(vec![bulk("unsubscribe"), Reply::Null, Reply::Integer(0)])
    );
    client.assert_usable().await;
    server.abort();
}

#[tokio::test]
async fn second_bare_unsubscribe_after_leaving_all_channels_replies_with_nil_channel() {
    let (address, server) = start_server().await;
    let mut client = RawClient::connect(&address).await;

    assert_eq!(
        client.cmd(&[b"SUBSCRIBE", b"a"]).await,
        push("subscribe", "a", Reply::Integer(1))
    );
    assert_eq!(
        client.cmd(&[b"UNSUBSCRIBE"]).await,
        push("unsubscribe", "a", Reply::Integer(0))
    );
    assert_eq!(
        client.cmd(&[b"UNSUBSCRIBE"]).await,
        Reply::Array(vec![bulk("unsubscribe"), Reply::Null, Reply::Integer(0)])
    );
    server.abort();
}

#[tokio::test]
async fn unsubscribe_from_named_channels_reports_current_count() {
    let (address, server) = start_server().await;
    let mut client = RawClient::connect(&address).await;

    client.send(&[b"SUBSCRIBE", b"a", b"b", b"c"]).await;
    for count in 1..=3 {
        assert!(
            matches!(client.read().await, Reply::Array(_)),
            "ack {count}"
        );
    }

    // Named channels are answered in request order; one we never joined still gets a reply.
    client.send(&[b"UNSUBSCRIBE", b"a", b"never", b"b"]).await;
    assert_eq!(
        client.read().await,
        push("unsubscribe", "a", Reply::Integer(2))
    );
    assert_eq!(
        client.read().await,
        push("unsubscribe", "never", Reply::Integer(2))
    );
    assert_eq!(
        client.read().await,
        push("unsubscribe", "b", Reply::Integer(1))
    );

    // Still subscribed to c, so regular commands are still blocked.
    client.send(&[b"GET", b"k"]).await;
    client
        .assert_error_starting_with("ERR Can't execute 'get'")
        .await;
    server.abort();
}

// ---------------------------------------------------------------------------
// 1.5 Only pub/sub commands are allowed while subscribed
// ---------------------------------------------------------------------------

#[tokio::test]
async fn regular_commands_are_rejected_while_subscribed() {
    let (address, server) = start_server().await;
    let mut client = RawClient::connect(&address).await;

    assert_eq!(
        client.cmd(&[b"SUBSCRIBE", b"a"]).await,
        push("subscribe", "a", Reply::Integer(1))
    );

    client.send(&[b"GET", b"k"]).await;
    client
        .assert_error_starting_with("ERR Can't execute 'get'")
        .await;
    client.send(&[b"SET", b"k", b"v"]).await;
    client
        .assert_error_starting_with("ERR Can't execute 'set'")
        .await;
    client.send(&[b"PUBLISH", b"a", b"m"]).await;
    client
        .assert_error_starting_with("ERR Can't execute 'publish'")
        .await;

    // Pub/sub commands still work, and the rejected SET did not run.
    assert_eq!(
        client.cmd(&[b"SUBSCRIBE", b"b"]).await,
        push("subscribe", "b", Reply::Integer(2))
    );
    // Bare UNSUBSCRIBE leaves both channels in any order; counts go 1, then 0.
    client.send(&[b"UNSUBSCRIBE"]).await;
    let mut channels = Vec::new();
    for expected_count in [1, 0] {
        match client.read().await {
            Reply::Array(parts) => match parts.as_slice() {
                [
                    Reply::Bulk(kind),
                    Reply::Bulk(channel),
                    Reply::Integer(count),
                ] if kind.as_slice() == b"unsubscribe" => {
                    assert_eq!(*count, expected_count);
                    channels.push(channel.clone());
                }
                other => panic!("unexpected unsubscribe reply {other:?}"),
            },
            other => panic!("unexpected unsubscribe reply {other:?}"),
        }
    }
    channels.sort();
    assert_eq!(channels, vec![b"a".to_vec(), b"b".to_vec()]);

    // After leaving every channel the connection is back to normal mode.
    assert_eq!(client.cmd(&[b"GET", b"k"]).await, Reply::Null);
    client.assert_usable().await;
    server.abort();
}

#[tokio::test]
async fn unknown_command_and_arity_errors_take_priority_while_subscribed() {
    let (address, server) = start_server().await;
    let mut client = RawClient::connect(&address).await;

    assert_eq!(
        client.cmd(&[b"SUBSCRIBE", b"a"]).await,
        push("subscribe", "a", Reply::Integer(1))
    );
    client.send(&[b"FOOBAR"]).await;
    client
        .assert_error_starting_with("ERR unknown command 'FOOBAR'")
        .await;
    assert_eq!(
        client.cmd(&[b"GET"]).await,
        Reply::Error("ERR wrong number of arguments for 'get' command".into())
    );
    server.abort();
}

#[tokio::test]
async fn ping_and_reset_are_allowed_while_subscribed() {
    let (address, server) = start_server().await;
    let mut client = RawClient::connect(&address).await;

    assert_eq!(
        client.cmd(&[b"SUBSCRIBE", b"a"]).await,
        push("subscribe", "a", Reply::Integer(1))
    );

    // Client libraries use PING as a health check on subscribed connections.
    assert_eq!(
        client.cmd(&[b"PING"]).await,
        Reply::Array(vec![bulk("pong"), bulk("")])
    );
    assert_eq!(
        client.cmd(&[b"PING", b"hi"]).await,
        Reply::Array(vec![bulk("pong"), bulk("hi")])
    );

    // RESET leaves subscribed mode.
    assert_eq!(client.cmd(&[b"RESET"]).await, Reply::Simple("RESET".into()));
    let mut publisher = RawClient::connect(&address).await;
    assert_eq!(
        publisher.cmd(&[b"PUBLISH", b"a", b"gone"]).await,
        Reply::Integer(0)
    );
    client.assert_usable().await;
    server.abort();
}

#[tokio::test]
async fn quit_while_subscribed_replies_ok_and_closes() {
    let (address, server) = start_server().await;
    let mut client = RawClient::connect(&address).await;

    assert_eq!(
        client.cmd(&[b"SUBSCRIBE", b"a"]).await,
        push("subscribe", "a", Reply::Integer(1))
    );
    assert_eq!(client.cmd(&[b"QUIT"]).await, ok());
    client.assert_closed().await;
    server.abort();
}

// ---------------------------------------------------------------------------
// 1.6 Keys, values and channel names are binary safe
// ---------------------------------------------------------------------------

#[tokio::test]
async fn non_utf8_keys_and_values_round_trip() {
    let (address, server) = start_server().await;
    let mut client = RawClient::connect(&address).await;

    let key: &[u8] = b"\xff\xfe\x00key";
    let value: &[u8] = b"\x00\x01\x02\xff";
    assert_eq!(client.cmd(&[b"SET", key, value]).await, ok());
    assert_eq!(client.cmd(&[b"GET", key]).await, bulk(value));

    // A different invalid-UTF-8 key must not collide with the first one.
    assert_eq!(client.cmd(&[b"GET", b"\xff\xfd\x00key"]).await, Reply::Null);
    server.abort();
}

#[tokio::test]
async fn values_with_crlf_and_empty_strings_round_trip() {
    let (address, server) = start_server().await;
    let mut client = RawClient::connect(&address).await;

    // RESP bulk strings are length-prefixed, so CRLF inside a value is just data.
    assert_eq!(client.cmd(&[b"SET", b"crlf", b"a\r\nb\r\n"]).await, ok());
    assert_eq!(client.cmd(&[b"GET", b"crlf"]).await, bulk("a\r\nb\r\n"));

    assert_eq!(client.cmd(&[b"SET", b"", b""]).await, ok());
    assert_eq!(client.cmd(&[b"GET", b""]).await, bulk(""));
    server.abort();
}

#[tokio::test]
async fn large_value_sent_in_pieces_round_trips() {
    let (address, server) = start_server().await;
    let mut client = RawClient::connect(&address).await;

    let value: Vec<u8> = (0..2 * 1024 * 1024).map(|i| (i % 251) as u8).collect();
    let request = encode(&[b"SET", b"big", &value]);
    for chunk in request.chunks(64 * 1024) {
        client.send_raw(chunk).await;
        sleep(Duration::from_millis(1)).await;
    }
    assert_eq!(client.read().await, ok());
    assert_eq!(client.cmd(&[b"GET", b"big"]).await, Reply::Bulk(value));
    server.abort();
}

#[tokio::test]
async fn non_utf8_channel_names_work_for_pubsub() {
    let (address, server) = start_server().await;
    let channel: &[u8] = b"\xc3\x28chan";
    let mut subscriber = RawClient::connect(&address).await;
    let mut publisher = RawClient::connect(&address).await;

    assert_eq!(
        subscriber.cmd(&[b"SUBSCRIBE", channel]).await,
        push("subscribe", channel, Reply::Integer(1))
    );
    publisher.publish(channel, b"\xff").await;
    assert_eq!(
        subscriber.read().await,
        push("message", channel, bulk(b"\xff"))
    );
    server.abort();
}

#[tokio::test]
async fn non_utf8_command_name_is_an_unknown_command_not_a_crash() {
    let (address, server) = start_server().await;
    let mut client = RawClient::connect(&address).await;

    client.send(&[b"\xff\xfe", b"x"]).await;
    client
        .assert_error_starting_with("ERR unknown command")
        .await;
    client.assert_usable().await;
    server.abort();
}

// ---------------------------------------------------------------------------
// 1.7 Malformed RESP gets a protocol error, and only that connection closes
// ---------------------------------------------------------------------------

#[tokio::test]
async fn malformed_resp_gets_protocol_error_and_closes_only_that_connection() {
    let (address, server) = start_server().await;

    let cases: [&[u8]; 5] = [
        b"*1\r\n!3\r\nfoo\r\n",       // unknown type byte inside an array
        b"*2\r\n$3\r\nGET\r\n:1\r\n", // non-bulk argument
        b"*1\r\n$abc\r\n",            // invalid bulk length
        b"*abc\r\n",                  // invalid array length
        b"*1\r\n$-5\r\n",             // negative bulk length in a request
    ];

    for input in cases {
        let mut bad = RawClient::connect(&address).await;
        let mut bystander = RawClient::connect(&address).await;

        bad.send_raw(input).await;
        bad.assert_error_starting_with("ERR Protocol error").await;
        bad.assert_closed().await;

        // Other connections and the server itself keep working.
        bystander.assert_usable().await;
        RawClient::connect(&address).await.assert_usable().await;
    }
    server.abort();
}

#[tokio::test]
async fn protocol_error_after_valid_commands_answers_them_first() {
    let (address, server) = start_server().await;
    let mut client = RawClient::connect(&address).await;

    let mut batch = encode(&[b"SET", b"a", b"1"]);
    batch.extend(encode(&[b"GET", b"a"]));
    batch.extend_from_slice(b"*1\r\n$abc\r\n");
    client.send_raw(&batch).await;

    assert_eq!(client.read().await, ok());
    assert_eq!(client.read().await, bulk("1"));
    client
        .assert_error_starting_with("ERR Protocol error")
        .await;
    client.assert_closed().await;
    server.abort();
}

#[tokio::test]
async fn empty_and_null_arrays_are_ignored() {
    let (address, server) = start_server().await;
    let mut client = RawClient::connect(&address).await;

    // Redis sends no reply for these, so the first reply must be the GET's.
    let mut batch = b"*0\r\n*-1\r\n".to_vec();
    batch.extend(encode(&[b"GET", b"missing"]));
    client.send_raw(&batch).await;
    assert_eq!(client.read().await, Reply::Null);
    client.assert_usable().await;
    server.abort();
}

#[tokio::test]
async fn request_split_across_many_writes_is_reassembled() {
    let (address, server) = start_server().await;
    let mut client = RawClient::connect(&address).await;

    for byte in encode(&[b"SET", b"split", b"value"]) {
        client.send_raw(&[byte]).await;
        sleep(Duration::from_millis(1)).await;
    }
    assert_eq!(client.read().await, ok());
    assert_eq!(client.cmd(&[b"GET", b"split"]).await, bulk("value"));
    server.abort();
}

// ---------------------------------------------------------------------------
// 1.8 A failed accept() does not stop the server
// ---------------------------------------------------------------------------

/// Limiting file descriptors relies on `sh -c "ulimit -n ..."`, so this test is Unix only.
#[cfg(unix)]
mod accept_failure {
    use super::*;
    use std::process::{Child, Command, Stdio};

    const SERVER_BINARY: &str = env!("CARGO_BIN_EXE_my-mini-redis-server");

    /// Kills the server process when the test ends, even on panic.
    struct ChildGuard(Child);

    impl Drop for ChildGuard {
        fn drop(&mut self) {
            let _ = self.0.kill();
            let _ = self.0.wait();
        }
    }

    /// Starts the server binary with at most 32 open files, so accept() fails with
    /// EMFILE once about 25 clients are connected. Retries on a new port if another
    /// process grabbed the chosen one before the server bound it.
    async fn start_fd_limited_server() -> (String, ChildGuard) {
        for _ in 0..3 {
            let address = {
                let probe = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
                probe.local_addr().unwrap().to_string()
            };
            let mut server = ChildGuard(
                Command::new("sh")
                    .arg("-c")
                    .arg("ulimit -n 32 && exec \"$0\"")
                    .arg(SERVER_BINARY)
                    .env("MY_MINI_REDIS_ADDRESS", &address)
                    .stdout(Stdio::null())
                    .stderr(Stdio::null())
                    .spawn()
                    .expect("failed to start server binary"),
            );

            let deadline = Instant::now() + Duration::from_secs(5);
            loop {
                if server.0.try_wait().unwrap().is_some() {
                    break; // could not bind the port: try another one
                }
                if TcpStream::connect(&address).await.is_ok() {
                    return (address, server);
                }
                assert!(Instant::now() < deadline, "server never started listening");
                sleep(Duration::from_millis(20)).await;
            }
        }
        panic!("could not start the server on a free port");
    }

    /// One SET/GET round trip with short timeouts; false on any failure.
    async fn try_round_trip(address: &str) -> bool {
        let attempt = async {
            let mut client = RawClient::from_stream(TcpStream::connect(address).await.ok()?);
            client
                .writer
                .write_all(&encode(&[b"SET", b"health", b"check"]))
                .await
                .ok()?;
            read_reply(&mut client.reader).await.ok()
        };
        matches!(timeout(Duration::from_secs(1), attempt).await, Ok(Some(reply)) if reply == ok())
    }

    #[tokio::test]
    async fn server_survives_running_out_of_file_descriptors() {
        if external_target().is_some() {
            eprintln!("skipped: this test starts this project's own server binary");
            return;
        }
        let (address, mut server) = start_fd_limited_server().await;

        // Hold more connections than the fd limit allows the server to accept.
        let mut flood = Vec::new();
        for _ in 0..64 {
            if let Ok(stream) = TcpStream::connect(&address).await {
                flood.push(stream);
            }
        }
        sleep(Duration::from_millis(500)).await;

        // Prove accept() is failing: a late client is queued by the kernel but never served.
        if let Ok(stream) = TcpStream::connect(&address).await {
            let mut late = RawClient::from_stream(stream);
            late.send(&[b"GET", b"k"]).await;
            late.assert_no_reply_within(Duration::from_millis(500))
                .await;
        }

        assert!(
            server.0.try_wait().unwrap().is_none(),
            "server process exited after accept() failed"
        );

        // Once fds are free again the server must recover within 10s. A retry
        // backoff longer than that (e.g. doubling up to 64s) fails this test on purpose.
        drop(flood);
        let deadline = Instant::now() + Duration::from_secs(10);
        while !try_round_trip(&address).await {
            assert!(
                server.0.try_wait().unwrap().is_none(),
                "server process exited after accept() failed"
            );
            assert!(
                Instant::now() < deadline,
                "server did not serve new clients within 10s of fds being freed"
            );
            sleep(Duration::from_millis(100)).await;
        }
    }
}

// ---------------------------------------------------------------------------
// 1.10 A slow subscriber is not silently dropped
// ---------------------------------------------------------------------------

#[tokio::test]
async fn slow_subscriber_receives_every_message_in_order() {
    let (address, server) = start_server().await;
    let mut subscriber = RawClient::connect(&address).await;
    let mut publisher = RawClient::connect(&address).await;

    assert_eq!(
        subscriber.cmd(&[b"SUBSCRIBE", b"firehose"]).await,
        push("subscribe", "firehose", Reply::Integer(1))
    );

    // ~12.8 MB total: well past the socket buffers, so the server must queue
    // messages for the subscriber. Still under Redis's 32 MB pub/sub hard limit.
    //
    // Each PUBLISH must reply before the subscriber reads anything. That is how
    // Redis behaves: a slow subscriber never blocks publishers. So a fix that
    // applies backpressure to the publisher fails this test on purpose.
    const MESSAGES: usize = 400;
    const SIZE: usize = 32 * 1024;
    let payload = |i: usize| {
        let mut bytes = format!("{i:08}").into_bytes();
        bytes.resize(SIZE, b'x');
        bytes
    };

    for i in 0..MESSAGES {
        publisher.publish(b"firehose", &payload(i)).await;
    }

    for i in 0..MESSAGES {
        assert_eq!(
            subscriber.read().await,
            push("message", "firehose", bulk(payload(i))),
            "message {i} missing or out of order"
        );
    }

    // Still subscribed afterwards.
    publisher.publish(b"firehose", b"after").await;
    assert_eq!(
        subscriber.read().await,
        push("message", "firehose", bulk("after"))
    );
    server.abort();
}

// ---------------------------------------------------------------------------
// 1.11 Pipelined requests are answered in order
// ---------------------------------------------------------------------------

#[tokio::test]
async fn pipelined_commands_are_answered_in_order() {
    let (address, server) = start_server().await;
    let mut client = RawClient::connect(&address).await;

    const COUNT: usize = 1000;
    let mut batch = Vec::new();
    for i in 0..COUNT {
        let key = format!("key:{i}");
        let value = format!("value:{i}");
        batch.extend(encode(&[b"SET", key.as_bytes(), value.as_bytes()]));
        batch.extend(encode(&[b"GET", key.as_bytes()]));
    }
    client.send_raw(&batch).await;

    for i in 0..COUNT {
        assert_eq!(client.read().await, ok(), "SET {i}");
        assert_eq!(client.read().await, bulk(format!("value:{i}")), "GET {i}");
    }
    server.abort();
}

// ---------------------------------------------------------------------------
// 1.12 Wrong argument counts are rejected
// ---------------------------------------------------------------------------

#[tokio::test]
async fn wrong_number_of_arguments_is_rejected() {
    let (address, server) = start_server().await;
    let mut client = RawClient::connect(&address).await;

    let wrong_arity: [(&[&[u8]], &str); 7] = [
        (&[b"GET"], "get"),
        (&[b"GET", b"a", b"b"], "get"),
        (&[b"SET"], "set"),
        (&[b"SET", b"a"], "set"),
        (&[b"PUBLISH", b"ch"], "publish"),
        (&[b"PUBLISH", b"ch", b"m", b"extra"], "publish"),
        (&[b"SUBSCRIBE"], "subscribe"),
    ];
    for (args, name) in wrong_arity {
        assert_eq!(
            client.cmd(args).await,
            Reply::Error(format!(
                "ERR wrong number of arguments for '{name}' command"
            )),
            "{args:?}"
        );
    }

    // SET with an unknown trailing option is a syntax error and does not store anything.
    assert_eq!(
        client.cmd(&[b"SET", b"a", b"b", b"bogus"]).await,
        Reply::Error("ERR syntax error".into())
    );
    assert_eq!(client.cmd(&[b"GET", b"a"]).await, Reply::Null);

    client.assert_usable().await;
    server.abort();
}

// ---------------------------------------------------------------------------
// 1.13 Inline (plain text) commands
// ---------------------------------------------------------------------------

#[tokio::test]
async fn inline_commands_are_accepted() {
    let (address, server) = start_server().await;
    let mut client = RawClient::connect(&address).await;

    client
        .send_raw(b"SET greeting hello\r\nGET greeting\r\n")
        .await;
    assert_eq!(client.read().await, ok());
    assert_eq!(client.read().await, bulk("hello"));

    // Bare \n line ending, extra spaces, and empty lines (which get no reply).
    client.send_raw(b"\r\n  GET   greeting  \n").await;
    assert_eq!(client.read().await, bulk("hello"));

    // Quoted arguments, including escapes.
    client
        .send_raw(b"SET quoted \"hello world\\n\"\r\nGET quoted\r\n")
        .await;
    assert_eq!(client.read().await, ok());
    assert_eq!(client.read().await, bulk("hello world\n"));

    // \xHH and \t escapes in double quotes; single quotes with an escaped quote.
    client
        .send_raw(b"SET esc \"\\x41\\tB\"\r\nGET esc\r\nSET sq 'it\\'s'\r\nGET sq\r\n")
        .await;
    assert_eq!(client.read().await, ok());
    assert_eq!(client.read().await, bulk("A\tB"));
    assert_eq!(client.read().await, ok());
    assert_eq!(client.read().await, bulk("it's"));

    // Inline and RESP requests can be mixed on one connection.
    client.assert_usable().await;
    server.abort();
}

#[tokio::test]
async fn inline_unknown_command_keeps_its_case() {
    let (address, server) = start_server().await;
    let mut client = RawClient::connect(&address).await;

    client.send_raw(b"fooBar x\r\n").await;
    assert_eq!(
        client.read().await,
        Reply::Error("ERR unknown command 'fooBar', with args beginning with: 'x' ".into())
    );
    client.assert_usable().await;
    server.abort();
}

#[tokio::test]
async fn inline_closing_quote_followed_by_text_is_a_protocol_error() {
    let (address, server) = start_server().await;
    let mut client = RawClient::connect(&address).await;

    client.send_raw(b"SET k \"abc\"def\r\n").await;
    client
        .assert_error_starting_with("ERR Protocol error: unbalanced quotes in request")
        .await;
    client.assert_closed().await;
    server.abort();
}

#[tokio::test]
async fn inline_command_with_unbalanced_quotes_is_a_protocol_error() {
    let (address, server) = start_server().await;
    let mut client = RawClient::connect(&address).await;

    client.send_raw(b"SET k \"unterminated\r\n").await;
    client
        .assert_error_starting_with("ERR Protocol error")
        .await;
    client.assert_closed().await;

    RawClient::connect(&address).await.assert_usable().await;
    server.abort();
}

// ---------------------------------------------------------------------------
// 1.14 Messages from several channels arrive in publish order
// ---------------------------------------------------------------------------

#[tokio::test]
async fn messages_from_several_channels_arrive_in_publish_order() {
    let (address, server) = start_server().await;
    let mut subscriber = RawClient::connect(&address).await;
    let mut publisher = RawClient::connect(&address).await;

    subscriber.send(&[b"SUBSCRIBE", b"a"]).await;
    assert_eq!(
        subscriber.read().await,
        push("subscribe", "a", Reply::Integer(1))
    );
    subscriber.send(&[b"SUBSCRIBE", b"b"]).await;
    assert_eq!(
        subscriber.read().await,
        push("subscribe", "b", Reply::Integer(2))
    );

    // Pipeline the publishes in one write so several messages are pending on both
    // channels at once. Redis delivers them in the order it ran the PUBLISHes.
    // Kept small (8 per channel) so this checks ordering, not slow subscribers (1.10).
    const MESSAGES: usize = 16;
    let message = |i: usize| -> (&'static [u8], Vec<u8>) {
        let channel: &'static [u8] = if i.is_multiple_of(2) { b"a" } else { b"b" };
        (channel, i.to_string().into_bytes())
    };
    let mut pipeline = Vec::new();
    for i in 0..MESSAGES {
        let (channel, payload) = message(i);
        pipeline.extend(encode(&[b"PUBLISH", channel, &payload]));
    }
    publisher.send_raw(&pipeline).await;
    for _ in 0..MESSAGES {
        publisher.read().await;
    }

    for i in 0..MESSAGES {
        let (channel, payload) = message(i);
        assert_eq!(
            subscriber.read().await,
            push("message", channel, bulk(payload)),
            "message {i} out of order"
        );
    }
    server.abort();
}

// ---------------------------------------------------------------------------
// 1.15 A subscriber that falls behind keeps its subscription
// ---------------------------------------------------------------------------

#[tokio::test]
async fn subscriber_that_falls_behind_still_receives_later_messages() {
    let (address, server) = start_server().await;
    let mut subscriber = RawClient::connect(&address).await;
    let mut publisher = RawClient::connect(&address).await;

    assert_eq!(
        subscriber.cmd(&[b"SUBSCRIBE", b"firehose"]).await,
        push("subscribe", "firehose", Reply::Integer(1))
    );

    // ~12.8 MB the subscriber doesn't read yet: enough to fill the socket buffers and
    // any small per-channel queue, but under Redis's 32 MB pub/sub hard limit and
    // its 8 MB soft limit's 60 second grace period.
    const MESSAGES: usize = 400;
    let payload = vec![b'x'; 32 * 1024];
    for _ in 0..MESSAGES {
        publisher.publish(b"firehose", &payload).await;
    }
    publisher.publish(b"firehose", b"after").await;

    // Unlike 1.10, losing some of the backlog is tolerated here. What must not happen
    // is the server dropping the subscription without telling the client, which then
    // believes it is subscribed but never receives another message.
    loop {
        match subscriber.read().await {
            reply if reply == push("message", "firehose", bulk("after")) => break,
            Reply::Array(parts) if parts.first() == Some(&bulk("message")) => continue,
            other => panic!("unexpected reply while draining the backlog: {other:?}"),
        }
    }

    subscriber.send(&[b"UNSUBSCRIBE", b"firehose"]).await;
    assert_eq!(
        subscriber.read().await,
        push("unsubscribe", "firehose", Reply::Integer(0))
    );
    server.abort();
}

// ---------------------------------------------------------------------------
// 1.16 A subscriber far over the output buffer limit is disconnected
// ---------------------------------------------------------------------------

#[tokio::test]
async fn subscriber_over_the_output_buffer_limit_is_disconnected() {
    let (address, server) = start_server().await;
    let mut subscriber = RawClient::connect(&address).await;
    let mut publisher = RawClient::connect(&address).await;

    assert_eq!(
        subscriber.cmd(&[b"SUBSCRIBE", b"firehose"]).await,
        push("subscribe", "firehose", Reply::Integer(1))
    );

    // 64 MB the subscriber doesn't read: double Redis's 32 MB pub/sub hard limit, so
    // socket buffers can't absorb the difference. Publishers are never slowed down.
    const MESSAGES: usize = 2048;
    let payload = vec![b'x'; 32 * 1024];
    for _ in 0..MESSAGES {
        publisher.publish(b"firehose", &payload).await;
    }
    publisher.publish(b"firehose", b"after").await;

    // The subscriber gets whatever was sent before the cut, then the connection closes.
    let mut received = 0;
    loop {
        match timeout(REPLY_TIMEOUT, read_reply(&mut subscriber.reader)).await {
            Ok(Ok(reply)) => {
                assert_ne!(
                    reply,
                    push("message", "firehose", bulk("after")),
                    "subscriber was never disconnected"
                );
                received += 1;
            }
            Ok(Err(_)) => break, // EOF or reset: disconnected
            Err(_) => {
                panic!("subscriber neither disconnected nor sent more after {received} messages")
            }
        }
    }
    assert!(
        received < MESSAGES,
        "every message arrived, so nothing was cut off"
    );

    // The server and other clients are unaffected.
    publisher.assert_usable().await;
    server.abort();
}

// ---------------------------------------------------------------------------
// 1.17 Pipelined replies are sent without delay
// ---------------------------------------------------------------------------

#[tokio::test]
async fn pipelined_replies_are_not_delayed() {
    let (address, server) = start_server().await;
    let mut client = RawClient::connect(&address).await;

    // Redis sets TCP_NODELAY on client sockets. Without it, Nagle's algorithm holds a
    // small reply written while an earlier one is unacknowledged until the client's
    // delayed ACK, 40 ms on Linux, so each pipelined round trip stalls.
    const ROUNDS: u32 = 20;
    let mut batch = encode(&[b"PING"]);
    batch.extend(encode(&[b"PING"]));
    let started = Instant::now();
    for _ in 0..ROUNDS {
        client.send_raw(&batch).await;
        assert_eq!(client.read().await, Reply::Simple("PONG".into()));
        assert_eq!(client.read().await, Reply::Simple("PONG".into()));
    }
    let elapsed = started.elapsed();
    // Normally well under a millisecond per round; stalled, about 40 ms per round.
    assert!(
        elapsed < Duration::from_millis(10) * ROUNDS,
        "{ROUNDS} pipelined round trips took {elapsed:?}"
    );
    server.abort();
}

// ---------------------------------------------------------------------------
// 1.18 A publish reaches every subscriber of a crowded channel
// ---------------------------------------------------------------------------

#[tokio::test]
async fn publish_reaches_every_subscriber_of_a_crowded_channel() {
    let (address, server) = start_server().await;

    // More than the server writes inline per publish (512): the rest are sent by their
    // own connection's task, which the publisher only wakes.
    const SUBSCRIBERS: usize = 600;
    let mut subscribers = Vec::with_capacity(SUBSCRIBERS);
    for _ in 0..SUBSCRIBERS {
        let mut subscriber = RawClient::connect(&address).await;
        assert_eq!(
            subscriber.cmd(&[b"SUBSCRIBE", b"crowd"]).await,
            push("subscribe", "crowd", Reply::Integer(1))
        );
        subscribers.push(subscriber);
    }

    let mut publisher = RawClient::connect(&address).await;
    for payload in ["first", "second"] {
        assert_eq!(
            publisher
                .cmd(&[b"PUBLISH", b"crowd", payload.as_bytes()])
                .await,
            Reply::Integer(SUBSCRIBERS as i64)
        );
    }

    for (index, subscriber) in subscribers.iter_mut().enumerate() {
        for payload in ["first", "second"] {
            assert_eq!(
                subscriber.read().await,
                push("message", "crowd", bulk(payload)),
                "subscriber {index}"
            );
        }
    }
    server.abort();
}
