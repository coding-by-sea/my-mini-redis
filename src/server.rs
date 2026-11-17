use crate::cmd::Command;
use crate::connection::{Connection, ReadRequestError};
use crate::frame::Frame;
use crate::request::ProtocolError;
use crate::subscription::{self, QUEUE_LIMIT_BYTES, Subscriptions, Wakes};
use crate::{DEFAULT_ADDRESS, Db, lock};
use bytes::{Bytes, BytesMut};
use tokio::net::{TcpListener, TcpStream};
use tokio::time::{Duration, sleep};

/// How long to wait before accepting again after `accept` fails, for example because
/// the process ran out of file descriptors and must wait for connections to close.
const ACCEPT_RETRY_DELAY: Duration = Duration::from_millis(100);

pub struct Server {
    db: Db,
}

impl Default for Server {
    fn default() -> Self {
        Self::new()
    }
}

impl Server {
    pub fn new() -> Self {
        Self { db: Db::default() }
    }

    pub async fn run(&mut self) {
        let listener = TcpListener::bind(DEFAULT_ADDRESS).await.unwrap();
        self.run_with_listener(listener).await;
    }

    pub async fn run_with_listener(&mut self, listener: TcpListener) {
        loop {
            let socket = match listener.accept().await {
                Ok((socket, _)) => socket,
                Err(error) => {
                    eprintln!("ERROR accepting connection: {error}");
                    sleep(ACCEPT_RETRY_DELAY).await;
                    continue;
                }
            };
            // Send replies immediately, as Redis does. With Nagle's algorithm on, a
            // small reply written while an earlier one is unacknowledged waits for the
            // client's delayed ACK (40 ms on Linux), stalling pipelined clients.
            if let Err(error) = socket.set_nodelay(true) {
                eprintln!("ERROR setting TCP_NODELAY: {error}");
            }
            let db = self.db.clone();
            tokio::spawn(async move {
                if let Err(error) = Server::handle(socket, db).await {
                    eprintln!("ERROR on connection: {error:#}");
                }
            });
        }
    }

    /// Serves one connection until the client leaves. Mistakes in a request get an
    /// error reply and the connection stays open; malformed RESP gets a protocol
    /// error and closes the connection; I/O failures close it and are returned.
    async fn handle(socket: TcpStream, db: Db) -> anyhow::Result<()> {
        let mut connection = Connection::new(socket);
        let mut session = Session::new(db);

        loop {
            tokio::select! {
                request = connection.read_request() => {
                    let mut args = match request {
                        Ok(Some(args)) => args,
                        Ok(None) => return Ok(()),
                        Err(ReadRequestError::Protocol(error)) => {
                            return protocol_error(&mut connection, error).await;
                        }
                        Err(ReadRequestError::Io(error)) => return Err(error),
                    };
                    // Run every request that has already arrived, then send all the
                    // replies with one write instead of one per reply.
                    loop {
                        let (messages, replies, next) = session.execute(args);
                        connection.buffer_encoded(&messages).await?;
                        session.subscriptions.recycle(messages);
                        for reply in replies {
                            connection.buffer_frame(reply).await?;
                        }
                        if let Next::Close = next {
                            connection.flush().await?;
                            return Ok(());
                        }
                        match connection.buffered_request() {
                            Ok(Some(more)) => args = more,
                            Ok(None) => break,
                            Err(error) => return protocol_error(&mut connection, error).await,
                        }
                    }
                    // Subscribers this batch published to send its messages now, each
                    // with one write.
                    session.wakes.wake_all();
                    connection.flush().await?;
                }
                () = session.subscriptions.ready() => {
                    if session.subscriptions.overflowed() {
                        anyhow::bail!(
                            "closed subscriber: over {} MB of messages queued",
                            QUEUE_LIMIT_BYTES / (1024 * 1024)
                        );
                    }
                    // Send everything queued in one write.
                    let messages = session.subscriptions.take_queued();
                    if !messages.is_empty() {
                        connection.buffer_encoded(&messages).await?;
                        connection.flush().await?;
                    }
                    session.subscriptions.recycle(messages);
                }
            }
        }
    }
}

enum Next {
    Continue,
    Close,
}

/// A connection's state. Dropping it, however the connection ends, removes the
/// connection's subscriptions from the db.
struct Session {
    db: Db,
    subscriptions: Subscriptions,
    /// Subscribers this connection's PUBLISHes queued messages for, woken after each
    /// batch of requests.
    wakes: Wakes,
}

impl Session {
    fn new(db: Db) -> Self {
        Self {
            db,
            subscriptions: Subscriptions::new(),
            wakes: Wakes::default(),
        }
    }

    /// Runs one request. Returns the encoded messages already queued for this connection,
    /// which go out before the replies as in Redis, then the replies.
    ///
    /// The db lock is only held for the step of a command that reads or changes
    /// shared state. SUBSCRIBE, UNSUBSCRIBE and RESET also take the queued messages
    /// under the lock, so that no message for a channel can arrive after its
    /// unsubscribe acknowledgment. For other commands it is enough to take what is
    /// queued when the command runs: a PUBLISH that finished earlier has already
    /// queued its message, and one running concurrently may land on either side, as
    /// in Redis.
    fn execute(&mut self, args: Vec<Bytes>) -> (BytesMut, Vec<Frame>, Next) {
        let command = match Command::from_args(args) {
            Ok(command) => command,
            Err(message) => return self.reply(vec![error_frame(message)], Next::Continue),
        };
        if !self.subscriptions.is_empty() && !command.allowed_while_subscribed() {
            let error = error_frame(format!(
                "ERR Can't execute '{}': only (P|S)SUBSCRIBE / (P|S)UNSUBSCRIBE / PING / QUIT / RESET are allowed in this context",
                command.name()
            ));
            return self.reply(vec![error], Next::Continue);
        }

        match command {
            Command::Get(cmd) => {
                let value = lock(&self.db).entries.get(cmd.key()).cloned();
                self.reply(vec![value.map_or(Frame::Null, Frame::Bulk)], Next::Continue)
            }
            Command::Set(cmd) => {
                lock(&self.db)
                    .entries
                    .insert(cmd.key().clone(), cmd.value());
                self.reply(vec![Frame::Simple("OK".into())], Next::Continue)
            }
            Command::Publish(cmd) => {
                let receivers = subscription::publish(
                    &lock(&self.db),
                    cmd.channel(),
                    cmd.message(),
                    &mut self.wakes,
                );
                self.reply(vec![Frame::Integer(receivers as i64)], Next::Continue)
            }
            Command::Subscribe(cmd) => {
                let mut state = lock(&self.db);
                let messages = self.subscriptions.take_queued();
                let mut replies = Vec::with_capacity(cmd.channels().len());
                for channel in cmd.channels() {
                    self.subscriptions.subscribe(&mut state, channel.clone());
                    replies.push(push_frame(
                        "subscribe",
                        Frame::Bulk(channel.clone()),
                        self.subscriptions.count(),
                    ));
                }
                (messages, replies, Next::Continue)
            }
            Command::Unsubscribe(cmd) => {
                let mut state = lock(&self.db);
                let messages = self.subscriptions.take_queued();
                let channels = if cmd.channels().is_empty() {
                    self.subscriptions.channels()
                } else {
                    cmd.channels().to_vec()
                };
                let mut replies = Vec::with_capacity(channels.len().max(1));
                if channels.is_empty() {
                    replies.push(push_frame("unsubscribe", Frame::Null, 0));
                }
                for channel in channels {
                    self.subscriptions.unsubscribe(&mut state, &channel);
                    replies.push(push_frame(
                        "unsubscribe",
                        Frame::Bulk(channel),
                        self.subscriptions.count(),
                    ));
                }
                (messages, replies, Next::Continue)
            }
            Command::Ping(message) => {
                let pong = if self.subscriptions.is_empty() {
                    message.map_or(Frame::Simple("PONG".into()), Frame::Bulk)
                } else {
                    Frame::Array(vec![
                        Frame::Bulk(Bytes::from_static(b"pong")),
                        Frame::Bulk(message.unwrap_or_default()),
                    ])
                };
                self.reply(vec![pong], Next::Continue)
            }
            Command::Quit => self.reply(vec![Frame::Simple("OK".into())], Next::Close),
            Command::Reset => {
                let mut state = lock(&self.db);
                let messages = self.subscriptions.take_queued();
                self.subscriptions.unsubscribe_all(&mut state);
                (
                    messages,
                    vec![Frame::Simple("RESET".into())],
                    Next::Continue,
                )
            }
        }
    }

    /// Pairs `replies` with the messages queued so far, taken without the db lock,
    /// for commands that don't change subscriptions (see `execute`).
    fn reply(&mut self, replies: Vec<Frame>, next: Next) -> (BytesMut, Vec<Frame>, Next) {
        (self.subscriptions.take_queued(), replies, next)
    }
}

impl Drop for Session {
    fn drop(&mut self) {
        let mut state = lock(&self.db);
        self.subscriptions.unsubscribe_all(&mut state);
    }
}

/// An error reply. Redis replaces CR and LF with spaces, since an error is one line.
fn error_frame(message: String) -> Frame {
    Frame::Error(message.replace(['\r', '\n'], " "))
}

/// A pub/sub reply such as `[subscribe, channel, count]`.
fn push_frame(kind: &'static str, channel: Frame, count: usize) -> Frame {
    Frame::Array(vec![
        Frame::Bulk(Bytes::from_static(kind.as_bytes())),
        channel,
        Frame::Integer(count as i64),
    ])
}

/// Replies with a protocol error, sending any replies already buffered first, then
/// closes the connection by returning.
async fn protocol_error(
    connection: &mut Connection,
    ProtocolError(message): ProtocolError,
) -> anyhow::Result<()> {
    connection
        .buffer_frame(error_frame(format!("ERR Protocol error: {message}")))
        .await?;
    connection.flush().await
}

#[cfg(test)]
mod tests {
    //! 1.9: the pub/sub channel map must only hold channels that have subscribers.
    use super::Server;
    use crate::Db;
    use crate::cmd::{Publish, Subscribe, Unsubscribe};
    use crate::connection::Connection;
    use crate::frame::Frame;
    use bytes::Bytes;
    use tokio::net::{TcpListener, TcpStream};
    use tokio::time::{Duration, Instant, sleep, timeout};

    async fn start() -> (String, Db) {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap().to_string();
        let mut server = Server::new();
        let db = server.db.clone();
        tokio::spawn(async move { server.run_with_listener(listener).await });
        (address, db)
    }

    async fn connect(address: &str) -> Connection {
        Connection::new(TcpStream::connect(address).await.unwrap())
    }

    async fn request(connection: &mut Connection, frame: Frame) -> Frame {
        connection.write_frame(frame).await.unwrap();
        timeout(Duration::from_secs(5), connection.read_frame())
            .await
            .expect("no reply")
            .unwrap()
            .expect("connection closed")
    }

    fn channel_count(db: &Db) -> usize {
        db.lock().unwrap().subscribers.len()
    }

    async fn wait_for_channel_count(db: &Db, expected: usize) {
        let deadline = Instant::now() + Duration::from_secs(2);
        while channel_count(db) != expected {
            assert!(
                Instant::now() < deadline,
                "expected {expected} channels, found {}",
                channel_count(db)
            );
            sleep(Duration::from_millis(10)).await;
        }
    }

    #[tokio::test]
    async fn publish_without_subscribers_does_not_create_channels() {
        let (address, db) = start().await;
        let mut publisher = connect(&address).await;

        for i in 0..1000 {
            let frame = Publish::new(format!("ghost:{i}"), Bytes::from_static(b"x")).into_frame();
            request(&mut publisher, frame).await;
        }
        assert_eq!(channel_count(&db), 0);
    }

    #[tokio::test]
    async fn unsubscribe_removes_channel_with_no_subscribers_left() {
        let (address, db) = start().await;
        let mut first = connect(&address).await;
        let mut second = connect(&address).await;

        request(&mut first, Subscribe::new("news").into_frame()).await;
        request(&mut second, Subscribe::new("news").into_frame()).await;
        assert_eq!(channel_count(&db), 1);

        // One subscriber left: the channel stays.
        request(
            &mut first,
            Unsubscribe::new(vec!["news".into()]).into_frame(),
        )
        .await;
        assert_eq!(channel_count(&db), 1);

        // Last subscriber left: the channel is removed (cleanup may finish just after the reply).
        request(
            &mut second,
            Unsubscribe::new(vec!["news".into()]).into_frame(),
        )
        .await;
        wait_for_channel_count(&db, 0).await;
    }

    #[tokio::test]
    async fn disconnecting_subscriber_removes_its_channels() {
        let (address, db) = start().await;
        let mut subscriber = connect(&address).await;

        for channel in ["a", "b", "c"] {
            request(&mut subscriber, Subscribe::new(channel).into_frame()).await;
        }
        assert_eq!(channel_count(&db), 3);

        drop(subscriber);
        wait_for_channel_count(&db, 0).await;
    }
}
