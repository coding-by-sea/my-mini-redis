use crate::cmd::Command;
use crate::connection::{Connection, ReadRequestError};
use crate::frame::Frame;
use crate::request::ProtocolError;
use crate::subscription::{self, Message, QUEUE_LIMIT_BYTES, Subscriptions};
use crate::{DEFAULT_ADDRESS, Db, lock};
use bytes::Bytes;
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
        let overflow = session.subscriptions.overflow_signal();

        loop {
            tokio::select! {
                request = connection.read_request() => {
                    let args = match request {
                        Ok(Some(args)) => args,
                        Ok(None) => return Ok(()),
                        Err(ReadRequestError::Protocol(ProtocolError(message))) => {
                            connection
                                .write_frame(error_frame(format!("ERR Protocol error: {message}")))
                                .await?;
                            return Ok(());
                        }
                        Err(ReadRequestError::Io(error)) => return Err(error),
                    };
                    let (replies, next) = session.execute(args);
                    for reply in replies {
                        connection.write_frame(reply).await?;
                    }
                    if let Next::Close = next {
                        return Ok(());
                    }
                }
                message = session.subscriptions.next_message() => {
                    connection.write_frame(message_frame(message)).await?;
                }
                () = overflow.wait() => {
                    anyhow::bail!(
                        "closed subscriber: over {} MB of messages queued",
                        QUEUE_LIMIT_BYTES / (1024 * 1024)
                    );
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
}

impl Session {
    fn new(db: Db) -> Self {
        Self {
            db,
            subscriptions: Subscriptions::new(),
        }
    }

    /// Runs one request and returns the frames to send, in order.
    fn execute(&mut self, args: Vec<Bytes>) -> (Vec<Frame>, Next) {
        let mut state = lock(&self.db);
        // Messages published before this command go out before its reply, as in Redis.
        // Taking them under the lock means none can slip in between.
        let mut replies: Vec<Frame> = self
            .subscriptions
            .take_queued()
            .into_iter()
            .map(message_frame)
            .collect();

        let command = match Command::from_args(args) {
            Ok(command) => command,
            Err(message) => {
                replies.push(error_frame(message));
                return (replies, Next::Continue);
            }
        };
        if !self.subscriptions.is_empty() && !command.allowed_while_subscribed() {
            replies.push(error_frame(format!(
                "ERR Can't execute '{}': only (P|S)SUBSCRIBE / (P|S)UNSUBSCRIBE / PING / QUIT / RESET are allowed in this context",
                command.name()
            )));
            return (replies, Next::Continue);
        }

        match command {
            Command::Get(cmd) => {
                replies.push(
                    state
                        .entries
                        .get(cmd.key())
                        .cloned()
                        .map_or(Frame::Null, Frame::Bulk),
                );
            }
            Command::Set(cmd) => {
                state.entries.insert(cmd.key().clone(), cmd.value());
                replies.push(Frame::Simple("OK".into()));
            }
            Command::Publish(cmd) => {
                let receivers = subscription::publish(&state, cmd.channel(), cmd.message());
                replies.push(Frame::Integer(receivers as i64));
            }
            Command::Subscribe(cmd) => {
                for channel in cmd.channels() {
                    self.subscriptions.subscribe(&mut state, channel.clone());
                    replies.push(push_frame(
                        "subscribe",
                        Frame::Bulk(channel.clone()),
                        self.subscriptions.count(),
                    ));
                }
            }
            Command::Unsubscribe(cmd) => {
                let channels = if cmd.channels().is_empty() {
                    self.subscriptions.channels()
                } else {
                    cmd.channels().to_vec()
                };
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
            }
            Command::Ping(message) => replies.push(if self.subscriptions.is_empty() {
                message.map_or(Frame::Simple("PONG".into()), Frame::Bulk)
            } else {
                Frame::Array(vec![
                    Frame::Bulk(Bytes::from_static(b"pong")),
                    Frame::Bulk(message.unwrap_or_default()),
                ])
            }),
            Command::Quit => {
                replies.push(Frame::Simple("OK".into()));
                return (replies, Next::Close);
            }
            Command::Reset => {
                self.subscriptions.unsubscribe_all(&mut state);
                replies.push(Frame::Simple("RESET".into()));
            }
        }
        (replies, Next::Continue)
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

fn message_frame((channel, payload): Message) -> Frame {
    Frame::Array(vec![
        Frame::Bulk(Bytes::from_static(b"message")),
        Frame::Bulk(channel),
        Frame::Bulk(payload),
    ])
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
