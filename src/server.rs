use crate::cmd::Command;
use crate::connection::{ReadRequestError, RequestReader};
use crate::frame::Frame;
use crate::outbox::{Outbox, QUEUE_LIMIT_BYTES};
use crate::request::ProtocolError;
use crate::subscription::{self, Subscriptions, Wakes};
use crate::{DEFAULT_ADDRESS, Db, lock};
use bytes::{Bytes, BytesMut};
use std::sync::Arc;
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
        let (read, write) = socket.into_split();
        let mut reader = RequestReader::new(read);
        let mut session = Session::new(db, Outbox::new(Arc::new(write)));

        loop {
            tokio::select! {
                request = reader.read_request() => {
                    let args = match request {
                        Ok(Some(args)) => args,
                        Ok(None) => return Ok(()),
                        Err(ReadRequestError::Protocol(error)) => {
                            return session.protocol_error(error).await;
                        }
                        Err(ReadRequestError::Io(error)) => return Err(error),
                    };
                    if let Next::Close = session.serve_batch(&mut reader, args).await? {
                        return Ok(());
                    }
                }
                () = session.outbox.ready() => {
                    if session.outbox.overflowed() {
                        anyhow::bail!(
                            "closed subscriber: over {} MB of messages queued",
                            QUEUE_LIMIT_BYTES / (1024 * 1024)
                        );
                    }
                    session.outbox.flush().await?;
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
    /// Everything sent to the client: replies, and messages published to it.
    outbox: Outbox,
    /// Replies not yet moved to the outbox, so a batch takes its lock once.
    replies: BytesMut,
    subscriptions: Subscriptions,
    /// Subscribers this connection's PUBLISHes queued messages for, sent to after each
    /// batch of requests.
    wakes: Wakes,
}

impl Session {
    fn new(db: Db, outbox: Outbox) -> Self {
        Self {
            db,
            subscriptions: Subscriptions::new(outbox.clone()),
            outbox,
            replies: BytesMut::new(),
            wakes: Wakes::default(),
        }
    }

    /// Runs `args`, then every further request already received, and sends the
    /// replies with one write instead of one per reply. Returns `Next::Close` once the
    /// connection has been closed: by QUIT, or after a protocol error.
    ///
    /// This client's replies go out first, without waiting, then the messages the
    /// batch published, each subscriber's with one write. Waiting for a full socket
    /// comes last, so a client that is slow to read its replies doesn't hold up its
    /// subscribers.
    ///
    /// Like Redis's event loop, each client is served one batch at a time: the task
    /// yields after a batch. While a client's next batch is already waiting, reading
    /// it doesn't yield, so without this the task would run batch after batch in one
    /// poll (until tokio's budget of about 128 runs out), and no other worker would
    /// collect socket events meanwhile: publishers then waited 100+ ms behind other
    /// publishers, and PUBLISH ran at a quarter of its rate behind pipelined SETs. A
    /// single command that wrote to nobody else doesn't yield: its client almost
    /// always waits for the reply, so the next read suspends the task anyway, and
    /// yielding would only add a scheduler round trip (12% more CPU per request,
    /// measured).
    async fn serve_batch(
        &mut self,
        reader: &mut RequestReader,
        mut args: Vec<Bytes>,
    ) -> anyhow::Result<Next> {
        let mut commands = 0;
        loop {
            commands += 1;
            if let Next::Close = self.execute(args) {
                self.push_replies();
                self.outbox.finish().await?;
                return Ok(Next::Close);
            }
            match reader.buffered_request() {
                Ok(Some(more)) => args = more,
                Ok(None) => break,
                Err(error) => {
                    self.protocol_error(error).await?;
                    return Ok(Next::Close);
                }
            }
        }
        self.push_replies();
        self.outbox.write_now();
        let wrote_to_others = self.wakes.write_all();
        self.outbox.flush().await?;
        if commands > 1 || wrote_to_others {
            tokio::task::yield_now().await;
        }
        Ok(Next::Continue)
    }

    /// Replies with a protocol error, sending any replies already encoded first. The
    /// caller then closes the connection.
    async fn protocol_error(
        &mut self,
        ProtocolError(message): ProtocolError,
    ) -> anyhow::Result<()> {
        error_frame(format!("ERR Protocol error: {message}")).encode(&mut self.replies);
        self.push_replies();
        self.outbox.finish().await?;
        Ok(())
    }

    /// Runs one request, encoding its replies.
    ///
    /// A db lock is only held for the step of a command that reads or changes shared
    /// state: a key's shard for GET and SET, the pub/sub lock for the rest. Messages
    /// reach the outbox under the pub/sub lock, so SUBSCRIBE,
    /// UNSUBSCRIBE and RESET move their replies to the outbox before releasing it: a
    /// channel's messages then come after its subscribe acknowledgment and before its
    /// unsubscribe acknowledgment, as in Redis. Other replies of a subscribed
    /// connection are moved right after their command; a PUBLISH running concurrently
    /// may land on either side of them, as in Redis.
    fn execute(&mut self, args: Vec<Bytes>) -> Next {
        let next = self.run(args);
        if !self.subscriptions.is_empty() {
            self.push_replies();
        }
        next
    }

    fn run(&mut self, args: Vec<Bytes>) -> Next {
        let command = match Command::from_args(args) {
            Ok(command) => command,
            Err(message) => return self.reply(error_frame(message), Next::Continue),
        };
        if !self.subscriptions.is_empty() && !command.allowed_while_subscribed() {
            let error = error_frame(format!(
                "ERR Can't execute '{}': only (P|S)SUBSCRIBE / (P|S)UNSUBSCRIBE / PING / QUIT / RESET are allowed in this context",
                command.name()
            ));
            return self.reply(error, Next::Continue);
        }

        match command {
            Command::Get(cmd) => {
                let value = self.db.entries.get(cmd.key()).map(|value| value.clone());
                self.reply(value.map_or(Frame::Null, Frame::Bulk), Next::Continue)
            }
            Command::Set(cmd) => {
                self.db.entries.insert(cmd.key().clone(), cmd.value());
                self.reply(Frame::Simple("OK".into()), Next::Continue)
            }
            Command::Publish(cmd) => {
                let receivers = subscription::publish(
                    &lock(&self.db.registry),
                    cmd.channel(),
                    cmd.message(),
                    &mut self.wakes,
                );
                self.reply(Frame::Integer(receivers as i64), Next::Continue)
            }
            Command::Subscribe(cmd) => self.subscribe(cmd.channels()),
            Command::Unsubscribe(cmd) => self.unsubscribe(cmd.channels()),
            Command::Ping(message) => {
                let pong = if self.subscriptions.is_empty() {
                    message.map_or(Frame::Simple("PONG".into()), Frame::Bulk)
                } else {
                    Frame::Array(vec![
                        Frame::Bulk(Bytes::from_static(b"pong")),
                        Frame::Bulk(message.unwrap_or_default()),
                    ])
                };
                self.reply(pong, Next::Continue)
            }
            Command::Quit => self.reply(Frame::Simple("OK".into()), Next::Close),
            Command::Reset => self.reset(),
        }
    }

    // SUBSCRIBE, UNSUBSCRIBE and RESET move their replies to the outbox while still
    // holding the pub/sub lock, so they are ordered with the channels' messages.

    fn subscribe(&mut self, channels: &[Bytes]) -> Next {
        let mut registry = lock(&self.db.registry);
        for channel in channels {
            self.subscriptions.subscribe(&mut registry, channel.clone());
            let count = self.subscriptions.count();
            push_frame("subscribe", Frame::Bulk(channel.clone()), count).encode(&mut self.replies);
        }
        self.outbox.push(&mut self.replies);
        Next::Continue
    }

    /// Unsubscribes from `channels`, or from every channel if it is empty.
    fn unsubscribe(&mut self, channels: &[Bytes]) -> Next {
        let mut registry = lock(&self.db.registry);
        let channels = if channels.is_empty() {
            self.subscriptions.channels()
        } else {
            channels.to_vec()
        };
        if channels.is_empty() {
            push_frame("unsubscribe", Frame::Null, 0).encode(&mut self.replies);
        }
        for channel in channels {
            self.subscriptions.unsubscribe(&mut registry, &channel);
            let count = self.subscriptions.count();
            push_frame("unsubscribe", Frame::Bulk(channel), count).encode(&mut self.replies);
        }
        self.outbox.push(&mut self.replies);
        Next::Continue
    }

    fn reset(&mut self) -> Next {
        let mut registry = lock(&self.db.registry);
        self.subscriptions.unsubscribe_all(&mut registry);
        Frame::Simple("RESET".into()).encode(&mut self.replies);
        self.outbox.push(&mut self.replies);
        Next::Continue
    }

    fn reply(&mut self, reply: Frame, next: Next) -> Next {
        reply.encode(&mut self.replies);
        next
    }

    /// Moves the encoded replies to the outbox.
    fn push_replies(&mut self) {
        self.outbox.push(&mut self.replies);
    }
}

impl Drop for Session {
    fn drop(&mut self) {
        let mut registry = lock(&self.db.registry);
        self.subscriptions.unsubscribe_all(&mut registry);
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
        db.registry.lock().unwrap().len()
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

    /// Reads the next frame, failing the test on a timeout, a closed connection or
    /// bytes that don't parse, which is what interleaved writes would produce.
    async fn next_frame(connection: &mut Connection) -> Frame {
        timeout(Duration::from_secs(10), connection.read_frame())
            .await
            .expect("no frame")
            .expect("malformed frame")
            .expect("connection closed")
    }

    /// `[kind, channel, payload]` of a pub/sub push, or `None` for any other frame.
    fn push_parts(frame: &Frame) -> Option<(Bytes, Bytes, Frame)> {
        match frame {
            Frame::Array(parts) if parts.len() == 3 => match (&parts[0], &parts[1], &parts[2]) {
                (Frame::Bulk(kind), Frame::Bulk(channel), Frame::Bulk(payload)) => {
                    Some((kind.clone(), channel.clone(), Frame::Bulk(payload.clone())))
                }
                (Frame::Bulk(kind), Frame::Bulk(channel), Frame::Integer(count)) => {
                    Some((kind.clone(), channel.clone(), Frame::Integer(*count)))
                }
                _ => None,
            },
            _ => None,
        }
    }

    /// Several publishers write to the subscriber's socket while the subscriber's own
    /// task writes PING replies to it. Every frame must arrive whole, each publisher's
    /// messages complete and in order, and every PING answered. The subscriber pauses
    /// now and then, so its socket fills up: writes are then partial and the writer
    /// token is handed over, which is when stray writes would split a frame.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn concurrent_writers_keep_frames_whole_and_ordered() {
        const PUBLISHERS: usize = 4;
        const MESSAGES: usize = 600;
        const PIPELINE: usize = 50;
        let (address, _db) = start().await;
        let mut subscriber = connect(&address).await;
        request(&mut subscriber, Subscribe::new("c").into_frame()).await;

        let mut publishers = Vec::new();
        for publisher in 0..PUBLISHERS {
            let address = address.clone();
            publishers.push(tokio::spawn(async move {
                let mut connection = connect(&address).await;
                for batch in (0..MESSAGES).collect::<Vec<_>>().chunks(PIPELINE) {
                    for &seq in batch {
                        let size = if seq % 7 == 0 { 32 * 1024 } else { 64 };
                        let mut payload = format!("{publisher}:{seq}:").into_bytes();
                        payload.resize(size, b'x');
                        let frame = Publish::new("c", Bytes::from(payload)).into_frame();
                        connection.write_frame(frame).await.unwrap();
                    }
                    for _ in batch {
                        next_frame(&mut connection).await;
                    }
                }
            }));
        }

        let mut next_seq = [0usize; PUBLISHERS];
        let (mut pings, mut pongs) = (0, 0);
        while next_seq.iter().any(|&seq| seq < MESSAGES) || pongs < pings {
            if pings < 200 && next_seq.iter().sum::<usize>() % 10 == 0 {
                subscriber
                    .write_frame(Frame::Array(vec![Frame::Bulk(Bytes::from_static(b"PING"))]))
                    .await
                    .unwrap();
                pings += 1;
            }
            if (next_seq.iter().sum::<usize>() + pongs) % 100 == 99 {
                sleep(Duration::from_millis(2)).await;
            }
            let frame = next_frame(&mut subscriber).await;
            if matches!(&frame, Frame::Array(parts) if parts.len() == 2) {
                pongs += 1;
                continue;
            }
            let (kind, channel, payload) = push_parts(&frame).expect("unexpected frame");
            assert_eq!((&kind[..], &channel[..]), (&b"message"[..], &b"c"[..]));
            let payload = match payload {
                Frame::Bulk(payload) => payload,
                _ => panic!("message without a payload"),
            };
            let text = String::from_utf8_lossy(&payload);
            let mut fields = text.split(':');
            let publisher: usize = fields.next().unwrap().parse().unwrap();
            let seq: usize = fields.next().unwrap().parse().unwrap();
            assert_eq!(
                seq, next_seq[publisher],
                "publisher {publisher} out of order"
            );
            assert!(payload.len() == 64 || payload.len() == 32 * 1024);
            next_seq[publisher] += 1;
        }
        for publisher in publishers {
            publisher.await.unwrap();
        }
        assert_eq!(pongs, pings);
    }

    /// While a publisher publishes without pause, no message may arrive between an
    /// UNSUBSCRIBE acknowledgment and the next SUBSCRIBE acknowledgment, and each
    /// subscription's messages arrive in order.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn acknowledgments_stay_ordered_with_messages_under_load() {
        let (address, _db) = start().await;
        let mut subscriber = connect(&address).await;
        let stop = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
        let publisher = {
            let (address, stop) = (address.clone(), stop.clone());
            tokio::spawn(async move {
                let mut connection = connect(&address).await;
                let mut seq = 0u64;
                while !stop.load(std::sync::atomic::Ordering::Relaxed) {
                    for _ in 0..16 {
                        let frame = Publish::new("c", Bytes::from(seq.to_string())).into_frame();
                        connection.write_frame(frame).await.unwrap();
                        seq += 1;
                    }
                    for _ in 0..16 {
                        next_frame(&mut connection).await;
                    }
                }
            })
        };

        for _ in 0..50 {
            subscriber
                .write_frame(Subscribe::new("c").into_frame())
                .await
                .unwrap();
            let ack = next_frame(&mut subscriber).await;
            assert_eq!(
                push_parts(&ack).map(|(kind, _, _)| kind),
                Some(Bytes::from_static(b"subscribe")),
                "expected the subscribe acknowledgment first"
            );
            // Let some messages through, then unsubscribe while they keep coming.
            let mut last = None;
            let mut seen = 0;
            subscriber
                .write_frame(Unsubscribe::new(vec!["c".into()]).into_frame())
                .await
                .unwrap();
            loop {
                let frame = next_frame(&mut subscriber).await;
                let (kind, _, payload) = push_parts(&frame).expect("unexpected frame");
                if &kind[..] == b"unsubscribe" {
                    break;
                }
                assert_eq!(&kind[..], b"message");
                let payload = match payload {
                    Frame::Bulk(payload) => payload,
                    _ => panic!("message without a payload"),
                };
                let seq: u64 = String::from_utf8_lossy(&payload).parse().unwrap();
                assert!(
                    last.is_none_or(|last| seq == last + 1),
                    "messages out of order"
                );
                last = Some(seq);
                seen += 1;
            }
            let _ = seen;
            // Nothing for "c" may follow the unsubscribe acknowledgment: the next
            // frame must be the reply to this PING.
            subscriber
                .write_frame(Frame::Array(vec![Frame::Bulk(Bytes::from_static(b"PING"))]))
                .await
                .unwrap();
            let pong = next_frame(&mut subscriber).await;
            assert!(
                matches!(&pong, Frame::Simple(text) if text == "PONG"),
                "a message arrived after the unsubscribe acknowledgment: {pong:?}"
            );
        }
        stop.store(true, std::sync::atomic::Ordering::Relaxed);
        publisher.await.unwrap();
    }
}
