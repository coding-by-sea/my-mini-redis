use crate::DEFAULT_ADDRESS;
use crate::cmd::{Get, Publish, Set, Subscribe, Unsubscribe};
use crate::connection::Connection;
use crate::frame::Frame;
use bytes::Bytes;
use std::collections::{HashSet, VecDeque};
use tokio::io::{AsyncBufRead, AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::TcpStream;

pub struct Client {
    connection: Connection,
    subscriptions: HashSet<String>,
    /// Messages that arrived while waiting for another reply, for `next_message`.
    pending: VecDeque<Message>,
}

#[derive(Debug, PartialEq, Eq)]
pub struct Message {
    pub channel: String,
    pub payload: Bytes,
}

impl Client {
    pub async fn new() -> anyhow::Result<Self> {
        Self::new_with_address(DEFAULT_ADDRESS).await
    }

    pub async fn new_with_address(address: &str) -> anyhow::Result<Self> {
        let connection = TcpStream::connect(address).await?;
        Ok(Client {
            connection: Connection::new(connection),
            subscriptions: HashSet::new(),
            pending: VecDeque::new(),
        })
    }

    pub async fn set(&mut self, key: &str, value: Bytes) -> anyhow::Result<()> {
        let set_frame = Set::new(key, value).into_frame();
        self.connection.write_frame(set_frame).await?;
        match self.read_reply().await? {
            Frame::Simple(_) => Ok(()),
            Frame::Error(err) => anyhow::bail!(err),
            _ => anyhow::bail!("invalid response to SET"),
        }
    }

    pub async fn get(&mut self, key: &str) -> anyhow::Result<Option<Bytes>> {
        let get_frame = Get::new(key).into_frame();
        self.connection.write_frame(get_frame).await?;
        match self.read_reply().await? {
            Frame::Bulk(bytes) => Ok(Some(bytes)),
            Frame::Null => Ok(None),
            Frame::Error(err) => anyhow::bail!(err),
            _ => anyhow::bail!("invalid response to GET"),
        }
    }

    pub async fn subscribe(&mut self, channel: &str) -> anyhow::Result<()> {
        self.subscribe_multiple(&[channel]).await
    }

    /// Subscribes to every channel with a single SUBSCRIBE.
    pub async fn subscribe_multiple(&mut self, channels: &[&str]) -> anyhow::Result<()> {
        let subscribe_frame = Subscribe::new_multiple(channels).into_frame();
        self.connection.write_frame(subscribe_frame).await?;

        // One acknowledgment per channel, in request order.
        for channel in channels {
            match self.read_reply().await? {
                Frame::Array(parts) if matches_subscribe_ack(&parts, channel) => {
                    self.subscriptions.insert(channel.to_string());
                }
                Frame::Error(err) => anyhow::bail!(err),
                _ => anyhow::bail!("invalid response to SUBSCRIBE"),
            }
        }
        Ok(())
    }

    /// Unsubscribes from the given channels; an empty list unsubscribes from all.
    pub async fn unsubscribe(&mut self, channels: &[&str]) -> anyhow::Result<()> {
        if channels.is_empty() {
            return self.unsubscribe_all().await;
        }
        let unsubscribe_frame = Unsubscribe::new(channels).into_frame();
        self.connection.write_frame(unsubscribe_frame).await?;

        // One acknowledgment per channel, in request order.
        for _ in channels {
            self.read_unsubscribe_ack().await?;
        }
        Ok(())
    }

    pub async fn unsubscribe_all(&mut self) -> anyhow::Result<()> {
        let unsubscribe_frame = Unsubscribe::all().into_frame();
        self.connection.write_frame(unsubscribe_frame).await?;

        // One acknowledgment per subscribed channel, or a single one with a nil channel
        // if there were none. The last one reports a count of 0.
        while self.read_unsubscribe_ack().await? > 0 {}
        self.subscriptions.clear();
        Ok(())
    }

    /// Reads an `[unsubscribe, channel, count]` acknowledgment and returns the count.
    async fn read_unsubscribe_ack(&mut self) -> anyhow::Result<i64> {
        match self.read_reply().await? {
            Frame::Array(parts) => match parts.as_slice() {
                [Frame::Bulk(kind), channel, Frame::Integer(count)]
                    if kind.as_ref() == b"unsubscribe" =>
                {
                    if let Frame::Bulk(channel) = channel {
                        self.subscriptions
                            .remove(String::from_utf8_lossy(channel).as_ref());
                    }
                    Ok(*count)
                }
                _ => anyhow::bail!("invalid response to UNSUBSCRIBE"),
            },
            Frame::Error(err) => anyhow::bail!(err),
            _ => anyhow::bail!("invalid response to UNSUBSCRIBE"),
        }
    }

    pub async fn next_message(&mut self) -> anyhow::Result<Option<Message>> {
        if let Some(message) = self.pending.pop_front() {
            return Ok(Some(message));
        }
        match self.connection.read_frame().await? {
            Some(frame) => match parse_message(&frame) {
                Some(message) => Ok(Some(message)),
                None => match frame {
                    Frame::Error(err) => anyhow::bail!(err),
                    _ => anyhow::bail!("invalid subscription message"),
                },
            },
            None => Ok(None),
        }
    }

    pub async fn publish(&mut self, channel: &str, message: Bytes) -> anyhow::Result<u64> {
        let publish_frame = Publish::new(channel, message).into_frame();
        self.connection.write_frame(publish_frame).await?;
        match self.read_reply().await? {
            Frame::Integer(count) if count >= 0 => Ok(count as u64),
            Frame::Error(err) => anyhow::bail!(err),
            _ => anyhow::bail!("invalid response to PUBLISH"),
        }
    }

    /// Reads the reply to a command. Published messages can arrive first on a
    /// subscribed connection; they are kept for `next_message`.
    async fn read_reply(&mut self) -> anyhow::Result<Frame> {
        loop {
            let Some(frame) = self.connection.read_frame().await? else {
                anyhow::bail!("connection closed by server");
            };
            match parse_message(&frame) {
                Some(message) => self.pending.push_back(message),
                None => return Ok(frame),
            }
        }
    }

    pub async fn enter_subscribe_mode(&mut self) -> anyhow::Result<()> {
        // Tokio's stdin reads on a blocking-pool thread that the runtime waits for at
        // shutdown, so a pending read would keep the process alive after this returns.
        // A detached std thread doesn't block exit; it forwards stdin through a pipe.
        let (mut writer, reader) = tokio::io::duplex(4096);
        let runtime = tokio::runtime::Handle::current();
        std::thread::spawn(move || {
            for line in std::io::stdin().lines() {
                let Ok(line) = line else { break };
                let sent = runtime.block_on(async {
                    writer.write_all(line.as_bytes()).await?;
                    writer.write_all(b"\n").await
                });
                if sent.is_err() {
                    break;
                }
            }
        });
        self.enter_subscribe_mode_with_input(BufReader::new(reader))
            .await
    }

    /// Prints incoming messages while accepting `subscribe <channel>...` and
    /// `unsubscribe [channel...]` lines from `input`. Returns when the server closes
    /// the connection or the last subscription is removed.
    pub async fn enter_subscribe_mode_with_input<R>(&mut self, input: R) -> anyhow::Result<()>
    where
        R: AsyncBufRead + Unpin,
    {
        let mut lines = input.lines();
        let mut input_open = true;

        while let Some(message) = self.pending.pop_front() {
            print_message(&message);
        }

        loop {
            tokio::select! {
                // Poll commands first so a busy channel can't delay subscribe/unsubscribe.
                biased;

                line = lines.next_line(), if input_open => match line? {
                    Some(line) => self.send_subscribe_mode_command(&line).await?,
                    None => input_open = false,
                },
                // `read_frame` only awaits on `read_buf`, so dropping it mid-read loses no data.
                frame = self.connection.read_frame() => match frame? {
                    Some(frame) => {
                        if self.handle_subscribe_mode_frame(frame)? {
                            return Ok(());
                        }
                    }
                    None => return Ok(()),
                },
            }
        }
    }

    async fn send_subscribe_mode_command(&mut self, line: &str) -> anyhow::Result<()> {
        let mut words = line.split_whitespace();
        let Some(command) = words.next() else {
            return Ok(());
        };
        let channels: Vec<String> = words.map(str::to_string).collect();

        match command.to_ascii_lowercase().as_str() {
            "subscribe" if !channels.is_empty() => {
                for channel in channels {
                    self.connection
                        .write_frame(Subscribe::new(channel).into_frame())
                        .await?;
                }
            }
            "subscribe" => eprintln!("usage: subscribe <channel> [channel...]"),
            "unsubscribe" => {
                self.connection
                    .write_frame(Unsubscribe::new(channels).into_frame())
                    .await?;
            }
            _ => eprintln!("only subscribe and unsubscribe are allowed in subscribe mode"),
        }
        Ok(())
    }

    /// Returns true once no subscriptions remain.
    fn handle_subscribe_mode_frame(&mut self, frame: Frame) -> anyhow::Result<bool> {
        let parts = match frame {
            Frame::Array(parts) => parts,
            Frame::Error(err) => anyhow::bail!(err),
            _ => anyhow::bail!("invalid subscription message"),
        };

        match parts.as_slice() {
            [
                Frame::Bulk(kind),
                Frame::Bulk(channel),
                Frame::Bulk(payload),
            ] if kind.as_ref() == b"message" => {
                print_message(&Message {
                    channel: String::from_utf8_lossy(channel).into_owned(),
                    payload: payload.clone(),
                });
                Ok(false)
            }
            [Frame::Bulk(kind), Frame::Bulk(channel), Frame::Integer(_)]
                if kind.as_ref() == b"subscribe" =>
            {
                let channel = String::from_utf8(channel.to_vec())?;
                println!("subscribed to {}", channel);
                self.subscriptions.insert(channel);
                Ok(false)
            }
            [
                Frame::Bulk(kind),
                Frame::Bulk(channel),
                Frame::Integer(count),
            ] if kind.as_ref() == b"unsubscribe" => {
                let channel = String::from_utf8_lossy(channel);
                println!("unsubscribed from {}", channel);
                self.subscriptions.remove(channel.as_ref());
                Ok(*count == 0)
            }
            // A bare UNSUBSCRIBE with no subscriptions left.
            [Frame::Bulk(kind), Frame::Null, Frame::Integer(_)]
                if kind.as_ref() == b"unsubscribe" =>
            {
                Ok(true)
            }
            _ => anyhow::bail!("invalid subscription message"),
        }
    }
}

fn print_message(message: &Message) {
    println!(
        "{}: {}",
        message.channel,
        String::from_utf8_lossy(&message.payload)
    );
}

/// Returns the message if `frame` is a published `[message, channel, payload]`.
fn parse_message(frame: &Frame) -> Option<Message> {
    match frame {
        Frame::Array(parts) => match parts.as_slice() {
            [
                Frame::Bulk(kind),
                Frame::Bulk(channel),
                Frame::Bulk(payload),
            ] if kind.as_ref() == b"message" => Some(Message {
                channel: String::from_utf8_lossy(channel).into_owned(),
                payload: payload.clone(),
            }),
            _ => None,
        },
        _ => None,
    }
}

fn matches_subscribe_ack(parts: &[Frame], expected_channel: &str) -> bool {
    matches!(
        parts,
        [Frame::Bulk(kind), Frame::Bulk(channel), Frame::Integer(count)]
            if kind.as_ref() == b"subscribe"
                && channel.as_ref() == expected_channel.as_bytes()
                && *count >= 1
    )
}
