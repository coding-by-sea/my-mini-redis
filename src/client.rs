use crate::DEFAULT_ADDRESS;
use crate::cmd::{Get, Publish, Set, Subscribe, Unsubscribe};
use crate::connection::Connection;
use crate::frame::Frame;
use bytes::Bytes;
use std::collections::VecDeque;
use tokio::io::{AsyncBufRead, AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::TcpStream;

pub struct Client {
    connection: Connection,
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
            pending: VecDeque::new(),
        })
    }

    pub async fn set(&mut self, key: &str, value: Bytes) -> anyhow::Result<()> {
        let set_frame = Set::new(key, value).into_frame();
        self.connection.write_frame(set_frame).await?;
        match self.read_reply().await? {
            Frame::Simple(_) => Ok(()),
            _ => anyhow::bail!("invalid response to SET"),
        }
    }

    pub async fn get(&mut self, key: &str) -> anyhow::Result<Option<Bytes>> {
        let get_frame = Get::new(key).into_frame();
        self.connection.write_frame(get_frame).await?;
        match self.read_reply().await? {
            Frame::Bulk(bytes) => Ok(Some(bytes)),
            Frame::Null => Ok(None),
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
            match Push::from_frame(&self.read_reply().await?) {
                Some(Push::Subscribed {
                    channel: acknowledged,
                    count,
                }) if acknowledged == channel.as_bytes() && count >= 1 => {}
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
        Ok(())
    }

    /// Reads an `[unsubscribe, channel, count]` acknowledgment and returns the count.
    async fn read_unsubscribe_ack(&mut self) -> anyhow::Result<i64> {
        match Push::from_frame(&self.read_reply().await?) {
            Some(Push::Unsubscribed { count, .. }) => Ok(count),
            _ => anyhow::bail!("invalid response to UNSUBSCRIBE"),
        }
    }

    pub async fn next_message(&mut self) -> anyhow::Result<Option<Message>> {
        if let Some(message) = self.pending.pop_front() {
            return Ok(Some(message));
        }
        let Some(frame) = self.connection.read_frame().await? else {
            return Ok(None);
        };
        match Push::from_frame(&error_to_err(frame)?) {
            Some(Push::Message(message)) => Ok(Some(message)),
            _ => anyhow::bail!("invalid subscription message"),
        }
    }

    pub async fn publish(&mut self, channel: &str, message: Bytes) -> anyhow::Result<u64> {
        let publish_frame = Publish::new(channel, message).into_frame();
        self.connection.write_frame(publish_frame).await?;
        match self.read_reply().await? {
            Frame::Integer(count) if count >= 0 => Ok(count as u64),
            _ => anyhow::bail!("invalid response to PUBLISH"),
        }
    }

    /// Reads the reply to a command; an error reply is returned as an `Err`.
    /// Published messages can arrive first on a subscribed connection; they are kept
    /// for `next_message`.
    async fn read_reply(&mut self) -> anyhow::Result<Frame> {
        loop {
            let Some(frame) = self.connection.read_frame().await? else {
                anyhow::bail!("connection closed by server");
            };
            match Push::from_frame(&frame) {
                Some(Push::Message(message)) => self.pending.push_back(message),
                _ => return error_to_err(frame),
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
                self.connection
                    .write_frame(Subscribe::new_multiple(channels).into_frame())
                    .await?;
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
        match Push::from_frame(&error_to_err(frame)?) {
            Some(Push::Message(message)) => {
                print_message(&message);
                Ok(false)
            }
            Some(Push::Subscribed { channel, .. }) => {
                println!("subscribed to {}", String::from_utf8_lossy(&channel));
                Ok(false)
            }
            Some(Push::Unsubscribed { channel, count }) => {
                // The channel is nil for a bare UNSUBSCRIBE with no subscriptions left.
                if let Some(channel) = channel {
                    println!("unsubscribed from {}", String::from_utf8_lossy(&channel));
                }
                Ok(count == 0)
            }
            None => anyhow::bail!("invalid subscription message"),
        }
    }
}

/// A pub/sub push from the server.
enum Push {
    /// `[message, channel, payload]`: a published message.
    Message(Message),
    /// `[subscribe, channel, count]`: count is the subscriptions after this one.
    Subscribed { channel: Bytes, count: i64 },
    /// `[unsubscribe, channel, count]`: the channel is `None` (nil) when there was
    /// nothing to unsubscribe from.
    Unsubscribed { channel: Option<Bytes>, count: i64 },
}

impl Push {
    /// Returns `None` for any frame that isn't a pub/sub push.
    fn from_frame(frame: &Frame) -> Option<Push> {
        let Frame::Array(parts) = frame else {
            return None;
        };
        let [Frame::Bulk(kind), channel, last] = parts.as_slice() else {
            return None;
        };
        match (kind.as_ref(), channel, last) {
            (b"message", Frame::Bulk(channel), Frame::Bulk(payload)) => {
                Some(Push::Message(Message {
                    channel: String::from_utf8_lossy(channel).into_owned(),
                    payload: payload.clone(),
                }))
            }
            (b"subscribe", Frame::Bulk(channel), &Frame::Integer(count)) => {
                Some(Push::Subscribed {
                    channel: channel.clone(),
                    count,
                })
            }
            (b"unsubscribe", Frame::Bulk(channel), &Frame::Integer(count)) => {
                Some(Push::Unsubscribed {
                    channel: Some(channel.clone()),
                    count,
                })
            }
            (b"unsubscribe", Frame::Null, &Frame::Integer(count)) => Some(Push::Unsubscribed {
                channel: None,
                count,
            }),
            _ => None,
        }
    }
}

/// Turns an error reply into an `Err` carrying the server's message.
fn error_to_err(frame: Frame) -> anyhow::Result<Frame> {
    match frame {
        Frame::Error(message) => Err(anyhow::Error::msg(message)),
        frame => Ok(frame),
    }
}

fn print_message(message: &Message) {
    println!(
        "{}: {}",
        message.channel,
        String::from_utf8_lossy(&message.payload)
    );
}

#[cfg(test)]
mod tests {
    use super::{Message, Push};
    use crate::frame::Frame;
    use bytes::Bytes;

    fn bulk(text: &'static str) -> Frame {
        Frame::Bulk(Bytes::from_static(text.as_bytes()))
    }

    #[test]
    fn classifies_pub_sub_pushes() {
        let message = Frame::Array(vec![bulk("message"), bulk("news"), bulk("hi")]);
        assert!(matches!(
            Push::from_frame(&message),
            Some(Push::Message(Message { channel, payload })) if channel == "news" && payload == "hi"
        ));

        let subscribed = Frame::Array(vec![bulk("subscribe"), bulk("news"), Frame::Integer(1)]);
        assert!(matches!(
            Push::from_frame(&subscribed),
            Some(Push::Subscribed { channel, count: 1 }) if channel == "news"
        ));

        let unsubscribed = Frame::Array(vec![bulk("unsubscribe"), bulk("news"), Frame::Integer(0)]);
        assert!(matches!(
            Push::from_frame(&unsubscribed),
            Some(Push::Unsubscribed { channel: Some(channel), count: 0 }) if channel == "news"
        ));

        let nothing_to_leave =
            Frame::Array(vec![bulk("unsubscribe"), Frame::Null, Frame::Integer(0)]);
        assert!(matches!(
            Push::from_frame(&nothing_to_leave),
            Some(Push::Unsubscribed {
                channel: None,
                count: 0
            })
        ));
    }

    #[test]
    fn other_frames_are_not_pushes() {
        for frame in [
            Frame::Simple("OK".into()),
            Frame::Array(vec![bulk("message"), bulk("news")]),
            Frame::Array(vec![bulk("message"), bulk("news"), Frame::Integer(1)]),
            Frame::Array(vec![bulk("subscribe"), Frame::Null, Frame::Integer(1)]),
            Frame::Array(vec![bulk("pong"), bulk(""), bulk("")]),
        ] {
            assert!(Push::from_frame(&frame).is_none(), "{frame:?}");
        }
    }
}
