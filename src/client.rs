use crate::DEFAULT_ADDRESS;
use crate::cmd::{Get, Publish, Set, Subscribe, Unsubscribe};
use crate::connection::Connection;
use crate::frame::Frame;
use bytes::Bytes;
use tokio::net::TcpStream;
use std::collections::HashSet;
use tokio::io::{AsyncBufRead, AsyncBufReadExt, AsyncWriteExt, BufReader};

pub struct Client {
    connection: Connection,
    subscriptions: HashSet<String>,
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
        })
    }

    pub async fn set(&mut self, key: &str, value: Bytes) -> anyhow::Result<()> {
        let set_frame = Set::new(key, value).into_frame();
        self.connection.write_frame(set_frame).await?;
        let response = self.connection.read_frame().await?;
        match response {
            Some(Frame::Simple(_)) => Ok(()),
            Some(Frame::Error(err)) => anyhow::bail!(err),
            _ => unreachable!(),
        }
    }

    pub async fn get(&mut self, key: &str) -> anyhow::Result<Option<Bytes>> {
        let get_frame = Get::new(key).into_frame();
        self.connection.write_frame(get_frame).await?;
        let response = self.connection.read_frame().await?;
        match response {
            Some(Frame::Bulk(bytes)) => Ok(Some(bytes)),
            Some(Frame::Null) => Ok(None),
            None => Ok(None),
            _ => unreachable!(),
        }
    }

    pub async fn subscribe(&mut self, channel: &str) -> anyhow::Result<()> {
        let subscribe_frame = Subscribe::new(channel).into_frame();
        self.connection.write_frame(subscribe_frame).await?;
        let response = self.connection.read_frame().await?;
        match response {
            Some(Frame::Array(parts)) if matches_subscribe_ack(&parts, channel) => {
                self.subscriptions.insert(channel.to_string());
                Ok(())
            }
            Some(Frame::Error(err)) => anyhow::bail!(err),
            _ => anyhow::bail!("invalid response to SUBSCRIBE"),
        }
    }

    pub async fn subscribe_multiple(&mut self, channels: &[&str]) -> anyhow::Result<()> {
        for channel in channels {
            self.subscribe(channel).await?;
        }
        Ok(())
    }

    pub async fn unsubscribe(&mut self, channels: &[&str]) -> anyhow::Result<()> {
        let unsubscribe_frame = Unsubscribe::new(channels.iter().map(|s| s.to_string()).collect()).into_frame();
        self.connection.write_frame(unsubscribe_frame).await?;

        // Read acknowledgments for each channel
        for _ in 0..channels.len() {
            let response = self.connection.read_frame().await?;
            match response {
                Some(Frame::Array(parts)) if matches_unsubscribe_ack(&parts) => {
                    if let Some(Frame::Bulk(channel)) = parts.get(1) {
                        let channel_str = String::from_utf8_lossy(channel);
                        self.subscriptions.remove(channel_str.as_ref());
                    }
                }
                Some(Frame::Error(err)) => anyhow::bail!(err),
                _ => anyhow::bail!("invalid response to UNSUBSCRIBE"),
            }
        }
        Ok(())
    }

    pub async fn unsubscribe_all(&mut self) -> anyhow::Result<()> {
        let channels: Vec<String> = self.subscriptions.iter().cloned().collect();
        if channels.is_empty() {
            return Ok(());
        }

        let unsubscribe_frame = Unsubscribe::new(vec![]).into_frame();
        self.connection.write_frame(unsubscribe_frame).await?;

        // Read acknowledgments for each channel
        for _ in 0..channels.len() {
            let response = self.connection.read_frame().await?;
            match response {
                Some(Frame::Array(parts)) if matches_unsubscribe_ack(&parts) => {}
                Some(Frame::Error(err)) => anyhow::bail!(err),
                _ => anyhow::bail!("invalid response to UNSUBSCRIBE"),
            }
        }

        self.subscriptions.clear();
        Ok(())
    }

    pub async fn next_message(&mut self) -> anyhow::Result<Option<Message>> {
        match self.connection.read_frame().await? {
            Some(Frame::Array(parts)) if parts.len() == 3 => match (&parts[0], &parts[1], &parts[2]) {
                (Frame::Bulk(kind), Frame::Bulk(channel), Frame::Bulk(payload)) if kind.as_ref() == b"message" => {
                    Ok(Some(Message {
                        channel: String::from_utf8(channel.to_vec())?,
                        payload: payload.clone(),
                    }))
                }
                _ => anyhow::bail!("invalid subscription message"),
            },
            Some(Frame::Error(err)) => anyhow::bail!(err),
            None => Ok(None),
            _ => anyhow::bail!("invalid subscription message"),
        }
    }

    pub async fn publish(&mut self, channel: &str, message: Bytes) -> anyhow::Result<u64> {
        let publish_frame = Publish::new(channel, message).into_frame();
        self.connection.write_frame(publish_frame).await?;

        match self.connection.read_frame().await? {
            Some(Frame::Array(parts)) if parts.len() == 2 => match (&parts[0], &parts[1]) {
                (Frame::Bulk(kind), Frame::Integer(count)) if kind.as_ref() == b"publish" && *count >= 0 => {
                    Ok(*count as u64)
                }
                _ => anyhow::bail!("invalid response to PUBLISH"),
            },
            Some(Frame::Error(err)) => anyhow::bail!(err),
            _ => anyhow::bail!("invalid response to PUBLISH"),
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
        self.enter_subscribe_mode_with_input(BufReader::new(reader)).await
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
                    self.connection.write_frame(Subscribe::new(channel).into_frame()).await?;
                }
            }
            "subscribe" => eprintln!("usage: subscribe <channel> [channel...]"),
            "unsubscribe" => {
                self.connection.write_frame(Unsubscribe::new(channels).into_frame()).await?;
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
            [Frame::Bulk(kind), Frame::Bulk(channel), Frame::Bulk(payload)] if kind.as_ref() == b"message" => {
                println!("{}: {}", String::from_utf8_lossy(channel), String::from_utf8_lossy(payload));
                Ok(false)
            }
            [Frame::Bulk(kind), Frame::Bulk(channel), Frame::Integer(_)] if kind.as_ref() == b"subscribe" => {
                let channel = String::from_utf8(channel.to_vec())?;
                println!("subscribed to {}", channel);
                self.subscriptions.insert(channel);
                Ok(false)
            }
            // The server reports a count of 0 on every ack of a bare UNSUBSCRIBE, so the
            // local set decides when the last channel is gone.
            [Frame::Bulk(kind), Frame::Bulk(channel), Frame::Integer(_)] if kind.as_ref() == b"unsubscribe" => {
                let channel = String::from_utf8_lossy(channel);
                println!("unsubscribed from {}", channel);
                self.subscriptions.remove(channel.as_ref());
                Ok(self.subscriptions.is_empty())
            }
            _ => anyhow::bail!("invalid subscription message"),
        }
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

fn matches_unsubscribe_ack(parts: &[Frame]) -> bool {
    matches!(
        parts,
        [Frame::Bulk(kind), Frame::Bulk(_channel), Frame::Integer(_count)]
            if kind.as_ref() == b"unsubscribe"
    )
}
