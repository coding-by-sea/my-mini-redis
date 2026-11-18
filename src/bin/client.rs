use bytes::Bytes;
use clap::{Parser, Subcommand};
use my_redis::DEFAULT_ADDRESS;
use my_redis::client::Client;
use std::env;

#[derive(Debug, Parser)]
#[command(name = "my-redis", about = "A command-line client for my-redis")]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Debug, Subcommand)]
enum Command {
    /// Retrieve the value stored at a key.
    Get { key: String },
    /// Store a value at a key.
    Set { key: String, value: String },
    /// Publish a message to a channel.
    Publish { channel: String, message: String },
    /// Subscribe to one or more channels, then print messages as they arrive.
    /// While subscribed, type `subscribe <channel>...` or `unsubscribe [channel...]`
    /// to change subscriptions; leaving the last channel (or Ctrl-C) exits.
    Subscribe { channels: Vec<String> },
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let cli = Cli::parse();
    let address = env::var("MY_REDIS_ADDRESS").unwrap_or_else(|_| DEFAULT_ADDRESS.to_owned());
    let mut client = Client::new_with_address(&address).await?;

    match cli.command {
        Command::Get { key } => match client.get(&key).await? {
            Some(value) => println!("{}", String::from_utf8_lossy(&value)),
            None => println!("(nil)"),
        },
        Command::Set { key, value } => {
            client.set(&key, Bytes::from(value)).await?;
        }
        Command::Publish { channel, message } => {
            let recipients = client.publish(&channel, Bytes::from(message)).await?;
            println!("{}", recipients);
        }
        Command::Subscribe { channels } => {
            if channels.is_empty() {
                anyhow::bail!("at least one channel is required");
            }
            let channel_refs: Vec<&str> = channels.iter().map(|s| s.as_str()).collect();
            client.subscribe_multiple(&channel_refs).await?;
            client.enter_subscribe_mode().await?;
        }
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use clap::Parser;

    use super::{Cli, Command};

    #[test]
    fn parses_get() {
        let cli = Cli::try_parse_from(["client", "get", "greeting"]).unwrap();
        assert!(matches!(cli.command, Command::Get { key } if key == "greeting"));
    }

    #[test]
    fn parses_set() {
        let cli = Cli::try_parse_from(["client", "set", "greeting", "hello"]).unwrap();
        assert!(
            matches!(cli.command, Command::Set { key, value } if key == "greeting" && value == "hello")
        );
    }

    #[test]
    fn parses_publish() {
        let cli = Cli::try_parse_from(["client", "publish", "events", "created"]).unwrap();
        assert!(
            matches!(cli.command, Command::Publish { channel, message } if channel == "events" && message == "created")
        );
    }

    #[test]
    fn has_no_standalone_unsubscribe() {
        // Subscriptions belong to a connection, and each CLI run opens a new one.
        assert!(Cli::try_parse_from(["client", "unsubscribe", "events"]).is_err());
    }
}
