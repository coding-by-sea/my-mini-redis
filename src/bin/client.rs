use bytes::Bytes;
use clap::{Parser, Subcommand};
use my_redis::DEFAULT_ADDRESS;
use my_redis::client::Client;
use std::env;

#[derive(Debug, Parser)]
#[command(name = "my-mini-redis", about = "A command-line client for mini-redis")]
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
    /// Subscribe to one or more channels.
    Subscribe { channels: Vec<String> },
    /// Unsubscribe from one or more channels (empty = all).
    Unsubscribe { channels: Vec<String> },
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let cli = Cli::parse();
    let address = env::var("MY_MINI_REDIS_ADDRESS").unwrap_or_else(|_| DEFAULT_ADDRESS.to_owned());
    let mut client = Client::new_with_address(&address)
        .await
        .map_err(|error| anyhow::Error::msg(error.to_string()))?;

    match cli.command {
        Command::Get { key } => match client
            .get(&key)
            .await
            .map_err(|error| anyhow::Error::msg(error.to_string()))?
        {
            Some(value) => println!("{}", String::from_utf8_lossy(&value)),
            None => println!("(nil)"),
        },
        Command::Set { key, value } => {
            client
                .set(&key, Bytes::from(value))
                .await
                .map_err(|error| anyhow::Error::msg(error.to_string()))?;
        }
        Command::Publish { channel, message } => {
            let recipients = client
                .publish(&channel, Bytes::from(message))
                .await
                .map_err(|error| anyhow::Error::msg(error.to_string()))?;
            println!("{}", recipients);
        }
        Command::Subscribe { channels } => {
            if channels.is_empty() {
                anyhow::bail!("at least one channel is required");
            }
            let channel_refs: Vec<&str> = channels.iter().map(|s| s.as_str()).collect();
            client
                .subscribe_multiple(&channel_refs)
                .await
                .map_err(|error| anyhow::Error::msg(error.to_string()))?;
            client.enter_subscribe_mode().await?;
        }
        Command::Unsubscribe { channels } => {
            if channels.is_empty() {
                client
                    .unsubscribe_all()
                    .await
                    .map_err(|error| anyhow::Error::msg(error.to_string()))?;
            } else {
                let channel_refs: Vec<&str> = channels.iter().map(|s| s.as_str()).collect();
                client
                    .unsubscribe(&channel_refs)
                    .await
                    .map_err(|error| anyhow::Error::msg(error.to_string()))?;
            }
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
}
