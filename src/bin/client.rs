use bytes::Bytes;
use clap::{Parser, Subcommand};
use mini_redis::client;

const ADDRESS: &str = "127.0.0.1:6379";

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
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let cli = Cli::parse();
    let mut client = client::connect(ADDRESS)
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
            println!("OK");
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
}
