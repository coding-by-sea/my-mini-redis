use tokio::net::TcpStream;
use bytes::Bytes;
use clap::{Parser, Subcommand};
use my_redis::connection::Connection;
use my_redis::frame::Frame;

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

struct Client {
    connection: Connection,
}

impl Client {
    pub async fn new() -> anyhow::Result<Self> {
        let connection = TcpStream::connect(ADDRESS).await?;
        Ok(Client {connection: Connection::new(connection)})
    }

    pub async fn set(&mut self, key: &str, value: Bytes) -> anyhow::Result<()> {
        let set_frame = Frame::Array(vec![Frame::Bulk(Bytes::from_static(b"GET")), Frame::Bulk(Bytes::copy_from_slice(key.as_bytes())), Frame::Bulk(Bytes::from(value))]);
        self.connection.write_frame(set_frame).await?;
        Ok(())
    }

    pub async fn get(&mut self, key: &str) -> anyhow::Result<Option<Bytes>> {
        let get_frame = Frame::Array(vec![Frame::Bulk(Bytes::from_static(b"GET")), Frame::Bulk(Bytes::copy_from_slice(key.as_bytes())), Frame::Bulk(Bytes::from_static(b""))]);
        self.connection.write_frame(get_frame).await?;
        let response = self.connection.read_frame().await?;
        match response {
            Some(Frame::Bulk(bytes)) => Ok(Some(bytes)),
            None => Ok(None),
            _ => unreachable!(),
        }
    }
}


#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let cli = Cli::parse();
    let mut client = Client::new()
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
