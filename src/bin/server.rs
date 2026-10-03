use my_redis::{DEFAULT_ADDRESS, server::Server};
use std::env;
use tokio::net::TcpListener;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let address = env::var("MY_MINI_REDIS_ADDRESS").unwrap_or_else(|_| DEFAULT_ADDRESS.to_owned());
    let listener = TcpListener::bind(address).await?;
    Server::new().run_with_listener(listener).await;
    Ok(())
}
