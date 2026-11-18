use my_redis::{DEFAULT_ADDRESS, server::Server};
use std::env;
use std::io::IsTerminal;
use tokio::net::TcpListener;
use tracing::info;
use tracing_subscriber::EnvFilter;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // Logs go to stderr, filtered by RUST_LOG (default `info`: startup and errors).
    // `debug` adds connections opening and closing, `trace` every command.
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::builder()
                .with_default_directive(tracing::Level::INFO.into())
                .from_env_lossy(),
        )
        .with_writer(std::io::stderr)
        .with_ansi(std::io::stderr().is_terminal())
        .init();

    let address = env::var("MY_REDIS_ADDRESS").unwrap_or_else(|_| DEFAULT_ADDRESS.to_owned());
    let listener = TcpListener::bind(&address).await?;
    info!("listening on {}", listener.local_addr()?);
    Server::new().run_with_listener(listener).await;
    Ok(())
}
