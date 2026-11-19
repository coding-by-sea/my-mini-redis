use my_redis::{DEFAULT_ADDRESS, server::Server};
use std::env;
use std::io::IsTerminal;
use tokio::net::TcpListener;
use tracing::info;
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::util::SubscriberInitExt;
use tracing_subscriber::{EnvFilter, Layer};

#[cfg(all(feature = "console", not(tokio_unstable)))]
compile_error!("the `console` feature needs RUSTFLAGS=\"--cfg tokio_unstable\"");

// Telemetry is set up before the runtime starts and shut down after it stops: the
// OpenTelemetry exporter's HTTP client can't be created or dropped on a runtime thread.
fn main() -> anyhow::Result<()> {
    let telemetry = Telemetry::init()?;
    let result = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()?
        .block_on(serve());
    telemetry.shutdown();
    result
}

/// Serves until Ctrl-C, so that telemetry can be flushed on the way out.
async fn serve() -> anyhow::Result<()> {
    let address = env::var("MY_REDIS_ADDRESS").unwrap_or_else(|_| DEFAULT_ADDRESS.to_owned());
    let listener = TcpListener::bind(&address).await?;
    info!("listening on {}", listener.local_addr()?);
    let mut server = Server::new();
    tokio::select! {
        () = server.run_with_listener(listener) => {}
        result = tokio::signal::ctrl_c() => {
            result?;
            info!("shutting down");
        }
    }
    Ok(())
}

/// Where the server's spans and events go:
///
/// - stderr, filtered by RUST_LOG (default `info`: startup and errors). `debug` adds
///   connections opening and closing, `trace` every command.
/// - with the `console` feature, tokio-console, on 127.0.0.1:6669.
/// - with the `otel` feature, an OpenTelemetry collector over OTLP/HTTP
///   (OTEL_EXPORTER_OTLP_ENDPOINT, default http://localhost:4318), filtered by
///   RUST_LOG too: the connection spans need `debug`.
struct Telemetry {
    #[cfg(feature = "otel")]
    tracer_provider: opentelemetry_sdk::trace::SdkTracerProvider,
}

impl Telemetry {
    fn init() -> anyhow::Result<Self> {
        let logs = tracing_subscriber::fmt::layer()
            .with_writer(std::io::stderr)
            .with_ansi(std::io::stderr().is_terminal())
            .with_filter(env_filter());
        let registry = tracing_subscriber::registry().with(logs);

        // The console layer filters for itself: it needs tokio's own trace events.
        #[cfg(feature = "console")]
        let registry = registry.with(console_subscriber::spawn());

        #[cfg(feature = "otel")]
        let (registry, tracer_provider) = {
            use opentelemetry::trace::TracerProvider as _;
            let exporter = opentelemetry_otlp::SpanExporter::builder()
                .with_http()
                .build()?;
            // Batched: spans are exported from a background thread, not the request path.
            let tracer_provider = opentelemetry_sdk::trace::SdkTracerProvider::builder()
                .with_batch_exporter(exporter)
                .with_resource(
                    opentelemetry_sdk::Resource::builder()
                        .with_service_name("my-redis")
                        .build(),
                )
                .build();
            // Not the exporter's own HTTP client, since exporting its spans would make
            // more, nor tokio's instrumentation for tokio-console.
            let mut filter = env_filter();
            for target in [
                "hyper",
                "hyper_util",
                "h2",
                "reqwest",
                "tower",
                "opentelemetry",
                "tokio",
                "runtime",
            ] {
                filter = filter.add_directive(format!("{target}=off").parse()?);
            }
            let layer = tracing_opentelemetry::layer()
                .with_tracer(tracer_provider.tracer("my-redis"))
                .with_filter(filter);
            (registry.with(layer), tracer_provider)
        };

        registry.try_init()?;
        Ok(Self {
            #[cfg(feature = "otel")]
            tracer_provider,
        })
    }

    /// Exports the spans still queued.
    fn shutdown(self) {
        #[cfg(feature = "otel")]
        if let Err(error) = self.tracer_provider.shutdown() {
            eprintln!("ERROR exporting the last spans: {error}");
        }
    }
}

fn env_filter() -> EnvFilter {
    EnvFilter::builder()
        .with_default_directive(tracing::Level::INFO.into())
        .from_env_lossy()
}
