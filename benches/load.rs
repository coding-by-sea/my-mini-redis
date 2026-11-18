//! Load benchmark: my-mini-redis against a real Redis, under maximum load.
//!
//! ```text
//! scripts/bench.sh [OPTIONS]                 # recommended: raises the open-file limit
//! cargo bench --bench load -- [OPTIONS]      # see --help
//! ```
//!
//! The benchmark starts both servers itself (`redis-server` from `PATH`, if installed)
//! and runs every scenario against each, back to back, as fast as they answer. For
//! every run it records throughput, latency percentiles, server CPU (cores used and
//! CPU time per operation), server memory, and the load generator's own CPU, so it is
//! visible when the benchmark rather than the server is the bottleneck.
//!
//! Sections of the report:
//! - Summary: each scenario on both servers, with ratios.
//! - Memory: idle footprint, cost per connection and per key.
//! - Sweeps, varying one thing at a time: pipeline depth (fixed cost per request),
//!   connections (contention), payload size (cost per byte) and pub/sub fan-out.
//! - Single-threaded: my-mini-redis limited to one worker thread against Redis, which
//!   runs commands on one thread.
//!
//! The report is printed and written to `target/bench/` as Markdown and JSON.
//!
//! Under `cargo test` it does a short smoke run without Redis instead, so CI checks
//! that the benchmark still works.

use bytes::{Buf, BytesMut};
use clap::Parser;
use my_redis::client::Client;
use std::fmt::Write as _;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::{Duration, Instant};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::time::{sleep, timeout};

#[derive(Parser, Debug, Clone)]
#[command(about = "Load benchmark: my-mini-redis against a real Redis")]
struct Options {
    /// Benchmark a my-mini-redis that is already running (host:port) instead of
    /// starting one. Server CPU and memory can't be measured then.
    #[arg(long)]
    address: Option<String>,
    /// The my-mini-redis server binary to start, fresh for every run, instead of the
    /// one built with this benchmark: for comparing builds (see scripts/compare.sh).
    #[arg(long, conflicts_with = "address")]
    server: Option<PathBuf>,
    /// Compare against a Redis that is already running (host:port) instead of starting
    /// one. Server CPU and memory can't be measured then.
    #[arg(long)]
    redis_address: Option<String>,
    /// The redis-server binary to start for the comparison.
    #[arg(long, default_value = "redis-server")]
    redis_server: String,
    /// Skip the Redis comparison.
    #[arg(long)]
    no_redis: bool,
    /// Only run the summary scenarios and the memory section; skip the sweeps.
    #[arg(long)]
    quick: bool,
    /// Skip the memory section (idle footprint, memory per connection and per key).
    #[arg(long)]
    no_memory: bool,
    /// Seconds to measure each summary scenario.
    #[arg(long, default_value_t = 5.0)]
    duration: f64,
    /// Seconds of load before measuring starts.
    #[arg(long, default_value_t = 1.0)]
    warmup: f64,
    /// Seconds to measure each sweep point.
    #[arg(long, default_value_t = 2.0)]
    sweep_duration: f64,
    /// Seconds of warmup for each sweep point.
    #[arg(long, default_value_t = 0.5)]
    sweep_warmup: f64,
    /// Concurrent connections for the request/reply scenarios.
    #[arg(long, default_value_t = 50)]
    clients: usize,
    /// Commands each raw connection sends before waiting for their replies.
    #[arg(long, default_value_t = 16)]
    pipeline: usize,
    /// Value and message size in bytes (at least 8 for pub/sub timestamps).
    #[arg(long, default_value_t = 64)]
    payload: usize,
    /// Distinct keys used by SET and GET.
    #[arg(long, default_value_t = 10_000)]
    keyspace: usize,
    /// Subscriber connections in the pub/sub scenario.
    #[arg(long, default_value_t = 50)]
    subscribers: usize,
    /// Publisher connections in the pub/sub scenario.
    #[arg(long, default_value_t = 4)]
    publishers: usize,
    /// Channels the pub/sub subscribers are spread over; publishers rotate through them.
    #[arg(long, default_value_t = 1)]
    channels: usize,
    /// Idle connections opened to measure memory per connection.
    #[arg(long, default_value_t = 1000)]
    memory_connections: usize,
    /// Keys stored to measure memory per key (16-byte values; a fifth as many 1 KB values).
    #[arg(long, default_value_t = 100_000)]
    memory_keys: usize,
    /// Summary scenarios to run, comma separated. Default: all.
    #[arg(long, value_delimiter = ',', value_enum)]
    only: Vec<Scenario>,
    /// Where to write the Markdown and JSON reports.
    #[arg(long, default_value = "target/bench")]
    report_dir: PathBuf,
    /// Passed by `cargo bench`; without it the benchmark does a smoke run.
    #[arg(long, hide = true)]
    bench: bool,
}

#[derive(clap::ValueEnum, Clone, Copy, Debug, PartialEq, Eq)]
enum Scenario {
    Ping,
    Set,
    Get,
    Publish,
    ClientSet,
    ClientGet,
    Pubsub,
    /// Pipelined SET while the pub/sub scenario runs: how much they slow each other.
    Mixed,
}

impl Scenario {
    const ALL: [Scenario; 8] = [
        Scenario::Ping,
        Scenario::Set,
        Scenario::Get,
        Scenario::Publish,
        Scenario::ClientSet,
        Scenario::ClientGet,
        Scenario::Pubsub,
        Scenario::Mixed,
    ];

    fn describe(self, load: &Load) -> String {
        let pipeline = load.pipeline;
        match self {
            Scenario::Ping => format!("PING (raw, pipeline {pipeline})"),
            Scenario::Set => format!("SET (raw, pipeline {pipeline})"),
            Scenario::Get => format!("GET (raw, pipeline {pipeline})"),
            Scenario::Publish => format!("PUBLISH, no subscribers (raw, pipeline {pipeline})"),
            Scenario::ClientSet => "SET via Client (1 in flight)".into(),
            Scenario::ClientGet => "GET via Client (1 in flight)".into(),
            Scenario::Pubsub => format!(
                "PUBLISH to {} subscribers{} ({} publishers, pipeline {pipeline})",
                load.subscribers,
                channels_suffix(load),
                load.publishers
            ),
            Scenario::Mixed => format!(
                "SET (raw, pipeline {pipeline}) during PUBLISH to {} subscribers{}",
                load.subscribers,
                channels_suffix(load)
            ),
        }
    }
}

fn channels_suffix(load: &Load) -> String {
    if load.channels > 1 {
        format!(" on {} channels", load.channels)
    } else {
        String::new()
    }
}

/// The load for one run.
#[derive(Clone, Debug)]
struct Load {
    clients: usize,
    pipeline: usize,
    payload: usize,
    keyspace: usize,
    subscribers: usize,
    publishers: usize,
    channels: usize,
    warmup: f64,
    duration: f64,
}

impl Load {
    /// Caps the keyspace so large payloads don't need gigabytes of memory.
    fn keys(&self) -> usize {
        self.keyspace.min((256 << 20) / self.payload.max(1)).max(1)
    }
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let mut options = if std::env::args().any(|arg| arg == "--bench") {
        Options::parse()
    } else {
        // `cargo test` runs bench targets too, with its own arguments: do a quick smoke run.
        Options::parse_from([
            "load",
            "--no-redis",
            "--duration",
            "0.2",
            "--warmup",
            "0",
            "--sweep-duration",
            "0.1",
            "--sweep-warmup",
            "0",
            "--clients",
            "4",
            "--pipeline",
            "4",
            "--keyspace",
            "100",
            "--subscribers",
            "2",
            "--publishers",
            "1",
            "--memory-connections",
            "10",
            "--memory-keys",
            "500",
        ])
    };
    anyhow::ensure!(options.payload >= 8, "--payload must be at least 8 bytes");
    anyhow::ensure!(
        options.clients > 0
            && options.pipeline > 0
            && options.publishers > 0
            && options.channels > 0,
        "--clients, --pipeline, --publishers and --channels must be positive"
    );
    if options.only.is_empty() {
        options.only = Scenario::ALL.to_vec();
    }
    let started_at = std::time::SystemTime::now();

    let mini = match &options.address {
        Some(address) => ServerSpec::new("my-mini-redis", Kind::External(address.clone())),
        None => ServerSpec::new(
            "my-mini-redis",
            Kind::Mini {
                binary: mini_binary(&options),
                worker_threads: None,
            },
        ),
    };
    let redis = if options.no_redis {
        None
    } else if let Some(address) = &options.redis_address {
        Some(ServerSpec::new("Redis", Kind::External(address.clone())))
    } else {
        Some(ServerSpec::new(
            "Redis",
            Kind::Redis(options.redis_server.clone()),
        ))
    };
    // Start Redis once up front: to fail early if it's missing, and for its version.
    let (redis, redis_version) = match redis {
        Some(spec) => match spec.start() {
            Ok(target) => {
                let version = redis_version(&target.address).await;
                (Some(spec), version)
            }
            Err(error) => {
                eprintln!(
                    "warning: no Redis comparison ({error}); install redis-server or pass --redis-address"
                );
                (None, None)
            }
        },
        None => (None, None),
    };

    let mut bench = Bench {
        options: options.clone(),
        mini,
        redis,
        records: Vec::new(),
        memory: Vec::new(),
        progress: 0,
        total: 0,
    };
    bench.total = bench.planned_runs();

    if !options.no_memory {
        bench.memory_section().await?;
    }
    bench.summary_section().await?;
    if !options.quick {
        bench.sweep_sections().await?;
        bench.single_thread_section().await?;
    }

    let report = bench.report(started_at, redis_version.as_deref());
    println!("{}", report.markdown);
    std::fs::create_dir_all(&options.report_dir)?;
    let stamp = started_at.duration_since(std::time::UNIX_EPOCH)?.as_secs();
    for (extension, content) in [("md", &report.markdown), ("json", &report.json)] {
        let path = options.report_dir.join(format!("load-{stamp}.{extension}"));
        std::fs::write(&path, content)?;
        std::fs::write(
            options.report_dir.join(format!("latest.{extension}")),
            content,
        )?;
        eprintln!("wrote {}", path.display());
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// Orchestration
// ---------------------------------------------------------------------------

struct Bench {
    options: Options,
    mini: ServerSpec,
    redis: Option<ServerSpec>,
    records: Vec<Record>,
    memory: Vec<MemoryProfile>,
    progress: usize,
    total: usize,
}

/// One run of one scenario against one server.
struct Record {
    section: &'static str,
    /// What varies within the section, such as "pipeline 16".
    label: String,
    server: String,
    scenario: Scenario,
    load: Load,
    measurement: Measurement,
}

const SWEEPS: [(&str, &str); 5] = [
    ("pipeline", "Pipeline depth: SET, 50 connections"),
    ("connections", "Concurrent connections: SET, pipeline 16"),
    ("payload", "Payload size: SET, 50 connections, pipeline 16"),
    ("fan-out", "Pub/sub fan-out: 4 publishers, pipeline 16"),
    (
        "channels",
        "Pub/sub channels: 100 subscribers, 4 publishers, pipeline 16",
    ),
];

impl Bench {
    fn base_load(&self) -> Load {
        let o = &self.options;
        Load {
            clients: o.clients,
            pipeline: o.pipeline,
            payload: o.payload,
            keyspace: o.keyspace,
            subscribers: o.subscribers,
            publishers: o.publishers,
            channels: o.channels,
            warmup: o.warmup,
            duration: o.duration,
        }
    }

    fn sweep_load(&self) -> Load {
        Load {
            warmup: self.options.sweep_warmup,
            duration: self.options.sweep_duration,
            ..self.base_load()
        }
    }

    /// The points of a sweep: (label, scenario, load).
    fn sweep_points(&self, sweep: &str) -> Vec<(String, Scenario, Load)> {
        let base = self.sweep_load();
        let smoke = !self.options.bench;
        let pick =
            |full: &[usize], small: &[usize]| if smoke { small.to_vec() } else { full.to_vec() };
        match sweep {
            "pipeline" => pick(&[1, 4, 16, 64], &[1, 4])
                .into_iter()
                .map(|n| {
                    (
                        format!("{n}"),
                        Scenario::Set,
                        Load {
                            pipeline: n,
                            ..base.clone()
                        },
                    )
                })
                .collect(),
            "connections" => pick(&[1, 10, 50, 200], &[1, 4])
                .into_iter()
                .map(|n| {
                    (
                        format!("{n}"),
                        Scenario::Set,
                        Load {
                            clients: n,
                            ..base.clone()
                        },
                    )
                })
                .collect(),
            "payload" => pick(&[16, 1024, 16 * 1024, 64 * 1024], &[16, 1024])
                .into_iter()
                .map(|n| {
                    (
                        format_bytes(n as u64),
                        Scenario::Set,
                        Load {
                            payload: n,
                            ..base.clone()
                        },
                    )
                })
                .collect(),
            "fan-out" => pick(&[1, 10, 100], &[1, 2])
                .into_iter()
                .map(|n| {
                    (
                        format!("{n} subscriber{}", if n == 1 { "" } else { "s" }),
                        Scenario::Pubsub,
                        Load {
                            subscribers: n,
                            ..base.clone()
                        },
                    )
                })
                .collect(),
            "channels" => pick(&[1, 10, 100], &[1, 2])
                .into_iter()
                .map(|n| {
                    (
                        format!("{n} channel{}", if n == 1 { "" } else { "s" }),
                        Scenario::Pubsub,
                        Load {
                            subscribers: if smoke { 2 } else { 100 },
                            channels: n,
                            ..base.clone()
                        },
                    )
                })
                .collect(),
            _ => unreachable!(),
        }
    }

    fn servers(&self) -> usize {
        1 + self.redis.is_some() as usize
    }

    fn planned_runs(&self) -> usize {
        let mut runs = self.options.only.len() * self.servers();
        if !self.options.quick {
            runs += SWEEPS
                .iter()
                .map(|(sweep, _)| self.sweep_points(sweep).len())
                .sum::<usize>()
                * self.servers();
            runs += 3; // single-threaded my-mini-redis (Redis runs are reused from the summary)
        }
        runs
    }

    async fn run(
        &mut self,
        section: &'static str,
        label: String,
        scenario: Scenario,
        load: Load,
        server: Server,
    ) -> anyhow::Result<()> {
        let spec = match server {
            Server::Mini => &self.mini,
            Server::Redis => self.redis.as_ref().expect("checked by caller"),
        };
        self.progress += 1;
        eprintln!(
            "[{}/{}] {} - {}",
            self.progress,
            self.total,
            scenario.describe(&load),
            spec.name
        );
        // A fresh process per run, so memory and CPU reflect this run alone.
        let target = spec.start()?;
        let measurement = run_scenario(&target, scenario, &load).await?;
        self.records.push(Record {
            section,
            label,
            server: target.name.clone(),
            scenario,
            load,
            measurement,
        });
        Ok(())
    }

    async fn run_on_both(
        &mut self,
        section: &'static str,
        label: String,
        scenario: Scenario,
        load: Load,
    ) -> anyhow::Result<()> {
        self.run(section, label.clone(), scenario, load.clone(), Server::Mini)
            .await?;
        if self.redis.is_some() {
            self.run(section, label, scenario, load, Server::Redis)
                .await?;
        }
        Ok(())
    }

    async fn memory_section(&mut self) -> anyhow::Result<()> {
        eprintln!("memory: measuring idle footprint, connections and keys");
        self.memory
            .push(memory_profile(&self.mini.start()?, &self.options).await?);
        if let Some(redis) = &self.redis {
            self.memory
                .push(memory_profile(&redis.start()?, &self.options).await?);
        }
        Ok(())
    }

    async fn summary_section(&mut self) -> anyhow::Result<()> {
        let load = self.base_load();
        for scenario in self.options.only.clone() {
            self.run_on_both("summary", scenario.describe(&load), scenario, load.clone())
                .await?;
        }
        Ok(())
    }

    async fn sweep_sections(&mut self) -> anyhow::Result<()> {
        for (sweep, _) in SWEEPS {
            for (label, scenario, load) in self.sweep_points(sweep) {
                self.run_on_both(sweep, label, scenario, load).await?;
            }
        }
        Ok(())
    }

    async fn single_thread_section(&mut self) -> anyhow::Result<()> {
        if self.options.address.is_some() {
            eprintln!(
                "skipping the single-threaded comparison: --address points to an external server"
            );
            return Ok(());
        }
        let spec = ServerSpec::new(
            "my-mini-redis, 1 thread",
            Kind::Mini {
                binary: mini_binary(&self.options),
                worker_threads: Some(1),
            },
        );
        let load = self.base_load();
        for scenario in [Scenario::Ping, Scenario::Set, Scenario::Get] {
            self.progress += 1;
            eprintln!(
                "[{}/{}] {} - {}",
                self.progress,
                self.total,
                scenario.describe(&load),
                spec.name
            );
            let single = spec.start()?;
            let measurement = run_scenario(&single, scenario, &load).await?;
            self.records.push(Record {
                section: "single-thread",
                label: scenario.describe(&load),
                server: spec.name.clone(),
                scenario,
                load: load.clone(),
                measurement,
            });
        }
        Ok(())
    }
}

#[derive(Clone, Copy)]
enum Server {
    Mini,
    Redis,
}

// ---------------------------------------------------------------------------
// Servers
// ---------------------------------------------------------------------------

/// How to get a server for a run.
struct ServerSpec {
    name: String,
    kind: Kind,
}

enum Kind {
    /// This project's server binary, started fresh for every run.
    Mini {
        binary: PathBuf,
        worker_threads: Option<usize>,
    },
    /// A redis-server binary, started fresh for every run.
    Redis(String),
    /// An already running server; its CPU and memory can't be measured.
    External(String),
}

/// The server binary built together with this benchmark.
const BUILT_SERVER: &str = env!("CARGO_BIN_EXE_my-mini-redis-server");

/// The my-mini-redis binary to start: `--server` if given, else the one built with this
/// benchmark.
fn mini_binary(options: &Options) -> PathBuf {
    options
        .server
        .clone()
        .unwrap_or_else(|| PathBuf::from(BUILT_SERVER))
}

impl ServerSpec {
    fn new(name: &str, kind: Kind) -> Self {
        Self {
            name: name.into(),
            kind,
        }
    }

    fn describe(&self) -> String {
        match &self.kind {
            Kind::External(address) => format!("already running at {address}"),
            Kind::Mini { binary, .. } if binary != Path::new(BUILT_SERVER) => {
                format!("`{}`, fresh process per run", binary.display())
            }
            _ => "fresh process per run".into(),
        }
    }

    fn start(&self) -> anyhow::Result<Target> {
        let address = match &self.kind {
            Kind::External(address) => {
                return Ok(Target {
                    name: self.name.clone(),
                    address: address.clone(),
                    pid: None,
                    _process: None,
                });
            }
            _ => free_address()?,
        };
        let command = match &self.kind {
            Kind::Mini {
                binary,
                worker_threads,
            } => {
                let mut command = Command::new(binary);
                command.env("MY_MINI_REDIS_ADDRESS", &address);
                if let Some(threads) = worker_threads {
                    // Read by tokio's multi-threaded runtime.
                    command.env("TOKIO_WORKER_THREADS", threads.to_string());
                }
                command
            }
            Kind::Redis(binary) => {
                let port = address.rsplit_once(':').unwrap().1.to_string();
                let mut command = Command::new(binary);
                command.args([
                    "--port",
                    &port,
                    "--bind",
                    "127.0.0.1",
                    "--save",
                    "",
                    "--appendonly",
                    "no",
                ]);
                command
            }
            Kind::External(_) => unreachable!(),
        };
        Target::start(&self.name, address, command)
    }
}

struct Target {
    name: String,
    address: String,
    /// Process id when the benchmark started the server, for CPU and memory sampling.
    pid: Option<u32>,
    _process: Option<ServerProcess>,
}

impl Target {
    fn start(name: &str, address: String, mut command: Command) -> anyhow::Result<Self> {
        let child = command
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()?;
        let pid = child.id();
        let process = ServerProcess(child);
        let deadline = Instant::now() + Duration::from_secs(10);
        while std::net::TcpStream::connect(&address).is_err() {
            anyhow::ensure!(
                Instant::now() < deadline,
                "{name} did not start listening on {address}"
            );
            std::thread::sleep(Duration::from_millis(20));
        }
        Ok(Self {
            name: name.into(),
            address,
            pid: Some(pid),
            _process: Some(process),
        })
    }
}

/// A server process started by the benchmark; killed when dropped.
struct ServerProcess(Child);

impl Drop for ServerProcess {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

fn free_address() -> anyhow::Result<String> {
    Ok(std::net::TcpListener::bind("127.0.0.1:0")?
        .local_addr()?
        .to_string())
}

async fn redis_version(address: &str) -> Option<String> {
    let mut connection = RawConnection::connect(address).await.ok()?;
    let mut request = Vec::new();
    encode(&[b"INFO", b"server"], &mut request);
    connection.send(&request).await.ok()?;
    let info = connection
        .expect_reply(|reply| match reply {
            Reply::Bulk(Some(text)) => Ok(String::from_utf8_lossy(text).into_owned()),
            _ => anyhow::bail!("unexpected INFO reply"),
        })
        .await
        .ok()?;
    info.lines()
        .find_map(|line| line.strip_prefix("redis_version:"))
        .map(|version| version.trim().to_string())
}

// ---------------------------------------------------------------------------
// Process CPU and memory
// ---------------------------------------------------------------------------

/// Total CPU time (user + system) a process has used, in seconds.
async fn cpu_seconds(pid: Option<u32>) -> Option<f64> {
    let pid = pid?;
    tokio::task::spawn_blocking(move || {
        if cfg!(target_os = "linux") {
            let stat = std::fs::read_to_string(format!("/proc/{pid}/stat")).ok()?;
            // Fields after the command name; utime and stime are fields 14 and 15, in
            // clock ticks (100 per second on Linux).
            let fields: Vec<&str> = stat.rsplit_once(')')?.1.split_whitespace().collect();
            let ticks =
                fields.get(11)?.parse::<f64>().ok()? + fields.get(12)?.parse::<f64>().ok()?;
            Some(ticks / 100.0)
        } else {
            parse_clock(&ps(pid, "time=")?)
        }
    })
    .await
    .ok()?
}

/// Resident memory of a process, in KiB.
async fn rss_kib(pid: Option<u32>) -> Option<u64> {
    let pid = pid?;
    tokio::task::spawn_blocking(move || {
        if cfg!(target_os = "linux") {
            let status = std::fs::read_to_string(format!("/proc/{pid}/status")).ok()?;
            let line = status.lines().find(|line| line.starts_with("VmRSS:"))?;
            line.split_whitespace().nth(1)?.parse().ok()
        } else {
            ps(pid, "rss=")?.parse().ok()
        }
    })
    .await
    .ok()?
}

fn ps(pid: u32, field: &str) -> Option<String> {
    let output = Command::new("ps")
        .args(["-o", field, "-p", &pid.to_string()])
        .output()
        .ok()?;
    let text = String::from_utf8(output.stdout).ok()?;
    let text = text.trim();
    (!text.is_empty()).then(|| text.to_string())
}

/// Parses `ps -o time=` output such as `1:02.34`, `01:02:03` or `2-01:02:03`.
fn parse_clock(text: &str) -> Option<f64> {
    let (days, clock) = match text.split_once('-') {
        Some((days, clock)) => (days.parse::<f64>().ok()?, clock),
        None => (0.0, text),
    };
    let mut seconds = 0.0;
    for part in clock.split(':') {
        seconds = seconds * 60.0 + part.parse::<f64>().ok()?;
    }
    Some(days * 86_400.0 + seconds)
}

/// Waits for allocations to settle, then reads RSS.
async fn settled_rss(pid: Option<u32>) -> Option<u64> {
    sleep(Duration::from_millis(300)).await;
    rss_kib(pid).await
}

#[derive(Default, Clone)]
struct Usage {
    /// Seconds between the two CPU samples.
    seconds: f64,
    server_cpu: Option<f64>,
    client_cpu: Option<f64>,
    rss_before: Option<u64>,
    rss_peak: Option<u64>,
    rss_after: Option<u64>,
}

/// Samples server and load-generator CPU over the measured window, and server RSS
/// before, during and after it.
async fn sample_usage(pid: Option<u32>, window: Window) -> Usage {
    let me = Some(std::process::id());
    let rss_before = rss_kib(pid).await;
    tokio::time::sleep_until(window.measure_from.into()).await;
    let (server_start, client_start, start) = (
        cpu_seconds(pid).await,
        cpu_seconds(me).await,
        Instant::now(),
    );
    let mut rss_peak = rss_before;
    while Instant::now() < window.end {
        if let Some(rss) = rss_kib(pid).await {
            rss_peak = Some(rss_peak.unwrap_or(0).max(rss));
        }
        sleep(Duration::from_millis(100).min(window.end.saturating_duration_since(Instant::now())))
            .await;
    }
    let (server_end, client_end) = (cpu_seconds(pid).await, cpu_seconds(me).await);
    let delta = |start: Option<f64>, end: Option<f64>| Some(end? - start?);
    Usage {
        seconds: start.elapsed().as_secs_f64(),
        server_cpu: delta(server_start, server_end),
        client_cpu: delta(client_start, client_end),
        rss_before,
        rss_peak,
        rss_after: None,
    }
}

struct MemoryProfile {
    server: String,
    idle_kib: Option<u64>,
    connections: usize,
    per_connection_bytes: Option<f64>,
    connection_error: Option<String>,
    /// (value size, keys stored, bytes per key)
    per_key: Vec<(usize, usize, Option<f64>)>,
}

/// Idle footprint, then the RSS cost of idle connections and of stored keys.
async fn memory_profile(target: &Target, options: &Options) -> anyhow::Result<MemoryProfile> {
    let pid = target.pid;
    let idle_kib = settled_rss(pid).await;

    // Idle connections: each sends one PING so the server has fully accepted it.
    let before = settled_rss(pid).await;
    let mut connections = Vec::new();
    let mut connection_error = None;
    for _ in 0..options.memory_connections {
        match RawConnection::connect(&target.address).await {
            Ok(connection) => connections.push(connection),
            Err(error) => {
                connection_error = Some(format!(
                    "stopped at {} connections: {error}",
                    connections.len()
                ));
                break;
            }
        }
    }
    let mut ping = Vec::new();
    encode(&[b"PING"], &mut ping);
    for connection in &mut connections {
        connection.send(&ping).await?;
        connection.expect_reply(|reply| reply.ok()).await?;
    }
    let after = settled_rss(pid).await;
    let opened = connections.len();
    let per_connection_bytes = match (before, after) {
        (Some(before), Some(after)) if opened > 0 => {
            Some((after as f64 - before as f64) * 1024.0 / opened as f64)
        }
        _ => None,
    };
    drop(connections);

    // Keys: fresh names, so every SET adds a key.
    let mut per_key = Vec::new();
    let mut connection = RawConnection::connect(&target.address).await?;
    for (payload, keys) in [
        (16, options.memory_keys),
        (1024, (options.memory_keys / 5).max(1)),
    ] {
        let before = settled_rss(pid).await;
        let value = vec![b'v'; payload];
        let indexes: Vec<usize> = (0..keys).collect();
        for chunk in indexes.chunks(1000) {
            let mut batch = Vec::new();
            for index in chunk {
                encode(
                    &[b"SET", memory_key(payload, *index).as_bytes(), &value],
                    &mut batch,
                );
            }
            connection.send(&batch).await?;
            for _ in chunk {
                connection.expect_reply(|reply| reply.ok()).await?;
            }
        }
        let after = settled_rss(pid).await;
        let bytes = match (before, after) {
            (Some(before), Some(after)) => {
                Some((after as f64 - before as f64) * 1024.0 / keys as f64)
            }
            _ => None,
        };
        per_key.push((payload, keys, bytes));
    }

    Ok(MemoryProfile {
        server: target.name.clone(),
        idle_kib,
        connections: opened,
        per_connection_bytes,
        connection_error,
        per_key,
    })
}

fn memory_key(payload: usize, index: usize) -> String {
    format!("mem:{payload}:{index:08}")
}

// ---------------------------------------------------------------------------
// Scenarios
// ---------------------------------------------------------------------------

struct Measurement {
    ops: u64,
    latency: Histogram,
    pubsub: Option<PubsubResult>,
    usage: Usage,
}

struct PubsubResult {
    publishes: u64,
    deliveries: u64,
    latency: Histogram,
    disconnected: usize,
}

async fn run_scenario(
    target: &Target,
    scenario: Scenario,
    load: &Load,
) -> anyhow::Result<Measurement> {
    if matches!(scenario, Scenario::Get | Scenario::ClientGet) {
        populate(&target.address, load).await?;
    }
    let window = Window::new(load);
    let sampler = tokio::spawn(sample_usage(target.pid, window));
    let address = target.address.as_str();
    let (stats, pubsub) = match scenario {
        Scenario::Ping => (
            raw(address, load, window, |_, _| vec![b"PING".to_vec()]).await?,
            None,
        ),
        Scenario::Set => (raw(address, load, window, set_command).await?, None),
        Scenario::Get => (raw(address, load, window, get_command).await?, None),
        Scenario::Publish => (raw(address, load, window, publish_command).await?, None),
        Scenario::ClientSet => (with_client(address, load, window, false).await?, None),
        Scenario::ClientGet => (with_client(address, load, window, true).await?, None),
        Scenario::Pubsub => {
            let (stats, pubsub) = pubsub(address, load, window).await?;
            (stats, Some(pubsub))
        }
        Scenario::Mixed => {
            let (sets, published) = tokio::try_join!(
                raw(address, load, window, set_command),
                pubsub(address, load, window)
            )?;
            (sets, Some(published.1))
        }
    };
    let mut usage = sampler.await?;
    usage.rss_after = rss_kib(target.pid).await;
    Ok(Measurement {
        ops: stats.ops,
        latency: stats.latency,
        pubsub,
        usage,
    })
}

fn key(index: usize) -> Vec<u8> {
    format!("bench:key:{index}").into_bytes()
}

fn set_command(load: &Load, rng: &mut Rng) -> Vec<Vec<u8>> {
    vec![
        b"SET".to_vec(),
        key(rng.below(load.keys())),
        vec![b'x'; load.payload],
    ]
}

fn get_command(load: &Load, rng: &mut Rng) -> Vec<Vec<u8>> {
    vec![b"GET".to_vec(), key(rng.below(load.keys()))]
}

fn publish_command(load: &Load, _: &mut Rng) -> Vec<Vec<u8>> {
    vec![
        b"PUBLISH".to_vec(),
        b"bench:nobody".to_vec(),
        vec![b'x'; load.payload],
    ]
}

/// Raw, pipelined request/reply load. Each connection cycles through pre-encoded
/// batches of `pipeline` commands and waits for all their replies before the next.
async fn raw(
    address: &str,
    load: &Load,
    window: Window,
    command: fn(&Load, &mut Rng) -> Vec<Vec<u8>>,
) -> anyhow::Result<Stats> {
    let mut workers = Vec::new();
    for client in 0..load.clients {
        let (load, address) = (load.clone(), address.to_string());
        workers.push(tokio::spawn(async move {
            let mut rng = Rng::new(client as u64);
            // Distinct batches so SET/GET touch many keys; encoding stays out of the loop.
            let batches: Vec<Vec<u8>> = (0..16)
                .map(|_| {
                    let mut batch = Vec::new();
                    for _ in 0..load.pipeline {
                        let args = command(&load, &mut rng);
                        encode(
                            &args.iter().map(Vec::as_slice).collect::<Vec<_>>(),
                            &mut batch,
                        );
                    }
                    batch
                })
                .collect();

            let mut connection = RawConnection::connect(&address).await?;
            let mut stats = Stats::default();
            for batch in batches.iter().cycle() {
                let started = Instant::now();
                if started >= window.end {
                    break;
                }
                connection.send(batch).await?;
                for _ in 0..load.pipeline {
                    connection.expect_reply(|reply| reply.ok()).await?;
                }
                window.record(&mut stats, started, load.pipeline as u64);
            }
            Ok::<_, anyhow::Error>(stats)
        }));
    }
    join(workers).await
}

/// Request/reply load through the crate's `Client`: one request in flight per connection.
async fn with_client(
    address: &str,
    load: &Load,
    window: Window,
    get: bool,
) -> anyhow::Result<Stats> {
    let keys: Arc<Vec<String>> =
        Arc::new((0..load.keys()).map(|i| format!("bench:key:{i}")).collect());
    let payload = bytes::Bytes::from(vec![b'x'; load.payload]);
    let mut workers = Vec::new();
    for client in 0..load.clients {
        let (keys, payload, address) = (keys.clone(), payload.clone(), address.to_string());
        workers.push(tokio::spawn(async move {
            let mut rng = Rng::new(client as u64);
            let mut connection = Client::new_with_address(&address).await?;
            let mut stats = Stats::default();
            loop {
                let started = Instant::now();
                if started >= window.end {
                    break;
                }
                let key = &keys[rng.below(keys.len())];
                if get {
                    connection.get(key).await?;
                } else {
                    connection.set(key, payload.clone()).await?;
                }
                window.record(&mut stats, started, 1);
            }
            Ok::<_, anyhow::Error>(stats)
        }));
    }
    join(workers).await
}

/// Pub/sub fan-out at full speed: every message should reach every subscriber of its
/// channel. Subscriber `i` joins channel `i % channels`, and publishers rotate through
/// the channels. Returns the publishers' stats and what the subscribers received.
async fn pubsub(
    address: &str,
    load: &Load,
    window: Window,
) -> anyhow::Result<(Stats, PubsubResult)> {
    let channels: Arc<Vec<Vec<u8>>> = Arc::new(
        (0..load.channels)
            .map(|c| format!("bench:fanout:{c}").into_bytes())
            .collect(),
    );
    let mut subscribers = Vec::new();
    for index in 0..load.subscribers {
        let channel = index % load.channels;
        let mut connection = RawConnection::connect(address).await?;
        let mut request = Vec::new();
        encode(&[b"SUBSCRIBE", &channels[channel]], &mut request);
        connection.send(&request).await?;
        connection.expect_reply(|reply| reply.ok()).await?;
        subscribers.push((channel, connection));
    }

    // Timestamps in payloads are nanoseconds since `origin`, shared by every task.
    let origin = Instant::now();
    let publishing_done = Arc::new(AtomicBool::new(false));
    // Messages published so far, per channel.
    let published: Arc<Vec<AtomicU64>> =
        Arc::new((0..load.channels).map(|_| AtomicU64::new(0)).collect());

    let mut receivers = Vec::new();
    for (channel, mut connection) in subscribers {
        let (publishing_done, published) = (publishing_done.clone(), published.clone());
        receivers.push(tokio::spawn(async move {
            let mut stats = Stats::default();
            let mut received = 0u64;
            let mut disconnected = false;
            loop {
                if publishing_done.load(Ordering::Acquire)
                    && received >= published[channel].load(Ordering::Acquire)
                {
                    break;
                }
                let next = timeout(
                    Duration::from_secs(5),
                    connection.reply(|reply| reply.message_timestamp()),
                );
                let timestamp = match next.await {
                    Err(_) => break, // nothing more is coming
                    Ok(Ok(Some(timestamp))) => timestamp?,
                    Ok(Ok(None)) | Ok(Err(_)) => {
                        disconnected = true;
                        break;
                    }
                };
                let sent = origin + Duration::from_nanos(timestamp);
                received += 1;
                if sent >= window.measure_from {
                    stats.ops += 1;
                    stats.latency.record(sent.elapsed().as_nanos() as u64);
                }
            }
            Ok::<_, anyhow::Error>((stats, disconnected))
        }));
    }

    let mut publishers = Vec::new();
    for publisher in 0..load.publishers {
        let (load, address) = (load.clone(), address.to_string());
        let (channels, published) = (channels.clone(), published.clone());
        publishers.push(tokio::spawn(async move {
            let mut connection = RawConnection::connect(&address).await?;
            let mut stats = Stats::default();
            let mut payload = vec![b'x'; load.payload];
            let mut batch = Vec::new();
            let mut next_channel = publisher % load.channels;
            let mut counts = vec![0u64; load.channels];
            loop {
                let started = Instant::now();
                if started >= window.end {
                    break;
                }
                batch.clear();
                counts.iter_mut().for_each(|count| *count = 0);
                for _ in 0..load.pipeline {
                    let timestamp = origin.elapsed().as_nanos() as u64;
                    payload[..8].copy_from_slice(&timestamp.to_le_bytes());
                    encode(&[b"PUBLISH", &channels[next_channel], &payload], &mut batch);
                    counts[next_channel] += 1;
                    next_channel = (next_channel + 1) % load.channels;
                }
                connection.send(&batch).await?;
                for _ in 0..load.pipeline {
                    connection.expect_reply(|reply| reply.ok()).await?;
                }
                for (channel, count) in counts.iter().enumerate() {
                    if *count > 0 {
                        published[channel].fetch_add(*count, Ordering::AcqRel);
                    }
                }
                window.record(&mut stats, started, load.pipeline as u64);
            }
            Ok::<_, anyhow::Error>(stats)
        }));
    }

    let publish_stats = join(publishers).await?;
    publishing_done.store(true, Ordering::Release);
    let mut delivery = Stats::default();
    let mut disconnected = 0;
    for receiver in receivers {
        let (stats, was_disconnected) = receiver.await??;
        delivery.merge(&stats);
        disconnected += was_disconnected as usize;
    }
    let publishes = publish_stats.ops;
    Ok((
        publish_stats,
        PubsubResult {
            publishes,
            deliveries: delivery.ops,
            latency: delivery.latency,
            disconnected,
        },
    ))
}

/// Fills the keyspace so GET finds values.
async fn populate(address: &str, load: &Load) -> anyhow::Result<()> {
    let mut connection = RawConnection::connect(address).await?;
    let value = vec![b'x'; load.payload];
    for chunk in (0..load.keys()).collect::<Vec<_>>().chunks(1000) {
        let mut batch = Vec::new();
        for &index in chunk {
            encode(&[b"SET", &key(index), &value], &mut batch);
        }
        connection.send(&batch).await?;
        for _ in chunk {
            connection.expect_reply(|reply| reply.ok()).await?;
        }
    }
    Ok(())
}

async fn join(
    workers: Vec<tokio::task::JoinHandle<anyhow::Result<Stats>>>,
) -> anyhow::Result<Stats> {
    let mut total = Stats::default();
    for worker in workers {
        total.merge(&worker.await??);
    }
    Ok(total)
}

// ---------------------------------------------------------------------------
// Measurement
// ---------------------------------------------------------------------------

/// Warmup, then the measured window. Work that starts during warmup isn't counted.
#[derive(Clone, Copy)]
struct Window {
    measure_from: Instant,
    end: Instant,
}

impl Window {
    fn new(load: &Load) -> Self {
        let measure_from = Instant::now() + Duration::from_secs_f64(load.warmup);
        Self {
            measure_from,
            end: measure_from + Duration::from_secs_f64(load.duration),
        }
    }

    fn record(&self, stats: &mut Stats, started: Instant, ops: u64) {
        if started >= self.measure_from {
            stats.ops += ops;
            stats.latency.record(started.elapsed().as_nanos() as u64);
        }
    }
}

#[derive(Default)]
struct Stats {
    ops: u64,
    latency: Histogram,
}

impl Stats {
    fn merge(&mut self, other: &Stats) {
        self.ops += other.ops;
        self.latency.merge(&other.latency);
    }
}

/// Log-linear latency histogram in nanoseconds: exact below 64 ns, then 64 buckets per
/// power of two (under 1.6% error), so millions of samples take constant memory.
#[derive(Clone)]
struct Histogram {
    counts: Vec<u64>,
    total: u64,
    max: u64,
}

impl Default for Histogram {
    fn default() -> Self {
        Self {
            counts: vec![0; 64 * 60],
            total: 0,
            max: 0,
        }
    }
}

impl Histogram {
    fn index(value: u64) -> usize {
        if value < 64 {
            return value as usize;
        }
        let shift = 63 - value.leading_zeros() - 6;
        (((shift + 1) as usize) << 6) + ((value >> shift) as usize & 63)
    }

    /// The largest value that falls into bucket `index`.
    fn upper_bound(index: usize) -> u64 {
        if index < 64 {
            return index as u64;
        }
        let shift = (index >> 6) - 1;
        (((64 + (index & 63)) as u64 + 1) << shift) - 1
    }

    fn record(&mut self, value: u64) {
        self.counts[Self::index(value)] += 1;
        self.total += 1;
        self.max = self.max.max(value);
    }

    fn merge(&mut self, other: &Histogram) {
        for (count, other) in self.counts.iter_mut().zip(&other.counts) {
            *count += other;
        }
        self.total += other.total;
        self.max = self.max.max(other.max);
    }

    fn percentile(&self, quantile: f64) -> Option<u64> {
        if self.total == 0 {
            return None;
        }
        let target = ((quantile * self.total as f64).ceil() as u64).max(1);
        let mut seen = 0;
        for (index, count) in self.counts.iter().enumerate() {
            seen += count;
            if seen >= target {
                return Some(Self::upper_bound(index).min(self.max));
            }
        }
        Some(self.max)
    }
}

/// Numbers derived from a measurement, used by the report.
struct Derived {
    ops_per_sec: f64,
    /// p50, p90, p99, p99.9, max
    latency: [Option<u64>; 5],
    server_cores: Option<f64>,
    server_cpu_us_per_op: Option<f64>,
    client_cores: Option<f64>,
    rss_before: Option<u64>,
    rss_peak: Option<u64>,
    rss_after: Option<u64>,
}

impl Derived {
    fn new(record: &Record) -> Self {
        let m = &record.measurement;
        let usage = &m.usage;
        let per_second = |cpu: Option<f64>| {
            cpu.filter(|_| usage.seconds > 0.0)
                .map(|cpu| cpu / usage.seconds)
        };
        let ops_per_sec = m.ops as f64 / record.load.duration;
        let latency = histogram_summary(&m.latency);
        Self {
            ops_per_sec,
            latency,
            server_cores: per_second(usage.server_cpu),
            // CPU per operation uses the rate over the sampled interval.
            server_cpu_us_per_op: per_second(usage.server_cpu)
                .filter(|_| ops_per_sec > 0.0)
                .map(|cores| cores / ops_per_sec * 1e6),
            client_cores: per_second(usage.client_cpu),
            rss_before: usage.rss_before,
            rss_peak: usage.rss_peak,
            rss_after: usage.rss_after,
        }
    }
}

fn histogram_summary(histogram: &Histogram) -> [Option<u64>; 5] {
    [
        histogram.percentile(0.50),
        histogram.percentile(0.90),
        histogram.percentile(0.99),
        histogram.percentile(0.999),
        (histogram.total > 0).then_some(histogram.max),
    ]
}

/// xorshift64*: fast and good enough to spread keys.
struct Rng(u64);

impl Rng {
    fn new(seed: u64) -> Self {
        Self(seed.wrapping_mul(0x9E37_79B9_7F4A_7C15) | 1)
    }

    fn below(&mut self, bound: usize) -> usize {
        self.0 ^= self.0 >> 12;
        self.0 ^= self.0 << 25;
        self.0 ^= self.0 >> 27;
        (self.0.wrapping_mul(0x2545_F491_4F6C_DD1D) % bound as u64) as usize
    }
}

// ---------------------------------------------------------------------------
// Report
// ---------------------------------------------------------------------------

struct Report {
    markdown: String,
    json: String,
}

impl Bench {
    fn find(&self, section: &str, label: &str, server: &str) -> Option<&Record> {
        self.records
            .iter()
            .find(|r| r.section == section && r.label == label && r.server == server)
    }

    fn redis_name(&self) -> Option<&str> {
        self.redis.as_ref().map(|r| r.name.as_str())
    }

    fn report(&self, started_at: std::time::SystemTime, redis_version: Option<&str>) -> Report {
        let mut md = String::new();
        let cpus = std::thread::available_parallelism().map_or(0, |n| n.get());
        let o = &self.options;
        let _ = writeln!(md, "# Load benchmark: my-mini-redis vs Redis\n");
        let _ = writeln!(md, "| | |\n|---|---|");
        let _ = writeln!(
            md,
            "| Date (Unix time) | {} |",
            started_at
                .duration_since(std::time::UNIX_EPOCH)
                .map_or(0, |d| d.as_secs())
        );
        let _ = writeln!(
            md,
            "| Commit | {} |",
            git_commit().unwrap_or_else(|| "unknown".into())
        );
        let _ = writeln!(
            md,
            "| Machine | {} {}, {cpus} CPUs |",
            std::env::consts::OS,
            std::env::consts::ARCH
        );
        let _ = writeln!(md, "| my-mini-redis | {} |", self.mini.describe());
        let _ = writeln!(
            md,
            "| Redis | {} |",
            match (&self.redis, redis_version) {
                (Some(redis), Some(version)) => format!("{version}, {}", redis.describe()),
                (Some(redis), None) => redis.describe(),
                (None, _) => "not compared".into(),
            }
        );
        let _ = writeln!(
            md,
            "| Load | {} connections, pipeline {}, {} payload, {} keys, {}s measured after {}s warmup (sweeps: {}s after {}s) |\n",
            o.clients,
            o.pipeline,
            format_bytes(o.payload as u64),
            o.keyspace,
            o.duration,
            o.warmup,
            o.sweep_duration,
            o.sweep_warmup
        );
        let _ = writeln!(
            md,
            "**Reading the numbers.** Both servers and the load generator share this machine. Redis runs commands on one thread; my-mini-redis uses every core, so compare *CPU per op* for efficiency and *ops/s* for capacity. *Cores* is server CPU time divided by wall time. If the load generator's cores approach the machine's CPU count, it is the bottleneck and ops/s understates both servers. Latency for raw scenarios is per pipelined batch.\n"
        );

        self.summary_tables(&mut md);
        if !self.memory.is_empty() {
            self.memory_table(&mut md);
        }
        if !o.quick {
            for (sweep, title) in SWEEPS {
                self.sweep_table(&mut md, sweep, title);
            }
            self.single_thread_table(&mut md);
        }
        self.load_generator_table(&mut md, cpus);
        self.observations(&mut md, cpus);
        let _ = writeln!(
            md,
            "## Profiling next steps\n\n\
             - CPU profile the server under one scenario: `cargo build --release`, start `target/release/my-mini-redis-server`, run `scripts/bench.sh --address 127.0.0.1:6379 --no-redis --quick --only set --duration 30`, and attach a sampling profiler to the server (`samply record -p <pid>` on macOS, `perf record -g -p <pid>` on Linux).\n\
             - Compare allocation counts with a heap profiler (`heaptrack`, or Instruments' Allocations on macOS) on the same run; a high *CPU per op* with low *cores* points at per-request allocation or syscalls rather than contention.\n\
             - `JSON` with every number is next to this report for tracking changes over time.\n"
        );

        Report {
            markdown: md,
            json: self.json(started_at, redis_version),
        }
    }

    fn summary_tables(&self, md: &mut String) {
        let redis = self.redis_name();
        let _ = writeln!(md, "## Summary\n");
        let _ = writeln!(
            md,
            "| Scenario | my-mini-redis ops/s | Redis ops/s | mini / Redis | p50 mini / Redis | p99 mini / Redis | CPU per op mini / Redis | cores mini / Redis | peak RSS mini / Redis |\n|---|---:|---:|---:|---:|---:|---:|---:|---:|"
        );
        let summary: Vec<&Record> = self
            .records
            .iter()
            .filter(|r| r.section == "summary" && r.server == self.mini.name)
            .collect();
        for mini in &summary {
            let other = redis.and_then(|name| self.find("summary", &mini.label, name));
            let (a, b) = (Derived::new(mini), other.map(Derived::new));
            let b = b.as_ref();
            let _ = writeln!(
                md,
                "| {} | {} | {} | {} | {} | {} | {} | {} | {} |",
                mini.label,
                format_count(a.ops_per_sec),
                b.map_or("-".into(), |b| format_count(b.ops_per_sec)),
                ratio(Some(a.ops_per_sec), b.map(|b| b.ops_per_sec)),
                pair(a.latency[0], b.and_then(|b| b.latency[0]), format_nanos),
                pair(a.latency[2], b.and_then(|b| b.latency[2]), format_nanos),
                pair(
                    a.server_cpu_us_per_op,
                    b.and_then(|b| b.server_cpu_us_per_op),
                    |v| format!("{v:.2}us")
                ),
                pair(a.server_cores, b.and_then(|b| b.server_cores), |v| format!(
                    "{v:.2}"
                )),
                pair(a.rss_peak, b.and_then(|b| b.rss_peak), |v| format_bytes(
                    v * 1024
                )),
            );
        }

        let _ = writeln!(md, "\n### Latency detail\n");
        let _ = writeln!(
            md,
            "| Scenario | Server | p50 | p90 | p99 | p99.9 | max |\n|---|---|---:|---:|---:|---:|---:|"
        );
        for mini in &summary {
            for record in std::iter::once(*mini)
                .chain(redis.and_then(|name| self.find("summary", &mini.label, name)))
            {
                let d = Derived::new(record);
                let cells: Vec<String> = d
                    .latency
                    .iter()
                    .map(|v| v.map_or("-".into(), format_nanos))
                    .collect();
                let _ = writeln!(
                    md,
                    "| {} | {} | {} |",
                    record.label,
                    record.server,
                    cells.join(" | ")
                );
            }
        }

        let pubsub: Vec<&Record> = self
            .records
            .iter()
            .filter(|r| {
                r.measurement.pubsub.is_some()
                    && matches!(r.section, "summary" | "fan-out" | "channels")
            })
            .collect();
        if !pubsub.is_empty() {
            let _ = writeln!(md, "\n### Pub/sub delivery\n");
            let _ = writeln!(
                md,
                "Publishes are counted at the publishers; deliveries at the subscribers (publishes x subscribers when nothing is lost). *RSS growth* is how much the server's memory grew during the run, mostly queued messages.\n"
            );
            let _ = writeln!(
                md,
                "| Run | Server | publishes/s | deliveries/s | delivery p50 | p99 | p99.9 | max | disconnected | peak RSS | RSS growth |\n|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|"
            );
            for record in pubsub {
                let d = Derived::new(record);
                let p = record.measurement.pubsub.as_ref().unwrap();
                let latency = histogram_summary(&p.latency);
                let growth = match (d.rss_before, d.rss_peak) {
                    (Some(before), Some(peak)) => {
                        format_signed_bytes((peak as i64 - before as i64) * 1024)
                    }
                    _ => "-".into(),
                };
                let _ = writeln!(
                    md,
                    "| {} | {} | {} | {} | {} | {} | {} | {} | {} of {} | {} | {} |",
                    record.label,
                    record.server,
                    format_count(p.publishes as f64 / record.load.duration),
                    format_count(p.deliveries as f64 / record.load.duration),
                    latency[0].map_or("-".into(), format_nanos),
                    latency[2].map_or("-".into(), format_nanos),
                    latency[3].map_or("-".into(), format_nanos),
                    latency[4].map_or("-".into(), format_nanos),
                    p.disconnected,
                    record.load.subscribers,
                    d.rss_peak.map_or("-".into(), |v| format_bytes(v * 1024)),
                    growth,
                );
            }
        }
        md.push('\n');
    }

    fn memory_table(&self, md: &mut String) {
        let _ = writeln!(md, "## Memory\n");
        let _ = writeln!(
            md,
            "Resident memory (RSS) of each server process, read from the OS after letting allocations settle. Per-connection and per-key numbers are the RSS increase divided by the count, so they include allocator overhead. Freed memory is often kept by the allocator, so RSS rarely shrinks.\n"
        );
        let mini = self.memory.iter().find(|m| m.server == self.mini.name);
        let redis = self
            .redis_name()
            .and_then(|name| self.memory.iter().find(|m| m.server == name));
        let _ = writeln!(
            md,
            "| Measure | my-mini-redis | Redis | mini / Redis |\n|---|---:|---:|---:|"
        );
        let row = |md: &mut String,
                   name: &str,
                   a: Option<f64>,
                   b: Option<f64>,
                   format: &dyn Fn(f64) -> String| {
            let _ = writeln!(
                md,
                "| {name} | {} | {} | {} |",
                a.map_or("-".into(), format),
                b.map_or("-".into(), format),
                ratio(a, b)
            );
        };
        let bytes = |v: f64| format_bytes(v.max(0.0) as u64);
        row(
            md,
            "Idle RSS",
            mini.and_then(|m| m.idle_kib).map(|v| v as f64 * 1024.0),
            redis.and_then(|m| m.idle_kib).map(|v| v as f64 * 1024.0),
            &bytes,
        );
        let connections = mini.map_or(0, |m| m.connections);
        row(
            md,
            &format!("Per idle connection ({connections} open)"),
            mini.and_then(|m| m.per_connection_bytes),
            redis.and_then(|m| m.per_connection_bytes),
            &bytes,
        );
        for (index, (payload, keys, _)) in mini
            .map(|m| m.per_key.clone())
            .unwrap_or_default()
            .into_iter()
            .enumerate()
        {
            let a = mini.and_then(|m| m.per_key[index].2);
            let b = redis.and_then(|m| m.per_key.get(index).and_then(|k| k.2));
            row(
                md,
                &format!(
                    "Per key, {} value ({keys} keys)",
                    format_bytes(payload as u64)
                ),
                a,
                b,
                &bytes,
            );
            // Overhead beyond the key and value bytes themselves.
            let raw_size = (payload + memory_key(payload, 0).len()) as f64;
            row(
                md,
                &format!(
                    "  overhead per key beyond {} of key + value",
                    format_bytes(raw_size as u64)
                ),
                a.map(|v| v - raw_size),
                b.map(|v| v - raw_size),
                &bytes,
            );
        }
        for profile in [mini, redis].into_iter().flatten() {
            if let Some(error) = &profile.connection_error {
                let _ = writeln!(
                    md,
                    "\n{}: {error}. Raise the open-file limit (scripts/bench.sh does).",
                    profile.server
                );
            }
        }
        md.push('\n');
    }

    fn sweep_table(&self, md: &mut String, sweep: &str, title: &str) {
        let redis = self.redis_name();
        let _ = writeln!(md, "## {title}\n");
        let _ = writeln!(
            md,
            "| {sweep} | my-mini-redis ops/s | Redis ops/s | mini / Redis | p99 mini / Redis | CPU per op mini / Redis | cores mini / Redis | peak RSS mini / Redis |\n|---|---:|---:|---:|---:|---:|---:|---:|"
        );
        for mini in self
            .records
            .iter()
            .filter(|r| r.section == sweep && r.server == self.mini.name)
        {
            let a = Derived::new(mini);
            let b = redis
                .and_then(|name| self.find(sweep, &mini.label, name))
                .map(Derived::new);
            let b = b.as_ref();
            let _ = writeln!(
                md,
                "| {} | {} | {} | {} | {} | {} | {} | {} |",
                mini.label,
                format_count(a.ops_per_sec),
                b.map_or("-".into(), |b| format_count(b.ops_per_sec)),
                ratio(Some(a.ops_per_sec), b.map(|b| b.ops_per_sec)),
                pair(a.latency[2], b.and_then(|b| b.latency[2]), format_nanos),
                pair(
                    a.server_cpu_us_per_op,
                    b.and_then(|b| b.server_cpu_us_per_op),
                    |v| format!("{v:.2}us")
                ),
                pair(a.server_cores, b.and_then(|b| b.server_cores), |v| format!(
                    "{v:.2}"
                )),
                pair(a.rss_peak, b.and_then(|b| b.rss_peak), |v| format_bytes(
                    v * 1024
                )),
            );
        }
        md.push('\n');
    }

    fn single_thread_table(&self, md: &mut String) {
        let single: Vec<&Record> = self
            .records
            .iter()
            .filter(|r| r.section == "single-thread")
            .collect();
        if single.is_empty() {
            return;
        }
        let redis = self.redis_name();
        let _ = writeln!(
            md,
            "## Single-threaded: my-mini-redis with 1 worker thread vs Redis\n"
        );
        let _ = writeln!(
            md,
            "my-mini-redis started with `TOKIO_WORKER_THREADS=1`, so both servers run commands on one thread. Redis numbers are from the summary runs.\n"
        );
        let _ = writeln!(
            md,
            "| Scenario | mini (1 thread) ops/s | Redis ops/s | mini / Redis | p99 mini / Redis | CPU per op mini / Redis | cores mini / Redis |\n|---|---:|---:|---:|---:|---:|---:|"
        );
        for record in single {
            let a = Derived::new(record);
            let b = redis
                .and_then(|name| self.find("summary", &record.label, name))
                .map(Derived::new);
            let b = b.as_ref();
            let _ = writeln!(
                md,
                "| {} | {} | {} | {} | {} | {} | {} |",
                record.label,
                format_count(a.ops_per_sec),
                b.map_or("-".into(), |b| format_count(b.ops_per_sec)),
                ratio(Some(a.ops_per_sec), b.map(|b| b.ops_per_sec)),
                pair(a.latency[2], b.and_then(|b| b.latency[2]), format_nanos),
                pair(
                    a.server_cpu_us_per_op,
                    b.and_then(|b| b.server_cpu_us_per_op),
                    |v| format!("{v:.2}us")
                ),
                pair(a.server_cores, b.and_then(|b| b.server_cores), |v| format!(
                    "{v:.2}"
                )),
            );
        }
        md.push('\n');
    }

    fn load_generator_table(&self, md: &mut String, cpus: usize) {
        let _ = writeln!(md, "## Load generator\n");
        let _ = writeln!(
            md,
            "CPU used by the benchmark process itself during each summary run. Near {cpus} cores, the generator is saturated and the server isn't the limit.\n"
        );
        let _ = writeln!(
            md,
            "| Scenario | Server | load generator cores | server cores | total |\n|---|---|---:|---:|---:|"
        );
        for record in self.records.iter().filter(|r| r.section == "summary") {
            let d = Derived::new(record);
            let total = d.client_cores.zip(d.server_cores).map(|(a, b)| a + b);
            let _ = writeln!(
                md,
                "| {} | {} | {} | {} | {} |",
                record.label,
                record.server,
                d.client_cores.map_or("-".into(), |v| format!("{v:.2}")),
                d.server_cores.map_or("-".into(), |v| format!("{v:.2}")),
                total.map_or("-".into(), |v| format!("{v:.2} of {cpus}")),
            );
        }
        md.push('\n');
    }

    /// Facts computed from the data that point at where to look first.
    fn observations(&self, md: &mut String, cpus: usize) {
        let mut notes = Vec::new();
        if let Some(redis) = self.redis_name() {
            let mut ratios: Vec<(f64, &Record, Derived)> = self
                .records
                .iter()
                .filter(|r| r.section == "summary" && r.server == self.mini.name)
                .filter_map(|mini| {
                    let other = Derived::new(self.find("summary", &mini.label, redis)?);
                    let a = Derived::new(mini);
                    (other.ops_per_sec > 0.0).then_some((
                        a.ops_per_sec / other.ops_per_sec,
                        mini,
                        other,
                    ))
                })
                .collect();
            ratios.sort_by(|a, b| a.0.total_cmp(&b.0));
            if let Some((ratio, record, _)) = ratios.first() {
                notes.push(format!(
                    "Largest throughput gap: **{}** at {:.2}x Redis.",
                    record.label, ratio
                ));
            }
            if let Some((ratio, record, _)) = ratios.last() {
                notes.push(format!(
                    "Closest to (or ahead of) Redis: **{}** at {:.2}x.",
                    record.label, ratio
                ));
            }
            let cpu: Vec<f64> = self
                .records
                .iter()
                .filter(|r| r.section == "summary" && r.server == self.mini.name)
                .filter_map(|mini| {
                    let a = Derived::new(mini).server_cpu_us_per_op?;
                    let b = Derived::new(self.find("summary", &mini.label, redis)?)
                        .server_cpu_us_per_op?;
                    Some(a / b)
                })
                .collect();
            if !cpu.is_empty() {
                let mean = cpu.iter().sum::<f64>() / cpu.len() as f64;
                notes.push(format!(
                    "my-mini-redis uses on average **{mean:.1}x** the CPU time per operation that Redis does across the summary scenarios; that is the efficiency gap to close independently of core count."
                ));
            }
        }
        // Multi-threaded vs single-threaded my-mini-redis on the same scenario: if one
        // thread is faster or far cheaper per op, the extra threads mostly contend.
        for single in self.records.iter().filter(|r| r.section == "single-thread") {
            if let Some(multi) = self.find("summary", &single.label, &self.mini.name) {
                let (s, m) = (Derived::new(single), Derived::new(multi));
                if let (Some(s_cpu), Some(m_cpu), Some(m_cores)) = (
                    s.server_cpu_us_per_op,
                    m.server_cpu_us_per_op,
                    m.server_cores,
                ) {
                    notes.push(format!(
                        "**{}**: my-mini-redis on 1 thread does {} ops/s at {s_cpu:.2}us CPU/op; on all threads {} ops/s at {m_cpu:.2}us CPU/op using {m_cores:.1} cores. {:.1}x the CPU per op for {:.2}x the throughput points at contention between threads (the shared db lock, the scheduler) rather than command cost.",
                        single.label,
                        format_count(s.ops_per_sec),
                        format_count(m.ops_per_sec),
                        m_cpu / s_cpu,
                        m.ops_per_sec / s.ops_per_sec.max(1.0),
                    ));
                }
            }
        }
        // One connection: no contention, so the CPU per op ratio is per-request overhead.
        let single_connection_cpu = |server: &str| {
            self.find("connections", "1", server)
                .and_then(|record| Derived::new(record).server_cpu_us_per_op)
        };
        if let (Some(a), Some(b)) = (
            single_connection_cpu(&self.mini.name),
            self.redis_name().and_then(single_connection_cpu),
        ) {
            notes.push(format!(
                "With a single connection (no contention), my-mini-redis needs {a:.2}us CPU per SET against Redis's {b:.2}us ({:.1}x): the per-request cost of parsing, allocating and writing replies.",
                a / b
            ));
        }
        // Pub/sub: delivery far slower than publishing means messages pile up in queues.
        for record in self
            .records
            .iter()
            .filter(|r| r.section == "summary" && r.scenario == Scenario::Pubsub)
        {
            let pubsub = record.measurement.pubsub.as_ref().unwrap();
            let d = Derived::new(record);
            match (pubsub.latency.percentile(0.5), d.latency[0]) {
                (Some(delivery_p50), Some(publish_p50)) if delivery_p50 > 10 * publish_p50 => {
                    let growth = d
                        .rss_peak
                        .zip(d.rss_before)
                        .map(|(peak, before)| peak.saturating_sub(before) * 1024);
                    notes.push(format!(
                        "On {}, subscribers fall behind during **{}**: median delivery latency is {} while a publish takes {}, and server memory grew by {} of queued messages. The server accepts publishes faster than it can write them to subscribers.",
                        record.server,
                        record.label,
                        format_nanos(delivery_p50),
                        format_nanos(publish_p50),
                        growth.map_or("an unknown amount".into(), format_bytes),
                    ));
                }
                _ => {}
            }
        }
        // Memory per key.
        let first_key_cost = |server: &str| {
            self.memory
                .iter()
                .find(|m| m.server == server)
                .and_then(|m| m.per_key.first().copied())
        };
        if let (Some((payload, _, Some(a))), Some((_, _, Some(b)))) = (
            first_key_cost(&self.mini.name),
            self.redis_name().and_then(first_key_cost),
        ) {
            notes.push(format!(
                "Each key with a {} value costs {} in my-mini-redis and {} in Redis ({:.1}x).",
                format_bytes(payload as u64),
                format_bytes(a as u64),
                format_bytes(b as u64),
                a / b
            ));
        }
        // Machine saturation: generator and server compete for the same cores.
        for record in self.records.iter().filter(|r| r.section == "summary") {
            let d = Derived::new(record);
            match (d.client_cores, d.server_cores) {
                (Some(client), Some(server)) if client + server > 0.85 * cpus as f64 => {
                    notes.push(format!(
                        "During **{}** on {}, server and load generator together used {:.1} of {cpus} cores: the machine was close to saturated, which also slows the generator.",
                        record.label,
                        record.server,
                        client + server
                    ));
                }
                _ => {}
            }
        }
        let pipeline: Vec<&Record> = self
            .records
            .iter()
            .filter(|r| r.section == "pipeline" && r.server == self.mini.name)
            .collect();
        if let (Some(first), Some(last)) = (pipeline.first(), pipeline.last()) {
            let (a, b) = (Derived::new(first), Derived::new(last));
            if a.ops_per_sec > 0.0 {
                notes.push(format!(
                    "Pipelining from {} to {} multiplies my-mini-redis throughput by {:.1}x: the larger this is, the more time goes to per-request round trips and syscalls rather than command execution.",
                    first.label, last.label, b.ops_per_sec / a.ops_per_sec
                ));
            }
        }
        for record in self.records.iter().filter(|r| r.section == "summary") {
            let cores = Derived::new(record).client_cores.unwrap_or(0.0);
            if cores > 0.8 * cpus as f64 {
                notes.push(format!(
                    "The load generator used {cores:.1} of {cpus} cores during **{}** on {}: that result is limited by the generator.",
                    record.label, record.server
                ));
            }
            let disconnected = record
                .measurement
                .pubsub
                .as_ref()
                .map_or(0, |p| p.disconnected);
            if disconnected > 0 {
                notes.push(format!(
                    "{disconnected} of {} subscribers were disconnected on {} during **{}**: they fell further behind than the output buffer limit allows.",
                    record.load.subscribers, record.server, record.label
                ));
            }
        }
        if notes.is_empty() {
            return;
        }
        let _ = writeln!(md, "## Observations\n");
        for note in notes {
            let _ = writeln!(md, "- {note}");
        }
        md.push('\n');
    }

    fn json(&self, started_at: std::time::SystemTime, redis_version: Option<&str>) -> String {
        let mut out = String::from("{\n");
        let _ = writeln!(
            out,
            "  \"started_at\": {},\n  \"commit\": {},\n  \"os\": \"{}\",\n  \"arch\": \"{}\",\n  \"cpus\": {},\n  \"redis_version\": {},",
            started_at
                .duration_since(std::time::UNIX_EPOCH)
                .map_or(0, |d| d.as_secs()),
            json_string(git_commit().as_deref()),
            std::env::consts::OS,
            std::env::consts::ARCH,
            std::thread::available_parallelism().map_or(0, |n| n.get()),
            json_string(redis_version),
        );
        out.push_str("  \"memory\": [\n");
        let memory: Vec<String> = self
            .memory
            .iter()
            .map(|m| {
                let keys: Vec<String> = m
                    .per_key
                    .iter()
                    .map(|(payload, keys, bytes)| {
                        format!("{{\"value_bytes\": {payload}, \"keys\": {keys}, \"bytes_per_key\": {}}}", json_number(*bytes))
                    })
                    .collect();
                format!(
                    "    {{\"server\": {}, \"idle_rss_kib\": {}, \"connections\": {}, \"bytes_per_connection\": {}, \"per_key\": [{}]}}",
                    json_string(Some(&m.server)),
                    json_number(m.idle_kib.map(|v| v as f64)),
                    m.connections,
                    json_number(m.per_connection_bytes),
                    keys.join(", ")
                )
            })
            .collect();
        out.push_str(&memory.join(",\n"));
        out.push_str("\n  ],\n  \"runs\": [\n");
        let runs: Vec<String> = self
            .records
            .iter()
            .map(|r| {
                let d = Derived::new(r);
                let l = &r.load;
                let latency: Vec<String> = d.latency.iter().map(|v| json_number(v.map(|v| v as f64))).collect();
                let pubsub = match &r.measurement.pubsub {
                    Some(p) => {
                        let delivery: Vec<String> = histogram_summary(&p.latency).iter().map(|v| json_number(v.map(|v| v as f64))).collect();
                        format!(
                            ", \"channels\": {}, \"publishes_per_sec\": {:.1}, \"deliveries_per_sec\": {:.1}, \"delivery_latency_ns\": [{}], \"disconnected\": {}",
                            l.channels,
                            p.publishes as f64 / l.duration,
                            p.deliveries as f64 / l.duration,
                            delivery.join(", "),
                            p.disconnected
                        )
                    }
                    None => String::new(),
                };
                format!(
                    "    {{\"section\": {}, \"label\": {}, \"server\": {}, \"scenario\": \"{:?}\", \"clients\": {}, \"pipeline\": {}, \"payload\": {}, \"subscribers\": {}, \"publishers\": {}, \"duration_s\": {}, \"ops_per_sec\": {:.1}, \"latency_ns_p50_p90_p99_p999_max\": [{}], \"server_cores\": {}, \"server_cpu_us_per_op\": {}, \"client_cores\": {}, \"rss_kib_before_peak_after\": [{}, {}, {}]{pubsub}}}",
                    json_string(Some(r.section)),
                    json_string(Some(&r.label)),
                    json_string(Some(&r.server)),
                    r.scenario,
                    l.clients,
                    l.pipeline,
                    l.payload,
                    l.subscribers,
                    l.publishers,
                    l.duration,
                    d.ops_per_sec,
                    latency.join(", "),
                    json_number(d.server_cores),
                    json_number(d.server_cpu_us_per_op),
                    json_number(d.client_cores),
                    json_number(d.rss_before.map(|v| v as f64)),
                    json_number(d.rss_peak.map(|v| v as f64)),
                    json_number(d.rss_after.map(|v| v as f64)),
                )
            })
            .collect();
        out.push_str(&runs.join(",\n"));
        out.push_str("\n  ]\n}\n");
        out
    }
}

fn git_commit() -> Option<String> {
    let output = Command::new("git")
        .args(["rev-parse", "--short", "HEAD"])
        .output()
        .ok()?;
    let commit = String::from_utf8(output.stdout).ok()?.trim().to_string();
    let dirty = Command::new("git")
        .args(["status", "--porcelain", "--untracked-files=no"])
        .output()
        .ok()
        .is_some_and(|o| !o.stdout.is_empty());
    (!commit.is_empty()).then(|| {
        if dirty {
            format!("{commit} (uncommitted changes)")
        } else {
            commit
        }
    })
}

fn json_string(value: Option<&str>) -> String {
    match value {
        Some(text) => format!("\"{}\"", text.replace('\\', "\\\\").replace('"', "\\\"")),
        None => "null".into(),
    }
}

fn json_number(value: Option<f64>) -> String {
    match value {
        Some(v) if v.is_finite() => format!("{v:.3}"),
        _ => "null".into(),
    }
}

fn ratio(a: Option<f64>, b: Option<f64>) -> String {
    match (a, b) {
        (Some(a), Some(b)) if b > 0.0 => format!("{:.2}x", a / b),
        _ => "-".into(),
    }
}

/// "a / b", with "-" for missing values.
fn pair<T: Copy>(a: Option<T>, b: Option<T>, format: impl Fn(T) -> String) -> String {
    format!(
        "{} / {}",
        a.map_or("-".into(), &format),
        b.map_or("-".into(), &format)
    )
}

fn format_nanos(nanos: u64) -> String {
    match nanos {
        n if n < 1_000 => format!("{n}ns"),
        n if n < 1_000_000 => format!("{:.1}us", n as f64 / 1e3),
        n if n < 1_000_000_000 => format!("{:.2}ms", n as f64 / 1e6),
        n => format!("{:.2}s", n as f64 / 1e9),
    }
}

fn format_bytes(bytes: u64) -> String {
    match bytes {
        b if b < 1024 => format!("{b} B"),
        b if b < 1024 * 1024 => format!("{:.1} KiB", b as f64 / 1024.0),
        b if b < 1024 * 1024 * 1024 => format!("{:.1} MiB", b as f64 / (1024.0 * 1024.0)),
        b => format!("{:.2} GiB", b as f64 / (1024.0 * 1024.0 * 1024.0)),
    }
}

fn format_signed_bytes(bytes: i64) -> String {
    if bytes < 0 {
        format!("-{}", format_bytes(bytes.unsigned_abs()))
    } else {
        format!("+{}", format_bytes(bytes as u64))
    }
}

fn format_count(value: f64) -> String {
    let digits = format!("{value:.0}");
    let mut out = String::new();
    for (i, digit) in digits.chars().enumerate() {
        if i > 0 && (digits.len() - i) % 3 == 0 {
            out.push(',');
        }
        out.push(digit);
    }
    out
}

// ---------------------------------------------------------------------------
// Raw RESP
// ---------------------------------------------------------------------------

fn encode(args: &[&[u8]], out: &mut Vec<u8>) {
    out.extend_from_slice(format!("*{}\r\n", args.len()).as_bytes());
    for arg in args {
        out.extend_from_slice(format!("${}\r\n", arg.len()).as_bytes());
        out.extend_from_slice(arg);
        out.extend_from_slice(b"\r\n");
    }
}

/// A parsed reply that borrows from the read buffer.
enum Reply<'a> {
    Simple,
    Error(&'a [u8]),
    Integer,
    Bulk(Option<&'a [u8]>),
    Array(Vec<Reply<'a>>),
}

impl Reply<'_> {
    fn ok(self) -> anyhow::Result<()> {
        match self {
            Reply::Error(message) => {
                anyhow::bail!("server error: {}", String::from_utf8_lossy(message))
            }
            _ => Ok(()),
        }
    }

    /// The send timestamp in a `[message, channel, payload]` push.
    fn message_timestamp(self) -> anyhow::Result<u64> {
        match self {
            Reply::Array(parts) => match parts.as_slice() {
                [Reply::Bulk(Some(b"message")), _, Reply::Bulk(Some(payload))]
                    if payload.len() >= 8 =>
                {
                    Ok(u64::from_le_bytes(payload[..8].try_into().unwrap()))
                }
                _ => anyhow::bail!("expected a pub/sub message"),
            },
            other => other
                .ok()
                .and_then(|()| anyhow::bail!("expected a pub/sub message")),
        }
    }
}

/// Parses one reply at the start of `buf`; `None` if it isn't complete yet.
fn parse(buf: &[u8]) -> anyhow::Result<Option<(Reply<'_>, usize)>> {
    let Some(line_end) = buf.windows(2).position(|w| w == b"\r\n") else {
        return Ok(None);
    };
    let line = &buf[1..line_end];
    let after = line_end + 2;
    let number = || -> anyhow::Result<i64> { Ok(std::str::from_utf8(line)?.parse()?) };
    Ok(Some(match buf[0] {
        b'+' => (Reply::Simple, after),
        b'-' => (Reply::Error(line), after),
        b':' => (Reply::Integer, after),
        b'$' => match number()? {
            len if len < 0 => (Reply::Bulk(None), after),
            len => {
                let end = after + len as usize;
                if buf.len() < end + 2 {
                    return Ok(None);
                }
                (Reply::Bulk(Some(&buf[after..end])), end + 2)
            }
        },
        b'*' => {
            let count = number()?.max(0) as usize;
            let mut items = Vec::with_capacity(count);
            let mut position = after;
            for _ in 0..count {
                let Some((item, len)) = parse(&buf[position..])? else {
                    return Ok(None);
                };
                items.push(item);
                position += len;
            }
            (Reply::Array(items), position)
        }
        other => anyhow::bail!("unexpected reply type byte {:?}", other as char),
    }))
}

struct RawConnection {
    stream: TcpStream,
    buffer: BytesMut,
}

impl RawConnection {
    async fn connect(address: &str) -> anyhow::Result<Self> {
        let stream = TcpStream::connect(address).await?;
        stream.set_nodelay(true)?;
        Ok(Self {
            stream,
            buffer: BytesMut::with_capacity(64 * 1024),
        })
    }

    async fn send(&mut self, bytes: &[u8]) -> anyhow::Result<()> {
        self.stream.write_all(bytes).await?;
        Ok(())
    }

    /// Reads the next reply and passes it to `handle`; `None` once the server closes
    /// the connection. Cancel safe: it only awaits `read_buf`.
    async fn reply<T>(&mut self, handle: impl FnOnce(Reply<'_>) -> T) -> anyhow::Result<Option<T>> {
        loop {
            if let Some((reply, len)) = parse(&self.buffer)? {
                let result = handle(reply);
                self.buffer.advance(len);
                return Ok(Some(result));
            }
            if self.stream.read_buf(&mut self.buffer).await? == 0 {
                return Ok(None);
            }
        }
    }

    async fn expect_reply<T>(
        &mut self,
        handle: impl FnOnce(Reply<'_>) -> anyhow::Result<T>,
    ) -> anyhow::Result<T> {
        match self.reply(handle).await? {
            Some(result) => result,
            None => anyhow::bail!("server closed the connection"),
        }
    }
}
