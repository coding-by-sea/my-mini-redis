//! Starting servers and measuring their CPU and memory.

use crate::Options;
use crate::measure::Window;
use crate::resp::{RawConnection, Reply, encode};
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};
use tokio::time::sleep;

/// How to get a server for a run.
pub(crate) struct ServerSpec {
    pub(crate) name: String,
    kind: Kind,
}

pub(crate) enum Kind {
    /// This project's server binary, started fresh for every run.
    MyRedis {
        binary: PathBuf,
        worker_threads: Option<usize>,
    },
    /// A redis-server binary, started fresh for every run.
    Redis(String),
    /// An already running server; its CPU and memory can't be measured.
    External(String),
}

/// The server binary built together with this benchmark.
const BUILT_SERVER: &str = env!("CARGO_BIN_EXE_my-redis-server");

/// The my-redis binary to start: `--server` if given, else the one built with this
/// benchmark.
pub(crate) fn my_redis_binary(options: &Options) -> PathBuf {
    options
        .server
        .clone()
        .unwrap_or_else(|| PathBuf::from(BUILT_SERVER))
}

impl ServerSpec {
    pub(crate) fn new(name: &str, kind: Kind) -> Self {
        Self {
            name: name.into(),
            kind,
        }
    }

    pub(crate) fn describe(&self) -> String {
        match &self.kind {
            Kind::External(address) => format!("already running at {address}"),
            Kind::MyRedis { binary, .. } if binary != Path::new(BUILT_SERVER) => {
                format!("`{}`, fresh process per run", binary.display())
            }
            _ => "fresh process per run".into(),
        }
    }

    pub(crate) fn start(&self) -> anyhow::Result<Target> {
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
            Kind::MyRedis {
                binary,
                worker_threads,
            } => {
                let mut command = Command::new(binary);
                command.env("MY_REDIS_ADDRESS", &address);
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

pub(crate) struct Target {
    pub(crate) name: String,
    pub(crate) address: String,
    /// Process id when the benchmark started the server, for CPU and memory sampling.
    pub(crate) pid: Option<u32>,
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

pub(crate) async fn redis_version(address: &str) -> Option<String> {
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
pub(crate) async fn rss_kib(pid: Option<u32>) -> Option<u64> {
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
pub(crate) struct Usage {
    /// Seconds between the two CPU samples.
    pub(crate) seconds: f64,
    pub(crate) server_cpu: Option<f64>,
    pub(crate) client_cpu: Option<f64>,
    pub(crate) rss_before: Option<u64>,
    pub(crate) rss_peak: Option<u64>,
    pub(crate) rss_after: Option<u64>,
}

/// Samples server and load-generator CPU over the measured window, and server RSS
/// before, during and after it.
pub(crate) async fn sample_usage(pid: Option<u32>, window: Window) -> Usage {
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

pub(crate) struct MemoryProfile {
    pub(crate) server: String,
    pub(crate) idle_kib: Option<u64>,
    pub(crate) connections: usize,
    pub(crate) per_connection_bytes: Option<f64>,
    pub(crate) connection_error: Option<String>,
    /// (value size, keys stored, bytes per key)
    pub(crate) per_key: Vec<(usize, usize, Option<f64>)>,
}

/// Idle footprint, then the RSS cost of idle connections and of stored keys.
pub(crate) async fn memory_profile(
    target: &Target,
    options: &Options,
) -> anyhow::Result<MemoryProfile> {
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

pub(crate) fn memory_key(payload: usize, index: usize) -> String {
    format!("mem:{payload}:{index:08}")
}
