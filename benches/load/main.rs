//! Load benchmark: my-redis against a real Redis, under maximum load.
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
//! - Single-threaded: my-redis limited to one worker thread against Redis, which
//!   runs commands on one thread.
//!
//! The report is printed and written to `target/bench/` as Markdown and JSON.
//!
//! Under `cargo test` it does a short smoke run without Redis instead, so CI checks
//! that the benchmark still works.

use clap::Parser;
use std::path::PathBuf;

mod measure;
mod report;
mod resp;
mod scenarios;
mod servers;

use report::format_bytes;
use scenarios::{Measurement, run_scenario};
use servers::{Kind, MemoryProfile, ServerSpec, memory_profile, my_redis_binary, redis_version};

#[derive(Parser, Debug, Clone)]
#[command(about = "Load benchmark: my-redis against a real Redis")]
struct Options {
    /// Benchmark a my-redis that is already running (host:port) instead of
    /// starting one. Server CPU and memory can't be measured then.
    #[arg(long)]
    address: Option<String>,
    /// The my-redis server binary to start, fresh for every run, instead of the
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

    let my_redis = match &options.address {
        Some(address) => ServerSpec::new("my-redis", Kind::External(address.clone())),
        None => ServerSpec::new(
            "my-redis",
            Kind::MyRedis {
                binary: my_redis_binary(&options),
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
        my_redis,
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
    my_redis: ServerSpec,
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
            runs += 3; // single-threaded my-redis (Redis runs are reused from the summary)
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
            Server::MyRedis => &self.my_redis,
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
        self.run(
            section,
            label.clone(),
            scenario,
            load.clone(),
            Server::MyRedis,
        )
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
            .push(memory_profile(&self.my_redis.start()?, &self.options).await?);
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
            "my-redis, 1 thread",
            Kind::MyRedis {
                binary: my_redis_binary(&self.options),
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
    MyRedis,
    Redis,
}
