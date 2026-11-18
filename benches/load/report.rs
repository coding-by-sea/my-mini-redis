//! The Markdown and JSON reports.

use crate::measure::{Derived, histogram_summary};
use crate::servers::memory_key;
use crate::{Bench, Record, SWEEPS, Scenario};
use std::fmt::Write as _;
use std::process::Command;

pub(crate) struct Report {
    pub(crate) markdown: String,
    pub(crate) json: String,
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

    pub(crate) fn report(
        &self,
        started_at: std::time::SystemTime,
        redis_version: Option<&str>,
    ) -> Report {
        let mut md = String::new();
        let cpus = std::thread::available_parallelism().map_or(0, |n| n.get());
        let o = &self.options;
        let _ = writeln!(md, "# Load benchmark: my-redis vs Redis\n");
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
        let _ = writeln!(md, "| my-redis | {} |", self.my_redis.describe());
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
            "**Reading the numbers.** Both servers and the load generator share this machine. Redis runs commands on one thread; my-redis uses every core, so compare *CPU per op* for efficiency and *ops/s* for capacity. *Cores* is server CPU time divided by wall time. If the load generator's cores approach the machine's CPU count, it is the bottleneck and ops/s understates both servers. Latency for raw scenarios is per pipelined batch.\n"
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
             - CPU profile the server under one scenario: `cargo build --release`, start `target/release/my-redis-server`, run `scripts/bench.sh --address 127.0.0.1:6379 --no-redis --quick --only set --duration 30`, and attach a sampling profiler to the server (`samply record -p <pid>` on macOS, `perf record -g -p <pid>` on Linux).\n\
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
            "| Scenario | my-redis ops/s | Redis ops/s | my-redis / Redis | p50 my-redis / Redis | p99 my-redis / Redis | CPU per op my-redis / Redis | cores my-redis / Redis | peak RSS my-redis / Redis |\n|---|---:|---:|---:|---:|---:|---:|---:|---:|"
        );
        let summary: Vec<&Record> = self
            .records
            .iter()
            .filter(|r| r.section == "summary" && r.server == self.my_redis.name)
            .collect();
        for my_redis in &summary {
            let other = redis.and_then(|name| self.find("summary", &my_redis.label, name));
            let (a, b) = (Derived::new(my_redis), other.map(Derived::new));
            let b = b.as_ref();
            let _ = writeln!(
                md,
                "| {} | {} | {} | {} | {} | {} | {} | {} | {} |",
                my_redis.label,
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
        for my_redis in &summary {
            for record in std::iter::once(*my_redis)
                .chain(redis.and_then(|name| self.find("summary", &my_redis.label, name)))
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
        let my_redis = self.memory.iter().find(|m| m.server == self.my_redis.name);
        let redis = self
            .redis_name()
            .and_then(|name| self.memory.iter().find(|m| m.server == name));
        let _ = writeln!(
            md,
            "| Measure | my-redis | Redis | my-redis / Redis |\n|---|---:|---:|---:|"
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
            my_redis.and_then(|m| m.idle_kib).map(|v| v as f64 * 1024.0),
            redis.and_then(|m| m.idle_kib).map(|v| v as f64 * 1024.0),
            &bytes,
        );
        let connections = my_redis.map_or(0, |m| m.connections);
        row(
            md,
            &format!("Per idle connection ({connections} open)"),
            my_redis.and_then(|m| m.per_connection_bytes),
            redis.and_then(|m| m.per_connection_bytes),
            &bytes,
        );
        for (index, (payload, keys, _)) in my_redis
            .map(|m| m.per_key.clone())
            .unwrap_or_default()
            .into_iter()
            .enumerate()
        {
            let a = my_redis.and_then(|m| m.per_key[index].2);
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
        for profile in [my_redis, redis].into_iter().flatten() {
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
            "| {sweep} | my-redis ops/s | Redis ops/s | my-redis / Redis | p99 my-redis / Redis | CPU per op my-redis / Redis | cores my-redis / Redis | peak RSS my-redis / Redis |\n|---|---:|---:|---:|---:|---:|---:|---:|"
        );
        for my_redis in self
            .records
            .iter()
            .filter(|r| r.section == sweep && r.server == self.my_redis.name)
        {
            let a = Derived::new(my_redis);
            let b = redis
                .and_then(|name| self.find(sweep, &my_redis.label, name))
                .map(Derived::new);
            let b = b.as_ref();
            let _ = writeln!(
                md,
                "| {} | {} | {} | {} | {} | {} | {} | {} |",
                my_redis.label,
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
            "## Single-threaded: my-redis with 1 worker thread vs Redis\n"
        );
        let _ = writeln!(
            md,
            "my-redis started with `TOKIO_WORKER_THREADS=1`, so both servers run commands on one thread. Redis numbers are from the summary runs.\n"
        );
        let _ = writeln!(
            md,
            "| Scenario | my-redis (1 thread) ops/s | Redis ops/s | my-redis / Redis | p99 my-redis / Redis | CPU per op my-redis / Redis | cores my-redis / Redis |\n|---|---:|---:|---:|---:|---:|---:|"
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
                .filter(|r| r.section == "summary" && r.server == self.my_redis.name)
                .filter_map(|my_redis| {
                    let other = Derived::new(self.find("summary", &my_redis.label, redis)?);
                    let a = Derived::new(my_redis);
                    (other.ops_per_sec > 0.0).then_some((
                        a.ops_per_sec / other.ops_per_sec,
                        my_redis,
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
                .filter(|r| r.section == "summary" && r.server == self.my_redis.name)
                .filter_map(|my_redis| {
                    let a = Derived::new(my_redis).server_cpu_us_per_op?;
                    let b = Derived::new(self.find("summary", &my_redis.label, redis)?)
                        .server_cpu_us_per_op?;
                    Some(a / b)
                })
                .collect();
            if !cpu.is_empty() {
                let mean = cpu.iter().sum::<f64>() / cpu.len() as f64;
                notes.push(format!(
                    "my-redis uses on average **{mean:.1}x** the CPU time per operation that Redis does across the summary scenarios; that is the efficiency gap to close independently of core count."
                ));
            }
        }
        // Multi-threaded vs single-threaded my-redis on the same scenario: if one
        // thread is faster or far cheaper per op, the extra threads mostly contend.
        for single in self.records.iter().filter(|r| r.section == "single-thread") {
            if let Some(multi) = self.find("summary", &single.label, &self.my_redis.name) {
                let (s, m) = (Derived::new(single), Derived::new(multi));
                if let (Some(s_cpu), Some(m_cpu), Some(m_cores)) = (
                    s.server_cpu_us_per_op,
                    m.server_cpu_us_per_op,
                    m.server_cores,
                ) {
                    notes.push(format!(
                        "**{}**: my-redis on 1 thread does {} ops/s at {s_cpu:.2}us CPU/op; on all threads {} ops/s at {m_cpu:.2}us CPU/op using {m_cores:.1} cores. {:.1}x the CPU per op for {:.2}x the throughput points at contention between threads (the shared db lock, the scheduler) rather than command cost.",
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
            single_connection_cpu(&self.my_redis.name),
            self.redis_name().and_then(single_connection_cpu),
        ) {
            notes.push(format!(
                "With a single connection (no contention), my-redis needs {a:.2}us CPU per SET against Redis's {b:.2}us ({:.1}x): the per-request cost of parsing, allocating and writing replies.",
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
            first_key_cost(&self.my_redis.name),
            self.redis_name().and_then(first_key_cost),
        ) {
            notes.push(format!(
                "Each key with a {} value costs {} in my-redis and {} in Redis ({:.1}x).",
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
            .filter(|r| r.section == "pipeline" && r.server == self.my_redis.name)
            .collect();
        if let (Some(first), Some(last)) = (pipeline.first(), pipeline.last()) {
            let (a, b) = (Derived::new(first), Derived::new(last));
            if a.ops_per_sec > 0.0 {
                notes.push(format!(
                    "Pipelining from {} to {} multiplies my-redis throughput by {:.1}x: the larger this is, the more time goes to per-request round trips and syscalls rather than command execution.",
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

pub(crate) fn format_bytes(bytes: u64) -> String {
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
