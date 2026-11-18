//! Measurement windows, counters and latency histograms.

use crate::{Load, Record};
use std::time::{Duration, Instant};

/// Warmup, then the measured window. Work that starts during warmup isn't counted.
#[derive(Clone, Copy)]
pub(crate) struct Window {
    pub(crate) measure_from: Instant,
    pub(crate) end: Instant,
}

impl Window {
    pub(crate) fn new(load: &Load) -> Self {
        let measure_from = Instant::now() + Duration::from_secs_f64(load.warmup);
        Self {
            measure_from,
            end: measure_from + Duration::from_secs_f64(load.duration),
        }
    }

    pub(crate) fn record(&self, stats: &mut Stats, started: Instant, ops: u64) {
        if started >= self.measure_from {
            stats.ops += ops;
            stats.latency.record(started.elapsed().as_nanos() as u64);
        }
    }
}

#[derive(Default)]
pub(crate) struct Stats {
    pub(crate) ops: u64,
    pub(crate) latency: Histogram,
}

impl Stats {
    pub(crate) fn merge(&mut self, other: &Stats) {
        self.ops += other.ops;
        self.latency.merge(&other.latency);
    }
}

/// Log-linear latency histogram in nanoseconds: exact below 64 ns, then 64 buckets per
/// power of two (under 1.6% error), so millions of samples take constant memory.
#[derive(Clone)]
pub(crate) struct Histogram {
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

    pub(crate) fn record(&mut self, value: u64) {
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

    pub(crate) fn percentile(&self, quantile: f64) -> Option<u64> {
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
pub(crate) struct Derived {
    pub(crate) ops_per_sec: f64,
    /// p50, p90, p99, p99.9, max
    pub(crate) latency: [Option<u64>; 5],
    pub(crate) server_cores: Option<f64>,
    pub(crate) server_cpu_us_per_op: Option<f64>,
    pub(crate) client_cores: Option<f64>,
    pub(crate) rss_before: Option<u64>,
    pub(crate) rss_peak: Option<u64>,
    pub(crate) rss_after: Option<u64>,
}

impl Derived {
    pub(crate) fn new(record: &Record) -> Self {
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

pub(crate) fn histogram_summary(histogram: &Histogram) -> [Option<u64>; 5] {
    [
        histogram.percentile(0.50),
        histogram.percentile(0.90),
        histogram.percentile(0.99),
        histogram.percentile(0.999),
        (histogram.total > 0).then_some(histogram.max),
    ]
}

/// xorshift64*: fast and good enough to spread keys.
pub(crate) struct Rng(u64);

impl Rng {
    pub(crate) fn new(seed: u64) -> Self {
        Self(seed.wrapping_mul(0x9E37_79B9_7F4A_7C15) | 1)
    }

    pub(crate) fn below(&mut self, bound: usize) -> usize {
        self.0 ^= self.0 >> 12;
        self.0 ^= self.0 << 25;
        self.0 ^= self.0 >> 27;
        (self.0.wrapping_mul(0x2545_F491_4F6C_DD1D) % bound as u64) as usize
    }
}
