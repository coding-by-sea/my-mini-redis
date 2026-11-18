//! The workloads: raw pipelined RESP, the library client, and pub/sub fan-out.

use crate::measure::{Histogram, Rng, Stats, Window};
use crate::resp::{RawConnection, encode};
use crate::servers::{Target, Usage, rss_kib, sample_usage};
use crate::{Load, Scenario};
use my_redis::client::Client;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::{Duration, Instant};
use tokio::time::timeout;

pub(crate) struct Measurement {
    pub(crate) ops: u64,
    pub(crate) latency: Histogram,
    pub(crate) pubsub: Option<PubsubResult>,
    pub(crate) usage: Usage,
}

pub(crate) struct PubsubResult {
    pub(crate) publishes: u64,
    pub(crate) deliveries: u64,
    pub(crate) latency: Histogram,
    pub(crate) disconnected: usize,
}

pub(crate) async fn run_scenario(
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
