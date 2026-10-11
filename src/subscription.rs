use bytes::Bytes;
use tokio::sync::broadcast::Receiver;
use tokio_stream::wrappers::BroadcastStream;
use tokio_stream::{StreamExt, StreamMap};

pub struct SubscriptionState {
    streams: StreamMap<String, BroadcastStream<Bytes>>,
    subscription_count: usize,
}

impl SubscriptionState {
    pub fn new() -> Self {
        Self {
            streams: StreamMap::new(),
            subscription_count: 0,
        }
    }

    /// Subscribe to a channel. Returns true if newly subscribed, false if already subscribed.
    pub fn subscribe(&mut self, channel: String, receiver: Receiver<Bytes>) -> bool {
        if self.streams.contains_key(&channel) {
            false // Already subscribed
        } else {
            let stream = BroadcastStream::new(receiver);
            self.streams.insert(channel, stream);
            self.subscription_count += 1;
            true
        }
    }

    /// Unsubscribe from a specific channel. Returns true if was subscribed, false otherwise.
    pub fn unsubscribe(&mut self, channel: &str) -> bool {
        if self.streams.remove(channel).is_some() {
            self.subscription_count -= 1;
            true
        } else {
            false
        }
    }

    /// Unsubscribe from all channels. Returns the list of channels that were unsubscribed.
    pub fn unsubscribe_all(&mut self) -> Vec<String> {
        let channels: Vec<String> = self.streams.keys().cloned().collect();
        self.streams.clear();
        self.subscription_count = 0;
        channels
    }

    /// Get the current subscription count.
    pub fn count(&self) -> usize {
        self.subscription_count
    }

    /// Check if there are no active subscriptions.
    pub fn is_empty(&self) -> bool {
        self.subscription_count == 0
    }

    /// Receive the next message from any subscribed channel.
    /// Returns None if all channels are closed or there are no subscriptions.
    pub async fn next_message(&mut self) -> Option<(String, Bytes)> {
        loop {
            match self.streams.next().await {
                Some((channel, Ok(msg))) => {
                    return Some((channel, msg));
                }
                Some((channel, Err(e))) => {
                    // BroadcastStreamRecvError can be Lagged or Closed
                    eprintln!(
                        "Subscriber error on channel '{}': {:?}",
                        channel, e
                    );
                    // Remove the channel and continue
                    self.streams.remove(&channel);
                    if self.subscription_count > 0 {
                        self.subscription_count -= 1;
                    }
                    continue;
                }
                None => {
                    // All streams exhausted
                    return None;
                }
            }
        }
    }
}
