use std::process::Command;

use bytes::Bytes;
use my_redis::client::Client;
use my_redis::server::Server;
use tokio::net::TcpListener;
use tokio::time::{Duration, timeout};

const CLIENT_BINARY: &str = env!("CARGO_BIN_EXE_my-mini-redis-cli");

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn cli_set_and_get_round_trip_through_the_server() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap().to_string();
    let server = tokio::spawn(async move {
        Server::new().run_with_listener(listener).await;
    });

    assert_cli_output(&address, &["set", "greeting", "hello"], "");
    assert_cli_output(&address, &["get", "greeting"], "hello\n");
    assert_cli_output(&address, &["get", "missing"], "(nil)\n");
    assert_cli_output(&address, &["publish", "events", "created"], "0\n");

    server.abort();
    let _ = server.await;
}

#[tokio::test]
async fn multi_channel_subscription() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap().to_string();
    let server = tokio::spawn(async move {
        Server::new().run_with_listener(listener).await;
    });

    let mut subscriber = Client::new_with_address(&address).await.unwrap();
    subscriber.subscribe("events").await.unwrap();
    subscriber.subscribe("logs").await.unwrap();
    subscriber.subscribe("alerts").await.unwrap();

    let mut publisher = Client::new_with_address(&address).await.unwrap();

    // Publish to events
    assert_eq!(publisher.publish("events", Bytes::from("event1")).await.unwrap(), 1);
    let msg = timeout(Duration::from_secs(1), subscriber.next_message())
        .await
        .expect("should receive event")
        .unwrap()
        .unwrap();
    assert_eq!(msg.channel, "events");
    assert_eq!(msg.payload, Bytes::from("event1"));

    // Publish to logs
    assert_eq!(publisher.publish("logs", Bytes::from("log1")).await.unwrap(), 1);
    let msg = timeout(Duration::from_secs(1), subscriber.next_message())
        .await
        .expect("should receive log")
        .unwrap()
        .unwrap();
    assert_eq!(msg.channel, "logs");
    assert_eq!(msg.payload, Bytes::from("log1"));

    // Publish to alerts
    assert_eq!(publisher.publish("alerts", Bytes::from("alert1")).await.unwrap(), 1);
    let msg = timeout(Duration::from_secs(1), subscriber.next_message())
        .await
        .expect("should receive alert")
        .unwrap()
        .unwrap();
    assert_eq!(msg.channel, "alerts");
    assert_eq!(msg.payload, Bytes::from("alert1"));

    server.abort();
    let _ = server.await;
}

#[tokio::test]
async fn subscribe_multiple_channels_at_once() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap().to_string();
    let server = tokio::spawn(async move {
        Server::new().run_with_listener(listener).await;
    });

    let mut subscriber = Client::new_with_address(&address).await.unwrap();
    subscriber.subscribe_multiple(&["events", "logs"]).await.unwrap();

    let mut publisher = Client::new_with_address(&address).await.unwrap();

    assert_eq!(publisher.publish("events", Bytes::from("event1")).await.unwrap(), 1);
    assert_eq!(publisher.publish("logs", Bytes::from("log1")).await.unwrap(), 1);

    // Should receive both messages
    let msg1 = timeout(Duration::from_secs(1), subscriber.next_message())
        .await
        .expect("should receive first message")
        .unwrap()
        .unwrap();
    let msg2 = timeout(Duration::from_secs(1), subscriber.next_message())
        .await
        .expect("should receive second message")
        .unwrap()
        .unwrap();

    // Messages can arrive in any order
    let channels: Vec<String> = vec![msg1.channel, msg2.channel];
    assert!(channels.contains(&"events".to_string()));
    assert!(channels.contains(&"logs".to_string()));

    server.abort();
    let _ = server.await;
}

#[tokio::test]
async fn unsubscribe_from_specific_channel() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap().to_string();
    let server = tokio::spawn(async move {
        Server::new().run_with_listener(listener).await;
    });

    let mut subscriber = Client::new_with_address(&address).await.unwrap();
    subscriber.subscribe("events").await.unwrap();
    subscriber.subscribe("logs").await.unwrap();

    // Unsubscribe from events
    subscriber.unsubscribe(&["events"]).await.unwrap();

    let mut publisher = Client::new_with_address(&address).await.unwrap();

    // Publish to both channels
    publisher.publish("events", Bytes::from("event1")).await.unwrap();
    publisher.publish("logs", Bytes::from("log1")).await.unwrap();

    // Should only receive logs message
    let msg = timeout(Duration::from_secs(1), subscriber.next_message())
        .await
        .expect("should receive log message")
        .unwrap()
        .unwrap();
    assert_eq!(msg.channel, "logs");
    assert_eq!(msg.payload, Bytes::from("log1"));

    // Should not receive events message (timeout expected)
    let result = timeout(Duration::from_millis(100), subscriber.next_message()).await;
    assert!(result.is_err(), "should timeout waiting for event message");

    server.abort();
    let _ = server.await;
}

#[tokio::test]
async fn unsubscribe_from_all_channels() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap().to_string();
    let server = tokio::spawn(async move {
        Server::new().run_with_listener(listener).await;
    });

    let mut subscriber = Client::new_with_address(&address).await.unwrap();
    subscriber.subscribe("events").await.unwrap();
    subscriber.subscribe("logs").await.unwrap();
    subscriber.subscribe("alerts").await.unwrap();

    // Unsubscribe from all
    subscriber.unsubscribe_all().await.unwrap();

    let mut publisher = Client::new_with_address(&address).await.unwrap();

    // Publish to all channels
    publisher.publish("events", Bytes::from("event1")).await.unwrap();
    publisher.publish("logs", Bytes::from("log1")).await.unwrap();
    publisher.publish("alerts", Bytes::from("alert1")).await.unwrap();

    // Should not receive any messages (timeout expected)
    let result = timeout(Duration::from_millis(100), subscriber.next_message()).await;
    assert!(result.is_err(), "should timeout waiting for messages after unsubscribe all");

    server.abort();
    let _ = server.await;
}

#[tokio::test]
async fn multiple_subscribers_on_same_channel() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap().to_string();
    let server = tokio::spawn(async move {
        Server::new().run_with_listener(listener).await;
    });

    let mut subscriber1 = Client::new_with_address(&address).await.unwrap();
    subscriber1.subscribe("events").await.unwrap();

    let mut subscriber2 = Client::new_with_address(&address).await.unwrap();
    subscriber2.subscribe("events").await.unwrap();

    let mut publisher = Client::new_with_address(&address).await.unwrap();
    assert_eq!(publisher.publish("events", Bytes::from("broadcast")).await.unwrap(), 2);

    // Both subscribers should receive the message
    let msg1 = timeout(Duration::from_secs(1), subscriber1.next_message())
        .await
        .expect("subscriber1 should receive message")
        .unwrap()
        .unwrap();
    let msg2 = timeout(Duration::from_secs(1), subscriber2.next_message())
        .await
        .expect("subscriber2 should receive message")
        .unwrap()
        .unwrap();

    assert_eq!(msg1.channel, "events");
    assert_eq!(msg1.payload, Bytes::from("broadcast"));
    assert_eq!(msg2.channel, "events");
    assert_eq!(msg2.payload, Bytes::from("broadcast"));

    server.abort();
    let _ = server.await;
}

#[tokio::test]
async fn idempotent_subscribe() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap().to_string();
    let server = tokio::spawn(async move {
        Server::new().run_with_listener(listener).await;
    });

    let mut subscriber = Client::new_with_address(&address).await.unwrap();
    subscriber.subscribe("events").await.unwrap();
    subscriber.subscribe("events").await.unwrap(); // Subscribe again

    let mut publisher = Client::new_with_address(&address).await.unwrap();
    publisher.publish("events", Bytes::from("test")).await.unwrap();

    // Should receive the message only once (not duplicated)
    let msg = timeout(Duration::from_secs(1), subscriber.next_message())
        .await
        .expect("should receive message")
        .unwrap()
        .unwrap();
    assert_eq!(msg.payload, Bytes::from("test"));

    // Should not receive duplicate (timeout expected)
    let result = timeout(Duration::from_millis(100), subscriber.next_message()).await;
    assert!(result.is_err(), "should not receive duplicate message");

    server.abort();
    let _ = server.await;
}

fn assert_cli_output(address: &str, args: &[&str], expected_stdout: &str) {
    let output = Command::new(CLIENT_BINARY)
        .args(args)
        .env("MY_MINI_REDIS_ADDRESS", address)
        .output()
        .unwrap();

    assert!(
        output.status.success(),
        "client failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert_eq!(String::from_utf8_lossy(&output.stdout), expected_stdout);
}
