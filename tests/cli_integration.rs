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
    assert_eq!(
        publisher
            .publish("events", Bytes::from("event1"))
            .await
            .unwrap(),
        1
    );
    let msg = timeout(Duration::from_secs(1), subscriber.next_message())
        .await
        .expect("should receive event")
        .unwrap()
        .unwrap();
    assert_eq!(msg.channel, "events");
    assert_eq!(msg.payload, Bytes::from("event1"));

    // Publish to logs
    assert_eq!(
        publisher
            .publish("logs", Bytes::from("log1"))
            .await
            .unwrap(),
        1
    );
    let msg = timeout(Duration::from_secs(1), subscriber.next_message())
        .await
        .expect("should receive log")
        .unwrap()
        .unwrap();
    assert_eq!(msg.channel, "logs");
    assert_eq!(msg.payload, Bytes::from("log1"));

    // Publish to alerts
    assert_eq!(
        publisher
            .publish("alerts", Bytes::from("alert1"))
            .await
            .unwrap(),
        1
    );
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
    subscriber
        .subscribe_multiple(&["events", "logs"])
        .await
        .unwrap();

    let mut publisher = Client::new_with_address(&address).await.unwrap();

    assert_eq!(
        publisher
            .publish("events", Bytes::from("event1"))
            .await
            .unwrap(),
        1
    );
    assert_eq!(
        publisher
            .publish("logs", Bytes::from("log1"))
            .await
            .unwrap(),
        1
    );

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
    publisher
        .publish("events", Bytes::from("event1"))
        .await
        .unwrap();
    publisher
        .publish("logs", Bytes::from("log1"))
        .await
        .unwrap();

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
    publisher
        .publish("events", Bytes::from("event1"))
        .await
        .unwrap();
    publisher
        .publish("logs", Bytes::from("log1"))
        .await
        .unwrap();
    publisher
        .publish("alerts", Bytes::from("alert1"))
        .await
        .unwrap();

    // Should not receive any messages (timeout expected)
    let result = timeout(Duration::from_millis(100), subscriber.next_message()).await;
    assert!(
        result.is_err(),
        "should timeout waiting for messages after unsubscribe all"
    );

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
    assert_eq!(
        publisher
            .publish("events", Bytes::from("broadcast"))
            .await
            .unwrap(),
        2
    );

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
    publisher
        .publish("events", Bytes::from("test"))
        .await
        .unwrap();

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

#[tokio::test]
async fn subscribe_mode_accepts_subscribe_and_unsubscribe_commands() {
    use tokio::io::{AsyncWriteExt, BufReader};

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap().to_string();
    let server = tokio::spawn(async move {
        Server::new().run_with_listener(listener).await;
    });

    let mut subscriber = Client::new_with_address(&address).await.unwrap();
    subscriber.subscribe("events").await.unwrap();

    let (mut input, input_reader) = tokio::io::duplex(1024);
    let subscribe_mode = tokio::spawn(async move {
        subscriber
            .enter_subscribe_mode_with_input(BufReader::new(input_reader))
            .await
    });

    let mut publisher = Client::new_with_address(&address).await.unwrap();

    // Subscribing from inside subscribe mode adds the channel.
    input.write_all(b"subscribe logs\n").await.unwrap();
    timeout(Duration::from_secs(1), async {
        while publisher
            .publish("logs", Bytes::from("log1"))
            .await
            .unwrap()
            != 1
        {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("logs subscription should become active");

    // Unsubscribing from every channel ends subscribe mode.
    input.write_all(b"unsubscribe events logs\n").await.unwrap();
    timeout(Duration::from_secs(1), subscribe_mode)
        .await
        .expect("subscribe mode should exit once no channels remain")
        .unwrap()
        .unwrap();
    assert_eq!(
        publisher
            .publish("events", Bytes::from("event1"))
            .await
            .unwrap(),
        0
    );

    server.abort();
    let _ = server.await;
}

/// A `my-mini-redis-cli subscribe ...` process with piped stdin, stdout and stderr.
struct CliSubscriber {
    child: tokio::process::Child,
    stdin: Option<tokio::process::ChildStdin>,
    stdout: tokio::io::Lines<tokio::io::BufReader<tokio::process::ChildStdout>>,
    stderr: tokio::io::Lines<tokio::io::BufReader<tokio::process::ChildStderr>>,
}

impl CliSubscriber {
    fn spawn(address: &str, channels: &[&str]) -> Self {
        use std::process::Stdio;
        use tokio::io::{AsyncBufReadExt, BufReader};

        let mut child = tokio::process::Command::new(CLIENT_BINARY)
            .arg("subscribe")
            .args(channels)
            .env("MY_MINI_REDIS_ADDRESS", address)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .kill_on_drop(true)
            .spawn()
            .unwrap();

        CliSubscriber {
            stdin: child.stdin.take(),
            stdout: BufReader::new(child.stdout.take().unwrap()).lines(),
            stderr: BufReader::new(child.stderr.take().unwrap()).lines(),
            child,
        }
    }

    async fn send(&mut self, line: &str) {
        use tokio::io::AsyncWriteExt;

        let stdin = self.stdin.as_mut().expect("stdin already closed");
        stdin
            .write_all(format!("{line}\n").as_bytes())
            .await
            .unwrap();
        stdin.flush().await.unwrap();
    }

    fn close_stdin(&mut self) {
        self.stdin = None;
    }

    async fn expect_stdout(&mut self, expected: &str) {
        let line = timeout(Duration::from_secs(2), self.stdout.next_line())
            .await
            .unwrap_or_else(|_| panic!("timed out waiting for stdout line {expected:?}"))
            .unwrap();
        assert_eq!(line.as_deref(), Some(expected));
    }

    async fn expect_stderr_containing(&mut self, expected: &str) {
        let line = timeout(Duration::from_secs(2), self.stderr.next_line())
            .await
            .unwrap_or_else(|_| panic!("timed out waiting for stderr containing {expected:?}"))
            .unwrap()
            .expect("stderr closed");
        assert!(line.contains(expected), "unexpected stderr line: {line:?}");
    }

    async fn expect_exit_success(mut self) {
        let status = timeout(Duration::from_secs(2), self.child.wait())
            .await
            .expect("client should exit")
            .unwrap();
        assert!(status.success(), "client exited with {status}");
    }
}

async fn start_server() -> (String, tokio::task::JoinHandle<()>) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap().to_string();
    let server = tokio::spawn(async move {
        Server::new().run_with_listener(listener).await;
    });
    (address, server)
}

/// Publishes until one subscriber receives the message. The CLI subscribes before
/// printing anything, so this is how a test knows its initial subscriptions are live.
async fn publish_until_delivered(publisher: &mut Client, channel: &str, message: &str) {
    timeout(Duration::from_secs(2), async {
        while publisher
            .publish(channel, Bytes::from(message.to_owned()))
            .await
            .unwrap()
            == 0
        {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap_or_else(|_| panic!("no subscriber appeared on {channel}"));
}

#[tokio::test]
async fn cli_subscribe_mode_subscribes_to_more_channels() {
    let (address, server) = start_server().await;
    let mut cli = CliSubscriber::spawn(&address, &["events"]);
    let mut publisher = Client::new_with_address(&address).await.unwrap();

    publish_until_delivered(&mut publisher, "events", "event1").await;
    cli.expect_stdout("events: event1").await;

    cli.send("subscribe logs alerts").await;
    cli.expect_stdout("subscribed to logs").await;
    cli.expect_stdout("subscribed to alerts").await;

    assert_eq!(
        publisher
            .publish("logs", Bytes::from("log1"))
            .await
            .unwrap(),
        1
    );
    cli.expect_stdout("logs: log1").await;
    assert_eq!(
        publisher
            .publish("alerts", Bytes::from("alert1"))
            .await
            .unwrap(),
        1
    );
    cli.expect_stdout("alerts: alert1").await;

    server.abort();
    let _ = server.await;
}

#[tokio::test]
async fn cli_subscribe_mode_unsubscribes_from_named_channels() {
    let (address, server) = start_server().await;
    let mut cli = CliSubscriber::spawn(&address, &["events", "logs"]);
    let mut publisher = Client::new_with_address(&address).await.unwrap();

    publish_until_delivered(&mut publisher, "logs", "log1").await;
    cli.expect_stdout("logs: log1").await;

    cli.send("unsubscribe events").await;
    cli.expect_stdout("unsubscribed from events").await;

    // Still subscribed to logs, so the client keeps running.
    assert_eq!(
        publisher
            .publish("events", Bytes::from("event1"))
            .await
            .unwrap(),
        0
    );
    assert_eq!(
        publisher
            .publish("logs", Bytes::from("log2"))
            .await
            .unwrap(),
        1
    );
    cli.expect_stdout("logs: log2").await;

    cli.send("unsubscribe logs").await;
    cli.expect_stdout("unsubscribed from logs").await;
    cli.expect_exit_success().await;

    server.abort();
    let _ = server.await;
}

#[tokio::test]
async fn cli_subscribe_mode_bare_unsubscribe_leaves_every_channel_and_exits() {
    let (address, server) = start_server().await;
    let mut cli = CliSubscriber::spawn(&address, &["events"]);
    let mut publisher = Client::new_with_address(&address).await.unwrap();

    publish_until_delivered(&mut publisher, "events", "event1").await;
    cli.expect_stdout("events: event1").await;

    cli.send("subscribe logs").await;
    cli.expect_stdout("subscribed to logs").await;

    cli.send("unsubscribe").await;
    let mut left = Vec::new();
    for _ in 0..2 {
        let line = timeout(Duration::from_secs(2), cli.stdout.next_line())
            .await
            .expect("timed out waiting for unsubscribe acks")
            .unwrap()
            .expect("stdout closed");
        left.push(line);
    }
    left.sort();
    assert_eq!(left, ["unsubscribed from events", "unsubscribed from logs"]);
    cli.expect_exit_success().await;

    assert_eq!(
        publisher
            .publish("events", Bytes::from("event2"))
            .await
            .unwrap(),
        0
    );
    assert_eq!(
        publisher
            .publish("logs", Bytes::from("log1"))
            .await
            .unwrap(),
        0
    );

    server.abort();
    let _ = server.await;
}

#[tokio::test]
async fn cli_subscribe_mode_rejects_other_commands_and_keeps_listening() {
    let (address, server) = start_server().await;
    let mut cli = CliSubscriber::spawn(&address, &["events"]);
    let mut publisher = Client::new_with_address(&address).await.unwrap();

    publish_until_delivered(&mut publisher, "events", "event1").await;
    cli.expect_stdout("events: event1").await;

    cli.send("get greeting").await;
    cli.expect_stderr_containing("only subscribe and unsubscribe are allowed")
        .await;
    cli.send("subscribe").await;
    cli.expect_stderr_containing("usage: subscribe").await;
    // Blank lines are ignored.
    cli.send("").await;

    assert_eq!(
        publisher
            .publish("events", Bytes::from("event2"))
            .await
            .unwrap(),
        1
    );
    cli.expect_stdout("events: event2").await;

    server.abort();
    let _ = server.await;
}

#[tokio::test]
async fn cli_subscribe_mode_accepts_commands_in_any_case() {
    let (address, server) = start_server().await;
    let mut cli = CliSubscriber::spawn(&address, &["events"]);
    let mut publisher = Client::new_with_address(&address).await.unwrap();

    publish_until_delivered(&mut publisher, "events", "event1").await;
    cli.expect_stdout("events: event1").await;

    cli.send("SUBSCRIBE logs").await;
    cli.expect_stdout("subscribed to logs").await;
    cli.send("UnSubscribe events logs").await;
    cli.expect_stdout("unsubscribed from events").await;
    cli.expect_stdout("unsubscribed from logs").await;
    cli.expect_exit_success().await;

    server.abort();
    let _ = server.await;
}

#[tokio::test]
async fn cli_subscribe_mode_keeps_receiving_after_stdin_closes() {
    let (address, server) = start_server().await;
    let mut cli = CliSubscriber::spawn(&address, &["events"]);
    let mut publisher = Client::new_with_address(&address).await.unwrap();

    publish_until_delivered(&mut publisher, "events", "event1").await;
    cli.expect_stdout("events: event1").await;

    cli.close_stdin();
    // Give the client a moment to see EOF before publishing again.
    tokio::time::sleep(Duration::from_millis(50)).await;

    assert_eq!(
        publisher
            .publish("events", Bytes::from("event2"))
            .await
            .unwrap(),
        1
    );
    cli.expect_stdout("events: event2").await;

    server.abort();
    let _ = server.await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn cli_subscribe_mode_handles_commands_while_a_channel_is_busy() {
    use tokio::io::AsyncWriteExt;

    let (address, server) = start_server().await;
    let cli = CliSubscriber::spawn(&address, &["events"]);
    let mut publisher = Client::new_with_address(&address).await.unwrap();

    publish_until_delivered(&mut publisher, "events", "warmup").await;

    // Keep the subscriber's connection saturated with messages.
    let flood = tokio::spawn(async move {
        loop {
            publisher
                .publish("events", Bytes::from("noise"))
                .await
                .unwrap();
        }
    });

    // Drain stdout so the client never blocks on a full pipe, and report the unsubscribe ack.
    let CliSubscriber {
        mut child,
        stdin,
        mut stdout,
        ..
    } = cli;
    let saw_unsubscribe = tokio::spawn(async move {
        while let Some(line) = stdout.next_line().await.unwrap() {
            if line == "unsubscribed from events" {
                return true;
            }
        }
        false
    });

    tokio::time::sleep(Duration::from_millis(100)).await;
    let mut stdin = stdin.unwrap();
    stdin.write_all(b"unsubscribe events\n").await.unwrap();
    stdin.flush().await.unwrap();

    let acknowledged = timeout(Duration::from_secs(2), saw_unsubscribe)
        .await
        .expect("unsubscribe should be handled while the channel is busy")
        .unwrap();
    assert!(
        acknowledged,
        "stdout closed before the unsubscribe was acknowledged"
    );
    let status = timeout(Duration::from_secs(2), child.wait())
        .await
        .expect("client should exit")
        .unwrap();
    assert!(status.success(), "client exited with {status}");

    flood.abort();
    server.abort();
    let _ = server.await;
}
