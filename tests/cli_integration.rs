use std::process::Command;

use my_redis::server::Server;
use tokio::net::TcpListener;

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
