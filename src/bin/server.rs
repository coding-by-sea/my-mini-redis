use my_redis::server::Server;

#[tokio::main]
async fn main() {
    Server::new().run().await;
}
