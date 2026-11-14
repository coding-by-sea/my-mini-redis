use my_redis::client::Client;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let mut client = Client::new().await?;
    client.set("hello", "world".into()).await?;
    let result = client.get("hello").await?;
    println!("hello {:?}", result);
    Ok(())
}
