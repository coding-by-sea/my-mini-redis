use mini_redis::{client};
#[tokio::main]
async fn main() {
    let mut client = client::connect("127.0.0.1:6379").await.unwrap();
    client.set("hello", "world".into()).await.unwrap();
    let result = client.get("hello").await.unwrap();
    println!("hello {:?}", result.unwrap());
}
