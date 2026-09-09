use mini_redis::{client};
use tokio::sync::{mpsc, oneshot};

use bytes::Bytes;
type Responder<T> = oneshot::Sender<mini_redis::Result<T>>;
#[derive(Debug)]
enum Command {
    Get {
        key: String,
        resp: Responder<Option<Bytes>>,
    },
    Set {
        key: String,
        val: Bytes,
        resp: Responder<()>,
    }
}

#[tokio::main]
async fn main() {
    let (sender, mut receiver) = mpsc::channel::<Command>(10);
    let manager = tokio::spawn(async move {
        let mut client = client::connect("127.0.0.1:6379").await.unwrap();
        while let Some(cmd) = receiver.recv().await {
            match cmd {
                Command::Get { key, resp } => {
                    resp.send(client.get(&key).await).unwrap();
                }
                Command::Set { key, val, resp } => {
                    resp.send(client.set(&key, val.clone()).await).unwrap();
                }
            }
        }
    });
    {
        let sender = sender.clone();
        let (tx, rx) = oneshot::channel();
        sender.send(Command::Set { key: "hello".to_string(), val: Bytes::from("world"), resp: tx }).await.unwrap();
        println!("{:?}", rx.await.unwrap());
    }

    {
        let sender = sender.clone();
        let (tx, rx) = oneshot::channel();
        sender.send(Command::Get { key: "hello".to_string(), resp: tx }).await.unwrap();
        println!("{:?}", rx.await.unwrap());
    }
    manager.await.unwrap();
}