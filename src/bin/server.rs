use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use mini_redis::{Connection, Frame, Command};
use mini_redis::Command::{Set, Get};
use tokio::net::{TcpListener, TcpStream};
use bytes::Bytes;
type Db = Arc<Mutex<HashMap<String, Bytes>>>;

#[tokio::main]
async fn main() {
    let listener = TcpListener::bind("127.0.0.1:6379").await.unwrap();
    let db = Db::default();
    loop {
        let (socket, _) = listener.accept().await.unwrap();
        let db = db.clone();
        tokio::spawn(async move {
            handle(socket, db).await;
        });
    }
}

async fn handle(socket: TcpStream, db: Db) {
    let mut connection = Connection::new(socket);
    while let Some(frame) = connection.read_frame().await.unwrap() {
        println!("GOT: {:?}", frame);

        // Respond with an error
        let response = match Command::from_frame(frame).unwrap() {
            Set(cmd) => {
                let mut db = db.lock().unwrap();
                db.insert(cmd.key().to_string(), cmd.value().clone());
                Frame::Simple("OK".into())
            }
            Get(cmd) => {
                let db = db.lock().unwrap();
                if let Some(value) = db.get(&cmd.key().to_string()) {
                    Frame::Bulk(value.clone())
                } else {
                    Frame::Null
                }
            }
            _ => panic!("not implemented"),
        };
        connection.write_frame(&response).await.unwrap();
    }

}