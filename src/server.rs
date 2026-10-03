use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use bytes::Bytes;
use tokio::net::{TcpListener, TcpStream};
use crate::ADDRESS;
use crate::cmd::Command;
use crate::cmd::Command::{Get, Set};
use crate::connection::Connection;
use crate::frame::Frame;

type Db = Arc<Mutex<HashMap<String, Bytes>>>;



pub struct Server {
    db: Db,
}

impl Server {
    pub fn new() -> Self {
        Self {db: Db::default()}
    }
    
    pub async fn run(&mut self) {
        let listener = TcpListener::bind(ADDRESS).await.unwrap();
        loop {
            let (socket, _) = listener.accept().await.unwrap();
            let db = self.db.clone();
            tokio::spawn(async move {
                Server::handle(socket, db).await;
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
                    db.insert(cmd.key().to_string(), cmd.value());
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
            };
            connection.write_frame(response).await.unwrap();
        }

    }
}