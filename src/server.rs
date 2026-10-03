use tokio::net::{TcpListener, TcpStream};
use crate::{Db, ADDRESS};
use crate::cmd::Command::{self, Get, Set};
use crate::connection::Connection;


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
            let result = match Command::from_frame(frame).unwrap() {
                Set(cmd) => cmd.apply(db.clone(), &mut connection).await,
                Get(cmd) => cmd.apply(db.clone(), &mut connection).await
            };
            if let Err(e) = result {
                println!("ERROR: {:?}", e);
            }
        }

    }
}