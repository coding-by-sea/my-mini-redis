use crate::cmd::Command::{self, Get, Publish, Set, Subscribe};
use crate::connection::Connection;
use crate::{DEFAULT_ADDRESS, Db};
use tokio::net::{TcpListener, TcpStream};

pub struct Server {
    db: Db,
}

impl Server {
    pub fn new() -> Self {
        Self { db: Db::default() }
    }

    pub async fn run(&mut self) {
        let listener = TcpListener::bind(DEFAULT_ADDRESS).await.unwrap();
        self.run_with_listener(listener).await;
    }

    pub async fn run_with_listener(&mut self, listener: TcpListener) {
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
                Get(cmd) => cmd.apply(db.clone(), &mut connection).await,
                Subscribe(cmd) => cmd.apply(db.clone(), &mut connection).await,
                Publish(cmd) => cmd.apply(db.clone(), &mut connection).await,
            };
            if let Err(e) = result {
                println!("ERROR: {:?}", e);
            }
        }
    }
}
