use crate::cmd::Command::{self, Get, Publish, Set, Subscribe, Unsubscribe};
use crate::connection::Connection;
use crate::subscription::SubscriptionState;
use crate::frame::Frame;
use crate::{Db, DEFAULT_ADDRESS};
use bytes::Bytes;
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
        let mut subscription_state: Option<SubscriptionState> = None;

        loop {
            tokio::select! {
                // Read commands from client
                frame_result = connection.read_frame() => {
                    match frame_result {
                        Ok(Some(frame)) => {
                            println!("GOT: {:?}", frame);

                            let result = match Command::from_frame(frame) {
                                Ok(Subscribe(cmd)) => {
                                    match cmd.apply(db.clone(), &mut connection, &mut subscription_state).await {
                                        Ok(response) => connection.write_frame(response).await,
                                        Err(e) => Err(e),
                                    }
                                }
                                Ok(Unsubscribe(cmd)) => {
                                    match cmd.apply(&mut connection, &mut subscription_state).await {
                                        Ok(responses) => {
                                            let mut result = Ok(());
                                            for response in responses {
                                                if let Err(e) = connection.write_frame(response).await {
                                                    result = Err(e);
                                                    break;
                                                }
                                            }
                                            result
                                        }
                                        Err(e) => Err(e),
                                    }
                                }
                                Ok(Set(cmd)) => cmd.apply(db.clone(), &mut connection).await,
                                Ok(Get(cmd)) => cmd.apply(db.clone(), &mut connection).await,
                                Ok(Publish(cmd)) => cmd.apply(db.clone(), &mut connection).await,
                                Err(e) => {
                                    println!("ERROR parsing command: {:?}", e);
                                    Err(e)
                                }
                            };

                            if let Err(e) = result {
                                println!("ERROR: {:?}", e);
                            }
                        }
                        Ok(None) => {
                            // Connection closed
                            break;
                        }
                        Err(e) => {
                            println!("ERROR reading frame: {:?}", e);
                            break;
                        }
                    }
                }

                // Forward messages from subscribed channels (conditional select)
                msg = async {
                    match subscription_state.as_mut() {
                        Some(state) if !state.is_empty() => state.next_message().await,
                        _ => None,
                    }
                }, if subscription_state.as_ref().map(|s| !s.is_empty()).unwrap_or(false) => {
                    match msg {
                        Some((channel, payload)) => {
                            let result = connection.write_frame(Frame::Array(vec![
                                Frame::Bulk(Bytes::from_static(b"message")),
                                Frame::Bulk(Bytes::from(channel)),
                                Frame::Bulk(payload),
                            ])).await;

                            if let Err(e) = result {
                                println!("ERROR writing message: {:?}", e);
                                break;
                            }
                        }
                        None => {
                            // All channels closed
                            break;
                        }
                    }
                }
            }
        }
    }
}
