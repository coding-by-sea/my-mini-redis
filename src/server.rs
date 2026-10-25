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

#[cfg(test)]
mod tests {
    //! 1.9: the pub/sub channel map must only hold channels that have subscribers.
    use super::Server;
    use crate::Db;
    use crate::cmd::{Publish, Subscribe, Unsubscribe};
    use crate::connection::Connection;
    use crate::frame::Frame;
    use bytes::Bytes;
    use tokio::net::{TcpListener, TcpStream};
    use tokio::time::{Duration, Instant, sleep, timeout};

    async fn start() -> (String, Db) {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap().to_string();
        let mut server = Server::new();
        let db = server.db.clone();
        tokio::spawn(async move { server.run_with_listener(listener).await });
        (address, db)
    }

    async fn connect(address: &str) -> Connection {
        Connection::new(TcpStream::connect(address).await.unwrap())
    }

    async fn request(connection: &mut Connection, frame: Frame) -> Frame {
        connection.write_frame(frame).await.unwrap();
        timeout(Duration::from_secs(5), connection.read_frame())
            .await
            .expect("no reply")
            .unwrap()
            .expect("connection closed")
    }

    fn channel_count(db: &Db) -> usize {
        db.lock().unwrap().subscribers.len()
    }

    async fn wait_for_channel_count(db: &Db, expected: usize) {
        let deadline = Instant::now() + Duration::from_secs(2);
        while channel_count(db) != expected {
            assert!(
                Instant::now() < deadline,
                "expected {expected} channels, found {}",
                channel_count(db)
            );
            sleep(Duration::from_millis(10)).await;
        }
    }

    #[tokio::test]
    async fn publish_without_subscribers_does_not_create_channels() {
        let (address, db) = start().await;
        let mut publisher = connect(&address).await;

        for i in 0..1000 {
            let frame = Publish::new(format!("ghost:{i}"), Bytes::from_static(b"x")).into_frame();
            request(&mut publisher, frame).await;
        }
        assert_eq!(channel_count(&db), 0);
    }

    #[tokio::test]
    async fn unsubscribe_removes_channel_with_no_subscribers_left() {
        let (address, db) = start().await;
        let mut first = connect(&address).await;
        let mut second = connect(&address).await;

        request(&mut first, Subscribe::new("news").into_frame()).await;
        request(&mut second, Subscribe::new("news").into_frame()).await;
        assert_eq!(channel_count(&db), 1);

        // One subscriber left: the channel stays.
        request(&mut first, Unsubscribe::new(vec!["news".into()]).into_frame()).await;
        assert_eq!(channel_count(&db), 1);

        // Last subscriber left: the channel is removed (cleanup may finish just after the reply).
        request(&mut second, Unsubscribe::new(vec!["news".into()]).into_frame()).await;
        wait_for_channel_count(&db, 0).await;
    }

    #[tokio::test]
    async fn disconnecting_subscriber_removes_its_channels() {
        let (address, db) = start().await;
        let mut subscriber = connect(&address).await;

        for channel in ["a", "b", "c"] {
            request(&mut subscriber, Subscribe::new(channel).into_frame()).await;
        }
        assert_eq!(channel_count(&db), 3);

        drop(subscriber);
        wait_for_channel_count(&db, 0).await;
    }
}
