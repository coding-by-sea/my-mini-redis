mod get;
pub use get::Get;

mod set;
pub use set::Set;

mod subscribe;
pub use subscribe::Subscribe;

mod unsubscribe;
pub use unsubscribe::Unsubscribe;

mod publish;
pub use publish::Publish;

use crate::frame::Frame;
use crate::parse::Parse;
use anyhow::anyhow;

pub enum Command {
    Get(Get),
    Set(Set),
    Subscribe(Subscribe),
    Unsubscribe(Unsubscribe),
    Publish(Publish),
}

impl Command {
    pub fn from_frame(frame: Frame) -> anyhow::Result<Command> {
        let mut parse = Parse::new(frame)?;
        let string = parse.next_string()?;
        match string.to_lowercase().as_str() {
            "get" => Ok(Command::Get(Get::new(parse.next_string()?))),
            "set" => Ok(Command::Set(Set::new(
                parse.next_string()?,
                parse.next_bytes()?,
            ))),
            "subscribe" => Ok(Command::Subscribe(Subscribe::new(parse.next_string()?))),
            "unsubscribe" => {
                let mut channels = Vec::new();
                while let Ok(channel) = parse.next_string() {
                    channels.push(channel);
                }
                Ok(Command::Unsubscribe(Unsubscribe::new(channels)))
            }
            "publish" => Ok(Command::Publish(Publish::new(
                parse.next_string()?,
                parse.next_bytes()?,
            ))),
            _ => Err(anyhow!("cannot execute command {:?}", string)),
        }
    }
}
