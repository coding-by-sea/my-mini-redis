mod get;
pub use get::Get;

mod set;
pub use set::Set;

mod subscribe;
pub use subscribe::Subscribe;

use crate::frame::Frame;
use crate::parse::Parse;
use anyhow::anyhow;

pub enum Command {
    Get(Get),
    Set(Set),
    Subscribe(Subscribe),
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
            _ => Err(anyhow!("cannot execute command {:?}", string)),
        }
    }
}
