mod get;
pub use get::Get;

mod set;
pub use set::Set;

use anyhow::{anyhow};
use crate::frame::Frame;
use crate::parse::Parse;

pub enum Command {
    Get(Get),
    Set(Set),
}

impl Command {
    pub fn from_frame(frame: Frame) -> anyhow::Result<Command> {
        let mut parse = Parse::new(frame)?;
        let string = parse.next_string()?;
        match string.to_lowercase().as_str() {
            "get" => {
                Ok(Command::Get(
                    Get::new(parse.next_string()?),
                ))
            }
            "set" => {
                Ok(Command::Set(
                    Set::new(parse.next_string()?, parse.next_bytes()?),
                ))
            }
            _ => Err(anyhow!("cannot execute command {:?}", string)),
        }
    }
}

