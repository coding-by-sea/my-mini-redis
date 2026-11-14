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

use bytes::Bytes;

pub(crate) enum Command {
    Get(Get),
    Set(Set),
    Subscribe(Subscribe),
    Unsubscribe(Unsubscribe),
    Publish(Publish),
    Ping(Option<Bytes>),
    Quit,
    Reset,
}

impl Command {
    /// Parses a request into a command. On failure, returns the error reply Redis sends
    /// (such as an unknown command or a wrong number of arguments); the connection stays
    /// open. `args` must not be empty.
    pub(crate) fn from_args(args: Vec<Bytes>) -> Result<Command, String> {
        let name = args[0].to_ascii_lowercase();
        // Redis arity: positive means exactly that many arguments (including the name),
        // negative means at least that many.
        let arity: i32 = match name.as_slice() {
            b"get" => 2,
            b"set" => -3,
            b"publish" => 3,
            b"subscribe" => -2,
            b"unsubscribe" => -1,
            b"ping" => -1,
            b"quit" => -1,
            b"reset" => 1,
            _ => return Err(unknown_command(&args)),
        };
        let argc = args.len() as i32;
        let wrong_arity = if arity > 0 { argc != arity } else { argc < -arity };
        // PING takes at most one argument, which Redis checks inside the command.
        if wrong_arity || (name == b"ping" && argc > 2) {
            return Err(format!(
                "ERR wrong number of arguments for '{}' command",
                String::from_utf8_lossy(&name)
            ));
        }

        let mut args = args.into_iter().skip(1);
        Ok(match name.as_slice() {
            b"get" => Command::Get(Get::from_args(args)),
            b"set" => Command::Set(Set::from_args(args)?),
            b"publish" => Command::Publish(Publish::from_args(args)),
            b"subscribe" => Command::Subscribe(Subscribe::from_args(args)),
            b"unsubscribe" => Command::Unsubscribe(Unsubscribe::from_args(args)),
            b"ping" => Command::Ping(args.next()),
            b"quit" => Command::Quit,
            b"reset" => Command::Reset,
            _ => unreachable!("arity table and dispatch disagree"),
        })
    }

    /// Lowercase name, as Redis reports it in errors.
    pub(crate) fn name(&self) -> &'static str {
        match self {
            Command::Get(_) => "get",
            Command::Set(_) => "set",
            Command::Subscribe(_) => "subscribe",
            Command::Unsubscribe(_) => "unsubscribe",
            Command::Publish(_) => "publish",
            Command::Ping(_) => "ping",
            Command::Quit => "quit",
            Command::Reset => "reset",
        }
    }

    /// Whether a RESP2 connection with active subscriptions may run this command.
    pub(crate) fn allowed_while_subscribed(&self) -> bool {
        matches!(
            self,
            Command::Subscribe(_)
                | Command::Unsubscribe(_)
                | Command::Ping(_)
                | Command::Quit
                | Command::Reset
        )
    }
}

/// Redis's message: the name keeps its case, followed by the first arguments, each
/// quoted with a trailing space, until the list reaches 128 bytes.
fn unknown_command(args: &[Bytes]) -> String {
    let name = truncated(&args[0], 128);
    let mut listed = String::new();
    for arg in &args[1..] {
        if listed.len() >= 128 {
            break;
        }
        listed.push('\'');
        listed.push_str(&truncated(arg, 128 - listed.len() + 1));
        listed.push_str("' ");
    }
    format!("ERR unknown command '{name}', with args beginning with: {listed}")
}

fn truncated(bytes: &[u8], max: usize) -> String {
    String::from_utf8_lossy(&bytes[..bytes.len().min(max)]).into_owned()
}
