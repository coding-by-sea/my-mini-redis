//! A connection's output: a FIFO of encoded replies and pub/sub messages, written to
//! the socket by whoever holds the writer token.
//!
//! Like Redis, the task that runs PUBLISH also writes the message to the subscribers'
//! sockets, so in the common case no subscriber task is woken. That makes several
//! writers possible for one socket, so the outbox keeps Redis's rule that a socket has
//! one writer at a time:
//!
//! - Everything for a connection is appended to its outbox; nobody writes to the socket
//!   directly. Bytes leave in FIFO order, and a partial write leaves the rest at the
//!   head, so frames from different writers never interleave.
//! - Only the holder of the writer token writes. Others just append; the holder sends
//!   what they appended before it releases the token, which it only does with the
//!   outbox empty, checked under the lock. So nothing is left without a writer.
//! - The lock is never held during a syscall: the holder moves the queued bytes to a
//!   buffer only it uses. Publishers append while holding the pub/sub lock, so they must
//!   never wait behind a `sendto`.
//! - A writer other than the connection's own task never waits: when the socket is
//!   full it hands the token to the connection's task, which waits until the socket is
//!   writable.
//! - Each turn with the token is bounded: a writer sends what it took from the queue,
//!   then hands over (or, for the connection's task, yields) if more arrived. Without
//!   this, a subscriber fed by several publishers could keep one writer in a loop that
//!   never awaits, stalling its worker thread and the tasks queued on it for 100+ ms.

use crate::lock;
use bytes::{Buf, BytesMut};
use std::io;
use std::sync::{Arc, Mutex, MutexGuard};
use tokio::net::tcp::OwnedWriteHalf;
use tokio::sync::Notify;

/// Redis's default `client-output-buffer-limit pubsub` hard limit. A subscriber with
/// more than this queued (counted in encoded bytes, as Redis counts its output
/// buffer) is disconnected rather than buffered without bound.
pub(crate) const QUEUE_LIMIT_BYTES: usize = 32 * 1024 * 1024;

/// A send buffer larger than this is freed once empty rather than kept for reuse, so
/// one burst doesn't pin its memory for the life of the connection.
const SEND_BUFFER_KEEP_LIMIT: usize = 1024 * 1024;

#[derive(Clone)]
pub(crate) struct Outbox(Arc<Shared>);

struct Shared {
    socket: Arc<OwnedWriteHalf>,
    queue: Mutex<Queue>,
    /// Bytes taken from `queue`, being written. Only the token holder locks it, so it
    /// is never contended; it is a lock only to be shareable.
    sending: Mutex<BytesMut>,
    /// Wakes the connection's task: the token was handed to it, the queue overflowed,
    /// or it has bytes to send that no other writer will.
    ready: Notify,
}

#[derive(Default)]
struct Queue {
    bytes: BytesMut,
    /// Length of `sending` when it was taken, counted against the limit until written.
    in_flight: usize,
    writer: Writer,
    overflowed: bool,
}

#[derive(Default, PartialEq, Eq, Debug)]
enum Writer {
    /// Nobody writes. Bytes queued in this state have a writer scheduled: a publisher
    /// with this outbox in its `Wakes`, or the connection's task.
    #[default]
    Idle,
    /// Someone writes. It sends everything queued before going idle, or hands the
    /// token to the connection's task.
    Busy,
    /// The socket was full; the connection's task must take over.
    Owner,
}

enum Progress {
    /// Everything was written and the token released.
    Done,
    /// The socket is full; the token is still held.
    Blocked,
    /// What was taken is written, but more was queued meanwhile; the token is still
    /// held, so the turn ends without leaving those bytes without a writer.
    More,
}

impl Outbox {
    pub(crate) fn new(socket: Arc<OwnedWriteHalf>) -> Self {
        Self(Arc::new(Shared {
            socket,
            queue: Mutex::default(),
            sending: Mutex::default(),
            ready: Notify::new(),
        }))
    }

    /// Appends a published message. Returns whether the caller must see that it is
    /// written (`write_now` or `wake`): true when it made an idle outbox non-empty, so
    /// no writer is scheduled yet. Over the limit, what is queued is dropped and the
    /// connection's task is woken to close the connection instead.
    pub(crate) fn deliver(&self, message: &[u8]) -> bool {
        let mut queue = self.queue();
        if queue.overflowed {
            return false;
        }
        if queue.bytes.len() + queue.in_flight + message.len() > QUEUE_LIMIT_BYTES {
            queue.overflowed = true;
            queue.bytes = BytesMut::new();
            self.0.ready.notify_one();
            return false;
        }
        let schedule = queue.writer == Writer::Idle && queue.bytes.is_empty();
        queue.bytes.extend_from_slice(message);
        schedule
    }

    /// Appends the connection's own replies, leaving `replies` empty. The connection's
    /// task sends them with `flush`.
    pub(crate) fn push(&self, replies: &mut BytesMut) {
        if replies.is_empty() {
            return;
        }
        let mut queue = self.queue();
        if queue.bytes.is_empty() {
            std::mem::swap(&mut queue.bytes, replies);
        } else {
            queue.bytes.extend_from_slice(replies);
            replies.clear();
        }
    }

    /// Writes what is queued without waiting, unless someone else is writing. If the
    /// socket fills up or fails, or more is queued meanwhile, the connection's task is
    /// woken to take over.
    pub(crate) fn write_now(&self) {
        if !self.acquire(false) {
            return;
        }
        match self.write_some() {
            Ok(Progress::Done) => {}
            // On failure the connection's task retries the write and gets the error.
            Ok(Progress::Blocked | Progress::More) | Err(_) => {
                self.queue().writer = Writer::Owner;
                self.0.ready.notify_one();
            }
        }
    }

    /// Wakes the connection's task to write what is queued.
    pub(crate) fn wake(&self) {
        self.0.ready.notify_one();
    }

    /// Writes everything queued, waiting while the socket is full, unless another
    /// writer holds the token: it then sends this connection's bytes too. For the
    /// connection's own task only.
    pub(crate) async fn flush(&self) -> io::Result<()> {
        if !self.acquire(true) {
            return Ok(());
        }
        loop {
            match self.write_some() {
                Ok(Progress::Done) => return Ok(()),
                Ok(Progress::Blocked) => self.0.socket.writable().await?,
                // Keep the token, but let the worker run other tasks between turns.
                Ok(Progress::More) => tokio::task::yield_now().await,
                Err(error) => return Err(error),
            }
        }
    }

    /// Sends everything queued before the connection closes, including what another
    /// writer is still sending. Other writers never wait for the socket (they hand the
    /// token over instead), so this only yields while one finishes.
    pub(crate) async fn finish(&self) -> io::Result<()> {
        loop {
            self.flush().await?;
            {
                let queue = self.queue();
                if queue.writer == Writer::Idle && queue.bytes.is_empty() {
                    return Ok(());
                }
            }
            tokio::task::yield_now().await;
        }
    }

    /// Waits until the connection's task may have something to do: write what is
    /// queued, or close because the outbox overflowed. It can return with nothing to
    /// do. Cancel safe: a wakeup sent while nobody waits is kept for the next call.
    pub(crate) async fn ready(&self) {
        self.0.ready.notified().await;
    }

    /// Whether the outbox went over its limit; the connection must then close.
    pub(crate) fn overflowed(&self) -> bool {
        self.queue().overflowed
    }

    /// Takes the writer token if there is something to write and nobody else is
    /// writing. The connection's task (`owner`) also takes a token handed to it.
    fn acquire(&self, owner: bool) -> bool {
        let mut queue = self.queue();
        let free = match queue.writer {
            Writer::Idle => !queue.bytes.is_empty(),
            Writer::Owner => owner,
            Writer::Busy => false,
        };
        if free {
            queue.writer = Writer::Busy;
        }
        free
    }

    /// Writes what is being sent plus, at most once, what is queued, until it is all
    /// sent (releasing the token if nothing more is queued), the socket is full, or
    /// more was queued meanwhile. The caller holds the token.
    fn write_some(&self) -> io::Result<Progress> {
        let mut sending = lock(&self.0.sending);
        let mut took = false;
        loop {
            if sending.is_empty() {
                if sending.capacity() > SEND_BUFFER_KEEP_LIMIT {
                    *sending = BytesMut::new();
                }
                let mut queue = self.queue();
                if queue.bytes.is_empty() {
                    queue.in_flight = 0;
                    queue.writer = Writer::Idle;
                    return Ok(Progress::Done);
                }
                if took {
                    return Ok(Progress::More);
                }
                took = true;
                // The queue gets the emptied buffer, so steady state allocates nothing.
                std::mem::swap(&mut *sending, &mut queue.bytes);
                queue.in_flight = sending.len();
            }
            match self.0.socket.try_write(&sending) {
                Ok(written) => sending.advance(written),
                Err(error) if error.kind() == io::ErrorKind::WouldBlock => {
                    return Ok(Progress::Blocked);
                }
                Err(error) if error.kind() == io::ErrorKind::Interrupted => {}
                Err(error) => return Err(error),
            }
        }
    }

    /// The lock is never held across anything that can panic, so poisoning is ignored
    /// as for the db locks.
    fn queue(&self) -> MutexGuard<'_, Queue> {
        lock(&self.0.queue)
    }
}
