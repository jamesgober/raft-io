//! The message-delivery seam and an in-memory implementation.
//!
//! The protocol core does not send anything itself — it emits
//! [`Action::Send`](crate::Action::Send), and a driver delivers the message.
//! [`RaftTransport`] is the trait that driver implements. Splitting "decide to
//! send" from "actually send" is what keeps the core deterministic and free of
//! networking: the same election can be replayed in a unit test with a transport
//! that just records messages, then run in production over TCP, with no change
//! to the protocol.
//!
//! [`MemoryTransport`] is the recording implementation used by the test harness
//! and examples.

use crate::error::Result;
use crate::message::Message;
use crate::types::NodeId;

/// Delivers protocol messages to peers.
///
/// A driver loop takes each [`Action::Send`](crate::Action::Send) a node emits
/// and calls [`send`](RaftTransport::send). How delivery happens — an in-process
/// queue, a channel, a socket — is entirely the implementor's concern; the
/// protocol only requires that a message handed to `send` is eventually
/// delivered to the target node's [`step`](crate::RaftNode::step) (Raft already
/// tolerates loss, reordering, and duplication, so "eventually, maybe" is a
/// sufficient contract).
///
/// # Examples
///
/// ```
/// use raft_io::{MemoryTransport, RaftTransport, Message, RequestVote};
///
/// let mut tx = MemoryTransport::new();
/// tx.send(2, Message::RequestVote(RequestVote {
///     term: 1, candidate: 1, last_log_index: 0, last_log_term: 0,
/// })).unwrap();
/// assert_eq!(tx.take().len(), 1);
/// ```
pub trait RaftTransport {
    /// Delivers `message` to node `to`.
    ///
    /// # Errors
    ///
    /// Returns an [`Error`](crate::Error) if the transport cannot accept the
    /// message. Note that Raft treats the network as unreliable regardless, so
    /// a delivery that is dropped after being accepted is not an error.
    fn send(&mut self, to: NodeId, message: Message) -> Result<()>;
}

/// An in-memory [`RaftTransport`] that records outgoing messages.
///
/// Instead of delivering anywhere, it appends each message to an outbox that a
/// test harness drains with [`take`](MemoryTransport::take) and routes to the
/// destination node by hand. This makes message ordering, loss, and partitions
/// something the test controls precisely.
///
/// # Examples
///
/// ```
/// use raft_io::{MemoryTransport, RaftTransport, Message, AppendEntries};
///
/// let mut tx = MemoryTransport::new();
/// tx.send(2, Message::AppendEntries(AppendEntries {
///     term: 1, leader: 1, prev_log_index: 0, prev_log_term: 0,
///     entries: Vec::new(), leader_commit: 0,
/// })).unwrap();
///
/// let pending = tx.take();
/// assert_eq!(pending[0].0, 2);          // destination
/// assert!(tx.take().is_empty());        // draining leaves it empty
/// ```
#[derive(Clone, Debug, Default)]
pub struct MemoryTransport {
    outbox: Vec<(NodeId, Message)>,
}

impl MemoryTransport {
    /// Creates an empty transport.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Removes and returns every queued `(destination, message)` pair.
    ///
    /// # Examples
    ///
    /// ```
    /// use raft_io::MemoryTransport;
    ///
    /// let mut tx = MemoryTransport::new();
    /// assert!(tx.take().is_empty());
    /// ```
    #[must_use]
    pub fn take(&mut self) -> Vec<(NodeId, Message)> {
        core::mem::take(&mut self.outbox)
    }

    /// Returns the number of queued messages without draining them.
    #[inline]
    #[must_use]
    pub fn pending(&self) -> usize {
        self.outbox.len()
    }
}

impl RaftTransport for MemoryTransport {
    #[inline]
    fn send(&mut self, to: NodeId, message: Message) -> Result<()> {
        self.outbox.push((to, message));
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used, clippy::expect_used)]

    use super::*;
    use crate::message::RequestVoteReply;

    fn reply(from: NodeId) -> Message {
        Message::RequestVoteReply(RequestVoteReply {
            term: 1,
            vote_granted: true,
            from,
        })
    }

    #[test]
    fn test_send_queues_in_order() {
        let mut tx = MemoryTransport::new();
        tx.send(2, reply(1)).unwrap();
        tx.send(3, reply(1)).unwrap();
        assert_eq!(tx.pending(), 2);
        let drained = tx.take();
        assert_eq!(drained[0].0, 2);
        assert_eq!(drained[1].0, 3);
    }

    #[test]
    fn test_take_drains() {
        let mut tx = MemoryTransport::new();
        tx.send(2, reply(1)).unwrap();
        let _ = tx.take();
        assert_eq!(tx.pending(), 0);
        assert!(tx.take().is_empty());
    }
}
