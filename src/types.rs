//! Core value types shared across the protocol.
//!
//! These are deliberately plain: a [`NodeId`], the monotonic [`Term`] and
//! [`Index`] counters, the [`Role`] a node currently plays, a single
//! [`LogEntry`], and the [`HardState`] that Raft requires to survive a restart.
//! They carry no behaviour beyond construction and small accessors, which keeps
//! them cheap to copy and trivial to serialize once framing lands.

/// Identifier for a node in the cluster.
///
/// Identifiers are opaque to the protocol; any scheme is fine as long as each
/// node in a cluster has a distinct, stable value. A plain integer keeps the
/// common case allocation-free and `Copy`.
pub type NodeId = u64;

/// A Raft term: a monotonically increasing logical clock.
///
/// Terms partition time into epochs, each beginning with an election. Every
/// message carries the sender's term; a node that sees a higher term steps down
/// and adopts it. Term `0` is the initial value before any election.
pub type Term = u64;

/// Position of an entry in the replicated log.
///
/// Indices are 1-based: the first appended entry has index `1`, and index `0`
/// is the sentinel meaning "before the first entry" (with term `0`). Using `0`
/// as a sentinel lets the `prev_log_index` consistency check at the head of the
/// log fall out without a special case.
pub type Index = u64;

/// The role a node currently plays in the consensus protocol.
///
/// A node is always in exactly one role. It starts as a [`Follower`], may
/// become a [`Candidate`] when it stops hearing from a leader, and becomes a
/// [`Leader`] if it wins an election.
///
/// [`Follower`]: Role::Follower
/// [`Candidate`]: Role::Candidate
/// [`Leader`]: Role::Leader
///
/// # Examples
///
/// ```
/// use raft_io::{RaftConfig, RaftNode, Role};
///
/// let node = RaftNode::new(RaftConfig::single(1));
/// assert_eq!(node.role(), Role::Follower);
/// ```
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum Role {
    /// Passive replica: serves the leader and votes in elections.
    Follower,
    /// Standing for election in the current term, collecting votes.
    Candidate,
    /// Won the election for the current term; drives replication.
    Leader,
}

/// A single command in the replicated log.
///
/// The [`command`](LogEntry::command) is opaque bytes: the protocol replicates
/// and orders entries but never interprets them. The application's state
/// machine decodes the bytes when the entry is applied. Each entry records the
/// [`term`](LogEntry::term) in which the leader created it and its
/// [`index`](LogEntry::index) in the log, which together identify it uniquely.
///
/// # Examples
///
/// ```
/// use raft_io::LogEntry;
///
/// let entry = LogEntry::new(2, 7, b"put k v".to_vec());
/// assert_eq!(entry.term, 2);
/// assert_eq!(entry.index, 7);
/// assert_eq!(entry.command, b"put k v");
/// ```
#[derive(Clone, Debug, PartialEq, Eq)]
#[cfg_attr(feature = "framing", derive(pack_io::Serialize, pack_io::Deserialize))]
pub struct LogEntry {
    /// Term in which the leader created this entry.
    pub term: Term,
    /// 1-based position of this entry in the log.
    pub index: Index,
    /// Opaque application command. The protocol never inspects these bytes.
    pub command: Vec<u8>,
}

impl LogEntry {
    /// Creates a log entry at `index` in `term` carrying `command`.
    ///
    /// # Examples
    ///
    /// ```
    /// use raft_io::LogEntry;
    ///
    /// let e = LogEntry::new(1, 1, vec![0xAB]);
    /// assert_eq!(e.command, vec![0xAB]);
    /// ```
    #[inline]
    #[must_use]
    pub fn new(term: Term, index: Index, command: Vec<u8>) -> Self {
        Self {
            term,
            index,
            command,
        }
    }
}

/// The state Raft must persist before responding to any RPC.
///
/// Safety depends on `current_term` and `voted_for` surviving a crash: a node
/// that forgot it had already voted in a term could vote twice and help elect
/// two leaders. The [`RaftLog`](crate::RaftLog) stores this alongside the log
/// entries; the in-memory [`MemoryLog`](crate::MemoryLog) keeps it in a field.
///
/// # Examples
///
/// ```
/// use raft_io::HardState;
///
/// let hs = HardState::default();
/// assert_eq!(hs.term, 0);
/// assert_eq!(hs.voted_for, None);
/// ```
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct HardState {
    /// The latest term the node has seen.
    pub term: Term,
    /// The candidate this node voted for in `term`, if any.
    pub voted_for: Option<NodeId>,
}

/// A point-in-time capture of the application's state machine, with the log
/// position it covers.
///
/// A snapshot lets the log discard the entries it subsumes (compaction) and lets
/// a leader catch up a follower that has fallen too far behind to replicate
/// entry by entry. [`index`](Snapshot::index) and [`term`](Snapshot::term) are
/// the last log entry the snapshot includes — its replacement "sentinel" once
/// the entries up to there are gone — and [`data`](Snapshot::data) is the opaque
/// serialized state the application produces and restores. The protocol moves
/// the bytes but never interprets them.
///
/// # Examples
///
/// ```
/// use raft_io::Snapshot;
///
/// let snap = Snapshot::new(10, 3, b"serialized state".to_vec());
/// assert_eq!(snap.index, 10);
/// assert_eq!(snap.term, 3);
/// ```
#[derive(Clone, Debug, PartialEq, Eq)]
#[cfg_attr(feature = "framing", derive(pack_io::Serialize, pack_io::Deserialize))]
pub struct Snapshot {
    /// Index of the last log entry the snapshot includes.
    pub index: Index,
    /// Term of the last log entry the snapshot includes.
    pub term: Term,
    /// Opaque serialized state machine state. The protocol never inspects it.
    pub data: Vec<u8>,
}

impl Snapshot {
    /// Creates a snapshot covering the log through `index` (created in `term`),
    /// carrying serialized state `data`.
    ///
    /// # Examples
    ///
    /// ```
    /// use raft_io::Snapshot;
    ///
    /// let snap = Snapshot::new(5, 2, vec![1, 2, 3]);
    /// assert_eq!(snap.data, vec![1, 2, 3]);
    /// ```
    #[inline]
    #[must_use]
    pub fn new(index: Index, term: Term, data: Vec<u8>) -> Self {
        Self { index, term, data }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_log_entry_new_sets_all_fields() {
        let e = LogEntry::new(3, 9, vec![1, 2, 3]);
        assert_eq!(e.term, 3);
        assert_eq!(e.index, 9);
        assert_eq!(e.command, vec![1, 2, 3]);
    }

    #[test]
    fn test_hard_state_default_is_term_zero_no_vote() {
        let hs = HardState::default();
        assert_eq!(
            hs,
            HardState {
                term: 0,
                voted_for: None
            }
        );
    }

    #[test]
    fn test_role_is_copy_and_comparable() {
        let r = Role::Leader;
        let copy = r;
        assert_eq!(r, copy);
        assert_ne!(Role::Follower, Role::Candidate);
    }
}
