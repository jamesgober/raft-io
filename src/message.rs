//! The RPC messages nodes exchange.
//!
//! Raft defines two RPCs. [`RequestVote`] drives elections; [`AppendEntries`]
//! replicates the log and doubles as the leader's heartbeat. Each has a reply.
//! The protocol core never sends these itself — it emits
//! [`Action::Send`](crate::Action::Send) carrying a [`Message`], and the caller
//! delivers it through a [`RaftTransport`](crate::RaftTransport). Keeping the
//! messages as plain data is what lets a test harness route them in memory and,
//! later, a framing layer put them on a wire.
//!
//! In `v0.2`, [`AppendEntries`] is used only as an empty heartbeat that keeps a
//! follower from starting a needless election. Carrying real entries — the
//! replication pipeline — arrives in `v0.3`; the fields are already present so
//! the wire shape does not change underneath callers.

use crate::types::{Index, LogEntry, NodeId, Snapshot, Term};

/// A candidate's request for a vote in an election.
///
/// Sent by a [`Candidate`](crate::Role::Candidate) to every peer when it starts
/// an election. A recipient grants its vote only if it has not already voted in
/// this term and the candidate's log is at least as up to date as its own — the
/// election restriction that keeps a node missing committed entries from
/// becoming leader.
///
/// # Examples
///
/// ```
/// use raft_io::RequestVote;
///
/// let rv = RequestVote {
///     term: 4, candidate: 2, last_log_index: 9, last_log_term: 3, force: false,
/// };
/// assert_eq!(rv.candidate, 2);
/// ```
#[derive(Clone, Debug, PartialEq, Eq)]
#[cfg_attr(feature = "framing", derive(pack_io::Serialize, pack_io::Deserialize))]
pub struct RequestVote {
    /// The candidate's term.
    pub term: Term,
    /// The candidate requesting the vote.
    pub candidate: NodeId,
    /// Index of the candidate's last log entry.
    pub last_log_index: Index,
    /// Term of the candidate's last log entry.
    pub last_log_term: Term,
    /// A forced election, requested by the current leader as part of a
    /// [leadership transfer](crate::Event::TransferLeadership). A recipient
    /// honours it even within the leader-stickiness window, so the hand-off is
    /// not blocked by its own loyalty to the departing leader.
    pub force: bool,
}

/// A candidate's *pre-vote* probe, sent before it commits to a real election.
///
/// Pre-voting (Raft thesis §9.6) is a disruption guard. Before a node increments
/// its term and campaigns for real, it asks its peers whether they *would* vote
/// for it at the next term — without bumping anyone's term. A peer grants only if
/// it has no active leader and the candidate's log is up to date (the same
/// election restriction a real vote applies). The candidate runs a real
/// [`RequestVote`] election only once a quorum of pre-votes says yes.
///
/// The point is that a node partitioned away from the cluster never collects a
/// pre-vote majority, so it never inflates its term. When it rejoins it does not
/// force the established leader to step down, which is the disruption a plain
/// election would cause. Unlike [`RequestVote`], a pre-vote changes no persistent
/// state on either side.
///
/// # Examples
///
/// ```
/// use raft_io::PreVote;
///
/// // The `term` is the *hypothetical* term the candidate would campaign at —
/// // one past its current term — not a term it has adopted.
/// let pv = PreVote { term: 5, candidate: 2, last_log_index: 9, last_log_term: 3 };
/// assert_eq!(pv.candidate, 2);
/// ```
#[derive(Clone, Debug, PartialEq, Eq)]
#[cfg_attr(feature = "framing", derive(pack_io::Serialize, pack_io::Deserialize))]
pub struct PreVote {
    /// The hypothetical term the candidate would campaign at — one past its
    /// current term. It is *not* a term the candidate has adopted; a recipient
    /// neither stores it nor steps down for it.
    pub term: Term,
    /// The candidate seeking the pre-vote.
    pub candidate: NodeId,
    /// Index of the candidate's last log entry.
    pub last_log_index: Index,
    /// Term of the candidate's last log entry.
    pub last_log_term: Term,
}

/// A peer's response to a [`PreVote`].
///
/// `term` is the responder's *current* term, unchanged by the pre-vote. If it
/// exceeds the pre-candidate's term, the pre-candidate has fallen behind and
/// abandons the round; otherwise `vote_granted` tells it whether this peer would
/// support a real election. None of this touches persistent state.
///
/// # Examples
///
/// ```
/// use raft_io::PreVoteReply;
///
/// let reply = PreVoteReply { term: 4, vote_granted: true, from: 3 };
/// assert!(reply.vote_granted);
/// ```
#[derive(Clone, Debug, PartialEq, Eq)]
#[cfg_attr(feature = "framing", derive(pack_io::Serialize, pack_io::Deserialize))]
pub struct PreVoteReply {
    /// The responder's current term, unchanged by the pre-vote.
    pub term: Term,
    /// Whether the responder would grant a real vote under these conditions.
    pub vote_granted: bool,
    /// The node that produced this reply.
    pub from: NodeId,
}

/// A peer's response to a [`RequestVote`].
///
/// `from` names the responder so the candidate can count distinct votes without
/// depending on transport-level addressing. If `term` exceeds the candidate's
/// term, the candidate steps down instead of counting the vote.
///
/// # Examples
///
/// ```
/// use raft_io::RequestVoteReply;
///
/// let reply = RequestVoteReply { term: 4, vote_granted: true, from: 3 };
/// assert!(reply.vote_granted);
/// ```
#[derive(Clone, Debug, PartialEq, Eq)]
#[cfg_attr(feature = "framing", derive(pack_io::Serialize, pack_io::Deserialize))]
pub struct RequestVoteReply {
    /// The responder's current term, for the candidate to update itself.
    pub term: Term,
    /// Whether the responder granted its vote.
    pub vote_granted: bool,
    /// The node that produced this reply.
    pub from: NodeId,
}

/// The leader's replicate-and-heartbeat RPC.
///
/// The leader sends this to each follower. With an empty
/// [`entries`](AppendEntries::entries) list it is a pure heartbeat that asserts
/// leadership and resets the follower's election timer; with entries it
/// replicates the log (from `v0.3`). The `prev_log_index` / `prev_log_term`
/// pair lets the follower verify its log matches the leader's up to that point
/// before accepting anything new.
///
/// # Examples
///
/// ```
/// use raft_io::AppendEntries;
///
/// // An empty heartbeat for term 4 from node 1.
/// let hb = AppendEntries {
///     term: 4,
///     leader: 1,
///     prev_log_index: 9,
///     prev_log_term: 3,
///     entries: Vec::new(),
///     leader_commit: 7,
/// };
/// assert!(hb.entries.is_empty());
/// ```
#[derive(Clone, Debug, PartialEq, Eq)]
#[cfg_attr(feature = "framing", derive(pack_io::Serialize, pack_io::Deserialize))]
pub struct AppendEntries {
    /// The leader's term.
    pub term: Term,
    /// The leader sending the RPC, so followers can record it.
    pub leader: NodeId,
    /// Index of the log entry immediately preceding the new ones.
    pub prev_log_index: Index,
    /// Term of the entry at `prev_log_index`.
    pub prev_log_term: Term,
    /// Entries to store (empty for a heartbeat). Replication uses this in `v0.3`.
    pub entries: Vec<LogEntry>,
    /// The leader's commit index, so followers can advance their own.
    pub leader_commit: Index,
}

/// A follower's response to an [`AppendEntries`].
///
/// `success` is `true` when the follower's log matched at `prev_log_index` and
/// it accepted the RPC. `match_index` reports the highest log index the
/// follower now agrees on, which the leader uses to track replication progress.
///
/// On a rejection, the `conflict_*` fields let the leader skip the follower's
/// `next_index` back by a whole term in one round trip instead of decrementing
/// one entry at a time (the fast-backtracking optimisation from the Raft thesis,
/// §5.3). They are `0` on success and ignored.
///
/// # Examples
///
/// ```
/// use raft_io::AppendEntriesReply;
///
/// let ok = AppendEntriesReply {
///     term: 4,
///     success: true,
///     from: 2,
///     match_index: 9,
///     conflict_index: 0,
///     conflict_term: 0,
/// };
/// assert!(ok.success);
/// ```
#[derive(Clone, Debug, PartialEq, Eq)]
#[cfg_attr(feature = "framing", derive(pack_io::Serialize, pack_io::Deserialize))]
pub struct AppendEntriesReply {
    /// The follower's current term, for the leader to update itself.
    pub term: Term,
    /// Whether the follower accepted the RPC.
    pub success: bool,
    /// The node that produced this reply.
    pub from: NodeId,
    /// Highest log index the follower now matches with the leader.
    pub match_index: Index,
    /// On rejection, the index the leader should probe next (the follower's
    /// first index of `conflict_term`, or its log length plus one when the log
    /// is simply too short). `0` on success.
    pub conflict_index: Index,
    /// On rejection, the term of the follower's entry at `prev_log_index`, or
    /// `0` when the follower has no entry there. `0` on success.
    pub conflict_term: Term,
}

/// A leader's transfer of a [`Snapshot`] to a follower too far behind to
/// replicate entry by entry.
///
/// When a follower's next required entry has already been compacted out of the
/// leader's log, the leader sends this instead of an [`AppendEntries`]. The
/// follower installs the snapshot — replacing its state through
/// `snapshot.index` — then resumes normal replication from the tail.
///
/// # Examples
///
/// ```
/// use raft_io::{InstallSnapshot, Snapshot};
///
/// let rpc = InstallSnapshot { term: 5, leader: 1, snapshot: Snapshot::new(10, 3, vec![]) };
/// assert_eq!(rpc.snapshot.index, 10);
/// ```
#[derive(Clone, Debug, PartialEq, Eq)]
#[cfg_attr(feature = "framing", derive(pack_io::Serialize, pack_io::Deserialize))]
pub struct InstallSnapshot {
    /// The leader's term.
    pub term: Term,
    /// The leader sending the snapshot.
    pub leader: NodeId,
    /// The snapshot to install.
    pub snapshot: Snapshot,
}

/// A follower's response to an [`InstallSnapshot`].
///
/// `last_index` is the snapshot's index the follower has now installed, which
/// the leader uses to advance that follower's replication progress.
///
/// # Examples
///
/// ```
/// use raft_io::InstallSnapshotReply;
///
/// let reply = InstallSnapshotReply { term: 5, from: 2, last_index: 10 };
/// assert_eq!(reply.last_index, 10);
/// ```
#[derive(Clone, Debug, PartialEq, Eq)]
#[cfg_attr(feature = "framing", derive(pack_io::Serialize, pack_io::Deserialize))]
pub struct InstallSnapshotReply {
    /// The follower's current term, for the leader to update itself.
    pub term: Term,
    /// The node that produced this reply.
    pub from: NodeId,
    /// The snapshot index the follower has installed.
    pub last_index: Index,
}

/// A leader's signal telling `target` to start an election immediately.
///
/// Sent during a [leadership transfer](crate::Event::TransferLeadership): once the
/// target is fully caught up, the leader sends this so the target campaigns at
/// once instead of waiting out its election timeout, taking over with minimal
/// disruption.
///
/// # Examples
///
/// ```
/// use raft_io::TimeoutNow;
///
/// let rpc = TimeoutNow { term: 5, leader: 1 };
/// assert_eq!(rpc.leader, 1);
/// ```
#[derive(Clone, Debug, PartialEq, Eq)]
#[cfg_attr(feature = "framing", derive(pack_io::Serialize, pack_io::Deserialize))]
pub struct TimeoutNow {
    /// The leader's term.
    pub term: Term,
    /// The leader handing off leadership.
    pub leader: NodeId,
}

/// Any message a node can send or receive.
///
/// Wraps the RPCs and their replies. The enum is
/// [`#[non_exhaustive]`](https://doc.rust-lang.org/reference/attributes/type_system.html#the-non_exhaustive-attribute):
/// future versions may add variants, so a `match` over a `Message` must include
/// a wildcard arm.
///
/// # Examples
///
/// ```
/// use raft_io::{Message, RequestVote};
///
/// let msg = Message::RequestVote(RequestVote {
///     term: 1,
///     candidate: 1,
///     last_log_index: 0,
///     last_log_term: 0,
///     force: false,
/// });
/// match msg {
///     Message::RequestVote(rv) => assert_eq!(rv.term, 1),
///     _ => unreachable!(),
/// }
/// ```
#[non_exhaustive]
#[derive(Clone, Debug, PartialEq, Eq)]
#[cfg_attr(feature = "framing", derive(pack_io::Serialize, pack_io::Deserialize))]
pub enum Message {
    /// A candidate is probing for support before a real election.
    PreVote(PreVote),
    /// A peer is answering a pre-vote probe.
    PreVoteReply(PreVoteReply),
    /// A candidate is asking for a vote.
    RequestVote(RequestVote),
    /// A peer is answering a vote request.
    RequestVoteReply(RequestVoteReply),
    /// A leader is replicating entries or sending a heartbeat.
    AppendEntries(AppendEntries),
    /// A follower is answering an append.
    AppendEntriesReply(AppendEntriesReply),
    /// A leader is shipping a snapshot to a far-behind follower.
    InstallSnapshot(InstallSnapshot),
    /// A follower is acknowledging an installed snapshot.
    InstallSnapshotReply(InstallSnapshotReply),
    /// A leader is handing off leadership, telling the target to campaign now.
    TimeoutNow(TimeoutNow),
}

impl Message {
    /// Returns the term carried by the message, whatever its variant.
    ///
    /// The protocol checks the term of every inbound message first — a higher
    /// term forces the node to step down — so a single accessor avoids matching
    /// at each call site.
    ///
    /// # Examples
    ///
    /// ```
    /// use raft_io::{AppendEntriesReply, Message};
    ///
    /// let m = Message::AppendEntriesReply(AppendEntriesReply {
    ///     term: 5,
    ///     success: false,
    ///     from: 2,
    ///     match_index: 0,
    ///     conflict_index: 1,
    ///     conflict_term: 0,
    /// });
    /// assert_eq!(m.term(), 5);
    /// ```
    #[inline]
    #[must_use]
    pub fn term(&self) -> Term {
        match self {
            Self::PreVote(m) => m.term,
            Self::PreVoteReply(m) => m.term,
            Self::RequestVote(m) => m.term,
            Self::RequestVoteReply(m) => m.term,
            Self::AppendEntries(m) => m.term,
            Self::AppendEntriesReply(m) => m.term,
            Self::InstallSnapshot(m) => m.term,
            Self::InstallSnapshotReply(m) => m.term,
            Self::TimeoutNow(m) => m.term,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_message_term_reads_each_variant() {
        assert_eq!(
            Message::RequestVote(RequestVote {
                term: 1,
                candidate: 1,
                last_log_index: 0,
                last_log_term: 0,
                force: false,
            })
            .term(),
            1
        );
        assert_eq!(
            Message::RequestVoteReply(RequestVoteReply {
                term: 2,
                vote_granted: true,
                from: 1
            })
            .term(),
            2
        );
        assert_eq!(
            Message::AppendEntries(AppendEntries {
                term: 3,
                leader: 1,
                prev_log_index: 0,
                prev_log_term: 0,
                entries: Vec::new(),
                leader_commit: 0,
            })
            .term(),
            3
        );
        assert_eq!(
            Message::AppendEntriesReply(AppendEntriesReply {
                term: 4,
                success: true,
                from: 1,
                match_index: 0,
                conflict_index: 0,
                conflict_term: 0,
            })
            .term(),
            4
        );
    }
}
