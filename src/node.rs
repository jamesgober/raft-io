//! The deterministic protocol core: [`RaftNode`], [`Event`], and [`Action`].
//!
//! A [`RaftNode`] is a pure state machine. You drive it with [`step`], handing
//! it one [`Event`] — a logical [`Tick`], an inbound [`Message`], or a client
//! [`Propose`] — and it returns the [`Action`]s the outside world must carry
//! out: messages to send and committed commands to apply. It never reads a
//! clock, opens a socket, or touches a disk; all of that is the caller's job,
//! reached through the [`RaftLog`] it owns and the
//! [`RaftTransport`](crate::RaftTransport) the caller drives. That is what makes
//! the protocol reproducible from a seed and a sequence of events.
//!
//! # Scope at v0.2
//!
//! This release implements leader election with full term and vote safety, the
//! heartbeat that keeps a leader in place, and commit on a single-node cluster.
//! Multi-node log replication — carrying entries in [`AppendEntries`], tracking
//! each follower's progress, and advancing the commit index on a quorum —
//! arrives in `v0.3`. The message shapes already carry the fields that work
//! needs, so callers will not see a wire change.
//!
//! [`step`]: RaftNode::step
//! [`Tick`]: Event::Tick
//! [`Propose`]: Event::Propose
//! [`Message`]: crate::Message
//! [`AppendEntries`]: crate::AppendEntries
//! [`RaftLog`]: crate::RaftLog

use crate::config::RaftConfig;
use crate::error::{Error, Result};
use crate::log::{MemoryLog, RaftLog};
use crate::message::{AppendEntries, AppendEntriesReply, Message, RequestVote, RequestVoteReply};
use crate::rng::Rng;
use crate::types::{HardState, Index, LogEntry, NodeId, Role, Term};

/// An input handed to [`RaftNode::step`].
///
/// A node only ever changes state in response to an event. There are exactly
/// three, matching Raft's three sources of progress: the passage of (logical)
/// time, a message from a peer, and a request from a client.
///
/// # Examples
///
/// ```
/// use raft_io::{Event, Message, RequestVote};
///
/// let _tick = Event::Tick;
/// let _propose = Event::Propose(b"command".to_vec());
/// let _msg = Event::Message(Message::RequestVote(RequestVote {
///     term: 1, candidate: 2, last_log_index: 0, last_log_term: 0,
/// }));
/// ```
pub enum Event {
    /// One logical clock tick. The caller decides the wall-clock interval.
    Tick,
    /// A message arrived from a peer.
    Message(Message),
    /// A client proposes a command to be replicated and applied.
    ///
    /// Only a leader may accept a proposal; on any other node
    /// [`step`](RaftNode::step) returns [`Error::NotLeader`].
    Propose(Vec<u8>),
}

/// An instruction [`RaftNode::step`] returns for the caller to carry out.
///
/// The node decides *what* must happen; the caller makes it happen. Execute the
/// actions in the order returned: any state the protocol depends on has already
/// been persisted through the [`RaftLog`](crate::RaftLog) before a
/// [`Send`](Action::Send) is emitted, so honouring the order preserves Raft's
/// durability rule.
///
/// The enum is [`#[non_exhaustive]`](https://doc.rust-lang.org/reference/attributes/type_system.html#the-non_exhaustive-attribute):
/// a snapshot action joins it in `v0.5`, so a `match` must include a wildcard
/// arm.
///
/// # Examples
///
/// ```
/// use raft_io::{Action, Event, RaftConfig, RaftNode};
///
/// let mut node = RaftNode::new(RaftConfig::single(1));
/// while !node.is_leader() {
///     let _ = node.step(Event::Tick).unwrap();
/// }
/// for action in node.step(Event::Propose(b"x".to_vec())).unwrap() {
///     match action {
///         Action::Send { to, message } => { let _ = (to, message); }
///         Action::Apply { index, term, command } => { let _ = (index, term, command); }
///         _ => {}
///     }
/// }
/// ```
#[non_exhaustive]
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Action {
    /// Send `message` to node `to` via the transport.
    Send {
        /// Destination node.
        to: NodeId,
        /// The message to deliver.
        message: Message,
    },
    /// Apply a committed command to the application state machine.
    ///
    /// Applies are emitted in strictly increasing index order and each index is
    /// emitted at most once, so the caller can apply them blindly in sequence.
    Apply {
        /// Index of the committed entry.
        index: Index,
        /// Term the entry was created in.
        term: Term,
        /// The opaque command bytes to apply.
        command: Vec<u8>,
    },
}

/// A node in a Raft cluster: the deterministic consensus state machine.
///
/// Create one with [`new`](RaftNode::new) (Tier 1, in-memory log) or
/// [`with_log`](RaftNode::with_log) (Tier 3, your own [`RaftLog`]), then drive
/// it with [`step`](RaftNode::step). The generic `L` defaults to [`MemoryLog`],
/// so the common case never has to name it.
///
/// # Examples
///
/// ```
/// use raft_io::{Event, RaftConfig, RaftNode};
///
/// let mut node = RaftNode::new(RaftConfig::single(1));
/// assert!(!node.is_leader());
/// while !node.is_leader() {
///     let _ = node.step(Event::Tick).unwrap();
/// }
/// assert!(node.is_leader());
/// ```
pub struct RaftNode<L: RaftLog = MemoryLog> {
    id: NodeId,
    peers: Vec<NodeId>,
    quorum: usize,
    election_timeout_min: u32,
    election_timeout_max: u32,
    heartbeat_interval: u32,

    log: L,
    role: Role,
    current_term: Term,
    voted_for: Option<NodeId>,
    leader_id: Option<NodeId>,
    commit_index: Index,
    last_applied: Index,

    election_elapsed: u32,
    heartbeat_elapsed: u32,
    election_timeout: u32,
    votes: Vec<NodeId>,
    rng: Rng,
}

impl RaftNode<MemoryLog> {
    /// Creates a node from `config` backed by an in-memory [`MemoryLog`].
    ///
    /// This is the Tier-1 entry point: one call, no generic to name, no I/O to
    /// wire up. The node starts as a [`Follower`](Role::Follower) in term `0`.
    ///
    /// # Examples
    ///
    /// ```
    /// use raft_io::{RaftConfig, RaftNode, Role};
    ///
    /// let node = RaftNode::new(RaftConfig::new(1, [2, 3]));
    /// assert_eq!(node.role(), Role::Follower);
    /// assert_eq!(node.term(), 0);
    /// ```
    #[must_use]
    pub fn new(config: RaftConfig) -> Self {
        Self::with_log(config, MemoryLog::new())
    }
}

impl<L: RaftLog> RaftNode<L> {
    /// Creates a node from `config` backed by a caller-supplied `log`.
    ///
    /// This is the Tier-3 entry point: provide any [`RaftLog`] implementation —
    /// for example a durable, `wal-db`-backed store (`v0.4`). The node adopts
    /// the log's persisted [`HardState`](crate::HardState) on construction, so a
    /// store recovered from disk resumes in the term it last persisted and with
    /// the vote it last cast.
    ///
    /// # Examples
    ///
    /// ```
    /// use raft_io::{MemoryLog, RaftConfig, RaftNode};
    ///
    /// let node = RaftNode::with_log(RaftConfig::single(1), MemoryLog::new());
    /// assert_eq!(node.id(), 1);
    /// ```
    #[must_use]
    pub fn with_log(config: RaftConfig, log: L) -> Self {
        let hard = log.hard_state();
        let cluster_size = config.peers.len() + 1;
        let quorum = cluster_size / 2 + 1;
        let mut rng = Rng::new(config.seed);
        let election_timeout =
            rng.gen_range(config.election_timeout_min, config.election_timeout_max);
        Self {
            id: config.id,
            peers: config.peers,
            quorum,
            election_timeout_min: config.election_timeout_min,
            election_timeout_max: config.election_timeout_max,
            heartbeat_interval: config.heartbeat_interval,
            log,
            role: Role::Follower,
            current_term: hard.term,
            voted_for: hard.voted_for,
            leader_id: None,
            commit_index: 0,
            last_applied: 0,
            election_elapsed: 0,
            heartbeat_elapsed: 0,
            election_timeout,
            votes: Vec::new(),
            rng,
        }
    }

    // ---- accessors -------------------------------------------------------

    /// Returns this node's id.
    #[inline]
    #[must_use]
    pub fn id(&self) -> NodeId {
        self.id
    }

    /// Returns the role the node currently plays.
    #[inline]
    #[must_use]
    pub fn role(&self) -> Role {
        self.role
    }

    /// Returns `true` if the node is the leader.
    ///
    /// # Examples
    ///
    /// ```
    /// use raft_io::{Event, RaftConfig, RaftNode};
    ///
    /// let mut node = RaftNode::new(RaftConfig::single(1));
    /// while !node.is_leader() {
    ///     let _ = node.step(Event::Tick).unwrap();
    /// }
    /// assert!(node.is_leader());
    /// ```
    #[inline]
    #[must_use]
    pub fn is_leader(&self) -> bool {
        self.role == Role::Leader
    }

    /// Returns the node's current term.
    #[inline]
    #[must_use]
    pub fn term(&self) -> Term {
        self.current_term
    }

    /// Returns the leader the node currently recognises, if any.
    #[inline]
    #[must_use]
    pub fn leader(&self) -> Option<NodeId> {
        self.leader_id
    }

    /// Returns the highest log index known to be committed.
    #[inline]
    #[must_use]
    pub fn commit_index(&self) -> Index {
        self.commit_index
    }

    /// Returns the highest log index the node has applied.
    #[inline]
    #[must_use]
    pub fn last_applied(&self) -> Index {
        self.last_applied
    }

    /// Returns a shared reference to the underlying log.
    ///
    /// # Examples
    ///
    /// ```
    /// use raft_io::{RaftConfig, RaftNode, RaftLog};
    ///
    /// let node = RaftNode::new(RaftConfig::single(1));
    /// assert_eq!(node.log().last_index(), 0);
    /// ```
    #[inline]
    #[must_use]
    pub fn log(&self) -> &L {
        &self.log
    }

    // ---- the step function ----------------------------------------------

    /// Advances the state machine by one [`Event`] and returns the resulting
    /// [`Action`]s.
    ///
    /// This is the only way to drive a node. Feed it ticks to let time pass,
    /// inbound messages as they arrive, and client proposals; act on every
    /// returned action, in order. The call is deterministic: the same node state
    /// and the same event always produce the same actions.
    ///
    /// # Errors
    ///
    /// - [`Error::NotLeader`] if the event is [`Event::Propose`] and this node
    ///   is not the leader; the error carries the known leader so the caller can
    ///   redirect.
    /// - [`Error::Storage`] if the underlying [`RaftLog`] fails on the
    ///   durability path. A storage failure here is fatal to the node.
    ///
    /// # Examples
    ///
    /// ```
    /// use raft_io::{Action, Event, RaftConfig, RaftNode};
    ///
    /// let mut node = RaftNode::new(RaftConfig::single(1));
    /// while !node.is_leader() {
    ///     let _ = node.step(Event::Tick).unwrap();
    /// }
    /// let actions = node.step(Event::Propose(b"set x 1".to_vec())).unwrap();
    /// assert!(actions.iter().any(|a| matches!(a, Action::Apply { .. })));
    /// ```
    pub fn step(&mut self, event: Event) -> Result<Vec<Action>> {
        match event {
            Event::Tick => self.tick(),
            Event::Message(message) => self.handle_message(message),
            Event::Propose(command) => self.propose(command),
        }
    }

    // ---- tick handling ---------------------------------------------------

    fn tick(&mut self) -> Result<Vec<Action>> {
        let mut actions = Vec::new();
        match self.role {
            Role::Follower | Role::Candidate => {
                self.election_elapsed += 1;
                if self.election_elapsed >= self.election_timeout {
                    self.start_election(&mut actions)?;
                }
            }
            Role::Leader => {
                self.heartbeat_elapsed += 1;
                if self.heartbeat_elapsed >= self.heartbeat_interval {
                    self.heartbeat_elapsed = 0;
                    self.broadcast_heartbeat(&mut actions);
                }
            }
        }
        Ok(actions)
    }

    fn start_election(&mut self, actions: &mut Vec<Action>) -> Result<()> {
        self.role = Role::Candidate;
        self.current_term += 1;
        self.voted_for = Some(self.id);
        self.leader_id = None;
        self.votes.clear();
        self.votes.push(self.id);
        self.reset_election_timer();
        self.persist_hard_state()?;

        // A single-node cluster (or any cluster where one vote is a majority)
        // wins immediately.
        if self.votes.len() >= self.quorum {
            self.become_leader(actions);
            return Ok(());
        }

        let last_log_index = self.log.last_index();
        let last_log_term = self.log.last_term();
        for &peer in &self.peers {
            actions.push(Action::Send {
                to: peer,
                message: Message::RequestVote(RequestVote {
                    term: self.current_term,
                    candidate: self.id,
                    last_log_index,
                    last_log_term,
                }),
            });
        }
        Ok(())
    }

    fn become_leader(&mut self, actions: &mut Vec<Action>) {
        self.role = Role::Leader;
        self.leader_id = Some(self.id);
        self.heartbeat_elapsed = 0;
        // Establish authority right away, and (single-node) commit anything
        // outstanding from the current term.
        self.broadcast_heartbeat(actions);
        self.advance_commit_as_leader(actions);
    }

    fn broadcast_heartbeat(&self, actions: &mut Vec<Action>) {
        let prev_log_index = self.log.last_index();
        let prev_log_term = self.log.last_term();
        for &peer in &self.peers {
            actions.push(Action::Send {
                to: peer,
                message: Message::AppendEntries(AppendEntries {
                    term: self.current_term,
                    leader: self.id,
                    prev_log_index,
                    prev_log_term,
                    entries: Vec::new(),
                    leader_commit: self.commit_index,
                }),
            });
        }
    }

    // ---- proposals -------------------------------------------------------

    fn propose(&mut self, command: Vec<u8>) -> Result<Vec<Action>> {
        if self.role != Role::Leader {
            return Err(Error::NotLeader {
                leader: self.leader_id,
            });
        }
        let index = self.log.last_index() + 1;
        let entry = LogEntry::new(self.current_term, index, command);
        self.log.append(core::slice::from_ref(&entry))?;
        self.log.sync()?;

        let mut actions = Vec::new();
        // v0.3 replicates the entry to peers here. For now, a single-node leader
        // commits it at once; a multi-node leader holds it until replication.
        self.advance_commit_as_leader(&mut actions);
        Ok(actions)
    }

    /// Advances the commit index using what the leader knows.
    ///
    /// At `v0.2` the leader tracks only its own log, so this commits entries
    /// only when one node is a majority — that is, a single-node cluster. With
    /// peers present, the commit index moves once replication (v0.3) reports a
    /// quorum has the entry. Either way Raft's rule holds: only an entry from
    /// the current term is committed by counting replicas.
    fn advance_commit_as_leader(&mut self, actions: &mut Vec<Action>) {
        let last = self.log.last_index();
        // Replicas that hold `last`: just this leader for now (1).
        let replicas_with_last = 1;
        if replicas_with_last >= self.quorum
            && last > self.commit_index
            && self.log.term_at(last) == Some(self.current_term)
        {
            self.commit_index = last;
            self.drain_applies(actions);
        }
    }

    fn drain_applies(&mut self, actions: &mut Vec<Action>) {
        while self.last_applied < self.commit_index {
            self.last_applied += 1;
            if let Some(entry) = self.log.entry(self.last_applied) {
                actions.push(Action::Apply {
                    index: entry.index,
                    term: entry.term,
                    command: entry.command,
                });
            }
        }
    }

    // ---- message handling ------------------------------------------------

    fn handle_message(&mut self, message: Message) -> Result<Vec<Action>> {
        // Any message from a later term forces a step-down and term adoption,
        // before the message is interpreted in its own right.
        if message.term() > self.current_term {
            self.become_follower(message.term(), None)?;
        }

        let mut actions = Vec::new();
        match message {
            Message::RequestVote(rv) => self.handle_request_vote(rv, &mut actions)?,
            Message::RequestVoteReply(reply) => self.handle_vote_reply(reply, &mut actions),
            Message::AppendEntries(ae) => self.handle_append_entries(ae, &mut actions)?,
            Message::AppendEntriesReply(reply) => self.handle_append_reply(reply),
        }
        Ok(actions)
    }

    fn become_follower(&mut self, term: Term, leader: Option<NodeId>) -> Result<()> {
        let hard_state_changed = term > self.current_term;
        self.role = Role::Follower;
        if term > self.current_term {
            self.current_term = term;
            self.voted_for = None;
        }
        self.leader_id = leader;
        self.votes.clear();
        if hard_state_changed {
            self.persist_hard_state()?;
        }
        Ok(())
    }

    fn handle_request_vote(&mut self, rv: RequestVote, actions: &mut Vec<Action>) -> Result<()> {
        let mut granted = false;
        if rv.term >= self.current_term {
            let can_vote = match self.voted_for {
                None => true,
                Some(c) => c == rv.candidate,
            };
            let log_ok = self.candidate_log_up_to_date(rv.last_log_term, rv.last_log_index);
            if can_vote && log_ok {
                granted = true;
                self.voted_for = Some(rv.candidate);
                self.persist_hard_state()?;
                self.reset_election_timer();
            }
        }
        actions.push(Action::Send {
            to: rv.candidate,
            message: Message::RequestVoteReply(RequestVoteReply {
                term: self.current_term,
                vote_granted: granted,
                from: self.id,
            }),
        });
        Ok(())
    }

    /// The election restriction: a candidate's log must be at least as
    /// up to date as ours — a later last term wins, or an equal last term with
    /// at least as high an index.
    fn candidate_log_up_to_date(&self, cand_last_term: Term, cand_last_index: Index) -> bool {
        let my_term = self.log.last_term();
        let my_index = self.log.last_index();
        cand_last_term > my_term || (cand_last_term == my_term && cand_last_index >= my_index)
    }

    fn handle_vote_reply(&mut self, reply: RequestVoteReply, actions: &mut Vec<Action>) {
        if self.role != Role::Candidate || reply.term != self.current_term {
            return;
        }
        if reply.vote_granted && !self.votes.contains(&reply.from) {
            self.votes.push(reply.from);
            if self.votes.len() >= self.quorum {
                self.become_leader(actions);
            }
        }
    }

    fn handle_append_entries(
        &mut self,
        ae: AppendEntries,
        actions: &mut Vec<Action>,
    ) -> Result<()> {
        let mut success = false;
        let mut match_index = 0;

        if ae.term >= self.current_term {
            // A valid leader for our term: accept its authority.
            self.role = Role::Follower;
            self.leader_id = Some(ae.leader);
            self.reset_election_timer();

            let prev_ok = ae.prev_log_index == 0
                || self.log.term_at(ae.prev_log_index) == Some(ae.prev_log_term);
            if prev_ok {
                success = true;
                // v0.2 heartbeats carry no entries; v0.3 appends them here.
                match_index = ae.prev_log_index;
                if ae.leader_commit > self.commit_index {
                    self.commit_index = ae.leader_commit.min(self.log.last_index());
                    self.drain_applies(actions);
                }
            }
        }

        actions.push(Action::Send {
            to: ae.leader,
            message: Message::AppendEntriesReply(AppendEntriesReply {
                term: self.current_term,
                success,
                from: self.id,
                match_index,
            }),
        });
        Ok(())
    }

    /// Handles a follower's reply to a heartbeat or append.
    ///
    /// A higher term has already stepped the leader down in
    /// [`handle_message`](Self::handle_message). Tracking each follower's
    /// `match_index` and advancing the commit index on a quorum is the
    /// replication work of `v0.3`; at `v0.2` there is nothing further to do.
    fn handle_append_reply(&mut self, _reply: AppendEntriesReply) {}

    // ---- shared helpers --------------------------------------------------

    fn persist_hard_state(&mut self) -> Result<()> {
        self.log.set_hard_state(HardState {
            term: self.current_term,
            voted_for: self.voted_for,
        })?;
        self.log.sync()
    }

    fn reset_election_timer(&mut self) {
        self.election_elapsed = 0;
        self.election_timeout = self
            .rng
            .gen_range(self.election_timeout_min, self.election_timeout_max);
    }
}

#[cfg(test)]
mod tests {
    // Test setup uses unwrap/expect where a failure cannot be meaningfully
    // handled and should fail the test loudly. REPS permits this in test code.
    #![allow(clippy::unwrap_used, clippy::expect_used)]

    use super::*;

    fn drive_to_leader(node: &mut RaftNode) {
        for _ in 0..1_000 {
            if node.is_leader() {
                return;
            }
            let _ = node.step(Event::Tick).expect("tick");
        }
        panic!("node never became leader");
    }

    #[test]
    fn test_single_node_elects_itself() {
        let mut node = RaftNode::new(RaftConfig::single(1));
        drive_to_leader(&mut node);
        assert_eq!(node.role(), Role::Leader);
        assert_eq!(node.leader(), Some(1));
        assert_eq!(node.term(), 1);
    }

    #[test]
    fn test_single_node_commits_proposal() {
        let mut node = RaftNode::new(RaftConfig::single(1));
        drive_to_leader(&mut node);
        let actions = node.step(Event::Propose(b"a".to_vec())).unwrap();
        assert_eq!(node.commit_index(), 1);
        assert_eq!(node.last_applied(), 1);
        let applied: Vec<_> = actions
            .iter()
            .filter(|a| matches!(a, Action::Apply { .. }))
            .collect();
        assert_eq!(applied.len(), 1);
    }

    #[test]
    fn test_propose_to_follower_is_rejected() {
        let mut node = RaftNode::new(RaftConfig::new(1, [2, 3]));
        let err = node.step(Event::Propose(b"a".to_vec())).unwrap_err();
        assert!(matches!(err, Error::NotLeader { .. }));
    }

    #[test]
    fn test_candidate_requests_votes_from_peers() {
        let mut node = RaftNode::new(RaftConfig::new(1, [2, 3]));
        let mut sends = Vec::new();
        for _ in 0..1_000 {
            let actions = node.step(Event::Tick).unwrap();
            if !actions.is_empty() {
                sends = actions;
                break;
            }
        }
        assert_eq!(node.role(), Role::Candidate);
        let targets: Vec<NodeId> = sends
            .iter()
            .filter_map(|a| match a {
                Action::Send {
                    to,
                    message: Message::RequestVote(_),
                } => Some(*to),
                _ => None,
            })
            .collect();
        assert_eq!(targets.len(), 2);
        assert!(targets.contains(&2) && targets.contains(&3));
    }

    #[test]
    fn test_node_grants_one_vote_then_refuses_another_candidate() {
        let mut node = RaftNode::new(RaftConfig::new(1, [2, 3]));
        let grant = |node: &mut RaftNode, candidate: NodeId| -> bool {
            let actions = node
                .step(Event::Message(Message::RequestVote(RequestVote {
                    term: 5,
                    candidate,
                    last_log_index: 0,
                    last_log_term: 0,
                })))
                .unwrap();
            actions.iter().any(|a| {
                matches!(
                    a,
                    Action::Send { message: Message::RequestVoteReply(r), .. } if r.vote_granted
                )
            })
        };
        assert!(grant(&mut node, 2));
        assert!(!grant(&mut node, 3)); // already voted for 2 in term 5
    }

    #[test]
    fn test_higher_term_message_steps_node_down() {
        let mut node = RaftNode::new(RaftConfig::single(1));
        drive_to_leader(&mut node);
        let leader_term = node.term();
        let _ = node
            .step(Event::Message(Message::AppendEntries(AppendEntries {
                term: leader_term + 5,
                leader: 9,
                prev_log_index: 0,
                prev_log_term: 0,
                entries: Vec::new(),
                leader_commit: 0,
            })))
            .unwrap();
        assert_eq!(node.role(), Role::Follower);
        assert_eq!(node.term(), leader_term + 5);
        assert_eq!(node.leader(), Some(9));
    }

    #[test]
    fn test_stale_term_request_vote_is_refused() {
        let mut node = RaftNode::new(RaftConfig::single(1));
        drive_to_leader(&mut node); // now in term 1+
        let term = node.term();
        let actions = node
            .step(Event::Message(Message::RequestVote(RequestVote {
                term: term - 1,
                candidate: 2,
                last_log_index: 99,
                last_log_term: 99,
            })))
            .unwrap();
        let granted = actions.iter().any(|a| {
            matches!(
                a,
                Action::Send { message: Message::RequestVoteReply(r), .. } if r.vote_granted
            )
        });
        assert!(!granted);
    }

    #[test]
    fn test_heartbeat_resets_follower_election_timer() {
        let mut node = RaftNode::new(RaftConfig::new(1, [2, 3]).with_election_timeout(5, 5));
        // Tick a few times, then a heartbeat should keep it a follower.
        let _ = node.step(Event::Tick).unwrap();
        let _ = node.step(Event::Tick).unwrap();
        let _ = node
            .step(Event::Message(Message::AppendEntries(AppendEntries {
                term: 1,
                leader: 2,
                prev_log_index: 0,
                prev_log_term: 0,
                entries: Vec::new(),
                leader_commit: 0,
            })))
            .unwrap();
        assert_eq!(node.role(), Role::Follower);
        assert_eq!(node.leader(), Some(2));
        // It needs the full timeout again from zero; a single tick must not trip it.
        let _ = node.step(Event::Tick).unwrap();
        assert_eq!(node.role(), Role::Follower);
    }

    /// Elects node 1 leader of a `{1,2,3}` cluster by triggering its election
    /// and feeding it a granting vote from node 2 (self + 1 = quorum of 2).
    fn elect_multi_node_leader() -> RaftNode {
        let mut node = RaftNode::new(RaftConfig::new(1, [2, 3]).with_heartbeat_interval(2));
        for _ in 0..1_000 {
            let actions = node.step(Event::Tick).expect("tick");
            if !actions.is_empty() {
                break; // became a candidate and sent RequestVotes
            }
        }
        assert_eq!(node.role(), Role::Candidate);
        let term = node.term();
        let _ = node
            .step(Event::Message(Message::RequestVoteReply(
                RequestVoteReply {
                    term,
                    vote_granted: true,
                    from: 2,
                },
            )))
            .expect("vote reply");
        assert_eq!(node.role(), Role::Leader);
        node
    }

    #[test]
    fn test_vote_replies_elect_a_multi_node_leader() {
        let node = elect_multi_node_leader();
        assert_eq!(node.leader(), Some(1));
    }

    #[test]
    fn test_leader_emits_heartbeats_on_interval() {
        let mut node = elect_multi_node_leader();
        // First post-election tick: no heartbeat yet (interval 2).
        let first = node.step(Event::Tick).unwrap();
        assert!(first.is_empty());
        let second = node.step(Event::Tick).unwrap();
        let heartbeats = second
            .iter()
            .filter(|a| {
                matches!(
                    a,
                    Action::Send {
                        message: Message::AppendEntries(_),
                        ..
                    }
                )
            })
            .count();
        assert_eq!(heartbeats, 2);
    }

    #[test]
    fn test_persisted_hard_state_is_restored() {
        let mut log = MemoryLog::new();
        log.set_hard_state(HardState {
            term: 7,
            voted_for: Some(3),
        })
        .unwrap();
        let node = RaftNode::with_log(RaftConfig::single(1), log);
        assert_eq!(node.term(), 7);
    }

    #[test]
    fn test_vote_is_persisted_to_log() {
        let mut node = RaftNode::new(RaftConfig::new(1, [2, 3]));
        let _ = node
            .step(Event::Message(Message::RequestVote(RequestVote {
                term: 4,
                candidate: 2,
                last_log_index: 0,
                last_log_term: 0,
            })))
            .unwrap();
        assert_eq!(
            node.log().hard_state(),
            HardState {
                term: 4,
                voted_for: Some(2)
            }
        );
    }
}
