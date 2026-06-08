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
//! # Scope at v0.3
//!
//! This release implements the full replication pipeline on top of v0.2's
//! election layer: `AppendEntries` carries entries in bounded batches, the
//! leader tracks each follower's progress (probing for the match point, then
//! streaming with optimistic pipelining), rejections backtrack a whole term at a
//! time via a conflict hint, and the commit index advances once a quorum stores
//! an entry of the current term. Durable persistence (`wal-db`) is `v0.4` and
//! snapshots are `v0.5`.
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

/// How a leader is replicating to one follower.
///
/// A leader does not know where a new follower's log diverges from its own, so
/// it starts in `Probe`: it sends conservatively and waits for each reply,
/// backtracking on rejection until an append is accepted. Once the match point
/// is found it switches to `Replicate` and streams entries, advancing
/// optimistically without waiting — the pipelining that gives steady-state
/// throughput.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum ProgressState {
    Probe,
    Replicate,
}

/// The leader's view of one follower's replication progress.
#[derive(Clone, Copy, Debug)]
struct Progress {
    /// Index of the next entry to send this follower.
    next_index: Index,
    /// Highest index known to be replicated on this follower.
    match_index: Index,
    /// Whether we are still probing for the match point or streaming.
    state: ProgressState,
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
    max_batch: usize,

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
    /// Per-peer replication progress, aligned with `peers`. Non-empty only while
    /// this node is the leader.
    progress: Vec<Progress>,
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
            max_batch: config.max_batch,
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
            progress: Vec::new(),
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
                    self.replicate_to_all(&mut actions);
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
        self.progress.clear();
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
        // Initialise per-peer progress: optimistically assume each follower is
        // caught up (next = last + 1) and probe to find where it actually is.
        let next = self.log.last_index() + 1;
        self.progress = self
            .peers
            .iter()
            .map(|_| Progress {
                next_index: next,
                match_index: 0,
                state: ProgressState::Probe,
            })
            .collect();
        // Assert authority at once with an initial round of appends, and
        // (single-node) commit anything outstanding from the current term.
        self.replicate_to_all(actions);
        self.advance_commit(actions);
    }

    /// Sends an `AppendEntries` to every peer. On a heartbeat tick this both
    /// asserts leadership (empty append to caught-up followers) and probes or
    /// streams to those behind.
    fn replicate_to_all(&mut self, actions: &mut Vec<Action>) {
        for i in 0..self.peers.len() {
            self.send_append(i, actions);
        }
    }

    /// Streams freshly appended entries to peers already in `Replicate` state.
    /// Probing peers are driven by replies and heartbeats instead, so a busy
    /// proposer does not flood a lagging follower with redundant probes.
    fn replicate_to_streaming(&mut self, actions: &mut Vec<Action>) {
        for i in 0..self.peers.len() {
            if self.progress[i].state == ProgressState::Replicate {
                self.send_append(i, actions);
            }
        }
    }

    /// Builds and emits one `AppendEntries` for peer index `i`, carrying up to
    /// `max_batch` entries from that peer's `next_index`. In `Replicate` state a
    /// non-empty send advances `next_index` optimistically so the next batch can
    /// follow without waiting for the reply (pipelining).
    fn send_append(&mut self, i: usize, actions: &mut Vec<Action>) {
        let peer = self.peers[i];
        let next = self.progress[i].next_index;
        let state = self.progress[i].state;
        let prev_log_index = next - 1;
        let prev_log_term = self.log.term_at(prev_log_index).unwrap_or(0);

        let last = self.log.last_index();
        let entries = if last >= next {
            let to = last.min(next + self.max_batch as Index - 1);
            self.log.entries(next, to)
        } else {
            Vec::new()
        };
        let count = entries.len() as Index;

        actions.push(Action::Send {
            to: peer,
            message: Message::AppendEntries(AppendEntries {
                term: self.current_term,
                leader: self.id,
                prev_log_index,
                prev_log_term,
                entries,
                leader_commit: self.commit_index,
            }),
        });

        if count > 0 && state == ProgressState::Replicate {
            self.progress[i].next_index = next + count;
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
        // Stream the new entry to followers that are caught up; commit at once if
        // a quorum already holds it (the single-node case).
        self.replicate_to_streaming(&mut actions);
        self.advance_commit(&mut actions);
        Ok(actions)
    }

    /// Advances the commit index to the highest entry a quorum has stored.
    ///
    /// Counts, for each candidate index `n`, the leader plus every follower
    /// whose `match_index` reaches `n`. Raft's safety rule (§5.4.2) is enforced
    /// strictly: an entry is committed by counting replicas **only if it was
    /// created in the current term**. Older-term entries ride along once a
    /// current-term entry above them commits. A single-node cluster commits its
    /// own current-term tail immediately (quorum of one).
    fn advance_commit(&mut self, actions: &mut Vec<Action>) {
        let last = self.log.last_index();
        let mut new_commit = self.commit_index;
        let mut n = last;
        while n > self.commit_index {
            match self.log.term_at(n) {
                Some(term) if term == self.current_term => {
                    let mut replicas = 1; // the leader holds it
                    for p in &self.progress {
                        if p.match_index >= n {
                            replicas += 1;
                        }
                    }
                    if replicas >= self.quorum {
                        new_commit = n;
                        break; // highest such index found
                    }
                }
                // Terms never decrease down the log; once we pass below the
                // current term there is no current-term entry left to commit.
                Some(term) if term < self.current_term => break,
                _ => {}
            }
            n -= 1;
        }
        if new_commit > self.commit_index {
            self.commit_index = new_commit;
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
            Message::AppendEntriesReply(reply) => self.handle_append_reply(reply, &mut actions),
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
        self.progress.clear();
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
        let mut reply = AppendEntriesReply {
            term: self.current_term,
            success: false,
            from: self.id,
            match_index: 0,
            conflict_index: 0,
            conflict_term: 0,
        };

        // Reject a stale leader outright, telling it our (higher) term.
        if ae.term < self.current_term {
            actions.push(Action::Send {
                to: ae.leader,
                message: Message::AppendEntriesReply(reply),
            });
            return Ok(());
        }

        // A valid leader for our term: accept its authority and reset the timer.
        self.role = Role::Follower;
        self.leader_id = Some(ae.leader);
        self.reset_election_timer();

        // Log-consistency check at prev_log_index.
        let prev_ok =
            ae.prev_log_index == 0 || self.log.term_at(ae.prev_log_index) == Some(ae.prev_log_term);
        if !prev_ok {
            // Supply a conflict hint so the leader can skip back a whole term.
            let last = self.log.last_index();
            if ae.prev_log_index > last {
                reply.conflict_index = last + 1;
                reply.conflict_term = 0;
            } else {
                let conflict_term = self.log.term_at(ae.prev_log_index).unwrap_or(0);
                reply.conflict_term = conflict_term;
                reply.conflict_index = self.first_index_of_term(conflict_term, ae.prev_log_index);
            }
            actions.push(Action::Send {
                to: ae.leader,
                message: Message::AppendEntriesReply(reply),
            });
            return Ok(());
        }

        // The logs match up to prev_log_index. Append the new entries, resolving
        // any divergent tail, and report how far we now agree.
        let match_index = if ae.entries.is_empty() {
            ae.prev_log_index
        } else {
            self.append_from_leader(&ae.entries)?
        };

        if ae.leader_commit > self.commit_index {
            // Never commit past the last entry this RPC actually covers.
            self.commit_index = ae.leader_commit.min(match_index);
            self.drain_applies(actions);
        }

        reply.success = true;
        reply.match_index = match_index;
        actions.push(Action::Send {
            to: ae.leader,
            message: Message::AppendEntriesReply(reply),
        });
        Ok(())
    }

    /// Reconciles the leader's entries into the follower's log.
    ///
    /// Skips a prefix that already matches (same index and term), truncates the
    /// first divergent entry and everything after it, then appends the rest. The
    /// protocol guarantees a leader never sends entries that conflict below the
    /// commit index, so this never discards committed state. Returns the index
    /// of the last entry now stored from this batch.
    fn append_from_leader(&mut self, entries: &[LogEntry]) -> Result<Index> {
        let mut i = 0;
        while i < entries.len() {
            let entry = &entries[i];
            match self.log.term_at(entry.index) {
                Some(term) if term == entry.term => i += 1,
                Some(_) => {
                    // Divergence: drop the conflicting tail and stop scanning.
                    self.log.truncate(entry.index)?;
                    break;
                }
                None => break, // beyond our log; append from here on
            }
        }
        if i < entries.len() {
            self.log.append(&entries[i..])?;
            self.log.sync()?;
        }
        Ok(entries[entries.len() - 1].index)
    }

    fn handle_append_reply(&mut self, reply: AppendEntriesReply, actions: &mut Vec<Action>) {
        if self.role != Role::Leader || reply.term != self.current_term {
            return; // not leader, or a stale reply from another term
        }
        let Some(i) = self.peer_index(reply.from) else {
            return;
        };

        if reply.success {
            // match_index only ever advances, tolerating reordered duplicates.
            if reply.match_index > self.progress[i].match_index {
                self.progress[i].match_index = reply.match_index;
            }
            let want_next = self.progress[i].match_index + 1;
            if want_next > self.progress[i].next_index {
                self.progress[i].next_index = want_next;
            }
            self.progress[i].state = ProgressState::Replicate;
            self.advance_commit(actions);
            // Pipeline: if the follower is still behind, send the next batch now.
            if self.progress[i].next_index <= self.log.last_index() {
                self.send_append(i, actions);
            }
        } else {
            // Rejected: backtrack next_index using the follower's conflict hint,
            // drop to Probe, and retry at once.
            let next = self.progress[i].next_index;
            let matched = self.progress[i].match_index;
            self.progress[i].next_index =
                self.rejected_next(next, matched, reply.conflict_index, reply.conflict_term);
            self.progress[i].state = ProgressState::Probe;
            self.send_append(i, actions);
        }
    }

    /// Computes the `next_index` to retry after a rejection, using the conflict
    /// hint. Prefers to jump just past the leader's last entry of the conflict
    /// term; otherwise falls back to the follower's suggested index. The result
    /// never rises (a rejection only backtracks) and never drops at or below the
    /// confirmed `match_index`, which bounds probing and guarantees it converges.
    fn rejected_next(
        &self,
        current_next: Index,
        match_index: Index,
        conflict_index: Index,
        conflict_term: Term,
    ) -> Index {
        let floor = match_index + 1;
        let mut target = conflict_index.max(1);
        if conflict_term > 0 {
            if let Some(last) = self.last_index_of_term(conflict_term) {
                target = last + 1;
            }
        }
        let ceil = current_next.saturating_sub(1).max(floor);
        target.clamp(floor, ceil)
    }

    /// First index of the contiguous run of `term` ending at `upto`.
    fn first_index_of_term(&self, term: Term, upto: Index) -> Index {
        let mut i = upto;
        while i > 1 && self.log.term_at(i - 1) == Some(term) {
            i -= 1;
        }
        i
    }

    /// Highest index in the leader's log whose entry has `term`, if any. Relies
    /// on terms being non-decreasing down the log to stop early.
    fn last_index_of_term(&self, term: Term) -> Option<Index> {
        let mut i = self.log.last_index();
        while i >= 1 {
            match self.log.term_at(i) {
                Some(t) if t == term => return Some(i),
                Some(t) if t < term => return None,
                _ => {}
            }
            i -= 1;
        }
        None
    }

    fn peer_index(&self, id: NodeId) -> Option<usize> {
        self.peers.iter().position(|&p| p == id)
    }

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

    // ---- v0.3 replication --------------------------------------------------

    fn entry(term: Term, index: Index) -> LogEntry {
        LogEntry::new(term, index, vec![index as u8])
    }

    fn first_append_entries(actions: &[Action], to: NodeId) -> AppendEntries {
        actions
            .iter()
            .find_map(|a| match a {
                Action::Send {
                    to: dst,
                    message: Message::AppendEntries(ae),
                } if *dst == to => Some(ae.clone()),
                _ => None,
            })
            .expect("an AppendEntries to the peer")
    }

    /// Walks a `{1,2,3}` leader through replicating a proposal to follower 2 and
    /// confirms commit lands once a quorum (leader + one follower) holds it.
    #[test]
    fn test_leader_replicates_and_commits_on_quorum() {
        let mut node = elect_multi_node_leader();
        // Bring follower 2 into Replicate state with an accepted heartbeat.
        let _ = node
            .step(Event::Message(Message::AppendEntriesReply(
                AppendEntriesReply {
                    term: node.term(),
                    success: true,
                    from: 2,
                    match_index: 0,
                    conflict_index: 0,
                    conflict_term: 0,
                },
            )))
            .unwrap();

        // Propose: the entry streams to follower 2 but is not yet committed.
        let actions = node.step(Event::Propose(b"x".to_vec())).unwrap();
        assert_eq!(node.commit_index(), 0);
        let ae = first_append_entries(&actions, 2);
        assert_eq!(ae.entries.len(), 1);
        assert_eq!(ae.entries[0].index, 1);

        // Follower 2 acknowledges index 1: quorum reached, entry commits/applies.
        let applied = node
            .step(Event::Message(Message::AppendEntriesReply(
                AppendEntriesReply {
                    term: node.term(),
                    success: true,
                    from: 2,
                    match_index: 1,
                    conflict_index: 0,
                    conflict_term: 0,
                },
            )))
            .unwrap();
        assert_eq!(node.commit_index(), 1);
        assert!(
            applied
                .iter()
                .any(|a| matches!(a, Action::Apply { index: 1, .. }))
        );
    }

    #[test]
    fn test_follower_appends_streamed_entries() {
        let mut node = RaftNode::new(RaftConfig::new(5, [1]));
        let actions = node
            .step(Event::Message(Message::AppendEntries(AppendEntries {
                term: 2,
                leader: 1,
                prev_log_index: 0,
                prev_log_term: 0,
                entries: vec![entry(2, 1), entry(2, 2)],
                leader_commit: 2,
            })))
            .unwrap();
        assert_eq!(node.log().last_index(), 2);
        assert_eq!(node.commit_index(), 2);
        let reply = actions
            .iter()
            .find_map(|a| match a {
                Action::Send {
                    message: Message::AppendEntriesReply(r),
                    ..
                } => Some(r.clone()),
                _ => None,
            })
            .expect("a reply");
        assert!(reply.success);
        assert_eq!(reply.match_index, 2);
    }

    #[test]
    fn test_follower_truncates_divergent_tail() {
        // Follower already holds [t1@1, t2@2]; leader overwrites index 2 with t3.
        let mut log = MemoryLog::new();
        log.append(&[entry(1, 1), entry(2, 2)]).unwrap();
        let mut node = RaftNode::with_log(RaftConfig::new(5, [1]), log);

        let actions = node
            .step(Event::Message(Message::AppendEntries(AppendEntries {
                term: 3,
                leader: 1,
                prev_log_index: 1,
                prev_log_term: 1,
                entries: vec![entry(3, 2)],
                leader_commit: 0,
            })))
            .unwrap();
        assert_eq!(node.log().last_index(), 2);
        assert_eq!(node.log().entry(2).unwrap().term, 3);
        let reply = first_reply(&actions);
        assert!(reply.success);
        assert_eq!(reply.match_index, 2);
    }

    #[test]
    fn test_follower_rejects_short_log_with_length_hint() {
        let mut node = RaftNode::new(RaftConfig::new(5, [1]));
        let actions = node
            .step(Event::Message(Message::AppendEntries(AppendEntries {
                term: 2,
                leader: 1,
                prev_log_index: 3,
                prev_log_term: 1,
                entries: vec![entry(2, 4)],
                leader_commit: 0,
            })))
            .unwrap();
        let reply = first_reply(&actions);
        assert!(!reply.success);
        assert_eq!(reply.conflict_index, 1); // empty log => probe from index 1
        assert_eq!(reply.conflict_term, 0);
    }

    #[test]
    fn test_follower_rejects_term_mismatch_with_term_hint() {
        // Follower holds three term-1 entries; leader probes with a wrong term.
        let mut log = MemoryLog::new();
        log.append(&[entry(1, 1), entry(1, 2), entry(1, 3)])
            .unwrap();
        let mut node = RaftNode::with_log(RaftConfig::new(5, [1]), log);

        let actions = node
            .step(Event::Message(Message::AppendEntries(AppendEntries {
                term: 5,
                leader: 1,
                prev_log_index: 3,
                prev_log_term: 4, // follower has term 1 there
                entries: Vec::new(),
                leader_commit: 0,
            })))
            .unwrap();
        let reply = first_reply(&actions);
        assert!(!reply.success);
        assert_eq!(reply.conflict_term, 1);
        assert_eq!(reply.conflict_index, 1); // first index of the term-1 run
    }

    #[test]
    fn test_rejection_backtracks_then_converges() {
        // Leader 1 has [t1@1, t1@2, t1@3] and a fresh follower 2 that is empty.
        let mut log = MemoryLog::new();
        log.append(&[entry(1, 1), entry(1, 2), entry(1, 3)])
            .unwrap();
        log.set_hard_state(HardState {
            term: 1,
            voted_for: Some(1),
        })
        .unwrap();
        let mut leader =
            RaftNode::with_log(RaftConfig::new(1, [2]).with_election_timeout(5, 5), log);
        let mut follower = RaftNode::new(RaftConfig::new(2, [1]));

        // Elect leader 1 (2-node quorum is 2; feed a granting vote from 2).
        let mut pending = Vec::new();
        for _ in 0..50 {
            let acts = leader.step(Event::Tick).unwrap();
            if !acts.is_empty() {
                pending = acts;
                break;
            }
        }
        // The candidate's term is now 2; grant it.
        let _ = leader
            .step(Event::Message(Message::RequestVoteReply(
                RequestVoteReply {
                    term: leader.term(),
                    vote_granted: true,
                    from: 2,
                },
            )))
            .unwrap();
        assert!(leader.is_leader());
        let _ = pending;

        // Pump messages between the two until the follower catches up.
        let mut queue: Vec<(NodeId, Message)> = drain_sends(&mut leader);
        for _ in 0..100 {
            if follower.log().last_index() == 3 {
                break;
            }
            let mut next = Vec::new();
            for (to, msg) in queue.drain(..) {
                let acts = if to == 2 {
                    follower.step(Event::Message(msg)).unwrap()
                } else {
                    leader.step(Event::Message(msg)).unwrap()
                };
                next.extend(collect_sends(acts));
            }
            queue = next;
            if queue.is_empty() {
                queue = leader
                    .step(Event::Tick)
                    .unwrap()
                    .into_iter()
                    .filter_map(send_pair)
                    .collect();
            }
        }
        assert_eq!(follower.log().last_index(), 3);
        assert_eq!(follower.log().entry(3).unwrap().term, 1);
    }

    fn first_reply(actions: &[Action]) -> AppendEntriesReply {
        actions
            .iter()
            .find_map(|a| match a {
                Action::Send {
                    message: Message::AppendEntriesReply(r),
                    ..
                } => Some(r.clone()),
                _ => None,
            })
            .expect("an AppendEntriesReply")
    }

    fn send_pair(a: Action) -> Option<(NodeId, Message)> {
        match a {
            Action::Send { to, message } => Some((to, message)),
            _ => None,
        }
    }

    fn collect_sends(actions: Vec<Action>) -> Vec<(NodeId, Message)> {
        actions.into_iter().filter_map(send_pair).collect()
    }

    fn drain_sends(node: &mut RaftNode) -> Vec<(NodeId, Message)> {
        let acts = node.step(Event::Tick).unwrap();
        collect_sends(acts)
    }

    // ---- durability contract ----------------------------------------------

    /// A [`RaftLog`] wrapper that counts [`sync`](RaftLog::sync) calls, to prove
    /// the node makes state durable before it replies.
    #[derive(Default)]
    struct SyncCountLog {
        inner: MemoryLog,
        syncs: std::cell::Cell<u32>,
    }

    impl SyncCountLog {
        fn syncs(&self) -> u32 {
            self.syncs.get()
        }
    }

    impl RaftLog for SyncCountLog {
        fn last_index(&self) -> Index {
            self.inner.last_index()
        }
        fn last_term(&self) -> Term {
            self.inner.last_term()
        }
        fn term_at(&self, index: Index) -> Option<Term> {
            self.inner.term_at(index)
        }
        fn entry(&self, index: Index) -> Option<LogEntry> {
            self.inner.entry(index)
        }
        fn append(&mut self, entries: &[LogEntry]) -> Result<()> {
            self.inner.append(entries)
        }
        fn truncate(&mut self, from: Index) -> Result<()> {
            self.inner.truncate(from)
        }
        fn hard_state(&self) -> HardState {
            self.inner.hard_state()
        }
        fn set_hard_state(&mut self, state: HardState) -> Result<()> {
            self.inner.set_hard_state(state)
        }
        fn sync(&mut self) -> Result<()> {
            self.syncs.set(self.syncs.get() + 1);
            self.inner.sync()
        }
    }

    #[test]
    fn test_granting_a_vote_persists_and_syncs_before_replying() {
        let mut node = RaftNode::with_log(RaftConfig::new(1, [2, 3]), SyncCountLog::default());
        let actions = node
            .step(Event::Message(Message::RequestVote(RequestVote {
                term: 4,
                candidate: 2,
                last_log_index: 0,
                last_log_term: 0,
            })))
            .unwrap();
        // The grant was produced...
        assert!(actions.iter().any(|a| matches!(
            a,
            Action::Send { message: Message::RequestVoteReply(r), .. } if r.vote_granted
        )));
        // ...and the vote was durably synced as part of handling it.
        assert!(
            node.log().syncs() >= 1,
            "vote must be synced before the reply"
        );
        assert_eq!(node.log().hard_state().voted_for, Some(2));
    }

    #[test]
    fn test_rejected_vote_makes_no_durable_write() {
        // Node already at term 5 having voted; a stale lower-term request changes
        // nothing and must not force a sync.
        let mut log = SyncCountLog::default();
        log.set_hard_state(HardState {
            term: 5,
            voted_for: Some(9),
        })
        .unwrap();
        let mut node = RaftNode::with_log(RaftConfig::new(1, [2, 3]), log);
        let before = node.log().syncs();
        let _ = node
            .step(Event::Message(Message::RequestVote(RequestVote {
                term: 3, // stale
                candidate: 2,
                last_log_index: 0,
                last_log_term: 0,
            })))
            .unwrap();
        assert_eq!(node.log().syncs(), before, "a no-op vote must not sync");
    }
}
