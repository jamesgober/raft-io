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
//! # Scope at v0.5
//!
//! The protocol is feature-complete bar membership changes (`v0.6`): leader
//! election with term and vote safety, the full replication pipeline (batched
//! `AppendEntries`, per-follower progress with optimistic pipelining,
//! conflict-hint backtracking, commit on a quorum), durable persistence and
//! crash recovery (the `WalLog`), and **snapshots with log compaction** — a
//! policy hint drives the application to snapshot, the log compacts behind it,
//! and a follower too far behind to replicate is caught up with an
//! `InstallSnapshot`.
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
use crate::message::{
    AppendEntries, AppendEntriesReply, InstallSnapshot, InstallSnapshotReply, Message, PreVote,
    PreVoteReply, RequestVote, RequestVoteReply, TimeoutNow,
};
use crate::rng::Rng;
use crate::types::{HardState, Index, LogEntry, NodeId, Role, Snapshot, Term};

/// Collects node ids into a sorted, de-duplicated configuration vector, so two
/// nodes that agree on the membership store it in the same order.
fn sorted_members(ids: impl IntoIterator<Item = NodeId>) -> Vec<NodeId> {
    let mut v: Vec<NodeId> = ids.into_iter().collect();
    v.sort_unstable();
    v.dedup();
    v
}

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
///     term: 1, candidate: 2, last_log_index: 0, last_log_term: 0, force: false,
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
    /// The application supplies a snapshot of its state machine through `index`.
    ///
    /// This is the reply to an [`Action::Snapshot`] hint: the application has
    /// serialized its state up to `index` into `data`. The node compacts the log
    /// up to `index`. A snapshot for an uncommitted or stale index is ignored.
    Snapshot {
        /// The log index the snapshot covers (must be applied and committed).
        index: Index,
        /// The serialized state machine state.
        data: Vec<u8>,
    },
    /// Add a voting server to the cluster.
    ///
    /// Only the leader may reconfigure; elsewhere [`step`](RaftNode::step)
    /// returns [`Error::NotLeader`]. One change is processed at a time — a request
    /// made while a previous configuration change is still uncommitted returns
    /// [`Error::ConfigInProgress`]. Adding a server already present is a no-op.
    AddServer(NodeId),
    /// Remove a voting server from the cluster.
    ///
    /// Same rules as [`AddServer`](Event::AddServer). Removing the leader makes
    /// it step down once the change commits.
    RemoveServer(NodeId),
    /// Ask the leader to transfer leadership to `target`.
    ///
    /// The leader brings `target` fully up to date, then signals it to start an
    /// election immediately so it takes over with minimal disruption. A no-op on
    /// a non-leader or when `target` is not a voter.
    TransferLeadership(NodeId),
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
/// future versions may add variants, so a `match` must include a wildcard arm.
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
    /// Take a snapshot of the state machine through `index` and return it.
    ///
    /// A hint emitted when the log has grown past the configured snapshot
    /// threshold. The application serializes its state up to `index` and feeds it
    /// back with [`Event::Snapshot`], after which the node compacts the log.
    /// Acting on the hint is optional but unbounded growth follows from ignoring
    /// it.
    Snapshot {
        /// The applied index the snapshot should cover.
        index: Index,
        /// Term of the entry at `index`.
        term: Term,
    },
    /// Reset the state machine to an installed snapshot.
    ///
    /// Emitted on a follower that received a leader's snapshot because it had
    /// fallen too far behind to replicate entry by entry. The application
    /// replaces its state with `data` (which represents the state through
    /// `index`); subsequent [`Apply`](Action::Apply) actions resume from
    /// `index + 1`.
    RestoreSnapshot {
        /// The index the snapshot covers.
        index: Index,
        /// Term of the entry at `index`.
        term: Term,
        /// The serialized state to restore.
        data: Vec<u8>,
    },
    /// The cluster's voting membership changed.
    ///
    /// Emitted whenever the node adopts a new configuration (as a leader
    /// appending the change, or a follower receiving it). The application should
    /// update its transport so it can reach the new members and stop reaching
    /// removed ones. Membership takes effect immediately on this action, before
    /// the change commits.
    MembershipChanged {
        /// The new voting membership.
        members: Vec<NodeId>,
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
    /// The follower this progress tracks.
    id: NodeId,
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
    /// Current voting membership (includes this node when it is a voter). The
    /// quorum and election logic read from this; it changes as configuration
    /// entries are appended.
    voters: Vec<NodeId>,
    /// The configuration in effect at the snapshot base (or the bootstrap
    /// configuration when there is no snapshot). Entries below the base are gone,
    /// so this anchors configuration recovery.
    base_config: Vec<NodeId>,
    /// Index of the configuration entry currently in effect, or `0` when the
    /// configuration comes from `base_config` rather than a live log entry.
    config_index: Index,
    election_timeout_min: u32,
    election_timeout_max: u32,
    heartbeat_interval: u32,
    max_batch: usize,
    snapshot_threshold: usize,

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
    /// Whether a pre-vote round is outstanding. While set, the node has not yet
    /// incremented its term — it is only probing whether a real election could
    /// be won (Raft §9.6). It remains a [`Follower`](Role::Follower) throughout.
    pre_voting: bool,
    /// Distinct peers that have granted the current pre-vote round.
    pre_votes: Vec<NodeId>,
    /// Per-peer replication progress, aligned with `peers`. Non-empty only while
    /// this node is the leader.
    progress: Vec<Progress>,
    /// Highest index a snapshot hint has already been emitted for, so the policy
    /// fires at most once per threshold crossing.
    snapshot_hinted_at: Index,
    /// The target of an in-progress leadership transfer, if any. While set, the
    /// leader declines new proposals so the transfer can complete.
    transfer_target: Option<NodeId>,
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
        // A recovered snapshot covers committed, already-applied state: start
        // commit and applied at its boundary so those entries are not re-emitted.
        // The application restores its state machine from `log.snapshot()`.
        let base = log.snapshot_index();

        // Bootstrap configuration: the snapshot's recorded membership if it has
        // one, otherwise this node plus its configured peers.
        let bootstrap = sorted_members(config.peers.iter().copied().chain([config.id]));
        let base_config = match log.snapshot() {
            Some(s) if !s.config.is_empty() => s.config,
            _ => bootstrap,
        };
        // The effective configuration is the latest config entry in the live log,
        // or `base_config` if there is none.
        let mut voters = base_config.clone();
        let mut config_index = 0;
        let mut i = log.last_index();
        while i > base {
            if let Some(members) = log.entry(i).and_then(|e| e.members()) {
                voters = members;
                config_index = i;
                break;
            }
            i -= 1;
        }

        let mut rng = Rng::new(config.seed);
        let election_timeout =
            rng.gen_range(config.election_timeout_min, config.election_timeout_max);
        Self {
            id: config.id,
            voters,
            base_config,
            config_index,
            election_timeout_min: config.election_timeout_min,
            election_timeout_max: config.election_timeout_max,
            heartbeat_interval: config.heartbeat_interval,
            max_batch: config.max_batch,
            snapshot_threshold: config.snapshot_threshold,
            log,
            role: Role::Follower,
            current_term: hard.term,
            voted_for: hard.voted_for,
            leader_id: None,
            commit_index: base,
            last_applied: base,
            election_elapsed: 0,
            heartbeat_elapsed: 0,
            election_timeout,
            votes: Vec::new(),
            pre_voting: false,
            pre_votes: Vec::new(),
            progress: Vec::new(),
            snapshot_hinted_at: base,
            transfer_target: None,
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

    /// Returns the current voting membership of the cluster.
    ///
    /// This reflects the latest configuration the node has in its log, which it
    /// adopts as soon as the configuration entry is appended (before it commits).
    ///
    /// # Examples
    ///
    /// ```
    /// use raft_io::{RaftConfig, RaftNode};
    ///
    /// let node = RaftNode::new(RaftConfig::new(1, [2, 3]));
    /// assert_eq!(node.members(), &[1, 2, 3]);
    /// ```
    #[inline]
    #[must_use]
    pub fn members(&self) -> &[NodeId] {
        &self.voters
    }

    // ---- configuration ---------------------------------------------------

    /// The number of votes (or replicas) that form a majority of the current
    /// voting membership.
    #[inline]
    fn quorum(&self) -> usize {
        self.voters.len() / 2 + 1
    }

    /// Whether this node is a voting member of the current configuration.
    #[inline]
    fn is_voter(&self) -> bool {
        self.voters.contains(&self.id)
    }

    /// Adopts `voters` as the new configuration (established by the entry at
    /// `config_index`, or `0` for the base configuration). Rebuilds leader
    /// progress for the new peer set and emits [`Action::MembershipChanged`] if
    /// the membership actually changed.
    fn set_config(&mut self, voters: Vec<NodeId>, config_index: Index, actions: &mut Vec<Action>) {
        let changed = voters != self.voters;
        self.voters = voters;
        self.config_index = config_index;
        if self.role == Role::Leader {
            self.rebuild_progress();
        }
        if changed {
            actions.push(Action::MembershipChanged {
                members: self.voters.clone(),
            });
        }
    }

    /// Scans the live log for the latest configuration entry and adopts it (or
    /// the base configuration if there is none). Used after a truncation or a
    /// snapshot install, where the in-effect configuration may have moved.
    fn refresh_config(&mut self, actions: &mut Vec<Action>) {
        let base = self.log.snapshot_index();
        let mut voters = self.base_config.clone();
        let mut config_index = 0;
        let mut i = self.log.last_index();
        while i > base {
            if let Some(members) = self.log.entry(i).and_then(|e| e.members()) {
                voters = members;
                config_index = i;
                break;
            }
            i -= 1;
        }
        self.set_config(voters, config_index, actions);
    }

    /// Rebuilds leader replication progress for the current peer set, preserving
    /// the match/next/state of peers that remain.
    fn rebuild_progress(&mut self) {
        let next = self.log.last_index() + 1;
        let old = core::mem::take(&mut self.progress);
        self.progress = self
            .voters
            .iter()
            .filter(|&&id| id != self.id)
            .map(|&id| {
                old.iter()
                    .find(|p| p.id == id)
                    .copied()
                    .unwrap_or(Progress {
                        id,
                        next_index: next,
                        match_index: 0,
                        state: ProgressState::Probe,
                    })
            })
            .collect();
    }

    /// Returns the index of `id` in the leader progress table, if present.
    fn progress_index(&self, id: NodeId) -> Option<usize> {
        self.progress.iter().position(|p| p.id == id)
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
            Event::Snapshot { index, data } => self.handle_snapshot_event(index, data),
            Event::AddServer(id) => self.change_membership(Some(id), None),
            Event::RemoveServer(id) => self.change_membership(None, Some(id)),
            Event::TransferLeadership(target) => self.transfer_leadership(target),
        }
    }

    // ---- tick handling ---------------------------------------------------

    fn tick(&mut self) -> Result<Vec<Action>> {
        let mut actions = Vec::new();
        match self.role {
            Role::Follower | Role::Candidate => {
                self.election_elapsed += 1;
                // Only a voting member campaigns; a node not in the configuration
                // (for example, removed, or not yet caught up) follows quietly. A
                // timeout opens a pre-vote round rather than a real election, so a
                // partitioned node cannot inflate its term (Raft §9.6).
                if self.election_elapsed >= self.election_timeout && self.is_voter() {
                    self.start_pre_vote(&mut actions)?;
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

    /// Opens a pre-vote round (Raft §9.6): asks peers whether they *would* vote
    /// for this node at the next term, without incrementing the term or casting a
    /// vote. A real election begins only once a quorum of pre-votes is collected.
    ///
    /// This is the disruption guard. A node partitioned from the cluster never
    /// gathers a pre-vote majority, so its term never climbs; when it rejoins it
    /// does not force the sitting leader to step down. The node stays a
    /// [`Follower`](Role::Follower) for the duration of the round — it has not
    /// truly campaigned.
    fn start_pre_vote(&mut self, actions: &mut Vec<Action>) -> Result<()> {
        self.role = Role::Follower;
        self.leader_id = None;
        self.pre_voting = true;
        self.pre_votes.clear();
        self.pre_votes.push(self.id);
        self.reset_election_timer();

        // A single-node cluster (or any where one grant is a majority) needs no
        // probe: campaign for real at once.
        if self.pre_votes.len() >= self.quorum() {
            return self.start_election(false, actions);
        }

        let last_log_index = self.log.last_index();
        let last_log_term = self.log.last_term();
        let term = self.current_term + 1; // the hypothetical term, not adopted
        let id = self.id;
        for &peer in &self.voters {
            if peer == id {
                continue;
            }
            actions.push(Action::Send {
                to: peer,
                message: Message::PreVote(PreVote {
                    term,
                    candidate: id,
                    last_log_index,
                    last_log_term,
                }),
            });
        }
        Ok(())
    }

    fn start_election(&mut self, force: bool, actions: &mut Vec<Action>) -> Result<()> {
        self.role = Role::Candidate;
        self.current_term += 1;
        self.voted_for = Some(self.id);
        self.leader_id = None;
        self.transfer_target = None;
        self.pre_voting = false;
        self.pre_votes.clear();
        self.progress.clear();
        self.votes.clear();
        self.votes.push(self.id);
        self.reset_election_timer();
        self.persist_hard_state()?;

        // A single-node cluster (or any cluster where one vote is a majority)
        // wins immediately.
        if self.votes.len() >= self.quorum() {
            self.become_leader(actions);
            return Ok(());
        }

        let last_log_index = self.log.last_index();
        let last_log_term = self.log.last_term();
        let term = self.current_term;
        let id = self.id;
        for &peer in &self.voters {
            if peer == id {
                continue;
            }
            actions.push(Action::Send {
                to: peer,
                message: Message::RequestVote(RequestVote {
                    term,
                    candidate: id,
                    last_log_index,
                    last_log_term,
                    force,
                }),
            });
        }
        Ok(())
    }

    fn become_leader(&mut self, actions: &mut Vec<Action>) {
        self.role = Role::Leader;
        self.leader_id = Some(self.id);
        self.heartbeat_elapsed = 0;
        self.transfer_target = None;
        self.pre_voting = false;
        self.pre_votes.clear();
        // Initialise per-peer progress for the current configuration: each
        // follower is assumed caught up (next = last + 1) and probed to find
        // where it actually is.
        self.rebuild_progress();
        // Assert authority at once with an initial round of appends, and
        // (single-node) commit anything outstanding from the current term.
        self.replicate_to_all(actions);
        self.advance_commit(actions);
    }

    /// Sends an `AppendEntries` to every peer. On a heartbeat tick this both
    /// asserts leadership (empty append to caught-up followers) and probes or
    /// streams to those behind.
    fn replicate_to_all(&mut self, actions: &mut Vec<Action>) {
        for i in 0..self.progress.len() {
            self.send_append(i, actions);
        }
    }

    /// Streams freshly appended entries to peers already in `Replicate` state.
    /// Probing peers are driven by replies and heartbeats instead, so a busy
    /// proposer does not flood a lagging follower with redundant probes.
    fn replicate_to_streaming(&mut self, actions: &mut Vec<Action>) {
        for i in 0..self.progress.len() {
            if self.progress[i].state == ProgressState::Replicate {
                self.send_append(i, actions);
            }
        }
    }

    /// Builds and emits one `AppendEntries` for the peer at progress index `i`,
    /// carrying up to `max_batch` entries from that peer's `next_index`. In
    /// `Replicate` state a non-empty send advances `next_index` optimistically so
    /// the next batch can follow without waiting for the reply (pipelining).
    fn send_append(&mut self, i: usize, actions: &mut Vec<Action>) {
        let next = self.progress[i].next_index;
        // If the entry preceding `next` has been compacted away, the follower is
        // too far behind to replicate from the log — send the snapshot instead.
        if next <= self.log.snapshot_index() {
            self.send_snapshot(i, actions);
            return;
        }

        let peer = self.progress[i].id;
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

    /// Sends the current snapshot to peer index `i`. Used when the follower needs
    /// an entry the leader has already compacted away. Progress stays in `Probe`
    /// until the reply confirms the install, so it is not advanced here.
    fn send_snapshot(&mut self, i: usize, actions: &mut Vec<Action>) {
        if let Some(snapshot) = self.log.snapshot() {
            self.progress[i].state = ProgressState::Probe;
            actions.push(Action::Send {
                to: self.progress[i].id,
                message: Message::InstallSnapshot(InstallSnapshot {
                    term: self.current_term,
                    leader: self.id,
                    snapshot,
                }),
            });
        }
    }

    // ---- proposals -------------------------------------------------------

    fn propose(&mut self, command: Vec<u8>) -> Result<Vec<Action>> {
        if self.role != Role::Leader || self.transfer_target.is_some() {
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
        let quorum = self.quorum();
        // The leader counts toward a quorum only while it is itself a voter; a
        // leader being removed (no longer a voter) needs a majority of the
        // remaining members before its own removal commits.
        let leader_holds = usize::from(self.is_voter());
        let mut new_commit = self.commit_index;
        let mut n = last;
        while n > self.commit_index {
            match self.log.term_at(n) {
                Some(term) if term == self.current_term => {
                    let mut replicas = leader_holds;
                    for p in &self.progress {
                        if p.match_index >= n {
                            replicas += 1;
                        }
                    }
                    if replicas >= quorum {
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
            // A leader that has just committed its own removal steps down.
            if self.role == Role::Leader
                && !self.is_voter()
                && self.config_index != 0
                && self.commit_index >= self.config_index
            {
                self.step_down_to_follower();
            }
        }
    }

    /// Drops leadership without changing term, after committing a configuration
    /// that no longer includes this node.
    fn step_down_to_follower(&mut self) {
        self.role = Role::Follower;
        self.leader_id = None;
        self.transfer_target = None;
        self.pre_voting = false;
        self.pre_votes.clear();
        self.progress.clear();
        self.votes.clear();
    }

    fn drain_applies(&mut self, actions: &mut Vec<Action>) {
        while self.last_applied < self.commit_index {
            self.last_applied += 1;
            // Configuration entries are protocol bookkeeping, not application
            // commands — they take effect on append and are never applied to the
            // state machine. The applied index still advances over them.
            if let Some(entry) = self.log.entry(self.last_applied) {
                if entry.members().is_none() {
                    actions.push(Action::Apply {
                        index: entry.index,
                        term: entry.term,
                        command: entry.command,
                    });
                }
            }
        }
        self.maybe_hint_snapshot(actions);
    }

    /// Emits a snapshot hint once the applied log has grown past the configured
    /// threshold beyond the last snapshot. Fires at most once per crossing.
    fn maybe_hint_snapshot(&mut self, actions: &mut Vec<Action>) {
        if self.snapshot_threshold == 0 {
            return;
        }
        let base = self.log.snapshot_index();
        let grown = self.last_applied.saturating_sub(base) as usize;
        if grown >= self.snapshot_threshold && self.last_applied > self.snapshot_hinted_at {
            if let Some(term) = self.log.term_at(self.last_applied) {
                self.snapshot_hinted_at = self.last_applied;
                actions.push(Action::Snapshot {
                    index: self.last_applied,
                    term,
                });
            }
        }
    }

    // ---- snapshots -------------------------------------------------------

    /// Handles the application's snapshot of its state machine through `index`.
    ///
    /// Compacts the log up to `index` if the snapshot is valid: it must cover a
    /// committed, already-applied index that is newer than any existing snapshot,
    /// and the entry at `index` must still be present so its term is known. An
    /// out-of-range or stale snapshot is ignored rather than treated as an error.
    fn handle_snapshot_event(&mut self, index: Index, data: Vec<u8>) -> Result<Vec<Action>> {
        if index > self.commit_index
            || index > self.last_applied
            || index <= self.log.snapshot_index()
        {
            return Ok(Vec::new());
        }
        let Some(term) = self.log.term_at(index) else {
            return Ok(Vec::new());
        };
        // Record the configuration in effect at `index` so a node catching up
        // from this snapshot — its config entries compacted — still knows the
        // membership.
        let config = self.config_at(index);
        self.base_config = config.clone();
        self.log
            .apply_snapshot(&Snapshot::with_config(index, term, config, data))?;
        self.log.sync()?;
        if self.snapshot_hinted_at < index {
            self.snapshot_hinted_at = index;
        }
        let mut actions = Vec::new();
        self.refresh_config(&mut actions);
        Ok(actions)
    }

    /// Returns the voting membership in effect at `index`: the latest live
    /// configuration entry at or below `index`, or the base configuration.
    fn config_at(&self, index: Index) -> Vec<NodeId> {
        let base = self.log.snapshot_index();
        let mut i = index.min(self.log.last_index());
        while i > base {
            if let Some(members) = self.log.entry(i).and_then(|e| e.members()) {
                return members;
            }
            i -= 1;
        }
        self.base_config.clone()
    }

    // ---- message handling ------------------------------------------------

    fn handle_message(&mut self, message: Message) -> Result<Vec<Action>> {
        // Leader stickiness (Raft §4.2.3): ignore a `RequestVote` — not even
        // adopting its term — while a leader we recognise is still active (we
        // heard from it within the minimum election timeout). This stops a
        // removed or partitioned server, which never hears heartbeats and so
        // keeps timing out, from disrupting the cluster with ever-higher terms. A
        // candidate (no recognised leader) is unaffected.
        if matches!(message, Message::RequestVote(ref rv) if !rv.force)
            && self.leader_id.is_some()
            && self.election_elapsed < self.election_timeout_min
        {
            return Ok(Vec::new());
        }

        let mut actions = Vec::new();

        // Pre-vote messages are hypothetical: they carry a term the sender has not
        // adopted, and neither side changes any persistent state for them. Handle
        // and return before the generic higher-term step-down below — a pre-vote
        // must never inflate our term, which is the whole point of the mechanism.
        let message = match message {
            Message::PreVote(pv) => {
                self.handle_pre_vote(pv, &mut actions);
                return Ok(actions);
            }
            Message::PreVoteReply(reply) => {
                self.handle_pre_vote_reply(reply, &mut actions)?;
                return Ok(actions);
            }
            other => other,
        };

        // Any other message from a later term forces a step-down and term
        // adoption, before the message is interpreted in its own right.
        if message.term() > self.current_term {
            self.become_follower(message.term(), None)?;
        }

        match message {
            Message::RequestVote(rv) => self.handle_request_vote(rv, &mut actions)?,
            Message::RequestVoteReply(reply) => self.handle_vote_reply(reply, &mut actions),
            Message::AppendEntries(ae) => self.handle_append_entries(ae, &mut actions)?,
            Message::AppendEntriesReply(reply) => self.handle_append_reply(reply, &mut actions),
            Message::InstallSnapshot(rpc) => self.handle_install_snapshot(rpc, &mut actions)?,
            Message::InstallSnapshotReply(reply) => {
                self.handle_install_snapshot_reply(reply, &mut actions);
            }
            Message::TimeoutNow(rpc) => self.handle_timeout_now(rpc, &mut actions)?,
            // Routed above, before the term step-down.
            Message::PreVote(_) | Message::PreVoteReply(_) => {}
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
        self.transfer_target = None;
        self.pre_voting = false;
        self.pre_votes.clear();
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
            if self.votes.len() >= self.quorum() {
                self.become_leader(actions);
            }
        }
    }

    /// Answers a peer's pre-vote probe. Grants only if we recognise no active
    /// leader (the same stickiness that guards a real vote), the probe's
    /// hypothetical term is not behind ours, and the candidate's log is at least
    /// as up to date as ours. A pre-vote changes no persistent state: we neither
    /// adopt its term nor record a vote, so a peer may grant several pre-votes in
    /// the same term — only the real [`RequestVote`] consumes the single vote.
    fn handle_pre_vote(&mut self, pv: PreVote, actions: &mut Vec<Action>) {
        let have_active_leader =
            self.leader_id.is_some() && self.election_elapsed < self.election_timeout_min;
        let granted = pv.term >= self.current_term
            && !have_active_leader
            && self.candidate_log_up_to_date(pv.last_log_term, pv.last_log_index);
        actions.push(Action::Send {
            to: pv.candidate,
            message: Message::PreVoteReply(PreVoteReply {
                term: self.current_term,
                vote_granted: granted,
                from: self.id,
            }),
        });
    }

    /// Counts a pre-vote reply. A reply carrying a higher term means real activity
    /// we are behind on, so we abandon the round and adopt that term. Otherwise a
    /// grant adds to the tally, and once a quorum is reached we begin the real
    /// election — only here does the term finally advance.
    fn handle_pre_vote_reply(
        &mut self,
        reply: PreVoteReply,
        actions: &mut Vec<Action>,
    ) -> Result<()> {
        if !self.pre_voting {
            return Ok(());
        }
        if reply.term > self.current_term {
            self.pre_voting = false;
            self.pre_votes.clear();
            return self.become_follower(reply.term, None);
        }
        if reply.vote_granted && !self.pre_votes.contains(&reply.from) {
            self.pre_votes.push(reply.from);
            if self.pre_votes.len() >= self.quorum() {
                self.start_election(false, actions)?;
            }
        }
        Ok(())
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
        // Recognising a leader ends any pre-vote round in progress.
        self.role = Role::Follower;
        self.leader_id = Some(ae.leader);
        self.pre_voting = false;
        self.reset_election_timer();

        // The entries up to `prev_log_index` are already subsumed by our
        // snapshot. This happens for a stale or reordered RPC after we compacted;
        // we cannot verify a compacted `prev_log_term`, so we simply report that
        // we already hold everything through the snapshot boundary and let the
        // leader resend the tail with a `prev` we can check.
        let base = self.log.snapshot_index();
        if ae.prev_log_index < base {
            if ae.leader_commit > self.commit_index {
                self.commit_index = ae.leader_commit.min(base);
                self.drain_applies(actions);
            }
            reply.success = true;
            reply.match_index = base;
            actions.push(Action::Send {
                to: ae.leader,
                message: Message::AppendEntriesReply(reply),
            });
            return Ok(());
        }

        // Log-consistency check at prev_log_index. `term_at` answers `Some(0)` at
        // the index-0 sentinel and `Some(base_term)` at the snapshot boundary, so
        // both the head-of-log and post-compaction cases fall out naturally.
        let prev_ok = self.log.term_at(ae.prev_log_index) == Some(ae.prev_log_term);
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
        let (match_index, truncated) = if ae.entries.is_empty() {
            (ae.prev_log_index, false)
        } else {
            self.append_from_leader(&ae.entries)?
        };

        // A configuration entry in the batch (or a truncation that removed one)
        // may have changed the membership we follow under; recompute it.
        if truncated || ae.entries.iter().any(|e| e.members().is_some()) {
            self.refresh_config(actions);
        }

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
    /// commit index, so this never discards committed state. Returns the index of
    /// the last entry now stored from this batch and whether a truncation
    /// occurred (which the caller uses to know the configuration may have moved).
    fn append_from_leader(&mut self, entries: &[LogEntry]) -> Result<(Index, bool)> {
        let mut i = 0;
        let mut truncated = false;
        while i < entries.len() {
            let entry = &entries[i];
            match self.log.term_at(entry.index) {
                Some(term) if term == entry.term => i += 1,
                Some(_) => {
                    // Divergence: drop the conflicting tail and stop scanning.
                    self.log.truncate(entry.index)?;
                    truncated = true;
                    break;
                }
                None => break, // beyond our log; append from here on
            }
        }
        if i < entries.len() {
            self.log.append(&entries[i..])?;
            self.log.sync()?;
        }
        Ok((entries[entries.len() - 1].index, truncated))
    }

    fn handle_append_reply(&mut self, reply: AppendEntriesReply, actions: &mut Vec<Action>) {
        if self.role != Role::Leader || reply.term != self.current_term {
            return; // not leader, or a stale reply from another term
        }
        let Some(i) = self.progress_index(reply.from) else {
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
            // A leadership transfer waits for the target to catch up; once it
            // matches the log, tell it to campaign immediately.
            self.maybe_send_timeout_now(reply.from, actions);
            // The step-down above may have cleared progress; guard the index.
            if self.role == Role::Leader && self.progress[i].next_index <= self.log.last_index() {
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

    /// Installs a snapshot shipped by the leader, on a follower too far behind to
    /// replicate from the log. The state machine is reset via
    /// [`Action::RestoreSnapshot`]; tail replication resumes afterward.
    fn handle_install_snapshot(
        &mut self,
        rpc: InstallSnapshot,
        actions: &mut Vec<Action>,
    ) -> Result<()> {
        if rpc.term < self.current_term {
            actions.push(Action::Send {
                to: rpc.leader,
                message: Message::InstallSnapshotReply(InstallSnapshotReply {
                    term: self.current_term,
                    from: self.id,
                    last_index: 0,
                }),
            });
            return Ok(());
        }

        // A valid leader for our term: accept its authority.
        self.role = Role::Follower;
        self.leader_id = Some(rpc.leader);
        self.pre_voting = false;
        self.reset_election_timer();

        let snap_index = rpc.snapshot.index;
        let snap_term = rpc.snapshot.term;
        // Install only if the snapshot advances us beyond what we already hold: a
        // follower that has caught up further via normal replication, or that
        // holds a newer snapshot, must not be dragged backwards by a stale or
        // reordered `InstallSnapshot`.
        if snap_index > self.log.snapshot_index() && snap_index > self.commit_index {
            // Adopt the configuration the snapshot carries before installing it,
            // so a node catching up this way knows the membership.
            if !rpc.snapshot.config.is_empty() {
                self.base_config = rpc.snapshot.config.clone();
            }
            self.log.apply_snapshot(&rpc.snapshot)?;
            self.log.sync()?;
            self.commit_index = snap_index;
            self.last_applied = snap_index;
            if snap_index > self.snapshot_hinted_at {
                self.snapshot_hinted_at = snap_index;
            }
            self.refresh_config(actions);
            actions.push(Action::RestoreSnapshot {
                index: snap_index,
                term: snap_term,
                data: rpc.snapshot.data,
            });
        }

        // Report how far we now agree: our snapshot boundary, or the snapshot's
        // index if we already cover it as committed.
        let last_index = self
            .log
            .snapshot_index()
            .max(snap_index.min(self.commit_index));
        actions.push(Action::Send {
            to: rpc.leader,
            message: Message::InstallSnapshotReply(InstallSnapshotReply {
                term: self.current_term,
                from: self.id,
                last_index,
            }),
        });
        Ok(())
    }

    /// Handles a follower's acknowledgement of an installed snapshot: advance its
    /// progress to the snapshot index and resume tail replication.
    fn handle_install_snapshot_reply(
        &mut self,
        reply: InstallSnapshotReply,
        actions: &mut Vec<Action>,
    ) {
        if self.role != Role::Leader || reply.term != self.current_term {
            return;
        }
        let Some(i) = self.progress_index(reply.from) else {
            return;
        };
        if reply.last_index > self.progress[i].match_index {
            self.progress[i].match_index = reply.last_index;
        }
        self.progress[i].next_index = self.progress[i].match_index + 1;
        self.progress[i].state = ProgressState::Replicate;
        self.advance_commit(actions);
        self.maybe_send_timeout_now(reply.from, actions);
        if self.role == Role::Leader && self.progress[i].next_index <= self.log.last_index() {
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

    // ---- membership changes ----------------------------------------------

    /// Appends a configuration entry that adds and/or removes a single voter,
    /// adopting the new membership immediately.
    ///
    /// One change at a time: a request made while a previous configuration entry
    /// is still uncommitted is rejected with [`Error::ConfigInProgress`]. A no-op
    /// change (adding a member already present, removing one absent) succeeds
    /// without appending anything.
    fn change_membership(
        &mut self,
        add: Option<NodeId>,
        remove: Option<NodeId>,
    ) -> Result<Vec<Action>> {
        if self.role != Role::Leader || self.transfer_target.is_some() {
            return Err(Error::NotLeader {
                leader: self.leader_id,
            });
        }
        // The previous configuration change must have committed first.
        if self.config_index > self.commit_index {
            return Err(Error::ConfigInProgress);
        }

        let mut members = self.voters.clone();
        if let Some(id) = add {
            if !members.contains(&id) {
                members.push(id);
            }
        }
        if let Some(id) = remove {
            members.retain(|&m| m != id);
        }
        let members = sorted_members(members);
        if members == self.voters {
            return Ok(Vec::new()); // nothing to do
        }

        let index = self.log.last_index() + 1;
        let entry = LogEntry::config(self.current_term, index, &members);
        self.log.append(core::slice::from_ref(&entry))?;
        self.log.sync()?;

        let mut actions = Vec::new();
        // Adopt the new configuration at once (Raft applies a config entry on
        // append, not on commit), rebuilding progress and announcing the change.
        self.set_config(members, index, &mut actions);
        self.replicate_to_all(&mut actions);
        self.advance_commit(&mut actions);
        Ok(actions)
    }

    // ---- leadership transfer ---------------------------------------------

    /// Begins a leadership transfer to `target`: catch it up, then signal it to
    /// campaign. A no-op on a non-leader, for a non-voting target, or when the
    /// target is this node.
    fn transfer_leadership(&mut self, target: NodeId) -> Result<Vec<Action>> {
        if self.role != Role::Leader || target == self.id || !self.voters.contains(&target) {
            return Ok(Vec::new());
        }
        self.transfer_target = Some(target);
        let mut actions = Vec::new();
        // If the target is already caught up, hand off now; otherwise bring it up
        // to date and the catch-up replies will trigger the hand-off.
        self.maybe_send_timeout_now(target, &mut actions);
        if self.transfer_target.is_some() {
            if let Some(i) = self.progress_index(target) {
                self.send_append(i, &mut actions);
            }
        }
        Ok(actions)
    }

    /// Sends a `TimeoutNow` to `target` if a transfer to it is pending and it has
    /// caught up to the leader's last log index.
    fn maybe_send_timeout_now(&mut self, target: NodeId, actions: &mut Vec<Action>) {
        if self.transfer_target != Some(target) {
            return;
        }
        let caught_up = self
            .progress_index(target)
            .is_some_and(|i| self.progress[i].match_index >= self.log.last_index());
        if caught_up {
            self.transfer_target = None;
            actions.push(Action::Send {
                to: target,
                message: Message::TimeoutNow(TimeoutNow {
                    term: self.current_term,
                    leader: self.id,
                }),
            });
        }
    }

    /// Handles a `TimeoutNow`: a voter starts an election immediately, taking
    /// over from a leader that is handing off.
    fn handle_timeout_now(&mut self, rpc: TimeoutNow, actions: &mut Vec<Action>) -> Result<()> {
        // Ignore a stale signal or one aimed at a node no longer in the cluster.
        if rpc.term < self.current_term || !self.is_voter() {
            return Ok(());
        }
        // A forced election: peers honour our vote request despite stickiness.
        self.start_election(true, actions)
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
    fn test_candidate_pre_votes_then_requests_votes_from_peers() {
        let mut node = RaftNode::new(RaftConfig::new(1, [2, 3]));
        let mut sends = Vec::new();
        for _ in 0..1_000 {
            let actions = node.step(Event::Tick).unwrap();
            if !actions.is_empty() {
                sends = actions;
                break;
            }
        }
        // A timeout opens a pre-vote round (the node is not yet a candidate, and
        // its term has not moved) — PreVotes go to both peers.
        assert_eq!(node.role(), Role::Follower);
        assert_eq!(node.term(), 0);
        let pre_targets: Vec<NodeId> = sends
            .iter()
            .filter_map(|a| match a {
                Action::Send {
                    to,
                    message: Message::PreVote(_),
                } => Some(*to),
                _ => None,
            })
            .collect();
        assert_eq!(pre_targets.len(), 2);
        assert!(pre_targets.contains(&2) && pre_targets.contains(&3));

        // Granting one pre-vote reaches a quorum and starts the real election:
        // now the term advances and RequestVotes go out to both peers.
        let actions = node
            .step(Event::Message(Message::PreVoteReply(PreVoteReply {
                term: node.term(),
                vote_granted: true,
                from: 2,
            })))
            .unwrap();
        assert_eq!(node.role(), Role::Candidate);
        assert_eq!(node.term(), 1);
        let targets: Vec<NodeId> = actions
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
    fn test_pre_vote_does_not_advance_term_or_persist() {
        // Repeated timeouts with no peers reachable must not inflate the term —
        // the disruption guard. A lone voter in a 3-node config never gets a
        // pre-vote quorum, so it pre-votes forever at term 0.
        let mut node = RaftNode::new(RaftConfig::new(1, [2, 3]).with_election_timeout(2, 2));
        for _ in 0..50 {
            let _ = node.step(Event::Tick).unwrap();
        }
        assert_eq!(node.role(), Role::Follower);
        assert_eq!(node.term(), 0);
        assert_eq!(node.log().hard_state().term, 0);
    }

    #[test]
    fn test_pre_vote_granted_when_no_leader_and_log_ok() {
        let mut node = RaftNode::new(RaftConfig::new(1, [2, 3]));
        let actions = node
            .step(Event::Message(Message::PreVote(PreVote {
                term: 1,
                candidate: 2,
                last_log_index: 0,
                last_log_term: 0,
            })))
            .unwrap();
        let granted = actions.iter().any(|a| {
            matches!(
                a,
                Action::Send { message: Message::PreVoteReply(r), .. } if r.vote_granted
            )
        });
        assert!(granted);
        // A pre-vote leaves the responder's term and vote untouched.
        assert_eq!(node.term(), 0);
        assert_eq!(node.log().hard_state().voted_for, None);
    }

    #[test]
    fn test_pre_vote_denied_for_behind_log() {
        // Responder holds a term-2 entry; a candidate with an empty log is behind.
        let mut log = MemoryLog::new();
        log.append(&[entry(2, 1)]).unwrap();
        let mut node = RaftNode::with_log(RaftConfig::new(1, [2, 3]), log);
        let actions = node
            .step(Event::Message(Message::PreVote(PreVote {
                term: 1,
                candidate: 2,
                last_log_index: 0,
                last_log_term: 0,
            })))
            .unwrap();
        let granted = actions.iter().any(|a| {
            matches!(
                a,
                Action::Send { message: Message::PreVoteReply(r), .. } if r.vote_granted
            )
        });
        assert!(!granted);
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
                    force: false,
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
                force: false,
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

    /// Elects node 1 leader of a `{1,2,3}` cluster: tick until it opens a
    /// pre-vote round, grant the pre-vote from node 2 (which starts the real
    /// election), then grant the real vote from node 2 (self + 1 = quorum of 2).
    fn elect_multi_node_leader() -> RaftNode {
        let mut node = RaftNode::new(RaftConfig::new(1, [2, 3]).with_heartbeat_interval(2));
        for _ in 0..1_000 {
            let actions = node.step(Event::Tick).expect("tick");
            if !actions.is_empty() {
                break; // opened a pre-vote round and sent PreVotes
            }
        }
        // Pre-vote does not advance the term; the reply carries the responder's.
        let _ = node
            .step(Event::Message(Message::PreVoteReply(PreVoteReply {
                term: node.term(),
                vote_granted: true,
                from: 2,
            })))
            .expect("pre-vote reply");
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
                force: false,
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

        // Elect leader 1 (2-node quorum is 2). Tick to a pre-vote round, grant the
        // pre-vote from 2 to start the real election, then grant the real vote.
        let mut pending = Vec::new();
        for _ in 0..50 {
            let acts = leader.step(Event::Tick).unwrap();
            if !acts.is_empty() {
                pending = acts;
                break;
            }
        }
        let _ = leader
            .step(Event::Message(Message::PreVoteReply(PreVoteReply {
                term: leader.term(),
                vote_granted: true,
                from: 2,
            })))
            .unwrap();
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
                force: false,
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

    // ---- v0.5 snapshots ---------------------------------------------------

    #[test]
    fn test_snapshot_hint_then_compaction() {
        // Single-node leader with a low threshold snapshots its own log.
        let mut node = RaftNode::new(RaftConfig::single(1).with_snapshot_threshold(2));
        drive_to_leader(&mut node);

        let mut hint = None;
        for _ in 0..4 {
            let actions = node.step(Event::Propose(b"c".to_vec())).unwrap();
            if let Some(Action::Snapshot { index, term }) = actions
                .iter()
                .find(|a| matches!(a, Action::Snapshot { .. }))
                .cloned()
            {
                hint = Some((index, term));
                break;
            }
        }
        let (index, _term) = hint.expect("a snapshot hint once the log grew");
        assert!(index >= 2);

        // Feed the snapshot back; the log compacts up to `index`.
        let _ = node
            .step(Event::Snapshot {
                index,
                data: b"state".to_vec(),
            })
            .unwrap();
        assert_eq!(node.log().snapshot_index(), index);
        assert_eq!(node.log().entry(1), None); // compacted away
        assert_eq!(node.commit_index(), node.commit_index()); // unchanged
    }

    #[test]
    fn test_snapshot_event_rejects_uncommitted_index() {
        let mut node = RaftNode::new(RaftConfig::single(1).with_snapshot_threshold(0));
        drive_to_leader(&mut node);
        let _ = node.step(Event::Propose(b"c".to_vec())).unwrap(); // commit index 1
        // An index beyond what is committed/applied is ignored, no compaction.
        let _ = node
            .step(Event::Snapshot {
                index: 99,
                data: vec![],
            })
            .unwrap();
        assert_eq!(node.log().snapshot_index(), 0);
    }

    #[test]
    fn test_leader_sends_install_snapshot_when_follower_is_behind() {
        // Leader 1 of {1,2,3} with a compacted log: a probe to a fresh follower
        // (next = 1 <= snapshot index) must be an InstallSnapshot, not an append.
        let mut log = MemoryLog::new();
        log.append(&[entry(1, 1), entry(1, 2), entry(1, 3)])
            .unwrap();
        log.apply_snapshot(&Snapshot::new(2, 1, b"snap".to_vec()))
            .unwrap();
        log.set_hard_state(HardState {
            term: 1,
            voted_for: Some(1),
        })
        .unwrap();
        let mut node =
            RaftNode::with_log(RaftConfig::new(1, [2, 3]).with_election_timeout(5, 5), log);
        // Drive an election and win it. Grant the pre-vote (starts the real
        // election) and then the real vote; each is ignored unless it applies.
        let mut elected = false;
        for _ in 0..50 {
            let _ = node.step(Event::Tick).unwrap();
            let _ = node
                .step(Event::Message(Message::PreVoteReply(PreVoteReply {
                    term: node.term(),
                    vote_granted: true,
                    from: 2,
                })))
                .unwrap();
            if node.role() == Role::Candidate {
                let _ = node
                    .step(Event::Message(Message::RequestVoteReply(
                        RequestVoteReply {
                            term: node.term(),
                            vote_granted: true,
                            from: 2,
                        },
                    )))
                    .unwrap();
            }
            if node.is_leader() {
                elected = true;
                break;
            }
        }
        assert!(elected);
        // A heartbeat round: peers start at next = last+1 = 4. Force a backtrack by
        // rejecting from node 2 down into the compacted range.
        let actions = node
            .step(Event::Message(Message::AppendEntriesReply(
                AppendEntriesReply {
                    term: node.term(),
                    success: false,
                    from: 2,
                    match_index: 0,
                    conflict_index: 1, // wants to go back to index 1 (compacted)
                    conflict_term: 0,
                },
            )))
            .unwrap();
        // Backtracking past the snapshot boundary yields an InstallSnapshot.
        assert!(actions.iter().any(|a| matches!(
            a,
            Action::Send {
                to: 2,
                message: Message::InstallSnapshot(_)
            }
        )));
    }

    #[test]
    fn test_follower_installs_snapshot_and_restores() {
        let mut node = RaftNode::new(RaftConfig::new(5, [1]));
        let actions = node
            .step(Event::Message(Message::InstallSnapshot(InstallSnapshot {
                term: 3,
                leader: 1,
                snapshot: Snapshot::new(8, 2, b"the state".to_vec()),
            })))
            .unwrap();
        assert_eq!(node.log().snapshot_index(), 8);
        assert_eq!(node.commit_index(), 8);
        // The follower asks the app to restore, and acknowledges the install.
        assert!(
            actions
                .iter()
                .any(|a| matches!(a, Action::RestoreSnapshot { index: 8, .. }))
        );
        assert!(actions.iter().any(|a| matches!(
            a,
            Action::Send { message: Message::InstallSnapshotReply(r), .. } if r.last_index == 8
        )));
    }

    #[test]
    fn test_node_recovers_applied_position_from_snapshot() {
        // A log opened with an existing snapshot starts applied at the boundary,
        // so the application (which restores from the snapshot) is not re-fed it.
        let mut log = MemoryLog::new();
        log.apply_snapshot(&Snapshot::new(6, 2, b"s".to_vec()))
            .unwrap();
        let node = RaftNode::with_log(RaftConfig::single(1), log);
        assert_eq!(node.commit_index(), 6);
        assert_eq!(node.last_applied(), 6);
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
                force: false,
            })))
            .unwrap();
        assert_eq!(node.log().syncs(), before, "a no-op vote must not sync");
    }

    // ---- v0.6 membership changes ------------------------------------------

    fn membership_changed(actions: &[Action]) -> Option<Vec<NodeId>> {
        actions.iter().find_map(|a| match a {
            Action::MembershipChanged { members } => Some(members.clone()),
            _ => None,
        })
    }

    #[test]
    fn test_node_reports_bootstrap_membership() {
        let node = RaftNode::new(RaftConfig::new(1, [3, 2]));
        assert_eq!(node.members(), &[1, 2, 3]); // sorted, includes self
    }

    #[test]
    fn test_add_server_adopts_config_immediately() {
        let mut node = RaftNode::new(RaftConfig::single(1));
        drive_to_leader(&mut node);
        let actions = node.step(Event::AddServer(2)).unwrap();
        assert_eq!(node.members(), &[1, 2]);
        assert_eq!(membership_changed(&actions), Some(vec![1, 2]));
        // The change is a configuration log entry, not an applied command.
        let last = node.log().last_index();
        assert_eq!(node.log().entry(last).unwrap().members(), Some(vec![1, 2]));
    }

    #[test]
    fn test_remove_server_adopts_config() {
        let mut node = elect_multi_node_leader(); // leader 1 of {1,2,3}
        let actions = node.step(Event::RemoveServer(3)).unwrap();
        assert_eq!(node.members(), &[1, 2]);
        assert_eq!(membership_changed(&actions), Some(vec![1, 2]));
    }

    #[test]
    fn test_add_existing_member_is_noop() {
        let mut node = elect_multi_node_leader();
        let actions = node.step(Event::AddServer(2)).unwrap();
        assert!(actions.is_empty());
        assert_eq!(node.members(), &[1, 2, 3]);
    }

    #[test]
    fn test_one_config_change_at_a_time() {
        // Single-node leader: adding node 2 makes the config entry uncommittable
        // alone (quorum becomes 2), so a second change is rejected until it lands.
        let mut node = RaftNode::new(RaftConfig::single(1));
        drive_to_leader(&mut node);
        let _ = node.step(Event::AddServer(2)).unwrap();
        let err = node.step(Event::AddServer(3)).unwrap_err();
        assert!(matches!(err, Error::ConfigInProgress));
    }

    #[test]
    fn test_change_membership_rejected_on_follower() {
        let mut node = RaftNode::new(RaftConfig::new(2, [1, 3]));
        let err = node.step(Event::AddServer(4)).unwrap_err();
        assert!(matches!(err, Error::NotLeader { .. }));
    }

    #[test]
    fn test_membership_recovered_from_config_entry() {
        // A log whose latest entry is a configuration change restores that config.
        let mut log = MemoryLog::new();
        log.append(&[
            LogEntry::new(1, 1, b"x".to_vec()),
            LogEntry::config(1, 2, &[1, 2, 3, 4]),
        ])
        .unwrap();
        let node = RaftNode::with_log(RaftConfig::new(1, [2, 3]), log);
        assert_eq!(node.members(), &[1, 2, 3, 4]);
    }

    #[test]
    fn test_membership_recovered_from_snapshot_config() {
        let mut log = MemoryLog::new();
        log.apply_snapshot(&Snapshot::with_config(
            5,
            2,
            vec![1, 2, 3, 4, 5],
            b"s".to_vec(),
        ))
        .unwrap();
        let node = RaftNode::with_log(RaftConfig::single(1), log);
        assert_eq!(node.members(), &[1, 2, 3, 4, 5]);
    }

    #[test]
    fn test_follower_adopts_config_from_append() {
        let mut node = RaftNode::new(RaftConfig::new(5, [1]));
        let actions = node
            .step(Event::Message(Message::AppendEntries(AppendEntries {
                term: 2,
                leader: 1,
                prev_log_index: 0,
                prev_log_term: 0,
                entries: vec![LogEntry::config(2, 1, &[1, 5, 9])],
                leader_commit: 0,
            })))
            .unwrap();
        assert_eq!(node.members(), &[1, 5, 9]);
        assert_eq!(membership_changed(&actions), Some(vec![1, 5, 9]));
    }

    // ---- v0.6 leadership transfer -----------------------------------------

    #[test]
    fn test_timeout_now_triggers_immediate_election() {
        let mut node = RaftNode::new(RaftConfig::new(1, [2, 3]).with_election_timeout(1000, 1000));
        // Far from its election timeout, yet TimeoutNow makes it campaign at once.
        let actions = node
            .step(Event::Message(Message::TimeoutNow(TimeoutNow {
                term: 0,
                leader: 2,
            })))
            .unwrap();
        assert_eq!(node.role(), Role::Candidate);
        assert!(actions.iter().any(|a| matches!(
            a,
            Action::Send {
                message: Message::RequestVote(_),
                ..
            }
        )));
    }

    #[test]
    fn test_transfer_to_caught_up_follower_sends_timeout_now() {
        let mut node = elect_multi_node_leader(); // leader 1 of {1,2,3}
        // Follower 2 acknowledges the leader's (empty) log, so it is caught up.
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
        let actions = node.step(Event::TransferLeadership(2)).unwrap();
        assert!(actions.iter().any(|a| matches!(
            a,
            Action::Send {
                to: 2,
                message: Message::TimeoutNow(_)
            }
        )));
    }

    #[test]
    fn test_transfer_to_non_voter_is_noop() {
        let mut node = elect_multi_node_leader();
        let actions = node.step(Event::TransferLeadership(99)).unwrap();
        assert!(actions.is_empty());
    }

    #[test]
    fn test_non_voter_does_not_start_election() {
        // A node not in its own configuration follows but never campaigns.
        let mut log = MemoryLog::new();
        log.append(&[LogEntry::config(1, 1, &[1, 2, 3])]).unwrap(); // self (5) excluded
        let mut node = RaftNode::with_log(
            RaftConfig::new(5, [1, 2, 3]).with_election_timeout(2, 2),
            log,
        );
        assert_eq!(node.members(), &[1, 2, 3]);
        for _ in 0..50 {
            let _ = node.step(Event::Tick).unwrap();
        }
        assert_eq!(node.role(), Role::Follower);
    }
}
