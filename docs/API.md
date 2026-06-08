<h1 align="center">
    <img width="99" alt="Rust logo" src="https://raw.githubusercontent.com/jamesgober/rust-collection/72baabd71f00e14aa9184efcb16fa3deddda3a0a/assets/rust-logo.svg">
    <br><b>raft-io</b><br>
    <sub><sup>API REFERENCE</sup></sub>
</h1>
<div align="center">
    <sup>
        <a href="../README.md" title="Project Home"><b>HOME</b></a>
        <span>&nbsp;│&nbsp;</span>
        <span>API</span>
        <span>&nbsp;│&nbsp;</span>
        <a href="./PROTOCOL.md" title="Protocol Specification"><b>PROTOCOL</b></a>
        <span>&nbsp;│&nbsp;</span>
        <a href="../CHANGELOG.md" title="Changelog"><b>CHANGELOG</b></a>
    </sup>
</div>
<br>

> Complete reference for every public item in `raft-io`, with examples.
>
> **Status: pre-1.0 (`v0.8`, alpha — feature complete, in consumer integration).**
> The public traits and the wire and WAL formats are frozen as of v0.7 (see
> [`PROTOCOL.md`](./PROTOCOL.md)); additions before `1.0` remain
> backward-compatible — the v0.8 pre-vote messages, for instance, are new
> `#[non_exhaustive]` enum variants that change no existing encoding. This document
> is the reference for every public item.

## Table of Contents

- [Overview](#overview)
- [Installation](#installation)
- [Quick Start](#quick-start)
- [The three tiers](#the-three-tiers)
- [Public API](#public-api)
  - [Value types](#value-types) — [`NodeId`](#nodeid--term--index), [`Term`](#nodeid--term--index), [`Index`](#nodeid--term--index), [`Role`](#role), [`EntryKind`](#entrykind), [`LogEntry`](#logentry), [`HardState`](#hardstate), [`Snapshot`](#snapshot)
  - [`RaftConfig`](#raftconfig)
  - [`RaftNode`](#raftnode)
  - [`Event`](#event)
  - [`Action`](#action)
  - [Messages](#messages) — [`Message`](#message), [`PreVote`](#prevote), [`PreVoteReply`](#prevotereply), [`RequestVote`](#requestvote), [`RequestVoteReply`](#requestvotereply), [`AppendEntries`](#appendentries), [`AppendEntriesReply`](#appendentriesreply), [`InstallSnapshot`](#installsnapshot), [`InstallSnapshotReply`](#installsnapshotreply)
  - [`RaftLog`](#raftlog), [`MemoryLog`](#memorylog) & [`WalLog`](#wallog)
  - [`RaftTransport`](#rafttransport) & [`MemoryTransport`](#memorytransport)
  - [`Error`](#error), [`Result`](#result) & [`framing`](#framing)
  - [`prelude`](#prelude)
- [Feature flags](#feature-flags)

---

## Overview

`raft-io` implements the Raft consensus algorithm as a **deterministic,
sans-I/O state machine**. You feed a [`RaftNode`](#raftnode) [`Event`](#event)s
— logical ticks, inbound [`Message`](#message)s, client proposals — through one
method, [`step`](#step), and it returns the [`Action`](#action)s the outside
world must perform: messages to send and committed commands to apply. The node
never reads a clock, opens a socket, or touches a disk; time, networking, and
storage are injected through the [`RaftLog`](#raftlog) and
[`RaftTransport`](#rafttransport) seams. That separation is what makes the core
provable — an entire run is reproducible from a seed and a sequence of events.

At `v0.6` the protocol is **feature complete**: leader election with full term and
vote safety, the complete multi-node log-replication pipeline (batched
[`AppendEntries`](#appendentries), per-follower progress with optimistic
pipelining, conflict-hint backtracking, commit on a quorum), durable persistence
and crash recovery ([`WalLog`](#wallog), `persistence` feature), snapshots with
log compaction ([`Action::Snapshot`](#action) → [`InstallSnapshot`](#installsnapshot)),
and **membership changes** — add or remove a voter one server at a time
([`Event::AddServer`](#event) / [`Event::RemoveServer`](#event)) and transfer
leadership ([`Event::TransferLeadership`](#event)). The `framing` feature adds
[`pack-io`](#framing) wire encoding for messages.

---

## Installation

```toml
[dependencies]
raft-io = "0.8"

# Optional features:
raft-io = { version = "0.8", features = ["persistence"] } # durable wal-db-backed `WalLog`
raft-io = { version = "0.8", features = ["framing"] }     # pack-io wire framing for messages
```

MSRV: Rust 1.85 (edition 2024).

---

## Quick Start

```rust
use raft_io::{Action, Event, RaftConfig, RaftNode};

let mut node = RaftNode::new(RaftConfig::single(1));

while !node.is_leader() {
    let _ = node.step(Event::Tick).expect("tick never fails in memory");
}

let actions = node.step(Event::Propose(b"set x = 1".to_vec())).unwrap();
assert!(actions.iter().any(|a| matches!(a, Action::Apply { .. })));
assert_eq!(node.commit_index(), 1);
```

Runnable examples cover each path end to end:

```bash
cargo run --example single_node         # elect + propose + apply, one node
cargo run --example in_memory_cluster   # a 3-node cluster electing a leader
cargo run --example replicated_log      # propose + replicate; all nodes agree
cargo run --example partition_recovery  # minority stalls, majority commits, heal
cargo run --example snapshot_catchup    # leader compacts; lagging node catches up via snapshot
cargo run --example membership          # add a node, remove a node, transfer leadership
cargo run --example persistent_node --features persistence  # log survives a restart
```

---

## The three tiers

The API is layered so the common case is trivial and the advanced case is still
reachable without ceremony.

- **Tier 1 — the lazy path.** [`RaftNode::new`](#new) with a
  [`RaftConfig`](#raftconfig). No builder, no generic to name, an in-memory log
  by default. This is the whole common case.
- **Tier 2 — the configured path.** [`RaftConfig`](#raftconfig)'s builder
  ([`with_election_timeout`](#with_election_timeout),
  [`with_heartbeat_interval`](#with_heartbeat_interval),
  [`with_seed`](#with_seed)) for tuning election and heartbeat timing.
- **Tier 3 — the power path.** The [`RaftLog`](#raftlog) and
  [`RaftTransport`](#rafttransport) traits, plugged in with
  [`RaftNode::with_log`](#with_log), for a durable store or a real transport.

---

## Public API

### Value types

#### `NodeId` / `Term` / `Index`

```rust
pub type NodeId = u64;
pub type Term = u64;
pub type Index = u64;
```

Three plain integer aliases that keep the hot path `Copy` and allocation-free.

- **`NodeId`** identifies a node. Opaque to the protocol; any scheme works as
  long as each node in a cluster has a distinct, stable value.
- **`Term`** is Raft's logical clock — a monotonically increasing epoch counter.
  Every message carries the sender's term; a node that sees a higher term steps
  down and adopts it. Term `0` precedes the first election.
- **`Index`** is a 1-based position in the log. The first appended entry has
  index `1`; index `0` is the sentinel "before the first entry" (term `0`).

```rust
use raft_io::{Index, NodeId, Term};

let id: NodeId = 3;
let term: Term = 5;
let index: Index = 42;
assert_eq!((id, term, index), (3, 5, 42));
```

#### `Role`

```rust
pub enum Role { Follower, Candidate, Leader }
```

The role a node currently plays. A node is always in exactly one. It starts a
`Follower`, becomes a `Candidate` when it stops hearing from a leader, and
becomes a `Leader` if it wins an election. `Copy`.

```rust
use raft_io::{RaftConfig, RaftNode, Role};

let node = RaftNode::new(RaftConfig::single(1));
assert_eq!(node.role(), Role::Follower);
```

#### `EntryKind`

```rust
pub enum EntryKind { Normal, Config }
```

What a [`LogEntry`](#logentry) carries. A `Normal` entry is an application command;
a `Config` entry carries a cluster configuration (the voting membership) and
drives a membership change — the protocol interprets its bytes and the application
never applies them.

#### `LogEntry`

```rust
pub struct LogEntry {
    pub term: Term,
    pub index: Index,
    pub kind: EntryKind,
    pub command: Vec<u8>,
}
```

A single entry in the replicated log. For a `Normal` entry, `command` is opaque
bytes the protocol orders and replicates but never interprets; for a `Config`
entry the bytes encode the voting membership. `term` and `index` together identify
an entry uniquely.

**Constructors & methods**

| Item | Signature | Description |
|---|---|---|
| `new` | `fn new(term, index, command: Vec<u8>) -> LogEntry` | A `Normal` command entry. |
| `config` | `fn config(term, index, members: &[NodeId]) -> LogEntry` | A `Config` entry carrying `members`. |
| `members` | `fn members(&self) -> Option<Vec<NodeId>>` | The membership of a `Config` entry, or `None` for a `Normal` one. |

```rust
use raft_io::{EntryKind, LogEntry};

let entry = LogEntry::new(2, 7, b"put k v".to_vec());
assert_eq!(entry.kind, EntryKind::Normal);
assert_eq!(entry.members(), None);

let cfg = LogEntry::config(3, 9, &[1, 2, 3]);
assert_eq!(cfg.members(), Some(vec![1, 2, 3]));
```

#### `HardState`

```rust
pub struct HardState {
    pub term: Term,
    pub voted_for: Option<NodeId>,
}
```

The state Raft must persist before responding to any RPC. Safety depends on
`term` and `voted_for` surviving a crash: a node that forgot it had already
voted in a term could vote twice and help elect two leaders. Stored by the
[`RaftLog`](#raftlog). Implements `Default` (term `0`, no vote).

```rust
use raft_io::HardState;

let hs = HardState { term: 4, voted_for: Some(2) };
assert_eq!(hs.term, 4);
assert_eq!(HardState::default().voted_for, None);
```

#### `Snapshot`

```rust
pub struct Snapshot {
    pub index: Index,
    pub term: Term,
    pub config: Vec<NodeId>,
    pub data: Vec<u8>,
}
```

A point-in-time capture of the application's state machine plus the log position
it covers. `index` / `term` are the last entry the snapshot includes — the log's
replacement boundary once earlier entries are compacted away — `config` is the
voting membership in effect at `index` (so a node catching up via snapshot, its
configuration entries compacted, still knows who is in the cluster), and `data` is
the opaque serialized state the application produces and restores. Build one with
`Snapshot::new(index, term, data)` (empty config) or `Snapshot::with_config(index,
term, config, data)`. The node fills the configuration in when it takes a snapshot.

```rust
use raft_io::Snapshot;

let snap = Snapshot::with_config(10, 3, vec![1, 2, 3], b"state".to_vec());
assert_eq!((snap.index, snap.term), (10, 3));
assert_eq!(snap.config, vec![1, 2, 3]);
```

---

### `RaftConfig`

Configuration for a single node: its id, its peers, and the timing that drives
elections and heartbeats. Timing is in **logical ticks**, not wall-clock time —
the caller decides how often to tick.

**Constructors**

| Method | Signature | Description |
|---|---|---|
| <a id="new-config"></a>`new` | `fn new(id: NodeId, peers: impl IntoIterator<Item = NodeId>) -> RaftConfig` | Node `id` with the given peers (the node filters itself out of `peers`). Defaults: `10..=20` tick election timeout, `3` tick heartbeat, RNG seed = `id`. |
| `single` | `fn single(id: NodeId) -> RaftConfig` | A one-node cluster: no peers, quorum of one. |

**Builder methods** (consume and return `self`, so they chain)

| Method | Signature | Description |
|---|---|---|
| <a id="with_election_timeout"></a>`with_election_timeout` | `fn with_election_timeout(self, min: u32, max: u32) -> Self` | Randomised election-timeout bounds, in ticks. The spread breaks split votes. Normalised so `min >= 1` and `max >= min`. |
| <a id="with_heartbeat_interval"></a>`with_heartbeat_interval` | `fn with_heartbeat_interval(self, interval: u32) -> Self` | Ticks between leader heartbeats. Keep it well below the election-timeout minimum. Normalised to `>= 1`. |
| <a id="with_max_batch"></a>`with_max_batch` | `fn with_max_batch(self, max_batch: usize) -> Self` | Maximum entries carried by one `AppendEntries`. Bounds message size and per-RPC work so a far-behind follower is caught up in steady chunks. Normalised to `>= 1`. Default `64`. |
| <a id="with_snapshot_threshold"></a>`with_snapshot_threshold` | `fn with_snapshot_threshold(self, threshold: usize) -> Self` | How many applied entries may accumulate beyond the last snapshot before the node emits an [`Action::Snapshot`](#action) hint. `0` (the default) disables snapshotting. |
| <a id="with_seed"></a>`with_seed` | `fn with_seed(self, seed: u64) -> Self` | Seed for the deterministic election-timeout RNG. Give peers distinct seeds (the default is the node id). |

**Accessors:** `id() -> NodeId`, `peers() -> &[NodeId]`,
`election_timeout() -> (u32, u32)`, `heartbeat_interval() -> u32`,
`max_batch() -> usize`, `snapshot_threshold() -> usize`, `seed() -> u64`.

```rust
use raft_io::RaftConfig;

// Tier 1: defaults.
let cfg = RaftConfig::new(1, [2, 3]);
assert_eq!(cfg.peers(), &[2, 3]);

// Tier 2: tuned timing for a faster-ticking deployment.
let tuned = RaftConfig::new(1, [2, 3])
    .with_election_timeout(150, 300)
    .with_heartbeat_interval(30)
    .with_seed(0xABCD);
assert_eq!(tuned.election_timeout(), (150, 300));
assert_eq!(tuned.heartbeat_interval(), 30);
```

Normalisation makes degenerate input safe rather than a panic:

```rust
use raft_io::RaftConfig;

let cfg = RaftConfig::single(1).with_election_timeout(30, 10); // max < min
assert_eq!(cfg.election_timeout(), (30, 30));
```

---

### `RaftNode`

```rust
pub struct RaftNode<L: RaftLog = MemoryLog> { /* … */ }
```

A node in a Raft cluster — the deterministic consensus state machine. The
generic `L` defaults to [`MemoryLog`](#memorylog), so the common case never has
to name it.

**Constructors**

| Method | Signature | Description |
|---|---|---|
| <a id="new"></a>`new` | `fn new(config: RaftConfig) -> RaftNode<MemoryLog>` | Tier 1. Backs the node with an in-memory log. Starts as a follower in term `0`. |
| <a id="with_log"></a>`with_log` | `fn with_log(config: RaftConfig, log: L) -> RaftNode<L>` | Tier 3. Backs the node with any [`RaftLog`](#raftlog). Adopts the log's persisted [`HardState`](#hardstate) on construction, so a store recovered from disk resumes in its last term and vote. |

**Accessors**

| Method | Returns | Description |
|---|---|---|
| `id` | `NodeId` | This node's id. |
| `role` | `Role` | The role the node currently plays. |
| `is_leader` | `bool` | Whether the node is the leader. |
| `term` | `Term` | The node's current term. |
| `leader` | `Option<NodeId>` | The leader the node currently recognises. |
| `commit_index` | `Index` | Highest log index known committed. |
| `last_applied` | `Index` | Highest log index applied. |
| `members` | `&[NodeId]` | The current voting membership of the cluster. |
| `log` | `&L` | Shared reference to the underlying log. |

#### `step`

```rust
pub fn step(&mut self, event: Event) -> Result<Vec<Action>>
```

The only way to drive a node. Hand it one [`Event`](#event); act on every
returned [`Action`](#action), **in order** — anything the protocol depends on is
persisted before a `Send` is emitted, so honouring the order preserves Raft's
durability rule. Deterministic: the same node state and the same event always
produce the same actions.

**Parameters**

- `event` — the input: [`Event::Tick`](#event), [`Event::Message`](#event), or
  [`Event::Propose`](#event).

**Errors**

- [`Error::NotLeader`](#error) — the event was a `Propose` and this node is not
  the leader; the error carries the known leader so the caller can redirect.
- [`Error::Storage`](#error) — the underlying [`RaftLog`](#raftlog) failed on the
  durability path. Fatal to the node.

**Example — single-node election and commit**

```rust
use raft_io::{Action, Event, RaftConfig, RaftNode};

let mut node = RaftNode::new(RaftConfig::single(1));
while !node.is_leader() {
    let _ = node.step(Event::Tick).unwrap();
}
let actions = node.step(Event::Propose(b"x".to_vec())).unwrap();
assert!(actions.iter().any(|a| matches!(a, Action::Apply { .. })));
```

**Example — a proposal to a follower is redirected**

```rust
use raft_io::{Error, Event, RaftConfig, RaftNode};

let mut node = RaftNode::new(RaftConfig::new(2, [1, 3]));
match node.step(Event::Propose(b"x".to_vec())) {
    Err(Error::NotLeader { leader }) => {
        // retry against `leader` once one is known
        let _ = leader;
    }
    _ => panic!("a fresh follower cannot accept proposals"),
}
```

**Example — driving a cluster (sketch)**

```rust
use raft_io::{Action, Event, Message, RaftConfig, RaftNode};

let mut node = RaftNode::new(RaftConfig::new(1, [2, 3]));
// On a timer, tick; on a received message, feed it in. Route every Send.
let actions = node.step(Event::Tick).unwrap();
for action in actions {
    match action {
        Action::Send { to, message } => {
            // deliver `message` to node `to` via your transport
            let _: (u64, Message) = (to, message);
        }
        Action::Apply { command, .. } => {
            // apply `command` to your state machine, in order
            let _ = command;
        }
        _ => {}
    }
}
```

---

### `Event`

```rust
pub enum Event {
    Tick,
    Message(Message),
    Propose(Vec<u8>),
    Snapshot { index: Index, data: Vec<u8> },
    AddServer(NodeId),
    RemoveServer(NodeId),
    TransferLeadership(NodeId),
}
```

The input to [`step`](#step). A node only changes state in response to an event:

- **`Tick`** — one logical clock tick. The caller picks the wall-clock interval.
- **`Message(Message)`** — a message arrived from a peer.
- **`Propose(Vec<u8>)`** — a client proposes a command. Only a leader may accept
  it; elsewhere [`step`](#step) returns [`Error::NotLeader`](#error).
- **`Snapshot { index, data }`** — the reply to an [`Action::Snapshot`](#action)
  hint: the application has serialized its state through `index` into `data`. The
  node compacts the log up to `index`. A snapshot for an uncommitted or stale
  index is ignored.
- **`AddServer(NodeId)`** / **`RemoveServer(NodeId)`** — add or remove a voting
  server (leader only; [`Error::NotLeader`](#error) elsewhere). One change at a
  time: a request while a previous one is uncommitted returns
  [`Error::ConfigInProgress`](#error). The change takes effect immediately on the
  leader, before it commits. Removing the leader makes it step down once the
  change commits.
- **`TransferLeadership(NodeId)`** — ask the leader to hand off to a voter `target`:
  it catches the target up, then signals it to campaign at once. A no-op on a
  non-leader or for a non-voting target.

```rust
use raft_io::{Event, Message, RequestVote};

let _tick = Event::Tick;
let _propose = Event::Propose(b"cmd".to_vec());
let _msg = Event::Message(Message::RequestVote(RequestVote {
    term: 1, candidate: 2, last_log_index: 0, last_log_term: 0,
}));
```

---

### `Action`

```rust
#[non_exhaustive]
pub enum Action {
    Send { to: NodeId, message: Message },
    Apply { index: Index, term: Term, command: Vec<u8> },
    Snapshot { index: Index, term: Term },
    RestoreSnapshot { index: Index, term: Term, data: Vec<u8> },
    MembershipChanged { members: Vec<NodeId> },
}
```

What [`step`](#step) returns for the caller to carry out. The node decides
*what*; the caller makes it happen.

- **`Send { to, message }`** — deliver `message` to node `to` via the transport.
- **`Apply { index, term, command }`** — apply a committed command to the state
  machine. Applies are emitted in strictly increasing index order, each index
  once, so they can be applied blindly in sequence. (Configuration entries are
  not applied, so applied indices may skip them.)
- **`Snapshot { index, term }`** — take a snapshot of the state machine through
  `index` and return it via [`Event::Snapshot`](#event). Emitted when the log
  grows past [`with_snapshot_threshold`](#with_snapshot_threshold).
- **`RestoreSnapshot { index, term, data }`** — reset the state machine to an
  installed snapshot (on a follower that received a leader's snapshot). Subsequent
  `Apply` actions resume from `index + 1`.
- **`MembershipChanged { members }`** — the voting membership changed. Update the
  transport so it can reach the new members. Emitted on append of the change,
  before it commits.

`#[non_exhaustive]`: a `match` must include a wildcard arm.

```rust
use raft_io::{Action, Event, RaftConfig, RaftNode};

let mut node = RaftNode::new(RaftConfig::single(1));
while !node.is_leader() {
    let _ = node.step(Event::Tick).unwrap();
}
for action in node.step(Event::Propose(b"x".to_vec())).unwrap() {
    match action {
        Action::Send { to, message } => { let _ = (to, message); }
        Action::Apply { index, command, .. } => { let _ = (index, command); }
        _ => {}
    }
}
```

---

### Messages

The RPCs nodes exchange. The protocol never sends these itself — it emits
[`Action::Send`](#action) carrying a [`Message`](#message), and the caller
delivers it through a [`RaftTransport`](#rafttransport).
[`AppendEntries`](#appendentries) carries log entries in bounded batches when a
follower is behind and is an empty heartbeat when it is caught up; on rejection
the reply carries a conflict hint so the leader can backtrack a whole term at a
time.

#### `Message`

```rust
#[non_exhaustive]
pub enum Message {
    PreVote(PreVote),
    PreVoteReply(PreVoteReply),
    RequestVote(RequestVote),
    RequestVoteReply(RequestVoteReply),
    AppendEntries(AppendEntries),
    AppendEntriesReply(AppendEntriesReply),
    InstallSnapshot(InstallSnapshot),
    InstallSnapshotReply(InstallSnapshotReply),
    TimeoutNow(TimeoutNow),
}
```

Wraps the RPCs and their replies. `#[non_exhaustive]` — match with a wildcard arm.
**`TimeoutNow { term, leader }`** is a leader's signal during a leadership
transfer, telling the target to campaign immediately rather than wait out its
election timeout.

**Method** — `term(&self) -> Term` returns the term carried by any variant,
which the protocol checks first on every inbound message.

```rust
use raft_io::{AppendEntriesReply, Message};

let m = Message::AppendEntriesReply(AppendEntriesReply {
    term: 5, success: false, from: 2, match_index: 0,
    conflict_index: 1, conflict_term: 0,
});
assert_eq!(m.term(), 5);
```

#### `PreVote`

```rust
pub struct PreVote {
    pub term: Term,
    pub candidate: NodeId,
    pub last_log_index: Index,
    pub last_log_term: Term,
}
```

A candidate's pre-vote probe, sent before it commits to a real election (Raft
thesis §9.6). `term` is the *hypothetical* term the candidate would campaign at —
one past its current term — **not** a term it has adopted. A peer grants only if
it recognises no active leader and the candidate's log is at least as up to date
as its own; unlike [`RequestVote`](#requestvote), a pre-vote changes no persistent
state on either side. A node partitioned from the cluster never wins a pre-vote
majority, so its term never inflates and it cannot disrupt the sitting leader when
it rejoins — that is the entire point.

#### `PreVoteReply`

```rust
pub struct PreVoteReply {
    pub term: Term,
    pub vote_granted: bool,
    pub from: NodeId,
}
```

A peer's answer to a [`PreVote`](#prevote). `term` is the responder's current term
(unchanged by the pre-vote); if it exceeds the pre-candidate's term, the
pre-candidate has fallen behind and abandons the round. `vote_granted` says
whether the peer would support a real election.

#### `RequestVote`

```rust
pub struct RequestVote {
    pub term: Term,
    pub candidate: NodeId,
    pub last_log_index: Index,
    pub last_log_term: Term,
    pub force: bool,
}
```

A candidate's request for a vote. A recipient grants it only if it has not voted
in this term and the candidate's log is at least as up to date as its own — the
election restriction that keeps a node missing committed entries off the throne.
`force` marks an authorised election during a [leadership transfer](#event); a
recipient honours it even within the leader-stickiness window (a node otherwise
ignores vote requests while a leader it recognises is still active, so a removed
or partitioned server cannot disrupt the cluster).

#### `RequestVoteReply`

```rust
pub struct RequestVoteReply {
    pub term: Term,
    pub vote_granted: bool,
    pub from: NodeId,
}
```

A peer's answer. `from` names the responder so the candidate counts distinct
votes without relying on transport addressing.

#### `AppendEntries`

```rust
pub struct AppendEntries {
    pub term: Term,
    pub leader: NodeId,
    pub prev_log_index: Index,
    pub prev_log_term: Term,
    pub entries: Vec<LogEntry>,
    pub leader_commit: Index,
}
```

The leader's replicate-and-heartbeat RPC. With an empty `entries` list it is a
pure heartbeat that asserts leadership and resets the follower's election timer.
`prev_log_index` / `prev_log_term` let the follower verify its log matches the
leader's up to that point.

#### `AppendEntriesReply`

```rust
pub struct AppendEntriesReply {
    pub term: Term,
    pub success: bool,
    pub from: NodeId,
    pub match_index: Index,
    pub conflict_index: Index,
    pub conflict_term: Term,
}
```

A follower's answer. `success` is `true` when the log matched at
`prev_log_index`; `match_index` reports the highest index the follower now
agrees on, which the leader uses to track replication progress. On a rejection
the `conflict_index` / `conflict_term` pair lets the leader skip its
`next_index` for this follower back by a whole term in one round trip instead of
decrementing one entry at a time (the fast-backtracking optimisation). Both are
`0` on success.

#### `InstallSnapshot`

```rust
pub struct InstallSnapshot {
    pub term: Term,
    pub leader: NodeId,
    pub snapshot: Snapshot,
}
```

The leader's transfer of a [`Snapshot`](#snapshot) to a follower too far behind
to replicate entry by entry — its next required entry has been compacted out of
the leader's log. The follower installs the snapshot (replacing its state through
`snapshot.index`, via [`Action::RestoreSnapshot`](#action)) and resumes tail
replication.

#### `InstallSnapshotReply`

```rust
pub struct InstallSnapshotReply {
    pub term: Term,
    pub from: NodeId,
    pub last_index: Index,
}
```

A follower's acknowledgement. `last_index` is the snapshot index the follower has
installed, which the leader uses to advance that follower's replication progress.

```rust
use raft_io::{AppendEntries, Message};

// An empty heartbeat for term 4 from node 1.
let heartbeat = Message::AppendEntries(AppendEntries {
    term: 4, leader: 1, prev_log_index: 9, prev_log_term: 3,
    entries: Vec::new(), leader_commit: 7,
});
assert_eq!(heartbeat.term(), 4);
```

---

### `RaftLog`

The boundary between the protocol and where the log actually lives. The node
reads through it and writes through it, and treats a returned `Ok` from `sync`
as the durability point.

```rust
pub trait RaftLog {
    fn last_index(&self) -> Index;
    fn last_term(&self) -> Term;
    fn term_at(&self, index: Index) -> Option<Term>;
    fn entry(&self, index: Index) -> Option<LogEntry>;
    fn entries(&self, from: Index, to: Index) -> Vec<LogEntry>; // has a default impl
    fn append(&mut self, entries: &[LogEntry]) -> Result<()>;
    fn truncate(&mut self, from: Index) -> Result<()>;
    fn hard_state(&self) -> HardState;
    fn set_hard_state(&mut self, state: HardState) -> Result<()>;
    fn sync(&mut self) -> Result<()>;
    fn snapshot_index(&self) -> Index;              // has a default impl (0)
    fn snapshot(&self) -> Option<Snapshot>;         // has a default impl (None)
    fn apply_snapshot(&mut self, s: &Snapshot) -> Result<()>; // default: error
}
```

| Method | Description |
|---|---|
| `last_index` | Index of the last entry, or `0` if empty. |
| `last_term` | Term of the last entry, or `0` if empty. |
| `term_at(index)` | Term at `index`; `Some(0)` for the sentinel `0`, `Some(base_term)` at a snapshot boundary, `None` below it or past the end. |
| `entry(index)` | The entry at `index`, or `None` (including compacted indices). |
| `entries(from, to)` | Entries in the inclusive range `[from, to]` (the leader's replication batch). Has a default impl over `entry`; override for a bulk read. |
| `append(entries)` | Append entries; the first index must be `last_index() + 1` and the batch contiguous. |
| `truncate(from)` | Remove every entry with index `>= from` (`from` must be above the snapshot boundary). |
| `hard_state` | The persisted [`HardState`](#hardstate). |
| `set_hard_state(state)` | Persist a new hard state. |
| `sync` | Flush preceding writes to durable storage. |
| `snapshot_index` | The index the log is compacted up to (`0` if none). Default `0`. |
| `snapshot` | The current [`Snapshot`](#snapshot), if any. Default `None`. |
| `apply_snapshot(s)` | Install a snapshot, compacting the prefix it subsumes (keeping a matching tail). The default errors, so a snapshot-unaware backend fails loudly. |

**Durability contract.** A backend may buffer writes, but once `sync` returns
`Ok`, every preceding `append`, `truncate`, and `set_hard_state` must be durable.
The node always calls `sync` before emitting a message that depends on that
state, honouring Raft's "persist before you respond" rule. Implementors map
their own errors into [`Error::Storage`](#error) via
[`Error::storage`](#error-helpers), so the trait's error type stays the crate's
own — no associated error type for callers to name.

### `MemoryLog`

The default, non-durable [`RaftLog`](#raftlog), backed by a `Vec`. Used by
[`RaftNode::new`](#new). For tests, examples, and the single-node path — not
production.

**Methods:** `new()`, `len() -> usize`, `is_empty() -> bool`, plus the full
[`RaftLog`](#raftlog) trait.

```rust
use raft_io::{HardState, LogEntry, MemoryLog, RaftLog};

let mut log = MemoryLog::new();
log.append(&[LogEntry::new(1, 1, b"a".to_vec())]).unwrap();
log.set_hard_state(HardState { term: 1, voted_for: Some(1) }).unwrap();
log.sync().unwrap();

assert_eq!(log.last_index(), 1);
assert_eq!(log.term_at(1), Some(1));
assert_eq!(log.term_at(0), Some(0)); // sentinel
assert_eq!(log.hard_state().voted_for, Some(1));
```

A non-contiguous append is rejected rather than allowed to corrupt the log:

```rust
use raft_io::{Error, LogEntry, MemoryLog, RaftLog};

let mut log = MemoryLog::new();
let err = log.append(&[LogEntry::new(1, 2, vec![])]).unwrap_err(); // expected index 1
assert!(matches!(err, Error::Storage { .. }));
```

### `WalLog`

_Requires the `persistence` feature._

A durable [`RaftLog`](#raftlog) backed by `wal-db`, whose entries and hard state
(term and vote) survive a process restart. This is what makes a node
crash-recoverable: Raft's safety depends on `current_term`, `voted_for`, and the
log being durable before the node acts on them.

It is log-structured. Every mutation — an appended entry, a hard-state update, a
truncation — is encoded as a record and appended to a `wal-db` write-ahead log
(which frames and checksums each record); an in-memory index mirrors the current
state for fast reads. [`open`](#wallog) replays the records to rebuild that index
exactly. Reads are served from memory; writes become durable when
[`sync`](#raftlog) returns `Ok` — and the node always `sync`s before it replies,
honouring the "persist before you respond" rule.

**Constructor**

| Method | Signature | Description |
|---|---|---|
| `open` | `fn open(path: impl AsRef<Path>) -> Result<WalLog>` | Open (creating if absent) and recover the log at `path`. Returns [`Error::Storage`](#error) if the file cannot be opened or a record fails to decode. |

Plus the full [`RaftLog`](#raftlog) trait.

```rust,no_run
use raft_io::{LogEntry, RaftConfig, RaftLog, RaftNode, WalLog};

// Open a durable log and hand it to a node.
let log = WalLog::open("node-1.wal")?;
let mut node = RaftNode::with_log(RaftConfig::single(1), log);
# let _ = &mut node;

// After a restart, reopening the same path recovers the entries and the
// persisted term/vote.
let recovered = WalLog::open("node-1.wal")?;
assert_eq!(recovered.last_index(), node.log().last_index());
# Ok::<(), raft_io::Error>(())
```

Truncated entries remain physically in the WAL until log compaction (snapshots,
`v0.5`); replay still reconstructs the correct logical state. The byte-record
API of `wal-db` is used directly — `raft-io` frames its own records and does not
enable `wal-db`'s `pack-io` feature.

---

### `RaftTransport`

Delivers protocol messages to peers. A driver loop takes each
[`Action::Send`](#action) a node emits and calls `send`. How delivery happens —
an in-process queue, a channel, a socket — is the implementor's concern; the
protocol only needs a handed-off message to eventually reach the target's
[`step`](#step) (Raft tolerates loss, reordering, and duplication).

```rust
pub trait RaftTransport {
    fn send(&mut self, to: NodeId, message: Message) -> Result<()>;
}
```

### `MemoryTransport`

An in-memory [`RaftTransport`](#rafttransport) that records outgoing messages
instead of delivering them, so a test harness can route them by hand and control
ordering, loss, and partitions precisely.

**Methods:** `new()`, `take() -> Vec<(NodeId, Message)>` (drain),
`pending() -> usize`.

```rust
use raft_io::{AppendEntries, MemoryTransport, Message, RaftTransport};

let mut tx = MemoryTransport::new();
tx.send(2, Message::AppendEntries(AppendEntries {
    term: 1, leader: 1, prev_log_index: 0, prev_log_term: 0,
    entries: Vec::new(), leader_commit: 0,
})).unwrap();

let pending = tx.take();
assert_eq!(pending[0].0, 2);   // destination node
assert!(tx.take().is_empty()); // draining leaves it empty
```

---

### `Error`

```rust
#[non_exhaustive]
pub enum Error {
    NotLeader { leader: Option<NodeId> },
    Storage { context: &'static str, detail: String },
    Encoding { context: &'static str, detail: String },
    ConfigInProgress,
}
```

Everything that can go wrong while driving a node. Built on `error-forge`: it
implements `error_forge::ForgeError`, exposing stable `kind` / `caption` and
severity (`is_retryable` / `is_fatal`) metadata, and is an ordinary
`std::error::Error`. `#[non_exhaustive]` — match with a wildcard arm.

| Variant | Meaning | Severity |
|---|---|---|
| `NotLeader { leader }` | A proposal reached a non-leader. `leader` is the best-known leader for the caller to redirect to (`None` during an election). | Retryable, not fatal. |
| `Storage { context, detail }` | A [`RaftLog`](#raftlog) backend operation failed. `context` names the operation; `detail` is the backend's message. | Fatal, not retryable. |
| `Encoding { context, detail }` | A message failed to [encode or decode](#framing) (the `framing` feature). | Not fatal — drop the malformed message. |
| `ConfigInProgress` | A membership change was requested while a previous one is still uncommitted. | Retryable once the in-flight change completes. |

<a id="error-helpers"></a>**Helpers** — `Error::storage(context, source)` and
`Error::encoding(context, source)` build the respective error from any `Display`
source, so backends and the framing layer map their errors without naming the
fields.

```rust
use raft_io::Error;

let err = Error::NotLeader { leader: Some(3) };
assert_eq!(err.to_string(), "not the leader; current leader is node 3");

let io = std::io::Error::new(std::io::ErrorKind::Other, "disk full");
assert!(Error::storage("append entries", io).to_string().contains("disk full"));
```

### `Result`

```rust
pub type Result<T, E = Error> = core::result::Result<T, E>;
```

The crate's result alias, defaulting its error to [`Error`](#error), so most
signatures read `Result<T>`.

---

### `framing`

_Requires the `framing` feature._

Typed wire encoding for [`Message`](#message), built on `pack-io`. The protocol
emits [`Action::Send`](#action) carrying a `Message` and leaves delivery to you;
this module supplies the codec when your transport needs one. The message types
derive `pack_io::Serialize` / `Deserialize` under the feature.

| Function | Signature | Description |
|---|---|---|
| `encode` | `fn encode(message: &Message) -> Result<Vec<u8>>` | Serialize a message to wire bytes. |
| `decode` | `fn decode(bytes: &[u8]) -> Result<Message>` | Read a message back. A failure is [`Error::Encoding`](#error) — treat it like a dropped message, not a crash. |

```rust
# #[cfg(feature = "framing")] {
use raft_io::{framing, Message, RequestVote};

let msg = Message::RequestVote(RequestVote {
    term: 4, candidate: 2, last_log_index: 9, last_log_term: 3,
});
let bytes = framing::encode(&msg).unwrap();
assert_eq!(framing::decode(&bytes).unwrap(), msg);
# }
```

---

### `prelude`

```rust
use raft_io::prelude::*;
```

The everyday surface in one import: [`RaftNode`](#raftnode),
[`RaftConfig`](#raftconfig), the [`Event`](#event) / [`Action`](#action)
vocabulary, [`Error`](#error) / [`Result`](#result), the
[`RaftLog`](#raftlog) / [`RaftTransport`](#rafttransport) seams with their
in-memory implementations ([`MemoryLog`](#memorylog),
[`MemoryTransport`](#memorytransport) is *not* re-exported — bring it in
explicitly when needed), and the [`NodeId`](#nodeid--term--index) /
[`Term`](#nodeid--term--index) / [`Index`](#nodeid--term--index) /
[`Role`](#role) aliases. With the `persistence` feature it also re-exports
[`WalLog`](#wallog). The message and other value types stay at the crate root,
for when you implement a transport or inspect a [`LogEntry`](#logentry).

```rust
use raft_io::prelude::*;

let mut node = RaftNode::new(RaftConfig::single(1));
while !node.is_leader() {
    let _ = node.step(Event::Tick).unwrap();
}
assert!(node.is_leader());
```

---

## Feature flags

| Feature | Default | Description |
|---------|---------|-------------|
| `persistence` | no | Adds [`WalLog`](#wallog), a durable `wal-db`-backed [`RaftLog`](#raftlog). The in-memory path is unaffected when off. |
| `framing` | no | Adds [`framing`](#framing) — `pack-io` wire encoding for [`Message`](#message). Derives `pack_io` traits on the message types. |

All flags are additive; the protocol is unchanged when they are off.

---

<sub>Copyright &copy; 2026 <strong>James Gober</strong>. All rights reserved.</sub>
