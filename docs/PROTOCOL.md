<h1 align="center">
    <img width="99" alt="Rust logo" src="https://raw.githubusercontent.com/jamesgober/rust-collection/72baabd71f00e14aa9184efcb16fa3deddda3a0a/assets/rust-logo.svg">
    <br><b>raft-io</b><br>
    <sub><sup>PROTOCOL SPECIFICATION</sup></sub>
</h1>
<div align="center">
    <sup>
        <a href="../README.md" title="Project Home"><b>HOME</b></a>
        <span>&nbsp;│&nbsp;</span>
        <a href="./API.md" title="API Reference"><b>API</b></a>
        <span>&nbsp;│&nbsp;</span>
        <span>PROTOCOL</span>
    </sup>
</div>
<br>

> Normative specification of the consensus protocol `raft-io` implements: its
> state, its messages, the on-wire and on-disk byte formats, and the invariants
> it guarantees. This is the contract a second implementation would have to honour
> to interoperate, and the surface frozen at `1.0`.
>
> The key words **MUST**, **MUST NOT**, **SHOULD**, and **MAY** are to be
> interpreted as in [RFC 2119](https://datatracker.ietf.org/doc/html/rfc2119).

## Status and freeze

As of **v0.7** the public traits ([`RaftLog`](./API.md#raftlog),
[`RaftTransport`](./API.md#rafttransport)), the message set and its wire framing,
and the durable WAL record format are **frozen**: they will not change in a
backward-incompatible way before `2.0`. New message variants or record tags MAY be
added (the relevant enums are `#[non_exhaustive]` and the framing prepends a
variant tag), but existing variants, fields, tags, and their meanings are fixed.
The `PreVote` / `PreVoteReply` messages added in **v0.8** are exactly such a
backward-compatible addition: new enum variants that leave every prior message's
encoding untouched.

This document is the authority for those formats. It is based on Diego Ongaro's
Raft (the dissertation, *Consensus: Bridging Theory and Practice*); section
references like §5.3 are to that work.

## 1. Model

A cluster is a set of **nodes**, each identified by a `NodeId` (an opaque,
stable `u64`). At any time a node is a **follower**, **candidate**, or **leader**.
Time is divided into **terms** (`u64`, monotonically increasing); each term begins
with an election. Each node holds a **log** of entries, indexed from `1`
(`Index`, a `u64`; index `0` is the sentinel "before the first entry", with term
`0`).

The protocol core is a deterministic state machine. It MUST NOT read a clock, open
a socket, or perform I/O directly: the passage of time is delivered as logical
*tick* events, messages are delivered as events, and storage and transport are
reached only through the trait seams. Given the same initial state, configuration
seed, and ordered sequence of events, two nodes MUST produce identical output.

## 2. State

**Persistent state** — MUST be durable before the node responds to any RPC that
depends on it:

| Field | Meaning |
|---|---|
| `current_term` | Latest term the node has seen. |
| `voted_for` | The candidate this node voted for in `current_term`, if any. |
| `log[]` | The log entries (and the snapshot that subsumes a prefix of them). |

`current_term` and `voted_for` together are the **hard state**. A node MUST
persist a change to the hard state, and any appended or truncated log entries,
*before* it sends a message whose correctness depends on that change (§3.8).

**Volatile state** — reconstructed after a restart: `commit_index`,
`last_applied`, role, and, on a leader, the per-follower replication progress.
After recovering a snapshot at index *S*, a node MUST initialise both
`commit_index` and `last_applied` to *S* (the snapshotted state is committed and
already applied; the application restores it from the snapshot, §5).

## 3. Log entries and replication

### 3.1 Entries

A log entry has a `term`, an `index`, a `kind`, and an opaque `command`:

```
LogEntry { term: u64, index: u64, kind: EntryKind, command: bytes }
EntryKind = Normal | Config
```

A `Normal` entry's `command` is an application command; the protocol orders and
replicates it but MUST NOT interpret it. A `Config` entry's `command` encodes a
cluster configuration (§6) and MUST NOT be delivered to the application's state
machine.

### 3.2 Configuration encoding

A configuration is the set of voting member ids. It is encoded as the member ids
in ascending order, each as a little-endian `u64`, concatenated. A decoder MUST
ignore a trailing partial 8-byte group (only reachable through corruption).

### 3.3 Leader election

A follower that reaches a randomised election timeout (chosen per node from a
seeded generator so the protocol stays deterministic) and is a voting member
seeks leadership. It does so in two phases: a **pre-vote** probe (§3.3.1) and then,
if that succeeds, a real election. In the real election it increments
`current_term`, votes for itself, persists the hard state, and sends `RequestVote`
to every other voter. A candidate that collects votes from a **majority of the
current configuration** becomes leader.

A node grants a vote iff: the request's term is not stale; the node has not
already voted for a different candidate in that term; and the candidate's log is
**at least as up to date** as the node's own — a higher last-log term wins, or an
equal last-log term with at least as high a last index (§5.4.1). A node MUST
persist `voted_for` before replying that it granted.

#### 3.3.1 Pre-vote (§9.6)

Before a real election, a voter that times out SHOULD run a pre-vote round: it
sends `PreVote` to every other voter carrying the **hypothetical** term it would
campaign at (one past its current term) and its last-log position, **without**
incrementing its own term, casting a vote, or persisting anything. It remains a
follower for the duration.

A node grants a pre-vote iff it would grant a real vote under the same conditions:
the hypothetical term is not behind its own, the candidate's log is at least as up
to date (§5.4.1), and leader stickiness (§3.4) does not apply — that is, it does
not currently recognise an active leader. A `PreVoteReply` reports the responder's
unchanged current term and whether it would grant. Because no state is consumed, a
node MAY grant pre-votes to several candidates in the same term; only the real
`RequestVote` consumes its single vote.

Only once the pre-candidate collects pre-votes from a **majority of the current
configuration** does it start the real election (above). A `PreVoteReply` carrying
a higher term means the pre-candidate has fallen behind: it MUST abandon the round
and adopt that term as a follower.

Pre-vote is the disruption guard: a node partitioned from the cluster never
collects a pre-vote majority, so its term never climbs, and on rejoin it cannot
force the established leader to step down. A forced election (`TimeoutNow`, §7)
skips pre-vote, since the leader has already vouched for the target.

### 3.4 Leader stickiness (§4.2.3)

A node MUST ignore a `RequestVote` — neither granting it nor adopting its term —
while it still recognises an active leader, that is, while fewer than the minimum
election-timeout ticks have passed since it last heard from the leader. This
prevents a removed or partitioned node, which no longer receives heartbeats and so
times out repeatedly, from disrupting a healthy cluster with ever-higher terms. A
`RequestVote` with `force = true` (a leadership transfer, §7) MUST bypass this
rule.

### 3.5 Replication

The leader appends a client proposal as a `Normal` entry and replicates the log
to each follower with `AppendEntries`, which carries `prev_log_index` /
`prev_log_term` (the entry preceding the batch), zero or more `entries`, and the
leader's `leader_commit`. A follower MUST reject the RPC if `prev_log_index` /
`prev_log_term` do not match its log; otherwise it appends the entries, deleting
any conflicting suffix first (§5.3), and never deleting committed entries.

On rejection a follower returns a **conflict hint** (`conflict_index`,
`conflict_term`) so the leader can move that follower's `next_index` back by a
whole term in one round trip rather than one entry at a time.

### 3.6 Commitment

An entry is **committed** once it is stored on a majority of the current
configuration **and** the leader has committed an entry from its own current term
(§5.4.2): the leader MUST NOT consider an entry from a previous term committed by
replica count alone. Committed entries are applied to the state machine in index
order; the leader that is itself not a voter (it is being removed) does not count
itself toward the majority.

### 3.7 Pipelining and flow control

The leader MAY stream successive batches to a caught-up follower without waiting
for each acknowledgement. A single `AppendEntries` MUST carry no more than a
configured maximum number of entries, bounding message size and per-RPC work.

### 3.8 Durability ordering

Within the handling of one event, a node MUST persist (append/truncate the log,
update and flush the hard state) before it emits any message that depends on the
new state. Honouring the order in which actions are returned is sufficient.

## 4. Messages

| Message | Direction | Purpose |
|---|---|---|
| `PreVote { term, candidate, last_log_index, last_log_term }` | candidate → voters | Probe support before a real election (§3.3.1). `term` is hypothetical. |
| `PreVoteReply { term, vote_granted, from }` | voter → candidate | Would-grant, without changing state. |
| `RequestVote { term, candidate, last_log_index, last_log_term, force }` | candidate → voters | Solicit a vote. |
| `RequestVoteReply { term, vote_granted, from }` | voter → candidate | Grant or deny. |
| `AppendEntries { term, leader, prev_log_index, prev_log_term, entries, leader_commit }` | leader → follower | Replicate / heartbeat. |
| `AppendEntriesReply { term, success, from, match_index, conflict_index, conflict_term }` | follower → leader | Accept, or reject with a conflict hint. |
| `InstallSnapshot { term, leader, snapshot }` | leader → follower | Catch up a far-behind follower (§5). |
| `InstallSnapshotReply { term, from, last_index }` | follower → leader | Acknowledge the installed index. |
| `TimeoutNow { term, leader }` | leader → target | Trigger an immediate (forced) election (§7). |

A node that receives any message with a term greater than its own MUST adopt that
term and revert to follower before handling the message — **except** where leader
stickiness applies to `RequestVote` (§3.4), and except for `PreVote` /
`PreVoteReply`, which are hypothetical and MUST NOT cause the receiver to adopt
the carried term (§3.3.1). A node MUST ignore a reply whose term does not match
its current term.

## 5. Snapshots and compaction

A node MAY capture its state machine in a **snapshot** and discard the log prefix
the snapshot subsumes (compaction):

```
Snapshot { index: u64, term: u64, config: [NodeId], data: bytes }
```

`index` / `term` are the last entry the snapshot includes — the log's new base,
for which `term_at(index)` MUST still answer so the consistency check at the
boundary works. `config` is the configuration in effect at `index`, carried so a
node that installs the snapshot still knows the membership (§6). `data` is the
opaque serialized state; the protocol MUST NOT interpret it. A node MUST snapshot
only through an applied (hence committed) index, so compaction never discards an
uncommitted entry.

When a follower's required next entry has been compacted out of the leader's log,
the leader sends `InstallSnapshot` instead of `AppendEntries`. A follower MUST
install a snapshot only if it advances beyond what the follower already holds
(its current snapshot and `commit_index`); otherwise it acknowledges the index it
already covers without moving its state backwards.

## 6. Configuration changes

Membership changes one server at a time (§4.1). The leader appends a `Config`
entry carrying the new voting set and **adopts it immediately on append**, before
it commits — combined with single-server changes this guarantees the old and new
majorities always overlap, so two disjoint majorities can never form.

- A leader MUST NOT begin a new configuration change while a previous `Config`
  entry is still uncommitted (one change in flight at a time).
- A node determines its current configuration as the latest `Config` entry in its
  log, or the configuration recorded in its snapshot, or the bootstrap
  configuration — in that order. On truncation or snapshot install it MUST
  recompute this.
- A leader removed from the new configuration MUST step down once that
  configuration entry commits.
- Only voting members run an election timer; a non-voter follows but MUST NOT
  campaign.

## 7. Leadership transfer

A leader MAY hand off to a voting `target`: it brings the target's log fully up to
date, then sends `TimeoutNow`. The target, on receiving `TimeoutNow`, immediately
starts a **forced** election (`RequestVote` with `force = true`, §3.4) rather than
waiting out its election timeout. While a transfer is in progress the leader MUST
decline new client proposals.

## 8. Wire framing (`framing` feature)

Messages are serialized with `pack-io`. The encoding is deterministic and
canonical: a value that decodes MUST re-encode to the identical bytes. An enum
(such as `Message`) is encoded as a varint variant tag followed by the variant's
fields in declaration order; integers are little-endian; a byte string and a
sequence are length-prefixed. A decoder MUST treat all input as untrusted: it MUST
fail with an error rather than panic on malformed bytes, and the transport SHOULD
drop a message that fails to decode (Raft already tolerates loss).

## 9. Durable log format (`persistence` feature)

`WalLog` is log-structured: every mutation is appended to a `wal-db` write-ahead
log as a self-describing, checksummed record, and recovery replays the records to
rebuild the in-memory index. A record is a one-byte **tag** followed by
little-endian fields:

| Tag | Record | Layout after the tag byte |
|---|---|---|
| `1` | Entry | `term:u64`, `index:u64`, `kind:u8` (`0`=Normal, `1`=Config), `len:u64`, `command:[u8; len]` |
| `2` | HardState | `term:u64`, `has_vote:u8`, `vote:u64` (the vote is meaningful only when `has_vote = 1`) |
| `3` | Truncate | `from:u64` (remove entries with index `≥ from`) |
| `4` | Snapshot | `index:u64`, `term:u64`, `config_len:u64`, `config:[u64; config_len]`, `data_len:u64`, `data:[u8; data_len]` |

Replay applies records in order: an Entry appends, a HardState replaces, a
Truncate drops the tail, a Snapshot compacts. Installing a snapshot writes the
snapshot record, re-persists the current hard state after it, then physically
drops every earlier record so the file stays bounded; this physical compaction is
an optimisation and its omission MUST NOT affect the recovered logical state. A
decoder MUST reject an unknown tag, a truncated record, or a length that does not
match the record, with an error rather than a panic.

## 10. Safety invariants

The protocol guarantees, and the test suite verifies under adversarial schedules:

1. **Election Safety** — at most one leader is elected per term.
2. **Leader Append-Only** — a leader never overwrites or deletes an entry in its
   own log; it only appends.
3. **Log Matching** — if two logs hold an entry with the same index and term, the
   logs are identical in every entry up through that index.
4. **Leader Completeness** — an entry committed in a term is present in the log of
   every leader of every later term.
5. **State Machine Safety** — if a node has applied an entry at a given index, no
   node ever applies a different entry at that index.

## 11. Versioning

The crate follows semantic versioning. The formats in §4, §8, and §9 and the trait
seams are frozen as of v0.7 and will not break before `2.0`; additions remain
backward-compatible via the `#[non_exhaustive]` enums and tagged encodings.

---

<sub>Copyright &copy; 2026 <strong>James Gober</strong>. All rights reserved.</sub>
