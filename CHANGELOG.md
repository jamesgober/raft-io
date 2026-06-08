# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

---


## [Unreleased]

### Added

### Changed

### Fixed

### Security

---

## [0.6.0] - 2026-06-08

Membership changes and leadership transfer. The protocol is now feature complete;
hardening and the API/protocol freeze follow in v0.7.

### Added

- Single-server membership changes: `Event::AddServer` / `Event::RemoveServer`
  append a configuration entry that the node adopts immediately (Raft applies a
  config change on append, not on commit). One change is processed at a time —
  a request made while a previous change is uncommitted returns the new
  `Error::ConfigInProgress`. A leader removed from the configuration steps down
  once the change commits.
- Dynamic quorum: the voting membership (`RaftNode::members`) drives elections
  and commitment and is recovered from the log or a snapshot on restart.
- `Action::MembershipChanged` notifies the application of the new membership so
  it can update its transport's peer set.
- Leadership transfer: `Event::TransferLeadership` brings the target up to date
  and sends it the new `TimeoutNow` message so it campaigns immediately.
- Leader stickiness (Raft §4.2.3): a node ignores a `RequestVote` while a leader
  it recognises is still active, so a removed or partitioned server cannot
  disrupt the cluster with ever-higher terms. A forced election during a
  leadership transfer (new `RequestVote.force` flag) bypasses it.
- `EntryKind` (`Normal` / `Config`) on `LogEntry`, with `LogEntry::config` and
  `LogEntry::members`. Snapshots carry the configuration (`Snapshot.config`,
  `Snapshot::with_config`) so a node catching up via snapshot knows the
  membership. `WalLog` and the `framing` codec persist and frame both.
- Membership test suite (`tests/membership.rs`): add and remove a server under
  load, leadership transfer, and an adversarial churn proptest. Plus unit tests
  for configuration recovery, one-change-at-a-time, and stickiness.
- `membership` example (add a node, remove a node, transfer leadership).

### Changed

- `LogEntry` gains a `kind` field, `RequestVote` a `force` field, and `Snapshot`
  a `config` field (all pre-1.0 shape changes). `Message` gains a `TimeoutNow`
  variant; `Event` and `Action` gain the membership/transfer variants.

### Fixed

- A follower that had already caught up past an `InstallSnapshot`'s index (via
  normal replication, or holding a newer snapshot) no longer installs the older
  snapshot and resets its state backwards; it acknowledges the index it already
  covers instead.

---

## [0.5.0] - 2026-06-08

Snapshots and log compaction, plus typed wire framing. A node can now bound its
log growth by snapshotting its state machine, and a follower too far behind to
replicate is caught up with an `InstallSnapshot`. The protocol is feature-complete
except for membership changes.

### Added

- Snapshots and log compaction. The `RaftLog` trait gains `snapshot`,
  `apply_snapshot`, and `snapshot_index`; `MemoryLog` and `WalLog` implement them,
  compacting the log behind a snapshot boundary (`base_index` / `base_term`).
- Snapshot-policy hint: `RaftConfig::with_max_batch`'s sibling
  `with_snapshot_threshold` (default `0` = disabled). When the applied log grows
  past the threshold the node emits an `Action::Snapshot` hint; the application
  serializes its state and returns it via `Event::Snapshot`, and the log compacts.
- `InstallSnapshot` / `InstallSnapshotReply` RPCs (new `Message` variants): the
  leader ships its snapshot to a follower whose next entry has been compacted
  away. The follower installs it (`Action::RestoreSnapshot` resets the state
  machine) and resumes tail replication.
- `Snapshot` value type (index, term, opaque data).
- `WalLog` snapshot durability: a snapshot record is persisted and the WAL is
  physically compacted (earlier records dropped, current hard state re-persisted),
  so the file stays bounded.
- `framing` feature: `framing::encode` / `framing::decode` for `Message`, built
  on `pack-io`; the message types derive `pack_io::Serialize` / `Deserialize`.
- `Error::Encoding` variant for framing failures.
- Snapshot test suite (`tests/snapshot.rs`): a lagging follower caught up via
  snapshot then tail, compaction-never-exceeds-applied, and an adversarial
  proptest with snapshots and partitions asserting no divergence. Plus
  `framing` round-trip tests for every message variant and node/log unit tests.
- `snapshot_catchup` example.

### Changed

- `Message` gains `InstallSnapshot` / `InstallSnapshotReply` variants. The follower
  log-consistency check now accounts for a compacted boundary, fixing a
  non-contiguous-append path a stale post-compaction RPC could otherwise trigger.

---

## [0.4.0] - 2026-06-08

Durable persistence and crash recovery. A node can now back its log with a
`wal-db`-backed store whose entries and hard state survive a restart, so a
crashed node recovers and rejoins without violating safety.

### Added

- `WalLog` (feature `persistence`): a durable `RaftLog` backed by `wal-db`. It is
  log-structured — each appended entry, hard-state update, and truncation is a
  checksummed WAL record — with an in-memory index for reads, rebuilt exactly by
  replaying the WAL on `WalLog::open`.
- `persistence` feature flag, wiring the optional `wal-db` dependency
  (byte-record API only; `raft-io` frames its own records).
- Crash-recovery test suite (`tests/recovery.rs`, feature-gated): a property test
  that interleaves node crashes into an adversarial schedule and asserts
  committed entries never diverge, plus deterministic tests that a fully
  replicated log survives a full-cluster restart and that hard state is durable.
- A durability-contract unit test proving the node persists and `sync`s its vote
  before replying, and makes no durable write on a no-op.
- `persistent_node` example (run with `--features persistence`): a node whose log
  survives being dropped and reopened.

### Changed

- CI now exercises both the default (in-memory) and `--all-features` (with
  `persistence`) build, test, and clippy paths.

---

## [0.3.0] - 2026-06-08

Log replication and a correct multi-node cluster. On top of v0.2's election
layer, a leader now replicates its log to followers, tracks each one's progress,
and advances the commit index once a quorum stores an entry — verified by an
adversarial property-test suite that reorders, drops, duplicates, and partitions
messages.

### Added

- Full `AppendEntries` replication: the leader carries log entries in bounded
  batches (`RaftConfig::with_max_batch`, default 64), tracks per-follower
  progress, and pipelines optimistically once a follower's match point is found.
- Fast conflict-hint backtracking: a rejected append carries `conflict_index` /
  `conflict_term` (new `AppendEntriesReply` fields) so the leader skips back a
  whole term in one round trip instead of one entry at a time.
- Commit on a quorum with Raft's current-term safety rule (§5.4.2): an entry is
  committed by counting replicas only if it was created in the current term.
- Followers reconcile divergent tails (truncate-then-append) and advance their
  commit index from the leader, applying committed entries in log order.
- `RaftConfig::with_max_batch` / `max_batch` — replication batch-size control.
- `RaftLog::entries(from, to)` — bulk range read for assembling replication
  batches (default implementation over `entry`; `MemoryLog` overrides with a
  slice copy).
- Adversarial `proptest` suite (`tests/replication.rs`): 3- and 5-node clusters
  driven through reordered / dropped / duplicated / partitioned schedules,
  asserting committed entries never diverge and applies stay in order. Plus
  deterministic replication, partition, and heal tests.
- Examples: `replicated_log` (propose and watch all nodes agree) and
  `partition_recovery` (minority stalls, majority commits, heal reconciles).
- `criterion` benchmark for the follower replication-receive path.

### Changed

- `AppendEntriesReply` gains `conflict_index` and `conflict_term` fields (the
  backtracking hint). Pre-1.0 wire-shape change.

---

## [0.2.0] - 2026-06-07

The deterministic sans-I/O protocol core: leader election with full term and
vote safety, the leader heartbeat, and single-node commit, all driven through
one `step(event) -> Vec<Action>` function with no clock and no I/O.

### Added

- `RaftNode` — the deterministic consensus state machine. Drive it with
  `step(Event) -> Result<Vec<Action>>`; it never reads a clock, opens a socket,
  or touches a disk.
- `Event` (`Tick`, `Message`, `Propose`) and `Action` (`Send`, `Apply`,
  `#[non_exhaustive]`) — the input and output vocabulary of `step`.
- Leader election with the term and vote-safety rules: one vote per term, the
  up-to-date-log election restriction, and step-down on a higher term. Election
  timeouts are randomised per node from a seeded, deterministic RNG.
- Single-node commit: a one-node cluster elects itself and commits and applies
  its own proposals immediately.
- `RaftConfig` with a Tier-2 builder (`with_election_timeout`,
  `with_heartbeat_interval`, `with_seed`) and the `new` / `single` constructors.
- `RaftLog` trait and the in-memory `MemoryLog`; the node adopts a log's
  persisted `HardState` on construction, the seam durable persistence plugs into
  in v0.4.
- `RaftTransport` trait and the in-memory `MemoryTransport`.
- `Message` (`#[non_exhaustive]`) with the `RequestVote` / `AppendEntries` RPCs
  and their replies.
- `Error` (`#[non_exhaustive]`) built on `error-forge`, with `Result<T>`.
- Value types: `NodeId`, `Term`, `Index`, `Role`, `LogEntry`, `HardState`.
- `proptest` suite asserting Election Safety (no two leaders in one term) over
  randomised schedules on 3- and 5-node clusters, plus a convergence test.
- `criterion` benchmarks for the `step` hot path.
- `docs/API.md` reference for the full public surface and a `docs/release/`
  release note.

### Changed

- `Cargo.toml`: the protocol core takes a hard dependency on `error-forge` for
  its error type. The `std` feature and the `persistence` / `framing` flags are
  deferred to the phases that introduce code using them (v0.4 / v0.5), since an
  optional dependency without a gated code path would violate REPS.

---

## [0.1.0] - 2026-05-30

Initial scaffold and repository bootstrap. No raft-io logic yet &mdash; this release establishes the structure, tooling, and quality gates the implementation will be built on.

### Added

- `Cargo.toml` with full crate metadata, Rust 2024 edition, MSRV 1.85, dual `Apache-2.0 OR MIT` license, `docs.rs` configuration, perf-tuned release profile.
- `std` feature flag (default) gating the eventual `no_std` core. The `persistence` and `framing` flags, plus their first-party dependencies, are deferred to the phases that introduce code using them (v0.4 / v0.5).
- Dev-dependencies for the test stack: `criterion`, `proptest`, and `loom` under `cfg(loom)`.
- `README.md` &mdash; overview, positioning, install, and "where it fits".
- `docs/API.md` reference skeleton.
- `REPS.md` compliance baseline at the repository root.
- `.github/workflows/ci.yml` &mdash; Linux/macOS/Windows CI matrix on stable and MSRV, plus loom and audit/deny jobs.
- `deny.toml`, `clippy.toml`, `rustfmt.toml`, `.gitattributes`, `.gitignore`.
- `.dev/` AI-editor briefing (`PROMPT.md`, `ROADMAP.md`) &mdash; gitignored.

[Unreleased]: https://github.com/jamesgober/raft-io/compare/v0.6.0...HEAD
[0.6.0]: https://github.com/jamesgober/raft-io/compare/v0.5.0...v0.6.0
[0.5.0]: https://github.com/jamesgober/raft-io/compare/v0.4.0...v0.5.0
[0.4.0]: https://github.com/jamesgober/raft-io/compare/v0.3.0...v0.4.0
[0.3.0]: https://github.com/jamesgober/raft-io/compare/v0.2.0...v0.3.0
[0.2.0]: https://github.com/jamesgober/raft-io/compare/v0.1.0...v0.2.0
[0.1.0]: https://github.com/jamesgober/raft-io/releases/tag/v0.1.0
