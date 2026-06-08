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

[Unreleased]: https://github.com/jamesgober/raft-io/compare/v0.4.0...HEAD
[0.4.0]: https://github.com/jamesgober/raft-io/compare/v0.3.0...v0.4.0
[0.3.0]: https://github.com/jamesgober/raft-io/compare/v0.2.0...v0.3.0
[0.2.0]: https://github.com/jamesgober/raft-io/compare/v0.1.0...v0.2.0
[0.1.0]: https://github.com/jamesgober/raft-io/releases/tag/v0.1.0
