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

[Unreleased]: https://github.com/jamesgober/raft-io/compare/v0.2.0...HEAD
[0.2.0]: https://github.com/jamesgober/raft-io/compare/v0.1.0...v0.2.0
[0.1.0]: https://github.com/jamesgober/raft-io/releases/tag/v0.1.0
