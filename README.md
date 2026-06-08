<h1 align="center">
    <img width="99" alt="Rust logo" src="https://raw.githubusercontent.com/jamesgober/rust-collection/72baabd71f00e14aa9184efcb16fa3deddda3a0a/assets/rust-logo.svg">
    <br>
    <b>raft-io</b>
    <br>
    <sub><sup>RAFT CONSENSUS ENGINE</sup></sub>
</h1>

<div align="center">
    <a href="https://crates.io/crates/raft-io"><img alt="Crates.io" src="https://img.shields.io/crates/v/raft-io"></a>
    <a href="https://crates.io/crates/raft-io" alt="Download raft-io"><img alt="Crates.io Downloads" src="https://img.shields.io/crates/d/raft-io?color=%230099ff"></a>
    <a href="https://docs.rs/raft-io" title="raft-io Documentation"><img alt="docs.rs" src="https://img.shields.io/docsrs/raft-io"></a>
    <a href="https://github.com/jamesgober/raft-io/actions"><img alt="GitHub CI" src="https://github.com/jamesgober/raft-io/actions/workflows/ci.yml/badge.svg"></a>
    <a href="https://github.com/rust-lang/rfcs/blob/master/text/2495-min-rust-version.md" title="MSRV"><img alt="MSRV" src="https://img.shields.io/badge/MSRV-1.85%2B-blue"></a>
</div>

<br>

<div align="left">
    <p>
        <strong>raft-io</strong> is a from-scratch implementation of the <b>Raft consensus algorithm</b> built as a clean, embeddable library rather than a framework. The protocol core is a <b>deterministic state machine</b> &mdash; you feed it events (messages, ticks, client proposals) and it returns actions (send these messages, persist these entries, apply this command). Time, networking, and storage are <em>your</em> concern, injected through traits, which is exactly what makes the consensus core provable.
    </p>
    <p>
        Most Rust Raft crates either bolt the protocol to a specific runtime and transport, or stay so abstract they ship no usable storage. <code>raft-io</code> separates the <b>protocol</b> (deterministic, sans-I/O) from the <b>drivers</b> (transport, log store, clock), ships a real <code>wal-db</code>-backed log under a feature flag, and keeps the common single-node-test path trivial.
    </p>
    <p>
        It is the coordination layer for Hive DB clustering, and it sits directly on top of <code>wal-db</code> for durable log persistence.
    </p>
    <br>
    <hr>
    <p>
        <strong>MSRV is 1.85+</strong> (Rust 2024 edition). Deterministic sans-I/O core. Pluggable transport + log store. Provable safety properties.
    </p>
    <blockquote>
        <strong>Status: pre-1.0, in active development.</strong> The wire protocol and trait seams are being designed and frozen across the 0.x series; <code>1.0.0</code> freezes the protocol and the public traits. See <a href="./CHANGELOG.md"><code>CHANGELOG.md</code></a> for detail.
    </blockquote>
</div>

<hr>
<br>

<h2>What it does</h2>

- **Leader election** &mdash; randomized-timeout election with term and vote safety; one leader per term _(live)_
- **Log replication** &mdash; batched append-entries with per-follower progress, optimistic pipelining, conflict-hint backtracking, and commit on a quorum _(live in v0.3)_
- **Deterministic core** &mdash; the state machine is pure and step-driven, so the whole protocol is testable without time or I/O _(live)_
- **Pluggable transport** &mdash; `RaftTransport` trait; in-memory for tests, real net for production _(live)_
- **Pluggable log store** &mdash; `RaftLog` trait; `wal-db`-backed under the `persistence` feature _(trait live; durable backend in v0.4)_
- **Snapshotting** &mdash; install-snapshot for log compaction and fast follower catch-up _(v0.5)_
- **Membership changes** &mdash; single-server add/remove via joint-consensus-safe reconfiguration _(v0.6)_


<br>

## Installation

```toml
[dependencies]
raft-io = "0.3"
```

<br>

## Quick Start

A node is a deterministic state machine. You hand it events with `step` and it
hands back actions to carry out. The single-node path needs nothing else — no
transport, no storage to wire up:

```rust
use raft_io::{Action, Event, RaftConfig, RaftNode};

// One node, no peers: it reaches quorum (itself) the moment it times out.
let mut node = RaftNode::new(RaftConfig::single(1));

// Drive logical ticks until it elects itself leader.
while !node.is_leader() {
    let _ = node.step(Event::Tick).expect("tick never fails in memory");
}
assert_eq!(node.leader(), Some(1));

// A leader commits its own proposals immediately (quorum of one).
for action in node.step(Event::Propose(b"set x = 1".to_vec())).unwrap() {
    if let Action::Apply { index, command, .. } = action {
        // hand `command` to your state machine, in log order
        assert_eq!(index, 1);
        assert_eq!(command, b"set x = 1");
    }
}
assert_eq!(node.commit_index(), 1);
```

A multi-node cluster works the same way: you route each `Action::Send` to the
target node's `step` through a transport of your choosing, and feed every node
logical ticks. The protocol is sans-I/O — *when* to tick and *how* to deliver
messages are yours to decide, which is what makes the whole thing testable
without a clock or a network.

Runnable examples show each path end to end:

```bash
cargo run --example single_node         # elect + propose + apply, one node
cargo run --example in_memory_cluster   # a 3-node cluster electing a leader
cargo run --example replicated_log      # propose + replicate; all nodes agree
cargo run --example partition_recovery  # minority stalls, majority commits, heal
```

<br>

## Status

This is `v0.3.0`: a correct multi-node cluster. On top of v0.2's election layer,
the full log-replication pipeline is implemented — batched `AppendEntries`,
per-follower progress with optimistic pipelining, conflict-hint backtracking, and
commit on a quorum (current-term rule). An adversarial property-test suite drives
3- and 5-node clusters through reordered, dropped, duplicated, and partitioned
message schedules and asserts that committed entries never diverge. Durable
persistence (`wal-db`) lands in `v0.4` and snapshots in `v0.5`, per the
<a href="./.dev/ROADMAP.md"><code>ROADMAP</code></a> (development copy). The full
public surface is documented in <a href="./docs/API.md"><code>docs/API.md</code></a>.

<hr>
<br>

## Where It Fits

`raft-io` is the consensus engine. It is consumed by:

- [`wal-db`](https://github.com/jamesgober/wal-db) &mdash; durable Raft log persistence (under `persistence`)
- [`pack-io`](https://github.com/jamesgober/pack-io) &mdash; typed RPC message framing (under `framing`)
- Hive DB &mdash; cluster coordination and replicated metadata

It stays foreign-compatible: usable standalone in any system that needs replicated, fault-tolerant state.

<br>

## Cross-Platform Support

**Tier 1 Support:**
- Linux (x86_64, aarch64)
- macOS (x86_64, Apple Silicon)
- Windows (x86_64)

Behavior is verified on each target by the CI matrix.

<br>

## Contributing

Before opening a PR, `cargo fmt --all`, `cargo clippy --all-targets --all-features -- -D warnings`, and `cargo test --all-features` must be clean. Hot-path changes require a `criterion` benchmark; correctness-critical paths require property and/or `loom` tests.

<br>

<div id="license">
    <h2>License</h2>
    <p>Licensed under either of</p>
    <ul>
        <li><b>Apache License, Version 2.0</b> &mdash; see <a href="./LICENSE-APACHE">LICENSE-APACHE</a></li>
        <li><b>MIT License</b> &mdash; see <a href="./LICENSE-MIT">LICENSE-MIT</a></li>
    </ul>
    <p>at your option.</p>
</div>

<div align="center">
  <h2></h2>
  <sup>COPYRIGHT <small>&copy;</small> 2026 <strong>JAMES GOBER.</strong></sup>
</div>
