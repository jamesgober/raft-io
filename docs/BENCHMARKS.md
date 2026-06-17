<h1 align="center">
    <img width="99" alt="Rust logo" src="https://raw.githubusercontent.com/jamesgober/rust-collection/72baabd71f00e14aa9184efcb16fa3deddda3a0a/assets/rust-logo.svg">
    <br><b>raft-io</b><br>
    <sub><sup>PERFORMANCE BASELINES</sup></sub>
</h1>
<div align="center">
    <sup>
        <a href="../README.md" title="Project Home"><b>HOME</b></a>
        <span>&nbsp;│&nbsp;</span>
        <a href="./API.md" title="API Reference"><b>API</b></a>
        <span>&nbsp;│&nbsp;</span>
        <a href="./PROTOCOL.md" title="Protocol Specification"><b>PROTOCOL</b></a>
        <span>&nbsp;│&nbsp;</span>
        <span>BENCHMARKS</span>
    </sup>
</div>
<br>

> Committed performance baselines for the protocol core, per REPS. These are the
> reference numbers a change is measured against; a regression beyond **5%** on a
> tracked metric is a blocker.

## What is measured

[`RaftNode::step`](https://docs.rs/raft-io/latest/raft_io/struct.RaftNode.html#method.step)
is the single entry point the whole system funnels through — every tick, every
RPC, every proposal, every read. The benchmarks (`benches/raft_bench.rs`,
[`criterion`](https://docs.rs/criterion)) isolate the shapes of `step` that
dominate a running cluster. Because the core is **sans-I/O**, these numbers are
pure protocol cost: no clock, socket, or disk is touched, so they are stable and
reproducible from the in-memory log. Real deployments add transport and storage
latency on top.

## Baseline

Measured on Windows 11 (x86-64), Rust stable, `--release` profile
(`opt-level = 3`, `lto = "fat"`, `codegen-units = 1`). Reported as criterion's
median estimate; absolute numbers are machine-specific — **track the ratio of
change on a fixed machine**, not the raw figure.

| Benchmark | Path | Median |
|---|---|---:|
| `step_follower_tick` | steady-state follower clock tick (no allocation) | ~8 ns |
| `step_leader_heartbeat_tick` | leader emits a heartbeat round to the peer set | ~19 ns |
| `step_handle_request_vote` | grant/deny an inbound vote (persists the vote) | ~93 ns |
| `step_single_node_read` | release a linearizable read (ReadIndex, quorum of one) | ~165 ns |
| `step_follower_install_snapshot` | install a leader's snapshot and restore | ~170 ns |
| `step_leader_commit_on_append_reply` | advance commit + apply on a follower ack | ~188 ns |
| `step_single_node_propose` | append + commit + apply one proposal | ~205 ns |
| `step_follower_append_batch_8` | append and apply a batch of 8 replicated entries | ~820 ns |

The steady-state events that fire on every cluster heartbeat — the follower tick
and leader heartbeat tick — are in the **single- to double-digit nanosecond**
range and allocation-free on the no-work path. The paths that mutate the log,
commit, or apply allocate the returned `Vec<Action>` (and clone command bytes) and
land in the **low hundreds of nanoseconds**; the 8-entry append is proportional to
the batch.

## Running them

```bash
cargo bench                 # full run, all metrics
cargo bench -- --save-baseline main      # save a named baseline
cargo bench -- --baseline main           # compare against it
```

Criterion writes detailed reports (including HTML) under `target/criterion/`. To
gate regressions, save a baseline on a known-good commit and compare a change
against it; investigate any tracked metric that regresses beyond 5%.

## Methodology notes

- The core is single-threaded and owns its state, so there is no lock or
  shared-state contention to measure; `loom` is not exercised.
- Setup is excluded from timing via criterion's `iter_batched` for benchmarks
  whose operation mutates state (proposals, commits, reads, snapshot installs), so
  only the `step` call itself is measured.
- `black_box` guards both the event passed in and the actions returned, so the
  optimizer cannot fold the work away.

---

<sub>Copyright &copy; 2026 <strong>James Gober</strong>. All rights reserved.</sub>
