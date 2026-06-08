//! Criterion benchmarks for the protocol hot path.
//!
//! [`RaftNode::step`](raft_io::RaftNode::step) is the single entry point the
//! whole system funnels through — every tick, every RPC, every proposal — so it
//! is the path whose cost matters. These benchmarks isolate the three shapes of
//! `step` that dominate a running cluster: the steady-state follower tick, a
//! leader committing a proposal, and handling an inbound vote request.
//!
//! Run with `cargo bench`. Track the reported times as the baseline; REPS treats
//! a regression beyond 5% on a tracked metric as a blocker.

use std::hint::black_box;

use criterion::{Criterion, criterion_group, criterion_main};
use raft_io::{Event, Message, RaftConfig, RaftNode, RequestVote};

/// Drives a fresh single-node cluster to leadership for proposal benchmarks.
fn fresh_leader() -> RaftNode {
    let mut node = RaftNode::new(RaftConfig::single(1));
    while !node.is_leader() {
        let _ = node.step(Event::Tick).expect("tick");
    }
    node
}

fn bench_step(c: &mut Criterion) {
    // Steady-state follower tick: the most frequent event in a healthy cluster.
    c.bench_function("step_follower_tick", |b| {
        let mut node =
            RaftNode::new(RaftConfig::new(1, [2, 3]).with_election_timeout(1_000, 1_000));
        b.iter(|| {
            let actions = node.step(black_box(Event::Tick)).expect("tick");
            black_box(actions);
        });
    });

    // Leader commits a proposal: append + commit + apply on a single node.
    c.bench_function("step_single_node_propose", |b| {
        b.iter_batched(
            fresh_leader,
            |mut node| {
                let actions = node
                    .step(black_box(Event::Propose(b"command".to_vec())))
                    .expect("propose");
                black_box(actions);
            },
            criterion::BatchSize::SmallInput,
        );
    });

    // Handling an inbound vote request: the election hot path.
    c.bench_function("step_handle_request_vote", |b| {
        b.iter_batched(
            || RaftNode::new(RaftConfig::new(1, [2, 3])),
            |mut node| {
                let actions = node
                    .step(black_box(Event::Message(Message::RequestVote(
                        RequestVote {
                            term: 1,
                            candidate: 2,
                            last_log_index: 0,
                            last_log_term: 0,
                        },
                    ))))
                    .expect("vote");
                black_box(actions);
            },
            criterion::BatchSize::SmallInput,
        );
    });
}

criterion_group!(benches, bench_step);
criterion_main!(benches);
