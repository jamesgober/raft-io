//! Criterion benchmarks for the protocol hot path.
//!
//! [`RaftNode::step`](raft_io::RaftNode::step) is the single entry point the
//! whole system funnels through — every tick, every RPC, every proposal, every
//! read — so it is the path whose cost matters. These benchmarks isolate the
//! shapes of `step` that dominate a running cluster:
//!
//! - **steady state**: a follower tick and a leader heartbeat tick (the most
//!   frequent events in a healthy cluster);
//! - **replication**: a follower applying a batch of entries, and a leader
//!   committing on a follower's acknowledgement;
//! - **proposals**: a single-node leader appending, committing, and applying;
//! - **elections**: handling an inbound vote request;
//! - **reads**: a linearizable read released on a single-node leader;
//! - **snapshots**: a follower installing a leader's snapshot.
//!
//! Run with `cargo bench`. Track the reported times as the baseline; REPS treats
//! a regression beyond 5% on a tracked metric as a blocker. Representative
//! numbers and the methodology live in `docs/BENCHMARKS.md`.

use std::hint::black_box;

use criterion::{BatchSize, Criterion, criterion_group, criterion_main};
use raft_io::{
    AppendEntries, AppendEntriesReply, Event, InstallSnapshot, LogEntry, Message, PreVoteReply,
    RaftConfig, RaftLog, RaftNode, RequestVote, RequestVoteReply, Snapshot,
};

/// Drives a fresh single-node cluster to leadership for single-node benchmarks.
fn fresh_leader() -> RaftNode {
    let mut node = RaftNode::new(RaftConfig::single(1));
    while !node.is_leader() {
        let _ = node.step(Event::Tick).expect("tick");
    }
    node
}

/// Elects node 1 the leader of a `{1,2,3}` cluster: tick to a pre-vote round,
/// then grant one pre-vote and one real vote from node 2.
fn fresh_multi_leader() -> RaftNode {
    let mut node = RaftNode::new(RaftConfig::new(1, [2, 3]).with_election_timeout(5, 5));
    for _ in 0..50 {
        let acts = node.step(Event::Tick).expect("tick");
        if !acts.is_empty() {
            break;
        }
    }
    let _ = node
        .step(Event::Message(Message::PreVoteReply(PreVoteReply {
            term: node.term(),
            vote_granted: true,
            from: 2,
        })))
        .expect("pre-vote");
    let _ = node
        .step(Event::Message(Message::RequestVoteReply(
            RequestVoteReply {
                term: node.term(),
                vote_granted: true,
                from: 2,
            },
        )))
        .expect("vote");
    assert!(node.is_leader());
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

    // Leader heartbeat tick: with a heartbeat interval of one, every tick emits a
    // round of appends to the peer set — the leader's steady-state cost.
    c.bench_function("step_leader_heartbeat_tick", |b| {
        let mut node = fresh_multi_leader();
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
            BatchSize::SmallInput,
        );
    });

    // Leader commits on a follower's acknowledgement: the replication-reply hot
    // path that advances the commit index and emits applies. Each iteration
    // starts from a leader holding one uncommitted entry.
    c.bench_function("step_leader_commit_on_append_reply", |b| {
        b.iter_batched(
            || {
                let mut node = fresh_multi_leader();
                let _ = node
                    .step(Event::Propose(b"command".to_vec()))
                    .expect("propose");
                node
            },
            |mut node| {
                let idx = node.log().last_index();
                let actions = node
                    .step(black_box(Event::Message(Message::AppendEntriesReply(
                        AppendEntriesReply {
                            term: node.term(),
                            success: true,
                            from: 2,
                            match_index: idx,
                            conflict_index: 0,
                            conflict_term: 0,
                        },
                    ))))
                    .expect("reply");
                black_box(actions);
            },
            BatchSize::SmallInput,
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
                            force: false,
                        },
                    ))))
                    .expect("vote");
                black_box(actions);
            },
            BatchSize::SmallInput,
        );
    });

    // Follower applying a batch of replicated entries: the replication receive
    // hot path. Each iteration starts from a fresh, empty follower.
    c.bench_function("step_follower_append_batch_8", |b| {
        let entries: Vec<LogEntry> = (1..=8)
            .map(|i| LogEntry::new(1, i, vec![i as u8]))
            .collect();
        b.iter_batched(
            || RaftNode::new(RaftConfig::new(1, [2, 3])),
            |mut node| {
                let actions = node
                    .step(black_box(Event::Message(Message::AppendEntries(
                        AppendEntries {
                            term: 1,
                            leader: 2,
                            prev_log_index: 0,
                            prev_log_term: 0,
                            entries: entries.clone(),
                            leader_commit: 8,
                        },
                    ))))
                    .expect("append");
                black_box(actions);
            },
            BatchSize::SmallInput,
        );
    });

    // Releasing a linearizable read on a single-node leader: confirm leadership
    // (a quorum of one) and serve at the applied index — the read hot path.
    c.bench_function("step_single_node_read", |b| {
        b.iter_batched(
            || {
                let mut node = fresh_leader();
                let _ = node
                    .step(Event::Propose(b"command".to_vec()))
                    .expect("propose");
                node
            },
            |mut node| {
                let actions = node.step(black_box(Event::Read { id: 1 })).expect("read");
                black_box(actions);
            },
            BatchSize::SmallInput,
        );
    });

    // Follower installing a leader's snapshot: reset state and resume — the
    // catch-up path for a far-behind follower.
    c.bench_function("step_follower_install_snapshot", |b| {
        let snapshot = Snapshot::new(64, 3, vec![0xAB; 256]);
        b.iter_batched(
            || RaftNode::new(RaftConfig::new(1, [2, 3])),
            |mut node| {
                let actions = node
                    .step(black_box(Event::Message(Message::InstallSnapshot(
                        InstallSnapshot {
                            term: 3,
                            leader: 2,
                            snapshot: snapshot.clone(),
                        },
                    ))))
                    .expect("install");
                black_box(actions);
            },
            BatchSize::SmallInput,
        );
    });
}

criterion_group!(benches, bench_step);
criterion_main!(benches);
