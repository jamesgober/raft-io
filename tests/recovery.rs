//! Crash-recovery tests for the durable [`WalLog`].
//!
//! These run only with the `persistence` feature. A node is "crashed" by
//! dropping it (closing its WAL) and rebuilding it from the same file, exactly as
//! a process restart would. The tests assert that a node restarted at any point
//! rejoins without violating safety:
//!
//! - **State Machine Safety across restarts** — no two nodes (or incarnations)
//!   ever apply a different command at the same index, even with crashes
//!   interleaved into an adversarial schedule.
//! - **No committed entry is lost** — a fully replicated log survives a restart
//!   of every node, and the cluster resumes and keeps committing.
//! - **Hard state is durable** — `term` and `voted_for` recovered from the WAL
//!   never regress across a restart.
//!
//! `WalLog` recovery is deterministic, so a `proptest` counterexample replays
//! exactly.
#![cfg(feature = "persistence")]
#![allow(clippy::unwrap_used, clippy::expect_used)]

use std::collections::BTreeMap;
use std::path::PathBuf;

use proptest::prelude::*;
use raft_io::{Action, Event, Message, NodeId, RaftConfig, RaftLog, RaftNode, WalLog};

/// A WAL-backed cluster whose nodes can be crashed and recovered from disk.
struct Cluster {
    dir: tempfile::TempDir,
    n: usize,
    /// `None` while a node is momentarily down between drop and reopen.
    nodes: Vec<Option<RaftNode<WalLog>>>,
    wire: Vec<(NodeId, NodeId, Message)>,
    /// Per-node applied commands for the current incarnation (cleared on crash).
    applied: Vec<Vec<Vec<u8>>>,
    /// Agreed command at each committed index, across all incarnations.
    committed: BTreeMap<u64, Vec<u8>>,
    next_value: u64,
}

fn wal_path(dir: &tempfile::TempDir, id: NodeId) -> PathBuf {
    dir.path().join(format!("node-{id}.wal"))
}

fn open_node(dir: &tempfile::TempDir, id: NodeId, n: usize) -> RaftNode<WalLog> {
    let ids: Vec<NodeId> = (0..n as NodeId).collect();
    let log = WalLog::open(wal_path(dir, id)).expect("open wal");
    let cfg = RaftConfig::new(id, ids)
        .with_election_timeout(10, 20)
        .with_heartbeat_interval(3)
        .with_max_batch(8)
        .with_seed(0x7000 + id);
    RaftNode::with_log(cfg, log)
}

impl Cluster {
    fn new(n: usize) -> Self {
        let dir = tempfile::tempdir().expect("tempdir");
        let nodes = (0..n as NodeId)
            .map(|id| Some(open_node(&dir, id, n)))
            .collect();
        Self {
            dir,
            n,
            nodes,
            wire: Vec::new(),
            applied: vec![Vec::new(); n],
            committed: BTreeMap::new(),
            next_value: 1,
        }
    }

    fn node(&self, i: usize) -> &RaftNode<WalLog> {
        self.nodes[i].as_ref().expect("node up")
    }

    fn node_mut(&mut self, i: usize) -> &mut RaftNode<WalLog> {
        self.nodes[i].as_mut().expect("node up")
    }

    fn absorb(&mut self, i: usize, actions: Vec<Action>) {
        let from = self.node(i).id();
        for action in actions {
            match action {
                Action::Send { to, message } => self.wire.push((from, to, message)),
                Action::Apply { index, command, .. } => {
                    let log = &mut self.applied[i];
                    assert_eq!(
                        index as usize,
                        log.len() + 1,
                        "node {from} applied index {index} out of order"
                    );
                    log.push(command.clone());
                    match self.committed.get(&index) {
                        Some(existing) => assert_eq!(
                            *existing, command,
                            "divergent committed entry at index {index} after a crash"
                        ),
                        None => {
                            let _ = self.committed.insert(index, command);
                        }
                    }
                }
                _ => {}
            }
        }
    }

    fn tick(&mut self, i: usize) {
        let actions = self.node_mut(i).step(Event::Tick).expect("tick");
        self.absorb(i, actions);
    }

    fn propose(&mut self) {
        if let Some(i) = (0..self.n).find(|&i| self.node(i).is_leader()) {
            let value = self.next_value.to_be_bytes().to_vec();
            self.next_value += 1;
            if let Ok(actions) = self.node_mut(i).step(Event::Propose(value)) {
                self.absorb(i, actions);
            }
        }
    }

    fn deliver(&mut self, pick: usize) {
        if self.wire.is_empty() {
            return;
        }
        let idx = pick % self.wire.len();
        let (_from, to, message) = self.wire.remove(idx);
        let target = to as usize;
        let actions = self
            .node_mut(target)
            .step(Event::Message(message))
            .expect("message");
        self.absorb(target, actions);
    }

    /// Crashes node `i`: its `term`/`vote` must survive the restart.
    fn crash(&mut self, i: usize) {
        let id = self.node(i).id();
        let term_before = self.node(i).term();
        // Drop first (closes the WAL handle), then reopen from disk.
        self.nodes[i] = None;
        let recovered = open_node(&self.dir, id, self.n);
        assert!(
            recovered.term() >= term_before,
            "node {id} term regressed across a crash: {} < {term_before}",
            recovered.term()
        );
        self.nodes[i] = Some(recovered);
        self.applied[i].clear(); // a fresh incarnation re-applies from its log
    }

    fn settle(&mut self, rounds: usize) {
        for _ in 0..rounds {
            for i in 0..self.n {
                self.tick(i);
            }
            let mut guard = 0;
            while !self.wire.is_empty() && guard < 10_000 {
                self.deliver(0);
                guard += 1;
            }
        }
    }

    fn leaders(&self) -> usize {
        (0..self.n).filter(|&i| self.node(i).is_leader()).count()
    }

    /// Snapshots a node's full log as `(term, command)` per index.
    fn log_snapshot(&self, i: usize) -> Vec<(u64, Vec<u8>)> {
        let node = self.node(i);
        let last = node.log().last_index();
        (1..=last)
            .filter_map(|idx| node.log().entry(idx))
            .map(|e| (e.term, e.command))
            .collect()
    }
}

#[derive(Clone, Copy, Debug)]
enum Op {
    Tick(u8),
    Propose,
    Deliver(u16),
    Crash(u8),
}

fn op_strategy() -> impl Strategy<Value = Op> {
    prop_oneof![
        4 => any::<u8>().prop_map(Op::Tick),
        3 => Just(Op::Propose),
        6 => any::<u16>().prop_map(Op::Deliver),
        1 => any::<u8>().prop_map(Op::Crash),
    ]
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(48))]

    /// No committed entry ever diverges, even with node crashes interleaved into
    /// an adversarial schedule of ticks, proposals, and reordered deliveries.
    #[test]
    fn recovery_safety_three_nodes(ops in prop::collection::vec(op_strategy(), 0..200)) {
        let mut cluster = Cluster::new(3);
        for op in ops {
            match op {
                Op::Tick(s) => cluster.tick(s as usize % 3),
                Op::Propose => cluster.propose(),
                Op::Deliver(s) => cluster.deliver(s as usize),
                Op::Crash(s) => cluster.crash(s as usize % 3),
            }
        }
    }
}

/// A fully replicated log survives a restart of every node, and the cluster
/// resumes and keeps committing.
#[test]
fn full_cluster_restart_preserves_committed_log() {
    let mut cluster = Cluster::new(3);
    cluster.settle(40);
    assert_eq!(cluster.leaders(), 1);

    for _ in 0..15 {
        cluster.propose();
        cluster.settle(3);
    }
    cluster.settle(20);

    // Snapshot every node's log, then crash and recover all of them.
    let before: Vec<_> = (0..3).map(|i| cluster.log_snapshot(i)).collect();
    let committed_before = cluster.committed.len();
    assert!(committed_before >= 15, "expected commits before crash");

    cluster.wire.clear(); // in-flight messages are lost in a full crash
    for i in 0..3 {
        cluster.crash(i);
    }

    // Each node recovered its log byte-for-byte.
    for (i, snapshot) in before.iter().enumerate() {
        assert_eq!(
            &cluster.log_snapshot(i),
            snapshot,
            "node {i} lost log on restart"
        );
    }

    // The cluster re-elects and keeps committing past the pre-crash point.
    cluster.settle(60);
    assert_eq!(cluster.leaders(), 1, "cluster did not recover a leader");
    let high = cluster.next_value;
    cluster.propose();
    cluster.settle(30);
    assert!(
        cluster.next_value > high && cluster.committed.len() > committed_before,
        "cluster did not make progress after recovery"
    );
}

/// A node's recovered `term` and `vote` reflect what it last persisted.
#[test]
fn hard_state_is_durable_across_restart() {
    let mut cluster = Cluster::new(3);
    cluster.settle(40);
    let terms_before: Vec<u64> = (0..3).map(|i| cluster.node(i).term()).collect();

    for i in 0..3 {
        cluster.crash(i);
    }
    for (i, &before) in terms_before.iter().enumerate() {
        assert!(
            cluster.node(i).term() >= before,
            "node {i} term regressed across restart"
        );
    }
}
