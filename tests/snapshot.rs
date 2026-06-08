//! Snapshot and log-compaction tests over a deterministic cluster.
//!
//! These exercise the v0.5 exit criteria:
//!
//! - **Catch-up via snapshot, then tail** — a follower that has fallen so far
//!   behind that the leader has compacted the entries it needs is brought current
//!   by an `InstallSnapshot`, then resumes normal replication.
//! - **Compaction never loses committed state** — even with snapshots taken
//!   throughout an adversarial schedule, no two nodes ever apply a different
//!   command at the same index, and a node restored from a snapshot agrees with
//!   the rest.
//!
//! The harness drives the snapshot policy end to end: when a node emits an
//! [`Action::Snapshot`] hint, the harness serializes a deterministic "state"
//! (the covered index) and feeds it back as an [`Event::Snapshot`]; when a node
//! emits [`Action::RestoreSnapshot`], the harness checks the restored state is
//! self-consistent and advances that node's applied position.

#![allow(clippy::unwrap_used, clippy::expect_used)]

use std::collections::BTreeMap;

use proptest::prelude::*;
use raft_io::{Action, Event, Message, NodeId, RaftConfig, RaftLog, RaftNode};

/// Encodes the application "state" captured by a snapshot through `index`.
fn snapshot_state(index: u64) -> Vec<u8> {
    index.to_be_bytes().to_vec()
}

struct Cluster {
    nodes: Vec<RaftNode>,
    wire: Vec<(NodeId, NodeId, Message)>,
    side: Option<Vec<bool>>,
    /// Highest index each node has applied or restored, for ordering checks.
    applied_through: Vec<u64>,
    /// Whether each node has been caught up by a snapshot at least once.
    restored: Vec<bool>,
    /// Agreed command at each applied index, across all nodes.
    committed: BTreeMap<u64, Vec<u8>>,
    next_value: u64,
}

impl Cluster {
    fn new(n: usize, snapshot_threshold: usize) -> Self {
        let ids: Vec<NodeId> = (0..n as NodeId).collect();
        let nodes = ids
            .iter()
            .map(|&id| {
                let cfg = RaftConfig::new(id, ids.clone())
                    .with_election_timeout(10, 20)
                    .with_heartbeat_interval(3)
                    .with_max_batch(4)
                    .with_snapshot_threshold(snapshot_threshold)
                    .with_seed(0x9000 + id);
                RaftNode::new(cfg)
            })
            .collect();
        Self {
            nodes,
            wire: Vec::new(),
            side: None,
            applied_through: vec![0; n],
            restored: vec![false; n],
            committed: BTreeMap::new(),
            next_value: 1,
        }
    }

    fn n(&self) -> usize {
        self.nodes.len()
    }

    fn connected(&self, a: NodeId, b: NodeId) -> bool {
        match &self.side {
            None => true,
            Some(side) => side[a as usize] == side[b as usize],
        }
    }

    fn absorb(&mut self, i: usize, actions: Vec<Action>) {
        let from = self.nodes[i].id();
        for action in actions {
            match action {
                Action::Send { to, message } => self.wire.push((from, to, message)),
                Action::Apply { index, command, .. } => {
                    assert_eq!(
                        index,
                        self.applied_through[i] + 1,
                        "node {from} applied index {index} out of order (through {})",
                        self.applied_through[i]
                    );
                    self.applied_through[i] = index;
                    match self.committed.get(&index) {
                        Some(existing) => {
                            assert_eq!(*existing, command, "divergent committed entry at {index}");
                        }
                        None => {
                            let _ = self.committed.insert(index, command);
                        }
                    }
                }
                Action::Snapshot { index, .. } => {
                    // Respond to the policy hint with the serialized state.
                    let reply = self.nodes[i]
                        .step(Event::Snapshot {
                            index,
                            data: snapshot_state(index),
                        })
                        .expect("snapshot event");
                    self.absorb(i, reply); // yields no further actions
                }
                Action::RestoreSnapshot { index, data, .. } => {
                    assert_eq!(data, snapshot_state(index), "restored state is corrupt");
                    assert!(
                        index >= self.applied_through[i],
                        "snapshot moved node {from} backwards"
                    );
                    self.applied_through[i] = index;
                    self.restored[i] = true;
                }
                _ => {}
            }
        }
    }

    fn tick(&mut self, i: usize) {
        let actions = self.nodes[i].step(Event::Tick).expect("tick");
        self.absorb(i, actions);
    }

    fn propose(&mut self) {
        if let Some(i) = (0..self.n()).find(|&i| self.nodes[i].is_leader()) {
            let value = self.next_value.to_be_bytes().to_vec();
            self.next_value += 1;
            if let Ok(actions) = self.nodes[i].step(Event::Propose(value)) {
                self.absorb(i, actions);
            }
        }
    }

    fn deliver(&mut self, pick: usize) {
        if self.wire.is_empty() {
            return;
        }
        let idx = pick % self.wire.len();
        let (from, to, message) = self.wire.remove(idx);
        if !self.connected(from, to) {
            return;
        }
        let target = to as usize;
        let actions = self.nodes[target]
            .step(Event::Message(message))
            .expect("message");
        self.absorb(target, actions);
    }

    fn settle(&mut self, rounds: usize) {
        for _ in 0..rounds {
            for i in 0..self.n() {
                self.tick(i);
            }
            let mut guard = 0;
            while !self.wire.is_empty() && guard < 20_000 {
                self.deliver(0);
                guard += 1;
            }
        }
    }

    fn leader(&self) -> Option<usize> {
        (0..self.n()).find(|&i| self.nodes[i].is_leader())
    }
}

/// A follower kept offline while the leader compacts its log is later caught up
/// by a snapshot, then by tail replication.
#[test]
fn lagging_follower_catches_up_via_snapshot() {
    let mut cluster = Cluster::new(3, 4);
    cluster.settle(40);
    let leader = cluster.leader().expect("a leader");
    // Isolate node 2 from the other two.
    let mut side = vec![false; 3];
    side[2] = true;
    cluster.side = Some(side);

    // Commit and snapshot a long run on the majority side; node 2 sees none of it.
    for _ in 0..40 {
        cluster.propose();
        cluster.settle(2);
    }
    cluster.settle(20);

    // The leader has compacted well past the start, so node 2's next entry is gone.
    let snap_index = cluster.nodes[leader].log().snapshot_index();
    assert!(snap_index > 0, "leader should have taken a snapshot");
    assert_eq!(
        cluster.applied_through[2], 0,
        "isolated node applied nothing"
    );

    // Heal and let node 2 catch up — it must do so via a snapshot.
    cluster.side = None;
    cluster.settle(80);

    assert!(
        cluster.restored[2],
        "node 2 should have caught up via a snapshot"
    );
    let leader_commit = cluster.nodes[leader].commit_index();
    assert_eq!(
        cluster.applied_through[2], leader_commit,
        "node 2 did not fully catch up"
    );
}

/// Compaction never drops committed-but-unapplied entries: a node only snapshots
/// through an index it has applied, which is at or below the commit index.
#[test]
fn snapshot_never_exceeds_applied() {
    let mut cluster = Cluster::new(3, 3);
    cluster.settle(40);
    for _ in 0..30 {
        cluster.propose();
        cluster.settle(2);
    }
    cluster.settle(20);
    for i in 0..3 {
        let snap = cluster.nodes[i].log().snapshot_index();
        let commit = cluster.nodes[i].commit_index();
        assert!(
            snap <= commit,
            "node {i} snapshot {snap} exceeded commit {commit}"
        );
        assert!(
            snap <= cluster.applied_through[i],
            "node {i} snapshot exceeded applied"
        );
    }
}

#[derive(Clone, Copy, Debug)]
enum Op {
    Tick(u8),
    Propose,
    Deliver(u16),
    Partition(u8),
    Heal,
}

fn op_strategy() -> impl Strategy<Value = Op> {
    prop_oneof![
        4 => any::<u8>().prop_map(Op::Tick),
        3 => Just(Op::Propose),
        6 => any::<u16>().prop_map(Op::Deliver),
        1 => any::<u8>().prop_map(Op::Partition),
        1 => Just(Op::Heal),
    ]
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(128))]

    /// With snapshots taken throughout an adversarial schedule, committed entries
    /// never diverge across the cluster.
    #[test]
    fn snapshotting_cluster_never_diverges(ops in prop::collection::vec(op_strategy(), 0..500)) {
        let mut cluster = Cluster::new(3, 4);
        for op in ops {
            match op {
                Op::Tick(s) => cluster.tick(s as usize % 3),
                Op::Propose => cluster.propose(),
                Op::Deliver(s) => cluster.deliver(s as usize),
                Op::Partition(m) => {
                    let side: Vec<bool> = (0..3).map(|i| (m >> i) & 1 == 1).collect();
                    if side.iter().all(|&s| s) || side.iter().all(|&s| !s) {
                        cluster.side = None;
                    } else {
                        cluster.side = Some(side);
                    }
                }
                Op::Heal => cluster.side = None,
            }
        }
    }
}
