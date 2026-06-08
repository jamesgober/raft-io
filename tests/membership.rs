//! Membership-change and leadership-transfer tests over a deterministic cluster.
//!
//! These exercise the v0.6 exit criterion — add and remove a server under load
//! without losing quorum safety — plus leadership transfer. The harness supports
//! a *dynamic* set of nodes: adding a voter creates a fresh node mid-run, and the
//! cluster routes only to nodes that exist.
//!
//! Throughout, the same safety invariant holds as in the other suites: no two
//! nodes ever apply a different command at the same index, across configuration
//! changes and leader changes. Configuration entries are protocol bookkeeping and
//! are not applied, so applied indices may skip them — the check requires strictly
//! increasing applied indices, not contiguous ones.

#![allow(clippy::unwrap_used, clippy::expect_used)]

use std::collections::BTreeMap;

use proptest::prelude::*;
use raft_io::{Action, Event, Message, NodeId, RaftConfig, RaftNode};

struct Cluster {
    /// Live nodes, by id. A removed node is kept (it simply follows) unless the
    /// test drops it.
    nodes: Vec<(NodeId, RaftNode)>,
    wire: Vec<(NodeId, NodeId, Message)>,
    /// Agreed command at each applied index, across all nodes.
    committed: BTreeMap<u64, Vec<u8>>,
    /// Highest index each node has applied, by id.
    applied: BTreeMap<NodeId, u64>,
    next_value: u64,
}

impl Cluster {
    fn new(n: usize) -> Self {
        let ids: Vec<NodeId> = (0..n as NodeId).collect();
        let nodes = ids
            .iter()
            .map(|&id| {
                let cfg = RaftConfig::new(id, ids.clone())
                    .with_election_timeout(10, 20)
                    .with_heartbeat_interval(3)
                    .with_seed(0xB000 + id);
                (id, RaftNode::new(cfg))
            })
            .collect();
        Self {
            nodes,
            wire: Vec::new(),
            committed: BTreeMap::new(),
            applied: BTreeMap::new(),
            next_value: 1,
        }
    }

    fn exists(&self, id: NodeId) -> bool {
        self.nodes.iter().any(|(i, _)| *i == id)
    }

    fn index_of(&self, id: NodeId) -> Option<usize> {
        self.nodes.iter().position(|(i, _)| *i == id)
    }

    fn absorb(&mut self, from: NodeId, actions: Vec<Action>) {
        for action in actions {
            match action {
                Action::Send { to, message } if self.exists(to) => {
                    self.wire.push((from, to, message));
                }
                Action::Apply { index, command, .. } => {
                    let through = self.applied.entry(from).or_insert(0);
                    assert!(
                        index > *through,
                        "node {from} applied index {index} out of order (through {through})"
                    );
                    *through = index;
                    match self.committed.get(&index) {
                        Some(existing) => {
                            assert_eq!(*existing, command, "divergent committed entry at {index}");
                        }
                        None => {
                            let _ = self.committed.insert(index, command);
                        }
                    }
                }
                _ => {}
            }
        }
    }

    fn tick(&mut self, idx: usize) {
        let (id, node) = &mut self.nodes[idx];
        let id = *id;
        let actions = node.step(Event::Tick).expect("tick");
        self.absorb(id, actions);
    }

    fn deliver(&mut self, pick: usize) {
        if self.wire.is_empty() {
            return;
        }
        let w = pick % self.wire.len();
        let (_from, to, message) = self.wire.remove(w);
        if let Some(idx) = self.index_of(to) {
            let actions = self.nodes[idx]
                .1
                .step(Event::Message(message))
                .expect("message");
            self.absorb(to, actions);
        }
    }

    fn settle(&mut self, rounds: usize) {
        for _ in 0..rounds {
            for i in 0..self.nodes.len() {
                self.tick(i);
            }
            let mut guard = 0;
            while !self.wire.is_empty() && guard < 50_000 {
                self.deliver(0);
                guard += 1;
            }
        }
    }

    fn leader(&self) -> Option<NodeId> {
        self.nodes
            .iter()
            .find(|(_, n)| n.is_leader())
            .map(|(i, _)| *i)
    }

    fn propose(&mut self) {
        if let Some(idx) = self.nodes.iter().position(|(_, n)| n.is_leader()) {
            let id = self.nodes[idx].0;
            let value = self.next_value.to_be_bytes().to_vec();
            self.next_value += 1;
            if let Ok(actions) = self.nodes[idx].1.step(Event::Propose(value)) {
                self.absorb(id, actions);
            }
        }
    }

    /// Creates a fresh node `new_id` and asks the current leader to add it.
    fn add_voter(&mut self, new_id: NodeId) {
        let mut members: Vec<NodeId> = self.nodes.iter().map(|(i, _)| *i).collect();
        members.push(new_id);
        // The new node knows the cluster it is joining; a long election timeout
        // keeps it from disrupting before the leader reaches it.
        let cfg = RaftConfig::new(new_id, members)
            .with_election_timeout(60, 80)
            .with_heartbeat_interval(3)
            .with_seed(0xB000 + new_id);
        self.nodes.push((new_id, RaftNode::new(cfg)));
        if let Some(idx) = self.nodes.iter().position(|(_, n)| n.is_leader()) {
            let id = self.nodes[idx].0;
            if let Ok(actions) = self.nodes[idx].1.step(Event::AddServer(new_id)) {
                self.absorb(id, actions);
            }
        }
    }

    /// Asks the leader to remove `victim`.
    fn remove_voter(&mut self, victim: NodeId) {
        if let Some(idx) = self.nodes.iter().position(|(_, n)| n.is_leader()) {
            let id = self.nodes[idx].0;
            if let Ok(actions) = self.nodes[idx].1.step(Event::RemoveServer(victim)) {
                self.absorb(id, actions);
            }
        }
    }

    fn members_of(&self, id: NodeId) -> Vec<NodeId> {
        self.index_of(id)
            .map(|i| self.nodes[i].1.members().to_vec())
            .unwrap_or_default()
    }
}

/// Add a server, then remove a different one, all while the cluster is taking
/// proposals — and confirm the membership converges and nothing diverges.
#[test]
fn add_and_remove_under_load() {
    let mut cluster = Cluster::new(3);
    cluster.settle(40);
    let leader = cluster.leader().expect("a leader");

    for _ in 0..15 {
        cluster.propose();
        cluster.settle(2);
    }

    // Add node 3 and let it catch up.
    cluster.add_voter(3);
    cluster.settle(80);
    for id in [0, 1, 2, 3] {
        assert_eq!(
            cluster.members_of(id),
            vec![0, 1, 2, 3],
            "node {id} missed the add"
        );
    }

    for _ in 0..15 {
        cluster.propose();
        cluster.settle(2);
    }

    // Remove a follower (not the leader) and let the change propagate.
    let victim = [0, 1, 2].into_iter().find(|&v| v != leader).unwrap();
    cluster.remove_voter(victim);
    cluster.settle(80);
    let expected: Vec<NodeId> = [0, 1, 2, 3].into_iter().filter(|&v| v != victim).collect();
    for id in [0, 1, 2, 3].into_iter().filter(|&v| v != victim) {
        assert_eq!(
            cluster.members_of(id),
            expected,
            "node {id} missed the remove"
        );
    }

    // The cluster keeps committing after both changes.
    let before = cluster.committed.len();
    for _ in 0..15 {
        cluster.propose();
        cluster.settle(2);
    }
    cluster.settle(20);
    assert!(
        cluster.committed.len() > before,
        "cluster stalled after reconfiguration"
    );
}

/// A leader hands off to a chosen follower, which takes over.
#[test]
fn leadership_transfers_to_target() {
    let mut cluster = Cluster::new(3);
    cluster.settle(40);
    let old = cluster.leader().expect("a leader");
    let target = (0..3).find(|&t| t != old).unwrap();

    let idx = cluster.index_of(old).unwrap();
    let actions = cluster.nodes[idx]
        .1
        .step(Event::TransferLeadership(target))
        .unwrap();
    cluster.absorb(old, actions);
    cluster.settle(40);

    assert_eq!(
        cluster.leader(),
        Some(target),
        "leadership did not transfer to the target"
    );
}

#[derive(Clone, Copy, Debug)]
enum Op {
    Tick(u8),
    Propose,
    Deliver(u16),
    AddServer,
    RemoveServer(u8),
}

fn op_strategy() -> impl Strategy<Value = Op> {
    prop_oneof![
        5 => any::<u8>().prop_map(Op::Tick),
        3 => Just(Op::Propose),
        7 => any::<u16>().prop_map(Op::Deliver),
        1 => Just(Op::AddServer),
        1 => any::<u8>().prop_map(Op::RemoveServer),
    ]
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(96))]

    /// Committed entries never diverge while servers are added and removed amid a
    /// randomised schedule. Membership is kept to a small range so a quorum always
    /// remains.
    #[test]
    fn membership_churn_never_diverges(ops in prop::collection::vec(op_strategy(), 0..400)) {
        let mut cluster = Cluster::new(3);
        // Reserve ids 3..=5 for additions.
        let mut next_add = 3u64;
        for op in ops {
            match op {
                Op::Tick(s) => {
                    let n = cluster.nodes.len();
                    if n > 0 {
                        cluster.tick(s as usize % n);
                    }
                }
                Op::Propose => cluster.propose(),
                Op::Deliver(s) => cluster.deliver(s as usize),
                Op::AddServer => {
                    // Cap growth so the test stays bounded.
                    if next_add <= 5 && !cluster.exists(next_add) {
                        cluster.add_voter(next_add);
                        next_add += 1;
                    }
                }
                Op::RemoveServer(s) => {
                    // Never shrink below three voters, so a quorum always exists.
                    let leader = cluster.leader();
                    let voters: Vec<NodeId> = leader
                        .map(|l| cluster.members_of(l))
                        .unwrap_or_default();
                    if voters.len() > 3 {
                        let victim = voters[s as usize % voters.len()];
                        if Some(victim) != leader {
                            cluster.remove_voter(victim);
                        }
                    }
                }
            }
        }
    }
}
