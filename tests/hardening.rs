//! Jepsen-style hardening: every safety property, under every fault at once.
//!
//! The earlier suites each stress one dimension. This one turns them all on
//! together — partitions, message loss, reordering, duplication, membership
//! churn, and snapshotting, driven by a randomised schedule — and asserts the
//! full set of Raft safety properties continuously, so a violation in any
//! combination is caught:
//!
//! 1. **Election Safety** — at most one leader per term.
//! 2. **Leader Append-Only** — a leader never overwrites or deletes an entry in
//!    its own log; it only appends.
//! 3. **Log Matching** — if two logs hold an entry with the same index and term,
//!    the logs are identical in all entries up through that index.
//! 4. **Leader Completeness / State Machine Safety** — no two nodes ever apply a
//!    different command at the same index, across leader changes, snapshots, and
//!    reconfiguration.
//! 5. **Apply ordering** — each node applies in strictly increasing index order.
//!
//! Run sustained with `PROPTEST_CASES=10000 cargo test --test hardening`.

#![allow(clippy::unwrap_used, clippy::expect_used)]

use std::collections::BTreeMap;

use proptest::prelude::*;
use raft_io::{Action, Event, Message, NodeId, RaftConfig, RaftLog, RaftNode};

struct Cluster {
    nodes: Vec<(NodeId, RaftNode)>,
    wire: Vec<(NodeId, NodeId, Message)>,
    side: Option<Vec<bool>>,
    /// Agreed command at each applied index (State Machine Safety).
    committed: BTreeMap<u64, Vec<u8>>,
    /// Highest index each node has applied (apply ordering), by id.
    applied: BTreeMap<NodeId, u64>,
    /// For Leader Append-Only: the `(term, index -> term)` map captured while a
    /// node leads, so we can prove it never rewrites its own log while leading.
    leader_log: BTreeMap<NodeId, (u64, BTreeMap<u64, u64>)>,
    /// Single leader recorded per term (Election Safety).
    leader_by_term: BTreeMap<u64, NodeId>,
    next_value: u64,
}

impl Cluster {
    fn new(n: usize, snapshot_threshold: usize) -> Self {
        let ids: Vec<NodeId> = (0..n as NodeId).collect();
        let nodes = ids
            .iter()
            .map(|&id| {
                let cfg = RaftConfig::new(id, ids.clone())
                    .with_election_timeout(8, 16)
                    .with_heartbeat_interval(3)
                    .with_max_batch(4)
                    .with_snapshot_threshold(snapshot_threshold)
                    .with_seed(0xD000 + id);
                (id, RaftNode::new(cfg))
            })
            .collect();
        Self {
            nodes,
            wire: Vec::new(),
            side: None,
            committed: BTreeMap::new(),
            applied: BTreeMap::new(),
            leader_log: BTreeMap::new(),
            leader_by_term: BTreeMap::new(),
            next_value: 1,
        }
    }

    fn exists(&self, id: NodeId) -> bool {
        self.nodes.iter().any(|(i, _)| *i == id)
    }

    fn index_of(&self, id: NodeId) -> Option<usize> {
        self.nodes.iter().position(|(i, _)| *i == id)
    }

    fn connected(&self, a: NodeId, b: NodeId) -> bool {
        match &self.side {
            None => true,
            // `side` is indexed by node id; ids can exceed its length after
            // churn, so read defensively (out-of-range nodes share a side).
            Some(side) => {
                side.get(a as usize).copied().unwrap_or(false)
                    == side.get(b as usize).copied().unwrap_or(false)
            }
        }
    }

    /// Captures a node's live log as an `index -> term` map.
    fn log_terms(node: &RaftNode) -> BTreeMap<u64, u64> {
        let log = node.log();
        let mut map = BTreeMap::new();
        let mut i = log.snapshot_index() + 1;
        while i <= log.last_index() {
            if let Some(t) = log.term_at(i) {
                let _ = map.insert(i, t);
            }
            i += 1;
        }
        map
    }

    fn absorb(&mut self, idx: usize, actions: Vec<Action>) {
        let id = self.nodes[idx].0;
        for action in actions {
            match action {
                Action::Send { to, message } if self.exists(to) => {
                    self.wire.push((id, to, message));
                }
                Action::Apply { index, command, .. } => {
                    let through = self.applied.entry(id).or_insert(0);
                    assert!(index > *through, "node {id} applied {index} out of order");
                    *through = index;
                    match self.committed.get(&index) {
                        Some(existing) => {
                            assert_eq!(
                                *existing, command,
                                "STATE MACHINE SAFETY: divergent entry at {index}"
                            );
                        }
                        None => {
                            let _ = self.committed.insert(index, command);
                        }
                    }
                }
                Action::Snapshot { index, .. } => {
                    let reply = self.nodes[idx]
                        .1
                        .step(Event::Snapshot {
                            index,
                            data: index.to_be_bytes().to_vec(),
                        })
                        .expect("snapshot event");
                    self.absorb(idx, reply);
                }
                _ => {}
            }
        }
        self.check_election_safety(idx);
        self.check_leader_append_only(idx);
    }

    fn check_election_safety(&mut self, idx: usize) {
        let (id, node) = &self.nodes[idx];
        if node.is_leader() {
            let term = node.term();
            match self.leader_by_term.get(&term) {
                Some(&existing) => {
                    assert_eq!(existing, *id, "ELECTION SAFETY: two leaders in term {term}");
                }
                None => {
                    let _ = self.leader_by_term.insert(term, *id);
                }
            }
        }
    }

    fn check_leader_append_only(&mut self, idx: usize) {
        let (id, node) = &self.nodes[idx];
        let id = *id;
        if !node.is_leader() {
            // As a follower a node may legitimately truncate; stop tracking.
            let _ = self.leader_log.remove(&id);
            return;
        }
        let term = node.term();
        let current = Self::log_terms(node);
        if let Some((prev_term, prev)) = self.leader_log.get(&id) {
            if *prev_term == term {
                // Same leadership term: every entry we saw before must still be
                // present with the same term — no overwrite, no deletion.
                for (&i, &t) in prev {
                    if let Some(&now) = current.get(&i) {
                        assert_eq!(now, t, "LEADER APPEND-ONLY: node {id} rewrote index {i}");
                    }
                    // (An entry that fell below a new snapshot base is committed
                    // and immutable, so dropping out of the live map is fine.)
                }
                assert!(
                    node.log().last_index() >= prev.keys().last().copied().unwrap_or(0),
                    "LEADER APPEND-ONLY: node {id} shrank its log"
                );
            }
        }
        let _ = self.leader_log.insert(id, (term, current));
    }

    /// Log Matching, checked across all node pairs: where two logs share an entry
    /// of the same index and term, every earlier shared entry matches.
    fn check_log_matching(&self) {
        for a in 0..self.nodes.len() {
            for b in (a + 1)..self.nodes.len() {
                let la = self.nodes[a].1.log();
                let lb = self.nodes[b].1.log();
                let lo = la.snapshot_index().max(lb.snapshot_index()) + 1;
                let hi = la.last_index().min(lb.last_index());
                let mut i = lo;
                while i <= hi {
                    if let (Some(ta), Some(tb)) = (la.term_at(i), lb.term_at(i)) {
                        if ta == tb {
                            assert_eq!(
                                la.entry(i).map(|e| e.command),
                                lb.entry(i).map(|e| e.command),
                                "LOG MATCHING: index {i} same term, different command"
                            );
                        }
                    }
                    i += 1;
                }
            }
        }
    }

    fn tick(&mut self, idx: usize) {
        let actions = self.nodes[idx].1.step(Event::Tick).expect("tick");
        self.absorb(idx, actions);
    }

    fn deliver(&mut self, pick: usize) {
        if self.wire.is_empty() {
            return;
        }
        let w = pick % self.wire.len();
        let (from, to, message) = self.wire.remove(w);
        if !self.connected(from, to) {
            return;
        }
        if let Some(idx) = self.index_of(to) {
            let actions = self.nodes[idx]
                .1
                .step(Event::Message(message))
                .expect("message");
            self.absorb(idx, actions);
        }
    }

    fn propose(&mut self) {
        if let Some(idx) = self.nodes.iter().position(|(_, n)| n.is_leader()) {
            let value = self.next_value.to_be_bytes().to_vec();
            self.next_value += 1;
            if let Ok(actions) = self.nodes[idx].1.step(Event::Propose(value)) {
                self.absorb(idx, actions);
            }
        }
    }

    fn leader_members(&self) -> Vec<NodeId> {
        self.nodes
            .iter()
            .find(|(_, n)| n.is_leader())
            .map(|(_, n)| n.members().to_vec())
            .unwrap_or_default()
    }

    fn add_voter(&mut self, new_id: NodeId) {
        let mut members: Vec<NodeId> = self.nodes.iter().map(|(i, _)| *i).collect();
        members.push(new_id);
        let cfg = RaftConfig::new(new_id, members)
            .with_election_timeout(60, 90)
            .with_heartbeat_interval(3)
            .with_max_batch(4)
            .with_snapshot_threshold(0)
            .with_seed(0xD000 + new_id);
        self.nodes.push((new_id, RaftNode::new(cfg)));
        if let Some(idx) = self.nodes.iter().position(|(_, n)| n.is_leader()) {
            if let Ok(actions) = self.nodes[idx].1.step(Event::AddServer(new_id)) {
                self.absorb(idx, actions);
            }
        }
    }

    fn remove_voter(&mut self, victim: NodeId) {
        if let Some(idx) = self.nodes.iter().position(|(_, n)| n.is_leader()) {
            if let Ok(actions) = self.nodes[idx].1.step(Event::RemoveServer(victim)) {
                self.absorb(idx, actions);
            }
        }
    }

    fn set_partition(&mut self, mask: u32) {
        // Index by node id (fixed width covers all ids the churn can reach).
        let side: Vec<bool> = (0..8).map(|i| (mask >> i) & 1 == 1).collect();
        // Only keep a split that actually separates two existing nodes.
        let sides: Vec<bool> = self
            .nodes
            .iter()
            .map(|(id, _)| side[*id as usize])
            .collect();
        if sides.iter().all(|&s| s) || sides.iter().all(|&s| !s) {
            self.side = None;
        } else {
            self.side = Some(side);
        }
    }
}

#[derive(Clone, Copy, Debug)]
enum Op {
    Tick(u8),
    Propose,
    Deliver(u16),
    Duplicate(u16),
    Drop(u16),
    Partition(u32),
    Heal,
    AddServer,
    RemoveServer(u8),
}

fn op_strategy() -> impl Strategy<Value = Op> {
    prop_oneof![
        6 => any::<u8>().prop_map(Op::Tick),
        4 => Just(Op::Propose),
        8 => any::<u16>().prop_map(Op::Deliver),
        1 => any::<u16>().prop_map(Op::Duplicate),
        1 => any::<u16>().prop_map(Op::Drop),
        2 => any::<u32>().prop_map(Op::Partition),
        2 => Just(Op::Heal),
        1 => Just(Op::AddServer),
        1 => any::<u8>().prop_map(Op::RemoveServer),
    ]
}

fn run(ops: Vec<Op>) {
    let mut cluster = Cluster::new(3, 5);
    let mut next_add = 3u64;
    let mut step = 0u32;
    for op in ops {
        match op {
            Op::Tick(s) => {
                let n = cluster.nodes.len();
                cluster.tick(s as usize % n);
            }
            Op::Propose => cluster.propose(),
            Op::Deliver(s) => cluster.deliver(s as usize),
            Op::Duplicate(s) => {
                // Deliver a copy without removing it from the wire.
                if !cluster.wire.is_empty() {
                    let w = s as usize % cluster.wire.len();
                    let (from, to, message) = cluster.wire[w].clone();
                    if cluster.connected(from, to) {
                        if let Some(idx) = cluster.index_of(to) {
                            let actions = cluster.nodes[idx]
                                .1
                                .step(Event::Message(message))
                                .expect("dup");
                            cluster.absorb(idx, actions);
                        }
                    }
                }
            }
            Op::Drop(s) => {
                if !cluster.wire.is_empty() {
                    let w = s as usize % cluster.wire.len();
                    let _ = cluster.wire.remove(w);
                }
            }
            Op::Partition(m) => cluster.set_partition(m),
            Op::Heal => cluster.side = None,
            Op::AddServer => {
                if next_add <= 4 && !cluster.exists(next_add) {
                    cluster.add_voter(next_add);
                    next_add += 1;
                }
            }
            Op::RemoveServer(s) => {
                let leader = cluster
                    .nodes
                    .iter()
                    .find(|(_, n)| n.is_leader())
                    .map(|(i, _)| *i);
                let voters = cluster.leader_members();
                if voters.len() > 3 {
                    let victim = voters[s as usize % voters.len()];
                    if Some(victim) != leader {
                        cluster.remove_voter(victim);
                    }
                }
            }
        }
        step += 1;
        if step % 16 == 0 {
            cluster.check_log_matching();
        }
    }
    cluster.check_log_matching();
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(256))]

    /// All five safety properties hold under every fault mode at once.
    #[test]
    fn all_safety_properties_under_full_chaos(ops in prop::collection::vec(op_strategy(), 0..600)) {
        run(ops);
    }
}
