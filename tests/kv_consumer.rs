//! Application-level safety for a real consumer: a replicated key-value store.
//!
//! The other suites check the protocol — that committed *commands* never diverge.
//! This checks the layer a real application cares about: that the **materialized
//! state machine** is correct and identical on every node, including state
//! reconstructed from a snapshot. A key-value store is driven through writes,
//! partitions, and snapshotting; after the cluster heals and settles, every
//! node's map MUST equal a single-threaded model built by applying the committed
//! command sequence in order.

#![allow(clippy::unwrap_used, clippy::expect_used)]

use std::collections::BTreeMap;

use proptest::prelude::*;
use raft_io::{Action, Event, Message, NodeId, RaftConfig, RaftNode};

/// The application state machine under test.
#[derive(Clone, Default, PartialEq, Eq, Debug)]
struct KvStore {
    map: BTreeMap<String, String>,
}

impl KvStore {
    fn apply(&mut self, command: &[u8]) {
        let text = String::from_utf8_lossy(command);
        let mut parts = text.splitn(3, '\t');
        match parts.next() {
            Some("PUT") => {
                if let (Some(k), Some(v)) = (parts.next(), parts.next()) {
                    let _ = self.map.insert(k.to_owned(), v.to_owned());
                }
            }
            Some("DEL") => {
                if let Some(k) = parts.next() {
                    let _ = self.map.remove(k);
                }
            }
            _ => {}
        }
    }

    fn snapshot(&self) -> Vec<u8> {
        let mut out = String::new();
        for (k, v) in &self.map {
            out.push_str(k);
            out.push('\t');
            out.push_str(v);
            out.push('\n');
        }
        out.into_bytes()
    }

    fn restore(&mut self, data: &[u8]) {
        self.map.clear();
        for line in String::from_utf8_lossy(data).lines() {
            if let Some((k, v)) = line.split_once('\t') {
                let _ = self.map.insert(k.to_owned(), v.to_owned());
            }
        }
    }
}

struct Node {
    id: NodeId,
    raft: RaftNode,
    store: KvStore,
}

struct Cluster {
    nodes: Vec<Node>,
    wire: Vec<(NodeId, NodeId, Message)>,
    side: Option<Vec<bool>>,
    /// The committed command at each index, the source of truth for the model.
    committed: BTreeMap<u64, Vec<u8>>,
    next_key: u64,
    /// `(node index, read index)` for every linearizable read that became ready.
    reads_ready: Vec<(usize, u64)>,
}

impl Cluster {
    fn new(n: usize, snapshot_threshold: usize) -> Self {
        let ids: Vec<NodeId> = (0..n as NodeId).collect();
        let nodes = ids
            .iter()
            .map(|&id| Node {
                id,
                raft: RaftNode::new(
                    RaftConfig::new(id, ids.clone())
                        .with_election_timeout(8, 16)
                        .with_heartbeat_interval(3)
                        .with_max_batch(4)
                        .with_snapshot_threshold(snapshot_threshold)
                        .with_seed(0xF000 + id),
                ),
                store: KvStore::default(),
            })
            .collect();
        Self {
            nodes,
            wire: Vec::new(),
            side: None,
            committed: BTreeMap::new(),
            next_key: 0,
            reads_ready: Vec::new(),
        }
    }

    fn connected(&self, a: NodeId, b: NodeId) -> bool {
        match &self.side {
            None => true,
            Some(side) => side[a as usize] == side[b as usize],
        }
    }

    fn absorb(&mut self, idx: usize, actions: Vec<Action>) {
        for action in actions {
            match action {
                Action::Send { to, message } => self.wire.push((self.nodes[idx].id, to, message)),
                Action::Apply { index, command, .. } => {
                    // Record the committed command (the model's source of truth)
                    // and apply it to this node's store.
                    match self.committed.get(&index) {
                        Some(existing) => {
                            assert_eq!(
                                *existing, command,
                                "divergent committed command at {index}"
                            );
                        }
                        None => {
                            let _ = self.committed.insert(index, command.clone());
                        }
                    }
                    self.nodes[idx].store.apply(&command);
                }
                Action::Snapshot { index, .. } => {
                    let data = self.nodes[idx].store.snapshot();
                    let reply = self.nodes[idx]
                        .raft
                        .step(Event::Snapshot { index, data })
                        .expect("snap");
                    self.absorb(idx, reply);
                }
                Action::RestoreSnapshot { data, .. } => self.nodes[idx].store.restore(&data),
                Action::ReadReady { index, .. } => self.reads_ready.push((idx, index)),
                _ => {}
            }
        }
    }

    fn tick(&mut self, idx: usize) {
        let actions = self.nodes[idx].raft.step(Event::Tick).expect("tick");
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
        if let Some(idx) = self.nodes.iter().position(|n| n.id == to) {
            let actions = self.nodes[idx]
                .raft
                .step(Event::Message(message))
                .expect("msg");
            self.absorb(idx, actions);
        }
    }

    fn write(&mut self) {
        if let Some(idx) = self.nodes.iter().position(|n| n.raft.is_leader()) {
            // Half puts to a small keyspace, occasional deletes — so values are
            // overwritten and removed, exercising real state evolution.
            let key = format!("k{}", self.next_key % 8);
            let command = if self.next_key % 5 == 0 {
                format!("DEL\t{key}").into_bytes()
            } else {
                format!("PUT\t{key}\t{}", self.next_key).into_bytes()
            };
            self.next_key += 1;
            if let Ok(actions) = self.nodes[idx].raft.step(Event::Propose(command)) {
                self.absorb(idx, actions);
            }
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

    /// The model state machine: every committed command applied in index order.
    fn model(&self) -> KvStore {
        let mut model = KvStore::default();
        for command in self.committed.values() {
            model.apply(command);
        }
        model
    }

    /// Drives the (healed) cluster until it is quiescent: one leader, every node
    /// applied to the same index, and no messages in flight. Returns whether it
    /// converged within `cap` rounds.
    ///
    /// Ticks one node per round, round-robin, rather than all at once: real nodes
    /// do not tick in lockstep, and lockstep maximises split votes, so a lockstep
    /// harness would mask convergence the protocol achieves in practice.
    fn settle_until_converged(&mut self, cap: usize) -> bool {
        let n = self.nodes.len();
        for round in 0..cap {
            self.tick(round % n);
            let mut guard = 0;
            while !self.wire.is_empty() && guard < 50_000 {
                self.deliver(0);
                guard += 1;
            }
            let one_leader = self.nodes.iter().filter(|n| n.raft.is_leader()).count() == 1;
            let applied: Vec<u64> = self.nodes.iter().map(|n| n.raft.last_applied()).collect();
            let all_same = applied.windows(2).all(|w| w[0] == w[1]);
            if one_leader && all_same {
                return true;
            }
        }
        false
    }

    /// Issues a linearizable read on the current leader and settles until it is
    /// ready, returning `(node index, read index, the store snapshot served)`.
    /// The store snapshot is the leader's map at the moment the read resolved —
    /// what the application would return to the client.
    fn linearizable_read(&mut self, id: u64) -> Option<(usize, u64, KvStore)> {
        let leader = self.nodes.iter().position(|n| n.raft.is_leader())?;
        self.reads_ready.clear();
        let actions = self.nodes[leader].raft.step(Event::Read { id }).ok()?;
        self.absorb(leader, actions);
        for _ in 0..400 {
            if let Some(&(idx, read_index)) = self.reads_ready.first() {
                return Some((idx, read_index, self.nodes[idx].store.clone()));
            }
            self.settle(1);
        }
        None
    }
}

/// After writes, a partition, healing, and snapshotting, every node's
/// materialized store equals the model — including any node whose state was
/// rebuilt from a snapshot.
#[test]
fn kv_store_converges_to_the_model_after_partition_and_snapshots() {
    let mut cluster = Cluster::new(5, 6);
    cluster.settle(40);

    // Writes, then isolate a minority, more writes (which snapshot), then heal.
    for _ in 0..15 {
        cluster.write();
        cluster.settle(3);
    }
    cluster.side = Some(vec![false, false, false, true, true]);
    for _ in 0..25 {
        cluster.write();
        cluster.settle(3);
    }
    cluster.side = None;
    assert!(
        cluster.settle_until_converged(800),
        "cluster did not converge"
    );

    let model = cluster.model();
    for node in &cluster.nodes {
        assert_eq!(
            node.store, model,
            "node {} diverged from the model",
            node.id
        );
    }
}

/// A linearizable read, served after the cluster has committed a series of
/// writes, reflects every committed write — it returns exactly the model state.
#[test]
fn linearizable_read_reflects_all_committed_writes() {
    let mut cluster = Cluster::new(5, 6);
    cluster.settle(40);
    for _ in 0..20 {
        cluster.write();
        cluster.settle(3);
    }
    assert!(cluster.settle_until_converged(800), "did not converge");

    let committed_before = cluster.committed.keys().next_back().copied().unwrap_or(0);
    let (_node, read_index, served) = cluster
        .linearizable_read(1)
        .expect("the read should resolve on a healthy cluster");
    // The read point covers everything committed before the read was issued.
    assert!(read_index >= committed_before, "read index behind commit");
    // On a quiesced cluster the served state equals the full committed model:
    // the read missed nothing that was committed.
    let model = cluster.model();
    assert_eq!(served, model, "linearizable read returned stale state");
}

/// Read-your-writes across a partition heal: after a minority is isolated, the
/// majority commits writes, the cluster heals, and a linearizable read then
/// reflects those writes (never the pre-partition state).
#[test]
fn linearizable_read_after_partition_sees_majority_writes() {
    let mut cluster = Cluster::new(5, 8);
    cluster.settle(40);
    for _ in 0..10 {
        cluster.write();
        cluster.settle(3);
    }
    // Isolate a minority; the majority keeps committing.
    cluster.side = Some(vec![false, false, false, true, true]);
    for _ in 0..15 {
        cluster.write();
        cluster.settle(3);
    }
    cluster.side = None;
    assert!(cluster.settle_until_converged(800), "did not converge");

    let (_node, _index, served) = cluster.linearizable_read(99).expect("read resolves");
    let model = cluster.model();
    assert_eq!(served, model, "read did not reflect the majority's writes");
}

#[derive(Clone, Copy, Debug)]
enum Op {
    Tick(u8),
    Write,
    Deliver(u16),
    Partition(u8),
    Heal,
}

fn op_strategy() -> impl Strategy<Value = Op> {
    prop_oneof![
        5 => any::<u8>().prop_map(Op::Tick),
        4 => Just(Op::Write),
        7 => any::<u16>().prop_map(Op::Deliver),
        2 => any::<u8>().prop_map(Op::Partition),
        2 => Just(Op::Heal),
    ]
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(128))]

    /// However the schedule runs, once healed and settled every node's key-value
    /// store equals the model built from the committed command sequence.
    #[test]
    fn kv_store_always_converges(ops in prop::collection::vec(op_strategy(), 0..400)) {
        let mut cluster = Cluster::new(3, 5);
        for op in ops {
            match op {
                Op::Tick(s) => cluster.tick(s as usize % 3),
                Op::Write => cluster.write(),
                Op::Deliver(s) => cluster.deliver(s as usize),
                Op::Partition(m) => {
                    let side: Vec<bool> = (0..3).map(|i| (m >> i) & 1 == 1).collect();
                    cluster.side = if side.iter().all(|&s| s) || side.iter().all(|&s| !s) {
                        None
                    } else {
                        Some(side)
                    };
                }
                Op::Heal => cluster.side = None,
            }
        }
        // Heal and settle to quiescence, then every node must match the model.
        cluster.side = None;
        let converged = cluster.settle_until_converged(3000);
        prop_assert!(converged, "cluster did not reach quiescence");
        let model = cluster.model();
        for node in &cluster.nodes {
            prop_assert_eq!(&node.store, &model, "node {} diverged from the model", node.id);
        }
        // A linearizable read on the healed cluster is never stale: its read
        // index covers every entry committed before the read was issued —
        // whatever schedule of partitions and reorderings preceded it.
        let committed_before = cluster.committed.keys().next_back().copied().unwrap_or(0);
        if let Some((_node, read_index, _served)) = cluster.linearizable_read(1) {
            prop_assert!(
                read_index >= committed_before,
                "read index {} is behind the committed point {}",
                read_index,
                committed_before
            );
        }
    }
}
