//! A replicated key-value store built on `raft-io` — the library's first real
//! consumer, end to end.
//!
//! This is what the sans-I/O core is *for*: the application supplies the state
//! machine (a `KvStore`) and a transport (here, in-memory mailboxes), and drives
//! the node with `step`. Committed commands arrive as `Action::Apply` and are
//! decoded and applied; the snapshot hooks (`Action::Snapshot` /
//! `Action::RestoreSnapshot`) serialize and restore the store so a far-behind or
//! freshly-added node can catch up. The demo elects a leader, replicates a series
//! of writes so every node converges, then adds a fourth node that catches up via
//! a snapshot and ends with the identical state.
//!
//! Run it with:
//!
//! ```text
//! cargo run --example kv_store
//! ```

use std::collections::{BTreeMap, VecDeque};

use raft_io::Message;
use raft_io::prelude::*;

// ---- the application state machine ---------------------------------------

/// A simple key-value store. Commands are tab-separated text: `PUT\tkey\tvalue`
/// or `DEL\tkey`. Snapshots are the map serialized as `key\tvalue` lines.
#[derive(Default)]
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

    fn get(&self, key: &str) -> Option<&String> {
        self.map.get(key)
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

fn put(key: &str, value: &str) -> Vec<u8> {
    format!("PUT\t{key}\t{value}").into_bytes()
}

fn del(key: &str) -> Vec<u8> {
    format!("DEL\t{key}").into_bytes()
}

// ---- a node: a Raft node plus its state machine --------------------------

struct KvNode {
    id: NodeId,
    raft: RaftNode,
    store: KvStore,
}

// ---- the cluster harness (in-memory transport) ---------------------------

struct Cluster {
    nodes: Vec<KvNode>,
    mailboxes: Vec<(NodeId, VecDeque<Message>)>,
}

impl Cluster {
    fn new(ids: &[NodeId], snapshot_threshold: usize) -> Self {
        let nodes = ids
            .iter()
            .map(|&id| KvNode {
                id,
                raft: RaftNode::new(
                    RaftConfig::new(id, ids.to_vec())
                        .with_snapshot_threshold(snapshot_threshold)
                        .with_seed(0xE000 + id),
                ),
                store: KvStore::default(),
            })
            .collect();
        let mailboxes = ids.iter().map(|&id| (id, VecDeque::new())).collect();
        Self { nodes, mailboxes }
    }

    fn mailbox(&mut self, id: NodeId) -> Option<&mut VecDeque<Message>> {
        self.mailboxes
            .iter_mut()
            .find(|(i, _)| *i == id)
            .map(|(_, m)| m)
    }

    /// Carries out the actions a node returned: route messages, apply committed
    /// commands to the state machine, and answer the snapshot hooks.
    fn absorb(&mut self, node_idx: usize, actions: Vec<Action>) {
        for action in actions {
            match action {
                Action::Send { to, message } => {
                    if let Some(mb) = self.mailbox(to) {
                        mb.push_back(message);
                    }
                }
                Action::Apply { command, .. } => self.nodes[node_idx].store.apply(&command),
                Action::Snapshot { index, .. } => {
                    // Serialize the state machine through `index` and hand it back.
                    let data = self.nodes[node_idx].store.snapshot();
                    let reply = self.nodes[node_idx]
                        .raft
                        .step(Event::Snapshot { index, data })
                        .expect("snapshot event");
                    self.absorb(node_idx, reply);
                }
                Action::RestoreSnapshot { data, .. } => {
                    self.nodes[node_idx].store.restore(&data);
                }
                _ => {}
            }
        }
    }

    fn step_round(&mut self) {
        for i in 0..self.nodes.len() {
            let actions = self.nodes[i].raft.step(Event::Tick).expect("tick");
            self.absorb(i, actions);
        }
        for i in 0..self.nodes.len() {
            let id = self.nodes[i].id;
            while let Some(message) = self.mailbox(id).and_then(VecDeque::pop_front) {
                let actions = self.nodes[i]
                    .raft
                    .step(Event::Message(message))
                    .expect("message");
                self.absorb(i, actions);
            }
        }
    }

    fn settle(&mut self, rounds: usize) {
        for _ in 0..rounds {
            self.step_round();
        }
    }

    fn leader(&self) -> Option<usize> {
        self.nodes.iter().position(|n| n.raft.is_leader())
    }

    /// Submits a write to the leader, redirecting on `NotLeader` like a client.
    fn write(&mut self, command: Vec<u8>) {
        let Some(i) = self.leader() else { return };
        match self.nodes[i].raft.step(Event::Propose(command)) {
            Ok(actions) => self.absorb(i, actions),
            Err(Error::NotLeader { leader }) => {
                // A real client would resend to `leader`; here the next settle
                // re-finds the leader, so we simply note it.
                let _ = leader;
            }
            Err(_) => {}
        }
    }
}

fn main() {
    let mut cluster = Cluster::new(&[0, 1, 2], 8);
    while cluster.leader().is_none() {
        cluster.step_round();
    }
    println!("leader: node {}", cluster.leader().unwrap());

    // Replicate a series of writes.
    for (k, v) in [
        ("alpha", "1"),
        ("beta", "2"),
        ("gamma", "3"),
        ("beta", "20"),
    ] {
        cluster.write(put(k, v));
        cluster.settle(4);
    }
    cluster.write(del("gamma"));
    cluster.settle(8);

    // Every node has converged on the same key-value state.
    println!("\nreplicated state (read from each node):");
    for node in &cluster.nodes {
        let beta = node
            .store
            .get("beta")
            .map(String::as_str)
            .unwrap_or("<none>");
        let gamma = node
            .store
            .get("gamma")
            .map(String::as_str)
            .unwrap_or("<none>");
        println!("  node {}: beta={beta}, gamma={gamma}", node.id);
    }

    // Drive enough writes to cross the snapshot threshold, then add a node that
    // must catch up from the snapshot — proving snapshot/restore round-trips real
    // application state.
    for i in 0..20 {
        cluster.write(put(&format!("k{i}"), &format!("v{i}")));
        cluster.settle(2);
    }
    cluster.settle(10);

    let mut fresh = KvNode {
        id: 3,
        raft: RaftNode::new(
            RaftConfig::new(3, vec![0, 1, 2, 3])
                .with_election_timeout(60, 80)
                .with_seed(0xE003),
        ),
        store: KvStore::default(),
    };
    let _ = &mut fresh;
    cluster.nodes.push(fresh);
    cluster.mailboxes.push((3, VecDeque::new()));
    if let Some(i) = cluster.leader() {
        let actions = cluster.nodes[i]
            .raft
            .step(Event::AddServer(3))
            .expect("add");
        cluster.absorb(i, actions);
    }
    cluster.settle(120);

    let leader = cluster.leader().unwrap();
    let want = cluster.nodes[leader].store.map.clone();
    let got = &cluster.nodes.iter().find(|n| n.id == 3).unwrap().store.map;
    println!(
        "\nadded node 3 — it holds {} keys (leader holds {})",
        got.len(),
        want.len()
    );
    assert_eq!(*got, want, "the added node must converge on the full state");
    println!("node 3's key-value state matches the cluster exactly");
}
