//! A lagging follower caught up by a snapshot after the leader compacts its log.
//!
//! A node is isolated while the rest of the cluster commits a long run of
//! entries. With a snapshot threshold set, the leader periodically asks the
//! application to snapshot and compacts its log — so when the isolated node
//! rejoins, the entries it needs are gone and it must be caught up by an
//! `InstallSnapshot`, then by tail replication.
//!
//! Run it with:
//!
//! ```text
//! cargo run --example snapshot_catchup
//! ```

use std::collections::VecDeque;

use raft_io::{Action, Event, Message, NodeId, RaftConfig, RaftLog, RaftNode};

struct Cluster {
    nodes: Vec<RaftNode>,
    mailboxes: Vec<VecDeque<Message>>,
    applied: Vec<u64>,
    restored_via_snapshot: Vec<bool>,
    side: Option<Vec<bool>>,
}

impl Cluster {
    fn new(n: usize, snapshot_threshold: usize) -> Self {
        let ids: Vec<NodeId> = (0..n as NodeId).collect();
        let nodes = ids
            .iter()
            .map(|&id| {
                RaftNode::new(
                    RaftConfig::new(id, ids.clone())
                        .with_max_batch(4)
                        .with_snapshot_threshold(snapshot_threshold)
                        .with_seed(0xA000 + id),
                )
            })
            .collect();
        Self {
            nodes,
            mailboxes: vec![VecDeque::new(); n],
            applied: vec![0; n],
            restored_via_snapshot: vec![false; n],
            side: None,
        }
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
                Action::Send { to, message } if self.connected(from, to) => {
                    self.mailboxes[to as usize].push_back(message);
                }
                Action::Apply { index, .. } => self.applied[i] = index,
                Action::Snapshot { index, .. } => {
                    // Serialize "state through index" and hand it back.
                    let reply = self.nodes[i]
                        .step(Event::Snapshot {
                            index,
                            data: index.to_be_bytes().to_vec(),
                        })
                        .expect("snapshot event");
                    self.absorb(i, reply);
                }
                Action::RestoreSnapshot { index, .. } => {
                    self.applied[i] = index;
                    self.restored_via_snapshot[i] = true;
                }
                _ => {}
            }
        }
    }

    fn step_round(&mut self) {
        for i in 0..self.nodes.len() {
            let actions = self.nodes[i].step(Event::Tick).expect("tick");
            self.absorb(i, actions);
        }
        for i in 0..self.nodes.len() {
            while let Some(message) = self.mailboxes[i].pop_front() {
                let actions = self.nodes[i]
                    .step(Event::Message(message))
                    .expect("message");
                self.absorb(i, actions);
            }
        }
    }

    fn leader(&self) -> Option<usize> {
        self.nodes.iter().position(RaftNode::is_leader)
    }

    fn propose(&mut self, command: &str) {
        if let Some(i) = self.leader() {
            let actions = self.nodes[i]
                .step(Event::Propose(command.as_bytes().to_vec()))
                .expect("leader accepts proposals");
            self.absorb(i, actions);
        }
    }
}

fn main() {
    let mut cluster = Cluster::new(3, 8);
    while cluster.leader().is_none() {
        cluster.step_round();
    }
    let leader = cluster.leader().unwrap();
    println!("leader elected: node {leader}");

    // Isolate node 2 and commit a long run on the majority.
    cluster.side = Some(vec![false, false, true]);
    println!("\npartition: {{0,1}} | {{2}}");
    for n in 0..60 {
        cluster.propose(&format!("entry-{n}"));
        cluster.step_round();
    }
    // The majority side's leader (node 2 was isolated) has compacted its log.
    let snap = (0..3)
        .map(|i| cluster.nodes[i].log().snapshot_index())
        .max()
        .unwrap();
    println!(
        "majority compacted its log up to index {snap}; node 2 has applied {}",
        cluster.applied[2]
    );

    // Heal — node 2 can no longer replicate from the log and must take a snapshot.
    cluster.side = None;
    println!("\nhealed; node 2 must catch up via a snapshot");
    for _ in 0..120 {
        cluster.step_round();
    }

    println!("\napplied per node: {:?}", cluster.applied);
    println!(
        "node 2 caught up via snapshot: {}",
        cluster.restored_via_snapshot[2]
    );
    assert!(
        cluster.restored_via_snapshot[2],
        "node 2 should use a snapshot"
    );
    assert_eq!(
        cluster.applied[2],
        cluster.nodes[leader].commit_index(),
        "node 2 should be fully caught up"
    );
    println!(
        "node 2 fully caught up to commit index {}",
        cluster.applied[2]
    );
}
