//! A five-node cluster that survives a network partition and recovers.
//!
//! A minority of nodes is cut off from the rest. The majority keeps a leader and
//! keeps committing; the isolated minority cannot (it lacks a quorum). When the
//! partition heals, the stragglers catch up and every node converges on the same
//! log — Raft's availability-under-a-quorum and self-healing properties, shown
//! end to end.
//!
//! Run it with:
//!
//! ```text
//! cargo run --example partition_recovery
//! ```

use std::collections::VecDeque;

use raft_io::{Action, Event, Message, NodeId, RaftConfig, RaftNode};

struct Cluster {
    nodes: Vec<RaftNode>,
    mailboxes: Vec<VecDeque<Message>>,
    applied: Vec<usize>,
    /// `side[i]` assigns node `i` to a partition group, or `None` when healed.
    side: Option<Vec<bool>>,
}

impl Cluster {
    fn new(n: usize) -> Self {
        let ids: Vec<NodeId> = (0..n as NodeId).collect();
        let nodes = ids
            .iter()
            .map(|&id| RaftNode::new(RaftConfig::new(id, ids.clone()).with_seed(0x4000 + id)))
            .collect();
        Self {
            nodes,
            mailboxes: vec![VecDeque::new(); n],
            applied: vec![0; n],
            side: None,
        }
    }

    fn connected(&self, a: NodeId, b: NodeId) -> bool {
        match &self.side {
            None => true,
            Some(side) => side[a as usize] == side[b as usize],
        }
    }

    fn absorb(&mut self, from: NodeId, node_idx: usize, actions: Vec<Action>) {
        for action in actions {
            match action {
                Action::Send { to, message } if self.connected(from, to) => {
                    self.mailboxes[to as usize].push_back(message);
                }
                Action::Apply { .. } => self.applied[node_idx] += 1,
                _ => {}
            }
        }
    }

    fn step_round(&mut self) {
        for i in 0..self.nodes.len() {
            let id = self.nodes[i].id();
            let actions = self.nodes[i].step(Event::Tick).expect("tick");
            self.absorb(id, i, actions);
        }
        for i in 0..self.nodes.len() {
            let id = self.nodes[i].id();
            while let Some(message) = self.mailboxes[i].pop_front() {
                let actions = self.nodes[i]
                    .step(Event::Message(message))
                    .expect("message");
                self.absorb(id, i, actions);
            }
        }
    }

    fn leader(&self) -> Option<usize> {
        self.nodes.iter().position(RaftNode::is_leader)
    }

    fn propose(&mut self, command: &str) {
        if let Some(i) = self.leader() {
            let id = self.nodes[i].id();
            let actions = self.nodes[i]
                .step(Event::Propose(command.as_bytes().to_vec()))
                .expect("leader accepts proposals");
            self.absorb(id, i, actions);
        }
    }
}

fn main() {
    let mut cluster = Cluster::new(5);
    while cluster.leader().is_none() {
        cluster.step_round();
    }
    println!("leader elected: node {}", cluster.leader().unwrap());

    // Cut nodes 3 and 4 off from the majority {0,1,2}.
    cluster.side = Some(vec![false, false, false, true, true]);
    println!("\npartition: {{0,1,2}} | {{3,4}}");

    for _ in 0..40 {
        cluster.propose("during-partition");
        cluster.step_round();
    }
    println!("applied per node during partition: {:?}", cluster.applied);
    let majority_progress = cluster.applied[0]
        .max(cluster.applied[1])
        .max(cluster.applied[2]);
    let minority_progress = cluster.applied[3].max(cluster.applied[4]);
    println!("  majority side committed {majority_progress}, minority side {minority_progress}");

    // Heal and let the minority catch up.
    cluster.side = None;
    println!("\nhealed; letting the cluster reconcile");
    for _ in 0..120 {
        cluster.step_round();
    }
    println!("applied per node after heal:      {:?}", cluster.applied);

    let target = cluster.applied.iter().copied().max().unwrap();
    let caught_up = cluster.applied.iter().filter(|&&a| a == target).count();
    println!("\n{caught_up}/5 nodes converged on {target} applied entries");
    assert!(caught_up == 5, "every node should converge after healing");
}
