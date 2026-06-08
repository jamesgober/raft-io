//! A three-node cluster replicating a series of commands to every node.
//!
//! This goes one step beyond `in_memory_cluster`: after a leader emerges it
//! proposes several commands, drives replication to completion, and prints each
//! node's applied log so you can see that all three agree, entry for entry and
//! in order — the State Machine Safety property the test suite proves.
//!
//! Run it with:
//!
//! ```text
//! cargo run --example replicated_log
//! ```

use std::collections::VecDeque;

use raft_io::{Action, Event, Message, NodeId, RaftConfig, RaftNode};

/// A hand-driven cluster that also records what each node applies.
struct Cluster {
    nodes: Vec<RaftNode>,
    mailboxes: Vec<VecDeque<Message>>,
    applied: Vec<Vec<String>>,
}

impl Cluster {
    fn new(n: usize) -> Self {
        let ids: Vec<NodeId> = (0..n as NodeId).collect();
        let nodes = ids
            .iter()
            .map(|&id| RaftNode::new(RaftConfig::new(id, ids.clone()).with_seed(0x3000 + id)))
            .collect();
        Self {
            nodes,
            mailboxes: vec![VecDeque::new(); n],
            applied: vec![Vec::new(); n],
        }
    }

    fn absorb(&mut self, node_idx: usize, actions: Vec<Action>) {
        for action in actions {
            match action {
                Action::Send { to, message } => {
                    if let Some(mb) = self.mailboxes.get_mut(to as usize) {
                        mb.push_back(message);
                    }
                }
                Action::Apply { command, .. } => {
                    self.applied[node_idx].push(String::from_utf8_lossy(&command).into_owned());
                }
                _ => {}
            }
        }
    }

    /// Ticks every node and drains every mailbox once.
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
    let mut cluster = Cluster::new(3);

    // Settle until a leader is elected.
    while cluster.leader().is_none() {
        cluster.step_round();
    }
    let leader = cluster.leader().expect("a leader");
    println!("node {leader} is leader\n");

    // Propose a series of commands and let them replicate.
    for command in ["set a=1", "set b=2", "del a", "set c=3", "incr b"] {
        cluster.propose(command);
        for _ in 0..6 {
            cluster.step_round();
        }
    }
    for _ in 0..12 {
        cluster.step_round();
    }

    // Every node should have applied the same commands, in the same order.
    for (id, log) in cluster.applied.iter().enumerate() {
        println!("node {id} applied {} entries:", log.len());
        for (i, command) in log.iter().enumerate() {
            println!("  #{}: {command}", i + 1);
        }
    }

    let first = &cluster.applied[0];
    assert!(
        cluster.applied.iter().all(|log| log == first),
        "all nodes must agree on the applied log"
    );
    println!("\nall nodes agree on the applied log");
}
