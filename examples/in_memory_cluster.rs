//! A three-node cluster electing a leader over an in-memory transport.
//!
//! `raft-io` is sans-I/O: a node emits `Action::Send` and you decide how to
//! deliver it. Here "delivery" is pushing the message onto the destination's
//! mailbox, and a round-robin scheduler ticks every node and drains every
//! mailbox until exactly one leader emerges. The same loop, with a real socket
//! in place of the mailbox, is how you would run a cluster for real.
//!
//! Run it with:
//!
//! ```text
//! cargo run --example in_memory_cluster
//! ```

use std::collections::VecDeque;

use raft_io::{Action, Event, Message, NodeId, RaftConfig, RaftNode};

/// A hand-driven cluster: nodes plus a mailbox per node.
struct Cluster {
    nodes: Vec<RaftNode>,
    mailboxes: Vec<VecDeque<Message>>,
}

impl Cluster {
    fn new(n: usize) -> Self {
        let ids: Vec<NodeId> = (0..n as NodeId).collect();
        let nodes = ids
            .iter()
            .map(|&id| {
                // Distinct seeds so the nodes do not all time out in lockstep.
                let cfg = RaftConfig::new(id, ids.clone()).with_seed(0x2000 + id);
                RaftNode::new(cfg)
            })
            .collect();
        Self {
            nodes,
            mailboxes: vec![VecDeque::new(); n],
        }
    }

    /// Routes emitted sends into destination mailboxes.
    fn route(&mut self, actions: Vec<Action>) {
        for action in actions {
            if let Action::Send { to, message } = action {
                if let Some(box_) = self.mailboxes.get_mut(to as usize) {
                    box_.push_back(message);
                }
            }
        }
    }

    fn leader(&self) -> Option<NodeId> {
        self.nodes.iter().find(|n| n.is_leader()).map(RaftNode::id)
    }
}

fn main() {
    let mut cluster = Cluster::new(3);

    for round in 0..100 {
        // Tick every node.
        for i in 0..cluster.nodes.len() {
            let actions = cluster.nodes[i].step(Event::Tick).expect("tick");
            cluster.route(actions);
        }
        // Drain every mailbox so votes and heartbeats flow this round.
        for i in 0..cluster.nodes.len() {
            while let Some(message) = cluster.mailboxes[i].pop_front() {
                let actions = cluster.nodes[i]
                    .step(Event::Message(message))
                    .expect("message");
                cluster.route(actions);
            }
        }

        if let Some(leader) = cluster.leader() {
            let term = cluster.nodes[leader as usize].term();
            println!("cluster elected node {leader} as leader in term {term} (round {round})");
            return;
        }
    }

    panic!("cluster did not converge on a leader");
}
