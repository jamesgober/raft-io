//! Reconfiguring a running cluster: add a server, remove a server, transfer
//! leadership.
//!
//! The cluster starts with three nodes, grows to four, shrinks back to three by
//! dropping a different node, then hands leadership to a chosen peer — all while
//! it would otherwise be serving traffic. Membership changes one server at a
//! time, which is what keeps every intermediate configuration's quorum
//! overlapping safely.
//!
//! Run it with:
//!
//! ```text
//! cargo run --example membership
//! ```

use std::collections::VecDeque;

use raft_io::{Action, Event, Message, NodeId, RaftConfig, RaftNode};

struct Cluster {
    nodes: Vec<(NodeId, RaftNode)>,
    mailboxes: Vec<(NodeId, VecDeque<Message>)>,
}

impl Cluster {
    fn new(ids: &[NodeId]) -> Self {
        let nodes = ids
            .iter()
            .map(|&id| {
                let cfg = RaftConfig::new(id, ids.to_vec()).with_seed(0xC000 + id);
                (id, RaftNode::new(cfg))
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

    fn absorb(&mut self, actions: Vec<Action>) {
        for action in actions {
            if let Action::Send { to, message } = action {
                if let Some(mb) = self.mailbox(to) {
                    mb.push_back(message);
                }
            }
        }
    }

    fn step_round(&mut self) {
        for i in 0..self.nodes.len() {
            let actions = self.nodes[i].1.step(Event::Tick).expect("tick");
            self.absorb(actions);
        }
        for i in 0..self.nodes.len() {
            let id = self.nodes[i].0;
            while let Some(message) = self.mailbox(id).and_then(VecDeque::pop_front) {
                let actions = self.nodes[i]
                    .1
                    .step(Event::Message(message))
                    .expect("message");
                self.absorb(actions);
            }
        }
    }

    fn settle(&mut self, rounds: usize) {
        for _ in 0..rounds {
            self.step_round();
        }
    }

    fn leader(&self) -> Option<NodeId> {
        self.nodes
            .iter()
            .find(|(_, n)| n.is_leader())
            .map(|(i, _)| *i)
    }

    fn leader_step(&mut self, event: Event) {
        if let Some(i) = self.nodes.iter().position(|(_, n)| n.is_leader()) {
            if let Ok(actions) = self.nodes[i].1.step(event) {
                self.absorb(actions);
            }
        }
    }

    fn members(&self) -> Vec<NodeId> {
        self.leader()
            .and_then(|l| self.nodes.iter().find(|(i, _)| *i == l))
            .map(|(_, n)| n.members().to_vec())
            .unwrap_or_default()
    }
}

fn main() {
    let mut cluster = Cluster::new(&[0, 1, 2]);
    while cluster.leader().is_none() {
        cluster.step_round();
    }
    println!("leader: node {}", cluster.leader().unwrap());
    println!("members: {:?}\n", cluster.members());

    // Add node 3. Start it knowing the cluster it is joining.
    let cfg = RaftConfig::new(3, vec![0, 1, 2, 3])
        .with_election_timeout(60, 80)
        .with_seed(0xC003);
    cluster.nodes.push((3, RaftNode::new(cfg)));
    cluster.mailboxes.push((3, VecDeque::new()));
    cluster.leader_step(Event::AddServer(3));
    cluster.settle(80);
    println!("added node 3 -> members: {:?}", cluster.members());

    // Remove node 0 (or another follower if 0 leads).
    let leader = cluster.leader().unwrap();
    let victim = [0, 1, 2].into_iter().find(|&v| v != leader).unwrap();
    cluster.leader_step(Event::RemoveServer(victim));
    cluster.settle(80);
    println!("removed node {victim} -> members: {:?}", cluster.members());

    // Transfer leadership to another member.
    let leader = cluster.leader().unwrap();
    let target = cluster
        .members()
        .into_iter()
        .find(|&t| t != leader)
        .unwrap();
    println!("\ntransferring leadership {leader} -> {target}");
    cluster.leader_step(Event::TransferLeadership(target));
    cluster.settle(40);
    println!("leader is now node {}", cluster.leader().unwrap());
    assert_eq!(
        cluster.leader(),
        Some(target),
        "leadership should have transferred"
    );
}
