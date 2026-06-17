//! Growing a cluster safely with a non-voting learner.
//!
//! Adding a fresh voter is risky: it counts toward the quorum the instant it
//! joins, yet starts with an empty log, so until it catches up a single other
//! failure can wedge the cluster. The fix is to add the node as a **learner**
//! first. A learner replicates the log and catches up like a follower but counts
//! toward no quorum, so adding one — however far behind it starts — never weakens
//! availability. Once it has caught up, you promote it to a voter.
//!
//! This demo elects a leader, commits a backlog of proposals, adds node 3 as a
//! learner and watches it catch up while the voter set is unchanged, then
//! promotes it and confirms it is a full voting member.
//!
//! Run it with:
//!
//! ```text
//! cargo run --example learner
//! ```

use std::collections::VecDeque;

use raft_io::{Action, Event, Message, NodeId, RaftConfig, RaftNode};

struct Cluster {
    nodes: Vec<(NodeId, RaftNode)>,
    mailboxes: Vec<(NodeId, VecDeque<Message>)>,
    /// Highest log index each node has applied, by id.
    applied: Vec<(NodeId, u64)>,
    next_value: u64,
}

impl Cluster {
    fn new(ids: &[NodeId]) -> Self {
        let nodes = ids
            .iter()
            .map(|&id| {
                let cfg = RaftConfig::new(id, ids.to_vec()).with_seed(0xD000 + id);
                (id, RaftNode::new(cfg))
            })
            .collect();
        let mailboxes = ids.iter().map(|&id| (id, VecDeque::new())).collect();
        let applied = ids.iter().map(|&id| (id, 0)).collect();
        Self {
            nodes,
            mailboxes,
            applied,
            next_value: 1,
        }
    }

    fn mailbox(&mut self, id: NodeId) -> Option<&mut VecDeque<Message>> {
        self.mailboxes
            .iter_mut()
            .find(|(i, _)| *i == id)
            .map(|(_, m)| m)
    }

    fn absorb(&mut self, from: NodeId, actions: Vec<Action>) {
        for action in actions {
            match action {
                Action::Send { to, message } => {
                    if let Some(mb) = self.mailbox(to) {
                        mb.push_back(message);
                    }
                }
                Action::Apply { index, .. } => {
                    if let Some(slot) = self.applied.iter_mut().find(|(i, _)| *i == from) {
                        slot.1 = index;
                    }
                }
                _ => {}
            }
        }
    }

    fn step_round(&mut self) {
        for i in 0..self.nodes.len() {
            let id = self.nodes[i].0;
            let actions = self.nodes[i].1.step(Event::Tick).expect("tick");
            self.absorb(id, actions);
        }
        for i in 0..self.nodes.len() {
            let id = self.nodes[i].0;
            while let Some(message) = self.mailbox(id).and_then(VecDeque::pop_front) {
                let actions = self.nodes[i]
                    .1
                    .step(Event::Message(message))
                    .expect("message");
                self.absorb(id, actions);
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

    /// Submits a proposal to the leader, if there is one.
    fn propose(&mut self) {
        if let Some(i) = self.nodes.iter().position(|(_, n)| n.is_leader()) {
            let id = self.nodes[i].0;
            let value = self.next_value.to_be_bytes().to_vec();
            self.next_value += 1;
            if let Ok(actions) = self.nodes[i].1.step(Event::Propose(value)) {
                self.absorb(id, actions);
            }
        }
    }

    fn leader_step(&mut self, event: Event) {
        if let Some(i) = self.nodes.iter().position(|(_, n)| n.is_leader()) {
            let id = self.nodes[i].0;
            if let Ok(actions) = self.nodes[i].1.step(event) {
                self.absorb(id, actions);
            }
        }
    }

    fn node(&self, id: NodeId) -> &RaftNode {
        &self.nodes.iter().find(|(i, _)| *i == id).unwrap().1
    }

    fn applied_through(&self, id: NodeId) -> u64 {
        self.applied
            .iter()
            .find(|(i, _)| *i == id)
            .map(|(_, a)| *a)
            .unwrap_or(0)
    }
}

fn main() {
    let mut cluster = Cluster::new(&[0, 1, 2]);
    while cluster.leader().is_none() {
        cluster.step_round();
    }
    let leader = cluster.leader().unwrap();
    println!("leader: node {leader}");

    // Commit a backlog the new node will have to catch up on.
    for _ in 0..20 {
        cluster.propose();
        cluster.settle(2);
    }
    println!(
        "committed a backlog: leader applied through index {}\n",
        cluster.applied_through(leader)
    );

    // Add node 3 as a LEARNER. The voter set stays {0, 1, 2}, so the commit
    // quorum is unchanged — availability is unaffected while node 3 is far behind.
    let cfg = RaftConfig::new(3, vec![0, 1, 2, 3])
        .with_election_timeout(60, 80)
        .with_seed(0xD003);
    cluster.nodes.push((3, RaftNode::new(cfg)));
    cluster.mailboxes.push((3, VecDeque::new()));
    cluster.applied.push((3, 0));
    cluster.leader_step(Event::AddLearner(3));
    cluster.settle(5);

    let leader = cluster.leader().unwrap();
    println!("added node 3 as a learner:");
    println!("  voters   : {:?}", cluster.node(leader).members());
    println!("  learners : {:?}", cluster.node(leader).learners());

    // The cluster keeps committing while the learner catches up.
    for _ in 0..5 {
        cluster.propose();
        cluster.settle(3);
    }
    cluster.settle(40);
    println!(
        "\nlearner caught up: node 3 applied through index {} (leader at {})",
        cluster.applied_through(3),
        cluster.applied_through(leader)
    );

    // Promote the caught-up learner to a full voting member.
    cluster.leader_step(Event::PromoteLearner(3));
    cluster.settle(40);
    let leader = cluster.leader().unwrap();
    println!("\npromoted node 3 to a voter:");
    println!("  voters   : {:?}", cluster.node(leader).members());
    println!("  learners : {:?}", cluster.node(leader).learners());

    assert_eq!(cluster.node(leader).members(), &[0, 1, 2, 3]);
    assert!(cluster.node(leader).learners().is_empty());
    assert!(
        cluster.applied_through(3) > 0,
        "the learner should have caught up"
    );
}
