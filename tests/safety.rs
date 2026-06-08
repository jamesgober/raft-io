//! Election-safety property tests over a deterministic in-memory cluster.
//!
//! The defining safety guarantee of Raft's election layer is **Election
//! Safety**: at most one leader can be elected in any given term. These tests
//! build a small cluster, drive it with randomised but reproducible schedules of
//! ticks and message deliveries, and assert that no two distinct nodes are ever
//! recorded as leader of the same term — across every ordering proptest can
//! find.
//!
//! The harness is the in-memory driver the protocol is designed for: each node's
//! [`Action::Send`](raft_io::Action) is routed into the destination's mailbox,
//! and the schedule decides whose turn it is to tick or to consume a message.
//! Because the core carries no clock and no I/O, a seed plus a schedule fully
//! determines the run, which is exactly what makes a counterexample replayable.

#![allow(clippy::unwrap_used, clippy::expect_used)]

use std::collections::{BTreeMap, VecDeque};

use proptest::prelude::*;
use raft_io::{Action, Event, Message, NodeId, RaftConfig, RaftNode};

/// A cluster of nodes with per-node mailboxes, driven by hand.
struct Cluster {
    nodes: Vec<RaftNode>,
    mailboxes: Vec<VecDeque<Message>>,
    /// The single leader recorded for each term, to detect a second one.
    leader_by_term: BTreeMap<u64, NodeId>,
}

impl Cluster {
    /// Builds an `n`-node cluster with ids `0..n` and distinct RNG seeds so the
    /// nodes do not jitter in lockstep.
    fn new(n: usize) -> Self {
        let ids: Vec<NodeId> = (0..n as NodeId).collect();
        let nodes = ids
            .iter()
            .map(|&id| {
                let cfg = RaftConfig::new(id, ids.clone())
                    .with_election_timeout(10, 20)
                    .with_heartbeat_interval(3)
                    .with_seed(0x1000 + id);
                RaftNode::new(cfg)
            })
            .collect();
        Self {
            nodes,
            mailboxes: vec![VecDeque::new(); n],
            leader_by_term: BTreeMap::new(),
        }
    }

    fn len(&self) -> usize {
        self.nodes.len()
    }

    /// Routes a node's emitted actions: sends land in mailboxes, applies are
    /// ignored (single-node commit is exercised elsewhere).
    fn route(&mut self, actions: Vec<Action>) {
        for action in actions {
            if let Action::Send { to, message } = action {
                if (to as usize) < self.mailboxes.len() {
                    self.mailboxes[to as usize].push_back(message);
                }
            }
        }
    }

    /// Records the current leader (if any) for `idx`, asserting Election Safety.
    fn observe(&mut self, idx: usize) {
        let node = &self.nodes[idx];
        if node.is_leader() {
            let term = node.term();
            let id = node.id();
            match self.leader_by_term.get(&term) {
                Some(&existing) => assert_eq!(
                    existing, id,
                    "two leaders in term {term}: node {existing} and node {id}"
                ),
                None => {
                    let _ = self.leader_by_term.insert(term, id);
                }
            }
        }
    }

    /// Delivers one tick to node `idx`.
    fn tick(&mut self, idx: usize) {
        let actions = self.nodes[idx].step(Event::Tick).expect("tick never fails");
        self.route(actions);
        self.observe(idx);
    }

    /// Delivers one queued message to node `idx`, if any is waiting.
    fn deliver(&mut self, idx: usize) {
        if let Some(message) = self.mailboxes[idx].pop_front() {
            let actions = self.nodes[idx]
                .step(Event::Message(message))
                .expect("message handling never fails in memory");
            self.route(actions);
            self.observe(idx);
        }
    }

    /// Number of nodes currently in the leader role.
    fn current_leaders(&self) -> usize {
        self.nodes.iter().filter(|n| n.is_leader()).count()
    }
}

/// One scheduled action in a randomised run.
#[derive(Clone, Copy, Debug)]
enum Op {
    Tick(usize),
    Deliver(usize),
}

fn op_strategy(n: usize) -> impl Strategy<Value = Op> {
    (0..2usize, 0..n).prop_map(|(kind, idx)| {
        if kind == 0 {
            Op::Tick(idx)
        } else {
            Op::Deliver(idx)
        }
    })
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(256))]

    /// No two leaders are ever recorded for the same term, whatever the
    /// interleaving of ticks and deliveries across a three-node cluster.
    #[test]
    fn election_safety_three_nodes(ops in prop::collection::vec(op_strategy(3), 0..400)) {
        let mut cluster = Cluster::new(3);
        for op in ops {
            match op {
                Op::Tick(i) => cluster.tick(i),
                Op::Deliver(i) => cluster.deliver(i),
            }
        }
    }

    /// The same invariant on a five-node cluster.
    #[test]
    fn election_safety_five_nodes(ops in prop::collection::vec(op_strategy(5), 0..600)) {
        let mut cluster = Cluster::new(5);
        for op in ops {
            match op {
                Op::Tick(i) => cluster.tick(i),
                Op::Deliver(i) => cluster.deliver(i),
            }
        }
    }
}

/// With a fair round-robin schedule, a cluster converges on exactly one leader
/// — the liveness counterpart to the safety properties above.
#[test]
fn cluster_converges_on_a_single_leader() {
    for n in [3usize, 5] {
        let mut cluster = Cluster::new(n);
        let mut converged = false;
        // Round-robin tick + drain for a bounded number of rounds.
        for _ in 0..500 {
            for i in 0..cluster.len() {
                cluster.tick(i);
            }
            for i in 0..cluster.len() {
                // Drain each mailbox fully so votes and heartbeats flow.
                while !cluster.mailboxes[i].is_empty() {
                    cluster.deliver(i);
                }
            }
            if cluster.current_leaders() == 1 {
                converged = true;
                break;
            }
        }
        assert!(
            converged,
            "{n}-node cluster did not converge on a single leader"
        );
    }
}
