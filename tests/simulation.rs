//! Seeded whole-cluster simulation: message loss, duplication and
//! reordering, partitions, crash-restart on a log that forgets everything
//! after its last `sync`, snapshots, membership changes (including an isolated
//! leader that holds an uncommitted configuration change while the rest of
//! the cluster elects a new leader), leadership transfer and ReadIndex reads.
//!
//! Checked continuously: election safety, leader completeness, log matching,
//! state machine safety (no index applied as two different entries, no node
//! diverging from another), applies in increasing order and never repeated,
//! commit indexes that never move backwards within one incarnation, snapshot
//! restores that never roll applied state back, and linearizable reads.
//!
//! `SIM_RUNS` (default 200) sets the number of seeds, `SIM_SEED` the first
//! seed, `SIM_STEPS` (default 20000) the operations per run.

#![allow(clippy::unwrap_used, clippy::expect_used)]

use std::collections::BTreeMap;

use raft_io::{
    Action, Event, HardState, LogEntry, MemoryLog, Message, NodeId, RaftConfig, RaftLog, RaftNode,
    Role, Snapshot,
};

/// A log that keeps a `durable` copy as of the last `sync`: a crash restarts
/// the node from it, losing everything written since.
#[derive(Clone, Default)]
struct CrashLog {
    live: MemoryLog,
    durable: MemoryLog,
}

impl RaftLog for CrashLog {
    fn last_index(&self) -> u64 {
        self.live.last_index()
    }
    fn last_term(&self) -> u64 {
        self.live.last_term()
    }
    fn term_at(&self, index: u64) -> Option<u64> {
        self.live.term_at(index)
    }
    fn entry(&self, index: u64) -> Option<LogEntry> {
        self.live.entry(index)
    }
    fn entries(&self, from: u64, to: u64) -> Vec<LogEntry> {
        self.live.entries(from, to)
    }
    fn append(&mut self, entries: &[LogEntry]) -> raft_io::Result<()> {
        self.live.append(entries)
    }
    fn truncate(&mut self, from: u64) -> raft_io::Result<()> {
        self.live.truncate(from)
    }
    fn hard_state(&self) -> HardState {
        self.live.hard_state()
    }
    fn set_hard_state(&mut self, state: HardState) -> raft_io::Result<()> {
        self.live.set_hard_state(state)
    }
    fn sync(&mut self) -> raft_io::Result<()> {
        self.durable = self.live.clone();
        Ok(())
    }
    fn snapshot_index(&self) -> u64 {
        self.live.snapshot_index()
    }
    fn snapshot(&self) -> Option<Snapshot> {
        self.live.snapshot()
    }
    fn apply_snapshot(&mut self, snapshot: &Snapshot) -> raft_io::Result<()> {
        self.live.apply_snapshot(snapshot)
    }
}

struct Rng(u64);

impl Rng {
    fn next(&mut self) -> u64 {
        self.0 = self
            .0
            .wrapping_mul(6_364_136_223_846_793_005)
            .wrapping_add(1_442_695_040_888_963_407);
        let mut x = self.0;
        x ^= x >> 33;
        x = x.wrapping_mul(0xff51_afd7_ed55_8ccd);
        x ^= x >> 33;
        x
    }
    fn below(&mut self, n: u64) -> u64 {
        self.next() % n.max(1)
    }
    fn chance(&mut self, pct: u64) -> bool {
        self.below(100) < pct
    }
}

const N: u64 = 5;

/// Running hash of the applied commands: two nodes with the same hash at an
/// index applied the same sequence.
fn chain(prev: u64, command: &[u8]) -> u64 {
    let mut x = prev ^ 0x9E37_79B9_7F4A_7C15;
    for &b in command {
        x = (x ^ u64::from(b)).wrapping_mul(0x100_0000_01B3);
    }
    x
}

struct Node {
    raft: RaftNode<CrashLog>,
    cfg: RaftConfig,
    up: bool,
    applied: u64,
    chain: u64,
    commit_seen: u64,
    reads: BTreeMap<u64, u64>,
}

struct Sim {
    rng: Rng,
    nodes: BTreeMap<NodeId, Node>,
    net: Vec<(NodeId, NodeId, Message)>,
    group: BTreeMap<NodeId, u8>,
    leader_by_term: BTreeMap<u64, NodeId>,
    committed: BTreeMap<u64, (u64, Vec<u8>)>,
    chain_at: BTreeMap<u64, u64>,
    max_committed: u64,
    next_cmd: u64,
    next_read: u64,
    violations: Vec<String>,
    isolated_changes: u64,
    /// The leader cut off by the last isolated change, while it is pending.
    isolated: Option<NodeId>,
    /// A newly elected leader that should try a membership change at once,
    /// before anything of its own term has committed.
    eager_change: Option<NodeId>,
}

impl Sim {
    fn new(seed: u64) -> Self {
        let ids: Vec<NodeId> = (1..=N).collect();
        let mut nodes = BTreeMap::new();
        for &id in &ids {
            let cfg = RaftConfig::new(id, ids.clone())
                .with_election_timeout(10, 25)
                .with_heartbeat_interval(3)
                .with_max_batch(4)
                .with_snapshot_threshold(16)
                .with_seed(seed ^ (id * 0x9E37));
            let _ = nodes.insert(
                id,
                Node {
                    raft: RaftNode::with_log(cfg.clone(), CrashLog::default()),
                    cfg,
                    up: true,
                    applied: 0,
                    chain: 0,
                    commit_seen: 0,
                    reads: BTreeMap::new(),
                },
            );
        }
        Self {
            rng: Rng(seed),
            nodes,
            net: Vec::new(),
            group: ids.iter().map(|&i| (i, 0)).collect(),
            leader_by_term: BTreeMap::new(),
            committed: BTreeMap::new(),
            chain_at: BTreeMap::new(),
            max_committed: 0,
            next_cmd: 0,
            next_read: 0,
            violations: Vec::new(),
            isolated_changes: 0,
            isolated: None,
            eager_change: None,
        }
    }

    fn violate(&mut self, message: String) {
        if self.violations.len() < 10 {
            self.violations.push(message);
        }
    }

    fn step(&mut self, id: NodeId, event: Event) {
        let node = self.nodes.get_mut(&id).unwrap();
        if !node.up {
            return;
        }
        let Ok(actions) = node.raft.step(event) else {
            return;
        };
        self.after(id, actions);
    }

    fn after(&mut self, id: NodeId, actions: Vec<Action>) {
        for action in actions {
            match action {
                Action::Send { to, message } => self.net.push((id, to, message)),
                Action::Apply {
                    index,
                    term,
                    command,
                } => self.on_apply(id, index, term, command),
                Action::Snapshot { index, .. } => {
                    let node = &self.nodes[&id];
                    if node.applied == index {
                        let data = node.chain.to_le_bytes().to_vec();
                        self.step(id, Event::Snapshot { index, data });
                    }
                }
                Action::RestoreSnapshot { index, data, .. } => {
                    let node = self.nodes.get_mut(&id).unwrap();
                    let restored = u64::from_le_bytes(data[..8].try_into().unwrap());
                    let applied = node.applied;
                    node.applied = index;
                    node.chain = restored;
                    if index < applied {
                        self.violate(format!(
                            "ROLLBACK: node {id} restored snapshot {index} below applied {applied}"
                        ));
                    }
                    if self.chain_at.get(&index).is_some_and(|&c| c != restored) {
                        self.violate(format!(
                            "SNAPSHOT CONTENT: node {id} restored wrong state at {index}"
                        ));
                    }
                }
                Action::ReadReady { id: read, index } => {
                    let node = self.nodes.get_mut(&id).unwrap();
                    if let Some(bound) = node.reads.remove(&read) {
                        if index < bound {
                            self.violate(format!(
                                "LINEARIZABILITY: read {read} on node {id} served at {index} < {bound}"
                            ));
                        }
                    }
                }
                _ => {}
            }
        }
        self.check_node(id);
    }

    fn on_apply(&mut self, id: NodeId, index: u64, term: u64, command: Vec<u8>) {
        let node = self.nodes.get_mut(&id).unwrap();
        let before = node.applied;
        node.applied = index;
        node.chain = chain(node.chain, &command);
        let chained = node.chain;
        if index <= before {
            self.violate(format!(
                "REAPPLY: node {id} applied index {index} again (applied {before})"
            ));
        }
        match self.committed.get(&index) {
            Some((t, c)) if *t != term || *c != command => {
                let earlier = *t;
                self.violate(format!(
                    "STATE MACHINE SAFETY: index {index} applied as term {term} by node {id}, earlier as term {earlier}"
                ));
            }
            Some(_) => {}
            None => {
                let _ = self.committed.insert(index, (term, command));
            }
        }
        match self.chain_at.get(&index) {
            Some(&c) if c != chained => {
                self.violate(format!(
                    "DIVERGED: node {id} state diverges at index {index}"
                ));
            }
            Some(_) => {}
            None => {
                let _ = self.chain_at.insert(index, chained);
            }
        }
        self.max_committed = self.max_committed.max(index);
    }

    fn check_node(&mut self, id: NodeId) {
        let node = &self.nodes[&id];
        let commit = node.raft.commit_index();
        let seen = node.commit_seen;
        if commit < seen {
            self.violate(format!(
                "COMMIT REGRESSION: node {id} commit_index {seen} -> {commit}"
            ));
        }
        let node = self.nodes.get_mut(&id).unwrap();
        node.commit_seen = node.commit_seen.max(commit);
        let node = &self.nodes[&id];
        if node.raft.role() != Role::Leader {
            return;
        }
        let term = node.raft.term();
        match self.leader_by_term.get(&term) {
            Some(&other) if other != id => {
                self.violate(format!(
                    "ELECTION SAFETY: term {term} has leaders {other} and {id}"
                ));
            }
            Some(_) => {}
            None => {
                let _ = self.leader_by_term.insert(term, id);
                if self.isolated.is_some_and(|old| old != id) {
                    self.eager_change = Some(id);
                }
                let log = node.raft.log();
                let base = log.snapshot_index();
                let missing = self
                    .committed
                    .iter()
                    .find(|&(&i, &(t, _))| i > base && log.term_at(i) != Some(t))
                    .map(|(&i, &(t, _))| (i, t));
                if let Some((i, t)) = missing {
                    self.violate(format!(
                        "LEADER COMPLETENESS: leader {id} of term {term} lacks committed index {i} term {t}"
                    ));
                }
            }
        }
    }

    fn check_log_matching(&mut self) {
        let ids: Vec<NodeId> = self.nodes.keys().copied().collect();
        for a in 0..ids.len() {
            for b in a + 1..ids.len() {
                let la = self.nodes[&ids[a]].raft.log();
                let lb = self.nodes[&ids[b]].raft.log();
                let base = la.snapshot_index().max(lb.snapshot_index());
                let mut i = la.last_index().min(lb.last_index());
                while i > base {
                    if la.term_at(i).is_some() && la.term_at(i) == lb.term_at(i) {
                        let bad = (base + 1..=i).find(|&j| la.entry(j) != lb.entry(j));
                        if let Some(j) = bad {
                            let (x, y) = (ids[a], ids[b]);
                            self.violate(format!(
                                "LOG MATCHING: nodes {x} and {y} agree at {i} but differ at {j}"
                            ));
                            return;
                        }
                        break;
                    }
                    i -= 1;
                }
            }
        }
    }

    fn connected(&self, a: NodeId, b: NodeId) -> bool {
        self.group[&a] == self.group[&b] && self.nodes[&a].up && self.nodes[&b].up
    }

    fn leader(&self) -> Option<NodeId> {
        self.nodes
            .iter()
            .filter(|(_, n)| n.up && n.raft.is_leader())
            .max_by_key(|(_, n)| n.raft.term())
            .map(|(i, _)| *i)
    }

    fn membership_event(&mut self, leader: NodeId) -> Option<Event> {
        let target = 1 + self.rng.below(N);
        let event = match self.rng.below(4) {
            0 => Event::RemoveServer(target),
            1 => Event::AddServer(target),
            2 => Event::AddLearner(target),
            _ => Event::PromoteLearner(target),
        };
        // Keep at least three voters so a quorum can always form again.
        if matches!(event, Event::RemoveServer(_)) && self.nodes[&leader].raft.members().len() <= 3
        {
            return None;
        }
        Some(event)
    }

    fn restart(&mut self, id: NodeId) {
        let node = self.nodes.get_mut(&id).unwrap();
        let durable = node.raft.log().durable.clone();
        let log = CrashLog {
            live: durable.clone(),
            durable,
        };
        let snapshot = log.live.snapshot();
        node.raft = RaftNode::with_log(node.cfg.clone(), log);
        node.up = true;
        node.reads.clear();
        node.commit_seen = node.raft.commit_index();
        match snapshot {
            Some(s) => {
                node.applied = s.index;
                node.chain = u64::from_le_bytes(s.data[..8].try_into().unwrap());
            }
            None => {
                node.applied = 0;
                node.chain = 0;
            }
        }
    }

    fn op(&mut self) {
        // A leader elected while an isolated leader holds an uncommitted
        // change tries a change of its own straight away; then the isolated
        // leader is reconnected to part of the cluster.
        if let Some(l) = self.eager_change.take() {
            if let Some(event) = self.membership_event(l) {
                self.step(l, event);
            }
            if let Some(old) = self.isolated.take() {
                for id in 1..=N {
                    let g = if id == old {
                        1
                    } else {
                        self.rng.below(2) as u8
                    };
                    let _ = self.group.insert(id, g);
                }
                let _ = self.group.insert(l, 0);
            }
            return;
        }
        match self.rng.below(1000) {
            0..=399 => {
                if self.net.is_empty() {
                    return;
                }
                let k = self.rng.below(self.net.len() as u64) as usize;
                let (from, to, message) = self.net.swap_remove(k);
                if !self.connected(from, to) {
                    return;
                }
                if self.rng.chance(3) {
                    self.net.push((from, to, message.clone()));
                }
                self.step(to, Event::Message(message));
            }
            400..=439 => {
                if !self.net.is_empty() {
                    let k = self.rng.below(self.net.len() as u64) as usize;
                    let _ = self.net.swap_remove(k);
                }
            }
            440..=799 => {
                let id = 1 + self.rng.below(N);
                self.step(id, Event::Tick);
            }
            800..=889 => {
                if let Some(l) = self.leader() {
                    if self.next_cmd < 400 {
                        self.next_cmd += 1;
                        let command = format!("c{}", self.next_cmd).into_bytes();
                        self.step(l, Event::Propose(command));
                    }
                }
            }
            890..=909 => {
                if let Some(l) = self.leader() {
                    self.next_read += 1;
                    let read = self.next_read;
                    let bound = self.max_committed;
                    let _ = self.nodes.get_mut(&l).unwrap().reads.insert(read, bound);
                    self.step(l, Event::Read { id: read });
                }
            }
            910..=929 => {
                for id in 1..=N {
                    let g = self.rng.below(2) as u8;
                    let _ = self.group.insert(id, g);
                }
            }
            930..=954 => {
                for id in 1..=N {
                    let _ = self.group.insert(id, 0);
                }
            }
            955..=969 => {
                let id = 1 + self.rng.below(N);
                if self.nodes[&id].up {
                    self.nodes.get_mut(&id).unwrap().up = false;
                    self.net.retain(|(_, to, _)| *to != id);
                } else {
                    self.restart(id);
                }
            }
            970..=979 => {
                if let Some(l) = self.leader() {
                    if let Some(event) = self.membership_event(l) {
                        self.step(l, event);
                    }
                }
            }
            980..=989 => {
                // The single-server membership-change hazard: cut the leader off
                // alone and have it start a change it cannot commit, while the
                // rest of the cluster moves on and changes the membership too.
                if let Some(l) = self.leader() {
                    if let Some(event) = self.membership_event(l) {
                        for id in 1..=N {
                            let _ = self.group.insert(id, u8::from(id == l));
                        }
                        self.isolated_changes += 1;
                        self.isolated = Some(l);
                        self.step(l, event);
                    }
                }
            }
            _ => {
                if let Some(l) = self.leader() {
                    let target = 1 + self.rng.below(N);
                    self.step(l, Event::TransferLeadership(target));
                }
            }
        }
    }
}

fn env(name: &str, default: u64) -> u64 {
    std::env::var(name)
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(default)
}

#[test]
fn seeded_cluster_simulation_holds_every_safety_property() {
    let runs = env("SIM_RUNS", 200);
    let first = env("SIM_SEED", 1);
    let steps = env("SIM_STEPS", 20_000);
    let mut failures: Vec<(u64, Vec<String>)> = Vec::new();
    let mut isolated_changes = 0;
    let mut applied = 0;
    for seed in first..first + runs {
        let mut sim = Sim::new(seed);
        for step in 0..steps {
            sim.op();
            if step % 250 == 0 {
                sim.check_log_matching();
            }
            if !sim.violations.is_empty() {
                break;
            }
        }
        sim.check_log_matching();
        isolated_changes += sim.isolated_changes;
        applied += sim.max_committed;
        if !sim.violations.is_empty() {
            failures.push((seed, sim.violations));
        }
    }
    assert!(
        failures.is_empty(),
        "{} of {runs} runs failed; first: {:?}",
        failures.len(),
        failures.first()
    );
    // The schedule really exercised what it claims to.
    assert!(
        isolated_changes > runs,
        "isolated changes {isolated_changes}"
    );
    assert!(applied > runs * 50, "too little progress: {applied}");
}
