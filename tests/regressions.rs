//! Deterministic regression tests for the 1.2.0 safety fixes: each drives a
//! small cluster by hand into the exact interleaving that used to break, and
//! checks the fixed behaviour.
//!
//! - a new leader may not change the membership before it commits an entry
//!   of its own term (the single-server membership-change rule);
//! - the election no-op keeps the learner set;
//! - a follower's commit index never moves backwards, and an older snapshot
//!   cannot roll applied state back;
//! - a leadership transfer to an unreachable node is abandoned;
//! - a sent snapshot is not resent on every heartbeat;
//! - replies and votes from faulty peers are not trusted blindly.

#![allow(clippy::unwrap_used, clippy::expect_used)]

use std::collections::{BTreeMap, BTreeSet};

use raft_io::{
    Action, AppendEntries, AppendEntriesReply, Error, Event, InstallSnapshot, LogEntry, Message,
    NodeId, RaftConfig, RaftLog, RaftNode, RequestVoteReply,
};

/// A cluster with partition groups: a message crosses only between nodes in
/// the same group. Records every `Apply` each node emits.
struct Net {
    nodes: BTreeMap<NodeId, RaftNode>,
    inflight: Vec<(NodeId, NodeId, Message)>,
    group: BTreeMap<NodeId, u8>,
    applied: BTreeMap<NodeId, BTreeMap<u64, (u64, Vec<u8>)>>,
    snapshot_sends: usize,
}

impl Net {
    fn new(voters: &[NodeId], extra: &[NodeId], seed: u64) -> Self {
        let mut nodes = BTreeMap::new();
        for &id in voters.iter().chain(extra) {
            let mut cfg = RaftConfig::new(id, voters.to_vec())
                .with_election_timeout(10, 20)
                .with_heartbeat_interval(3)
                .with_seed(seed + id * 7919);
            if extra.contains(&id) {
                // A learner-to-be never campaigns anyway; keep it quiet.
                cfg = cfg.with_election_timeout(1000, 2000);
            }
            nodes.insert(id, RaftNode::new(cfg));
        }
        let group = nodes.keys().map(|&i| (i, 0)).collect();
        Self {
            nodes,
            inflight: Vec::new(),
            group,
            applied: BTreeMap::new(),
            snapshot_sends: 0,
        }
    }

    fn absorb(&mut self, from: NodeId, actions: Vec<Action>) {
        for action in actions {
            match action {
                Action::Send { to, message } => {
                    if matches!(message, Message::InstallSnapshot(_)) {
                        self.snapshot_sends += 1;
                    }
                    self.inflight.push((from, to, message));
                }
                Action::Apply {
                    index,
                    term,
                    command,
                } => {
                    let _ = self
                        .applied
                        .entry(from)
                        .or_default()
                        .insert(index, (term, command));
                }
                _ => {}
            }
        }
    }

    fn step(&mut self, id: NodeId, event: Event) -> Result<(), Error> {
        let actions = self.nodes.get_mut(&id).unwrap().step(event)?;
        self.absorb(id, actions);
        Ok(())
    }

    fn deliver_all(&mut self) {
        for _ in 0..1000 {
            let batch = std::mem::take(&mut self.inflight);
            if batch.is_empty() {
                return;
            }
            for (from, to, message) in batch {
                if self.group.get(&from) == self.group.get(&to) && self.nodes.contains_key(&to) {
                    self.step(to, Event::Message(message)).unwrap();
                }
            }
        }
    }

    fn round(&mut self) {
        let ids: Vec<NodeId> = self.nodes.keys().copied().collect();
        for id in ids {
            self.step(id, Event::Tick).unwrap();
        }
        self.deliver_all();
    }

    fn elect(&mut self, id: NodeId) {
        let mut guard = 0;
        while !self.nodes[&id].is_leader() {
            self.step(id, Event::Tick).unwrap();
            self.deliver_all();
            guard += 1;
            assert!(guard < 1000, "node {id} never elected");
        }
    }

    fn leader_in(&self, ids: &[NodeId]) -> Option<NodeId> {
        ids.iter()
            .copied()
            .filter(|i| self.nodes[i].is_leader())
            .max_by_key(|i| self.nodes[i].term())
    }

    /// No index was applied as two different entries anywhere.
    fn assert_state_machine_safety(&self) {
        let mut seen: BTreeMap<u64, BTreeSet<(u64, Vec<u8>)>> = BTreeMap::new();
        for applies in self.applied.values() {
            for (index, entry) in applies {
                let _ = seen.entry(*index).or_default().insert(entry.clone());
            }
        }
        for (index, entries) in &seen {
            assert_eq!(entries.len(), 1, "index {index} applied as {entries:?}");
        }
    }
}

fn sends(actions: Vec<Action>) -> Vec<(NodeId, Message)> {
    actions
        .into_iter()
        .filter_map(|a| match a {
            Action::Send { to, message } => Some((to, message)),
            _ => None,
        })
        .collect()
}

/// The Ongaro (raft-dev, 2015) interleaving: an isolated leader holds an
/// uncommitted configuration change while a new leader, whose log is fully
/// committed, starts a different change. Before 1.2.0 the new leader accepted
/// the change at once and two different entries were committed at one index.
#[test]
fn membership_change_waits_for_a_current_term_commit() {
    let ids = [1, 2, 3, 4];
    let mut net = Net::new(&ids, &[], 0x51);
    net.elect(1);
    net.step(1, Event::Propose(b"a".to_vec())).unwrap();
    net.deliver_all();
    for _ in 0..4 {
        net.round();
    }
    let committed = net.nodes[&1].commit_index();
    assert!(ids.iter().all(|i| net.nodes[i].commit_index() == committed));

    // Node 1 is isolated and appends C1 = remove 4, which never leaves it.
    for &i in &ids {
        let _ = net.group.insert(i, u8::from(i == 1));
    }
    net.step(1, Event::RemoveServer(4)).unwrap();
    net.deliver_all();

    // Nodes 2, 3, 4 elect a leader whose log is fully committed. Messages
    // are delivered one at a time, stopping as soon as it wins, so its
    // election no-op has not been acknowledged yet.
    let l = 'elect: loop {
        for id in [2, 3, 4] {
            net.step(id, Event::Tick).unwrap();
        }
        while !net.inflight.is_empty() {
            let (from, to, m) = net.inflight.remove(0);
            if net.group.get(&from) == net.group.get(&to) {
                net.step(to, Event::Message(m)).unwrap();
            }
            if let Some(l) = net.leader_in(&[2, 3, 4]) {
                break 'elect l;
            }
        }
    };
    let z = [2, 3].into_iter().find(|&n| n != l).unwrap();

    // It may not start a membership change before its no-op commits.
    assert!(matches!(
        net.step(l, Event::RemoveServer(z)),
        Err(Error::ConfigInProgress)
    ));

    // Once the no-op commits (one round trip), the change is accepted.
    net.deliver_all();
    for _ in 0..2 {
        net.round();
    }
    let _ = net.group.insert(z, 2);
    net.step(l, Event::RemoveServer(z))
        .expect("accepted after the no-op commits");
    net.deliver_all();
    net.step(l, Event::Propose(b"X".to_vec())).unwrap();
    for _ in 0..3 {
        net.round();
    }

    // {1, z} can talk to each other; {L, y} are cut off from them.
    let _ = net.group.insert(1, 3);
    let _ = net.group.insert(z, 3);
    for _ in 0..200 {
        net.round();
    }
    // Heal and let everyone converge.
    for &i in &ids {
        let _ = net.group.insert(i, 0);
    }
    for _ in 0..50 {
        net.round();
    }
    net.assert_state_machine_safety();
    // Every node that kept up holds the same committed prefix.
    let leader = net.leader_in(&ids).expect("a leader after healing");
    let leader_log = net.nodes[&leader].log();
    for &i in &ids {
        let node = &net.nodes[&i];
        for index in 1..=node.commit_index().min(leader_log.last_index()) {
            assert_eq!(
                node.log().term_at(index),
                leader_log.term_at(index),
                "node {i} disagrees with leader {leader} at committed index {index}"
            );
        }
    }
}

/// A single-node cluster still reconfigures right after electing itself: its
/// no-op commits immediately.
#[test]
fn single_node_reconfigures_after_its_no_op() {
    let mut node = RaftNode::new(RaftConfig::single(1));
    while !node.is_leader() {
        let _ = node.step(Event::Tick).unwrap();
    }
    assert_eq!(node.commit_index(), node.log().last_index());
    let _ = node.step(Event::AddLearner(2)).unwrap();
    assert_eq!(node.learners(), &[2]);
}

/// The election no-op used to carry the voters only, so every node that
/// appended it forgot the learners.
#[test]
fn election_no_op_keeps_learners() {
    let mut net = Net::new(&[1, 2, 3], &[4], 0x77);
    net.elect(1);
    net.step(1, Event::AddLearner(4)).unwrap();
    for _ in 0..5 {
        net.round();
    }
    assert_eq!(net.nodes[&2].learners(), &[4]);

    // Leave an uncommitted tail on nodes 2 and 3, then cut node 1 off.
    net.step(1, Event::Propose(b"p".to_vec())).unwrap();
    let batch = std::mem::take(&mut net.inflight);
    for (from, to, m) in batch {
        if from == 1 && (to == 2 || to == 3) {
            net.step(to, Event::Message(m)).unwrap();
        }
    }
    net.inflight.clear();
    let _ = net.group.insert(1, 9);
    let l = loop {
        net.round();
        if let Some(l) = net.leader_in(&[2, 3]) {
            break l;
        }
    };
    for _ in 0..5 {
        net.round();
    }
    for id in [2, 3, 4] {
        assert_eq!(
            net.nodes[&id].learners(),
            &[4],
            "node {id} lost the learner"
        );
    }

    // Hand leadership to the other survivor; the learner keeps replicating.
    let other = if l == 2 { 3 } else { 2 };
    net.step(l, Event::TransferLeadership(other)).unwrap();
    for _ in 0..10 {
        net.round();
    }
    let l2 = net.leader_in(&[2, 3]).unwrap();
    assert_eq!(net.nodes[&l2].learners(), &[4]);
    for i in 0..5 {
        net.step(l2, Event::Propose(vec![b'q', i])).unwrap();
    }
    for _ in 0..10 {
        net.round();
    }
    assert_eq!(
        net.nodes[&4].log().last_index(),
        net.nodes[&l2].log().last_index()
    );
    net.step(l2, Event::PromoteLearner(4)).unwrap();
    for _ in 0..5 {
        net.round();
    }
    assert!(net.nodes[&l2].members().contains(&4));
}

fn three(seed: u64) -> BTreeMap<NodeId, RaftNode> {
    let ids = vec![1, 2, 3];
    ids.iter()
        .map(|&id| {
            let cfg = RaftConfig::new(id, ids.clone())
                .with_election_timeout(10, 20)
                .with_heartbeat_interval(3)
                .with_seed(seed * 101 + id);
            (id, RaftNode::new(cfg))
        })
        .collect()
}

fn pump(
    nodes: &mut BTreeMap<NodeId, RaftNode>,
    mut q: Vec<(NodeId, Message)>,
    down: &[NodeId],
    applies: &mut Vec<(NodeId, u64)>,
) {
    let mut guard = 0;
    while !q.is_empty() {
        let (to, m) = q.remove(0);
        guard += 1;
        if guard > 100_000 {
            break;
        }
        if down.contains(&to) {
            continue;
        }
        for a in nodes.get_mut(&to).unwrap().step(Event::Message(m)).unwrap() {
            match a {
                Action::Send { to: t, message } => q.push((t, message)),
                Action::Apply { index, .. } => applies.push((to, index)),
                _ => {}
            }
        }
    }
}

fn step_pump(
    nodes: &mut BTreeMap<NodeId, RaftNode>,
    id: NodeId,
    event: Event,
    down: &[NodeId],
    applies: &mut Vec<(NodeId, u64)>,
) {
    let actions = nodes.get_mut(&id).unwrap().step(event).unwrap();
    pump(nodes, sends(actions), down, applies);
}

/// A reordered append that covers less than the follower already committed
/// used to pull its commit index back, after which an older snapshot passed
/// the stale-snapshot check and rolled applied state back.
#[test]
fn follower_commit_never_regresses_and_old_snapshots_are_ignored() {
    let mut nodes = three(1);
    let mut applies = Vec::new();
    while !nodes[&1].is_leader() {
        step_pump(&mut nodes, 1, Event::Tick, &[], &mut applies);
    }
    for i in 0..10u8 {
        step_pump(&mut nodes, 1, Event::Propose(vec![i]), &[], &mut applies);
    }
    for _ in 0..3 {
        step_pump(&mut nodes, 1, Event::Tick, &[], &mut applies);
    }
    let term = nodes[&1].term();
    let c = nodes[&2].commit_index();
    assert_eq!(c, nodes[&1].log().last_index());
    assert_eq!(nodes[&2].last_applied(), c);

    // Two more entries commit with node 3 only.
    for i in 10..12u8 {
        step_pump(&mut nodes, 1, Event::Propose(vec![i]), &[2], &mut applies);
    }
    let leader_commit = nodes[&1].commit_index();
    assert_eq!(leader_commit, c + 2);
    // The leader snapshots below node 2's commit index.
    let snap_at = c - 2;
    let _ = nodes
        .get_mut(&1)
        .unwrap()
        .step(Event::Snapshot {
            index: snap_at,
            data: b"old state".to_vec(),
        })
        .unwrap();
    let snapshot = nodes[&1].log().snapshot().unwrap();

    // A current-term append whose window ends below node 2's commit index,
    // carrying the leader's newer commit index.
    let n2 = nodes[&2].log();
    let ae = AppendEntries {
        term,
        leader: 1,
        prev_log_index: c - 4,
        prev_log_term: n2.term_at(c - 4).unwrap(),
        entries: vec![n2.entry(c - 3).unwrap()],
        leader_commit,
    };
    let _ = nodes
        .get_mut(&2)
        .unwrap()
        .step(Event::Message(Message::AppendEntries(ae)))
        .unwrap();
    assert_eq!(nodes[&2].commit_index(), c, "commit index moved backwards");

    let actions = nodes
        .get_mut(&2)
        .unwrap()
        .step(Event::Message(Message::InstallSnapshot(InstallSnapshot {
            term,
            leader: 1,
            snapshot,
        })))
        .unwrap();
    assert!(
        !actions
            .iter()
            .any(|a| matches!(a, Action::RestoreSnapshot { .. })),
        "an older snapshot rolled applied state back"
    );
    assert_eq!(nodes[&2].last_applied(), c);

    // Catching up applies only the two new entries, each once.
    let before = applies.iter().filter(|(n, _)| *n == 2).count();
    for _ in 0..3 {
        step_pump(&mut nodes, 1, Event::Tick, &[], &mut applies);
    }
    let reapplied: Vec<u64> = applies
        .iter()
        .filter(|(n, _)| *n == 2)
        .skip(before)
        .map(|(_, i)| *i)
        .collect();
    assert_eq!(reapplied, vec![c + 1, c + 2]);
}

/// A transfer to a node that is down used to block writes forever.
#[test]
fn transfer_to_an_unreachable_target_is_abandoned() {
    let mut nodes = three(2);
    let mut applies = Vec::new();
    while !nodes[&1].is_leader() {
        step_pump(&mut nodes, 1, Event::Tick, &[], &mut applies);
    }
    step_pump(
        &mut nodes,
        1,
        Event::Propose(b"x".to_vec()),
        &[],
        &mut applies,
    );
    step_pump(
        &mut nodes,
        1,
        Event::Propose(b"y".to_vec()),
        &[2],
        &mut applies,
    );
    step_pump(
        &mut nodes,
        1,
        Event::TransferLeadership(2),
        &[2],
        &mut applies,
    );

    // While the transfer is pending the leader says why it declines.
    let err = nodes
        .get_mut(&1)
        .unwrap()
        .step(Event::Propose(b"z".to_vec()))
        .unwrap_err();
    assert!(
        matches!(err, Error::TransferInProgress { target: 2 }),
        "{err:?}"
    );
    assert!(matches!(
        nodes.get_mut(&1).unwrap().step(Event::AddLearner(9)),
        Err(Error::TransferInProgress { target: 2 })
    ));

    // After one (maximum) election timeout it is abandoned.
    for _ in 0..20 {
        step_pump(&mut nodes, 1, Event::Tick, &[2], &mut applies);
    }
    assert!(nodes[&1].is_leader());
    step_pump(
        &mut nodes,
        1,
        Event::Propose(b"z".to_vec()),
        &[2],
        &mut applies,
    );
    assert_eq!(nodes[&1].commit_index(), nodes[&1].log().last_index());
}

/// Once an abandoned transfer's target has been removed, a new transfer to it
/// is a no-op and writes keep flowing.
#[test]
fn transfer_to_a_removed_node_is_a_no_op() {
    let mut nodes = three(3);
    let mut applies = Vec::new();
    while !nodes[&1].is_leader() {
        step_pump(&mut nodes, 1, Event::Tick, &[], &mut applies);
    }
    for _ in 0..3 {
        step_pump(&mut nodes, 1, Event::Tick, &[], &mut applies);
    }
    step_pump(
        &mut nodes,
        1,
        Event::Propose(b"y".to_vec()),
        &[2],
        &mut applies,
    );
    step_pump(
        &mut nodes,
        1,
        Event::TransferLeadership(2),
        &[2],
        &mut applies,
    );
    assert!(
        nodes
            .get_mut(&1)
            .unwrap()
            .step(Event::Propose(vec![1]))
            .is_err()
    );
    // A config change is refused during the transfer too, so let it time
    // out first, then remove the node.
    for _ in 0..20 {
        step_pump(&mut nodes, 1, Event::Tick, &[2], &mut applies);
    }
    step_pump(&mut nodes, 1, Event::RemoveServer(2), &[2], &mut applies);
    step_pump(
        &mut nodes,
        1,
        Event::TransferLeadership(2),
        &[2],
        &mut applies,
    );
    assert!(
        nodes
            .get_mut(&1)
            .unwrap()
            .step(Event::Propose(vec![2]))
            .is_ok()
    );
}

/// A snapshot was rebuilt and resent on every heartbeat until the follower
/// replied.
#[test]
fn sent_snapshot_is_not_resent_on_every_heartbeat() {
    let mut net = Net::new(&[1, 2, 3], &[], 0x33);
    net.elect(1);
    // Node 3 falls behind while the others commit and compact.
    let _ = net.group.insert(3, 1);
    for i in 0..20u8 {
        net.step(1, Event::Propose(vec![i])).unwrap();
        net.deliver_all();
    }
    for _ in 0..3 {
        net.round();
    }
    let at = net.nodes[&1].commit_index();
    net.step(
        1,
        Event::Snapshot {
            index: at,
            data: b"state".to_vec(),
        },
    )
    .unwrap();
    // Node 3 comes back. Run until the leader discovers it needs the
    // snapshot and sends it once.
    let _ = net.group.insert(3, 0);
    net.snapshot_sends = 0;
    let mut guard = 0;
    while net.snapshot_sends == 0 {
        for id in [1, 2, 3] {
            net.step(id, Event::Tick).unwrap();
        }
        for _ in 0..10 {
            let batch = std::mem::take(&mut net.inflight);
            for (_, to, m) in batch {
                // The snapshot itself is still on its way.
                if !(to == 3 && matches!(m, Message::InstallSnapshot(_))) {
                    net.step(to, Event::Message(m)).unwrap();
                }
            }
        }
        guard += 1;
        assert!(guard < 100, "no snapshot was ever sent");
    }
    // Now the link to node 3 stalls (a large snapshot on a slow link): count
    // how many more snapshots the leader queues over 30 heartbeat intervals.
    net.snapshot_sends = 0;
    for _ in 0..90 {
        net.step(1, Event::Tick).unwrap();
        net.step(2, Event::Tick).unwrap();
        for _ in 0..10 {
            let batch = std::mem::take(&mut net.inflight);
            for (_, to, m) in batch {
                if to != 3 {
                    net.step(to, Event::Message(m)).unwrap();
                }
            }
        }
    }
    // At most one send per resend interval (two maximum election timeouts,
    // 40 ticks): two in 90 ticks, against thirty before 1.2.0.
    assert!(
        net.snapshot_sends <= 3,
        "{} snapshots sent",
        net.snapshot_sends
    );
    // Once the link recovers, the next resend gets through and node 3 catches
    // up.
    for _ in 0..60 {
        net.round();
    }
    assert_eq!(
        net.nodes[&3].log().last_index(),
        net.nodes[&1].log().last_index()
    );
}

/// A reply claiming more than the leader holds used to overflow, then count
/// toward commit.
#[test]
fn reply_beyond_the_leaders_log_is_ignored() {
    let mut nodes = three(4);
    let mut applies = Vec::new();
    while !nodes[&1].is_leader() {
        step_pump(&mut nodes, 1, Event::Tick, &[], &mut applies);
    }
    let term = nodes[&1].term();
    let bogus = AppendEntriesReply {
        term,
        success: true,
        from: 2,
        match_index: u64::MAX,
        conflict_index: 0,
        conflict_term: 0,
    };
    let _ = nodes
        .get_mut(&1)
        .unwrap()
        .step(Event::Message(Message::AppendEntriesReply(bogus)))
        .unwrap();
    let before = nodes[&1].commit_index();
    let actions = nodes
        .get_mut(&1)
        .unwrap()
        .step(Event::Propose(b"held by the leader only".to_vec()))
        .unwrap();
    assert!(!actions.iter().any(|a| matches!(a, Action::Apply { .. })));
    assert_eq!(nodes[&1].commit_index(), before);
}

/// Vote grants from ids outside the voting membership do not count.
#[test]
fn votes_from_non_voters_do_not_count() {
    let mut node = RaftNode::new(
        RaftConfig::new(1, [2, 3, 4, 5])
            .with_election_timeout(10, 20)
            .with_seed(9),
    );
    // Run until the node is a candidate (pre-vote granted by 2 and 3).
    let mut guard = 0;
    while node.role() != raft_io::Role::Candidate {
        for action in node.step(Event::Tick).unwrap() {
            if let Action::Send {
                message: Message::PreVote(pv),
                ..
            } = action
            {
                for from in [2, 3] {
                    let _ = node
                        .step(Event::Message(Message::PreVoteReply(
                            raft_io::PreVoteReply {
                                term: pv.term - 1,
                                vote_granted: true,
                                from,
                            },
                        )))
                        .unwrap();
                }
            }
        }
        guard += 1;
        assert!(guard < 1000);
    }
    let term = node.term();
    for from in [90, 91, 92] {
        let _ = node
            .step(Event::Message(Message::RequestVoteReply(
                RequestVoteReply {
                    term,
                    vote_granted: true,
                    from,
                },
            )))
            .unwrap();
    }
    assert!(!node.is_leader(), "elected by non-members");
}

/// Appends that a correct leader never sends are dropped without effect.
#[test]
fn malformed_appends_are_dropped() {
    let mut nodes = three(5);
    let mut applies = Vec::new();
    while !nodes[&1].is_leader() {
        step_pump(&mut nodes, 1, Event::Tick, &[], &mut applies);
    }
    for i in 0..4u8 {
        step_pump(&mut nodes, 1, Event::Propose(vec![i]), &[], &mut applies);
    }
    for _ in 0..3 {
        step_pump(&mut nodes, 1, Event::Tick, &[], &mut applies);
    }
    let term = nodes[&1].term();
    let follower = nodes.get_mut(&2).unwrap();
    let commit = follower.commit_index();
    let last = follower.log().last_index();
    let base = AppendEntries {
        term,
        leader: 1,
        prev_log_index: 0,
        prev_log_term: 0,
        entries: Vec::new(),
        leader_commit: commit,
    };
    let bad = [
        // Not contiguous after `prev_log_index`.
        AppendEntries {
            entries: vec![LogEntry::new(term, 5, b"gap".to_vec())],
            ..base.clone()
        },
        // An entry from a later term than the RPC.
        AppendEntries {
            prev_log_index: last,
            prev_log_term: follower.log().term_at(last).unwrap(),
            entries: vec![LogEntry::new(term + 1, last + 1, b"future".to_vec())],
            ..base.clone()
        },
        // Would overwrite a committed entry (index 1 holds a term-`term`
        // entry; this one claims term 0).
        AppendEntries {
            entries: vec![LogEntry::new(0, 1, b"rewrite".to_vec())],
            ..base.clone()
        },
    ];
    for ae in bad {
        let actions = follower
            .step(Event::Message(Message::AppendEntries(ae)))
            .unwrap();
        assert!(actions.is_empty(), "{actions:?}");
        assert_eq!(follower.commit_index(), commit);
        assert_eq!(follower.log().last_index(), last);
    }
}
