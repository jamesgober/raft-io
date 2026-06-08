//! A single-node cluster electing itself and committing proposals.
//!
//! This is the shortest end-to-end path through `raft-io`: one node, no peers,
//! the default in-memory log. It elects itself the moment its election timer
//! fires (it is its own majority) and commits each proposal immediately.
//!
//! Run it with:
//!
//! ```text
//! cargo run --example single_node
//! ```

use raft_io::{Action, Event, RaftConfig, RaftNode};

fn main() {
    // Tier 1: one call, no generic to name, no I/O to wire up.
    let mut node = RaftNode::new(RaftConfig::single(1));

    // Logical time is ours to advance. Tick until the node elects itself.
    let mut ticks = 0u32;
    while !node.is_leader() {
        let _ = node
            .step(Event::Tick)
            .expect("an in-memory tick never fails");
        ticks += 1;
    }
    println!(
        "node {} became leader in term {} after {ticks} ticks",
        node.id(),
        node.term()
    );

    // Propose a few commands. A single-node leader commits each at once.
    for command in [&b"set x = 1"[..], b"set y = 2", b"delete x"] {
        let actions = node
            .step(Event::Propose(command.to_vec()))
            .expect("the leader accepts proposals");

        for action in actions {
            if let Action::Apply { index, command, .. } = action {
                // In a real system this is where the state machine runs.
                println!("  applied #{index}: {}", String::from_utf8_lossy(&command));
            }
        }
    }

    println!("commit index is now {}", node.commit_index());
    assert_eq!(node.commit_index(), 3);
}
