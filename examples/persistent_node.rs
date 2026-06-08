//! A node whose log survives a restart, using the durable [`WalLog`].
//!
//! Requires the `persistence` feature:
//!
//! ```text
//! cargo run --example persistent_node --features persistence
//! ```
//!
//! The node elects itself, commits a few proposals, then is dropped — closing
//! its write-ahead log exactly as a process exit would. Reopening the same file
//! recovers the log and the persisted term/vote, and the node carries on.

use raft_io::{Action, Event, RaftConfig, RaftLog, RaftNode, WalLog};

fn drive_to_leader(node: &mut RaftNode<WalLog>) {
    while !node.is_leader() {
        let _ = node.step(Event::Tick).expect("tick never fails");
    }
}

fn propose(node: &mut RaftNode<WalLog>, command: &str) {
    let actions = node
        .step(Event::Propose(command.as_bytes().to_vec()))
        .expect("leader accepts proposals");
    for action in actions {
        if let Action::Apply { index, command, .. } = action {
            println!("  applied #{index}: {}", String::from_utf8_lossy(&command));
        }
    }
}

fn main() {
    // A throwaway directory for the example's WAL file.
    let dir = tempfile::tempdir().expect("tempdir");
    let path = dir.path().join("node.wal");

    // ---- first run: elect, propose, then "crash" by dropping the node ----
    {
        let log = WalLog::open(&path).expect("open wal");
        let mut node = RaftNode::with_log(RaftConfig::single(1), log);
        drive_to_leader(&mut node);
        println!("run 1: leader in term {}", node.term());
        propose(&mut node, "set x = 1");
        propose(&mut node, "set y = 2");
        println!("run 1: committed up to index {}\n", node.commit_index());
        // `node` drops here, flushing and closing the WAL.
    }

    // ---- restart: reopen the same file and recover ----
    let log = WalLog::open(&path).expect("reopen wal");
    let mut node = RaftNode::with_log(RaftConfig::single(1), log);
    println!(
        "run 2: recovered {} log entries, persisted term {}",
        node.log().last_index(),
        node.term()
    );
    assert_eq!(node.log().last_index(), 2, "log must survive the restart");
    for index in 1..=node.log().last_index() {
        let entry = node.log().entry(index).expect("recovered entry");
        println!(
            "  recovered #{index} (term {}): {}",
            entry.term,
            String::from_utf8_lossy(&entry.command)
        );
    }

    // The node resumes normally: it re-elects (in a higher term) and commits more.
    drive_to_leader(&mut node);
    println!("\nrun 2: leader again in term {}", node.term());
    propose(&mut node, "set z = 3");
    println!("run 2: committed up to index {}", node.commit_index());
    assert_eq!(node.commit_index(), 3);
}
