//! # raft-io
//!
//! A from-scratch implementation of the [Raft consensus algorithm], built as a
//! clean, embeddable library rather than a framework.
//!
//! The protocol core is a **deterministic state machine**: you feed it
//! [`Event`]s (logical ticks, inbound [`Message`]s, client proposals) and it
//! returns [`Action`]s (send these messages, apply this committed command).
//! Time, networking, and storage are *your* concern, injected through the
//! [`RaftLog`] and [`RaftTransport`] trait seams. That separation is exactly
//! what makes the consensus core provable: it contains no wall clock and no
//! I/O, so an entire cluster's behaviour can be reproduced from a seed and a
//! sequence of events.
//!
//! ## Status
//!
//! This is `v0.3`: the deterministic core, leader election with term and vote
//! safety, and the full multi-node log-replication pipeline — batched
//! `AppendEntries`, per-follower progress tracking with optimistic pipelining,
//! conflict-hint backtracking, and commit on a quorum. Durable persistence
//! (`wal-db`) lands in `v0.4` and snapshots in `v0.5`. See `docs/API.md` for the
//! full surface.
//!
//! ## The three tiers
//!
//! - **Tier 1** — the common case in a handful of calls, no builder and no
//!   generic to name: [`RaftNode::new`] with a [`RaftConfig`] and the default
//!   in-memory [`MemoryLog`].
//! - **Tier 2** — [`RaftConfig`]'s builder for tuning election and heartbeat
//!   timing.
//! - **Tier 3** — the [`RaftLog`] / [`RaftTransport`] traits for plugging in a
//!   durable store or a real transport.
//!
//! ## Example — a single-node cluster elects itself and commits
//!
//! ```
//! use raft_io::{Action, Event, RaftConfig, RaftNode};
//!
//! // One node, no peers: it reaches quorum (itself) the moment it times out.
//! let mut node = RaftNode::new(RaftConfig::single(1));
//!
//! // Drive logical ticks until the node becomes leader.
//! while !node.is_leader() {
//!     let _ = node.step(Event::Tick).expect("tick never fails in memory");
//! }
//! assert_eq!(node.leader(), Some(1));
//!
//! // A leader commits its own proposals immediately (quorum of one).
//! let actions = node.step(Event::Propose(b"set x = 1".to_vec())).unwrap();
//! assert!(actions.iter().any(|a| matches!(a, Action::Apply { .. })));
//! assert_eq!(node.commit_index(), 1);
//! ```
//!
//! [Raft consensus algorithm]: https://raft.github.io/

#![forbid(unsafe_code)]
#![deny(missing_docs)]
#![deny(unused_must_use)]
#![deny(unused_results)]
#![deny(clippy::unwrap_used)]
#![deny(clippy::expect_used)]
#![deny(clippy::todo)]
#![deny(clippy::unimplemented)]
#![deny(clippy::print_stdout)]
#![deny(clippy::print_stderr)]
#![deny(clippy::dbg_macro)]
#![cfg_attr(docsrs, feature(doc_cfg))]

mod config;
mod error;
mod log;
mod message;
mod node;
mod rng;
mod transport;
mod types;

pub use crate::config::RaftConfig;
pub use crate::error::{Error, Result};
pub use crate::log::{MemoryLog, RaftLog};
pub use crate::message::{
    AppendEntries, AppendEntriesReply, Message, RequestVote, RequestVoteReply,
};
pub use crate::node::{Action, Event, RaftNode};
pub use crate::transport::{MemoryTransport, RaftTransport};
pub use crate::types::{HardState, Index, LogEntry, NodeId, Role, Term};
