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
//! This is `v0.7`: **feature complete, hardened, and frozen.** The full protocol
//! — election, replication, durable crash recovery (`persistence`), snapshots,
//! membership changes, and leadership transfer — is in place and verified by a
//! kitchen-sink adversarial test suite that asserts all five Raft safety
//! properties under combined partitions, message loss/reorder/duplication,
//! membership churn, and snapshotting. The public traits and the wire and WAL
//! formats are frozen (see `docs/PROTOCOL.md`); the decode path is fuzzed.
//! Alpha/beta hardening against real consumers follows in `v0.8`+. See
//! `docs/API.md` for the full surface.
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
#[cfg(feature = "framing")]
#[cfg_attr(docsrs, doc(cfg(feature = "framing")))]
pub mod framing;
mod log;
mod message;
mod node;
mod rng;
mod transport;
mod types;
#[cfg(feature = "persistence")]
mod wal_log;

pub use crate::config::RaftConfig;
pub use crate::error::{Error, Result};
pub use crate::log::{MemoryLog, RaftLog};
pub use crate::message::{
    AppendEntries, AppendEntriesReply, InstallSnapshot, InstallSnapshotReply, Message, RequestVote,
    RequestVoteReply, TimeoutNow,
};
pub use crate::node::{Action, Event, RaftNode};
pub use crate::transport::{MemoryTransport, RaftTransport};
pub use crate::types::{EntryKind, HardState, Index, LogEntry, NodeId, Role, Snapshot, Term};
#[cfg(feature = "persistence")]
#[cfg_attr(docsrs, doc(cfg(feature = "persistence")))]
pub use crate::wal_log::WalLog;
