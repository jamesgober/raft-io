//! The crate error type.
//!
//! Every fallible operation in `raft-io` returns [`Result<T>`], whose error is
//! [`Error`]. The type integrates with the portfolio's `error-forge` framework
//! — it implements [`error_forge::ForgeError`], so callers get the stable
//! `kind` / `caption` / severity metadata other crates rely on — while still
//! behaving as an ordinary [`std::error::Error`].

use core::fmt;

use error_forge::ForgeError;

use crate::types::NodeId;

/// A specialised [`Result`](core::result::Result) for `raft-io` operations.
///
/// Defaults its error to [`Error`], so most signatures read `Result<T>`.
///
/// # Examples
///
/// ```
/// use raft_io::{Error, Result};
///
/// fn leader_only() -> Result<()> {
///     Err(Error::NotLeader { leader: Some(2) })
/// }
/// assert!(leader_only().is_err());
/// ```
pub type Result<T, E = Error> = core::result::Result<T, E>;

/// Everything that can go wrong while driving a [`RaftNode`](crate::RaftNode).
///
/// The type is `#[non_exhaustive]`: later phases (persistence, snapshots) add
/// variants without a major bump, so a `match` over it must include a wildcard
/// arm.
///
/// # Examples
///
/// ```
/// use raft_io::Error;
///
/// // A proposal sent to a follower is rejected with a hint to the leader.
/// let err = Error::NotLeader { leader: Some(3) };
/// assert_eq!(err.to_string(), "not the leader; current leader is node 3");
/// ```
#[non_exhaustive]
#[derive(Debug)]
pub enum Error {
    /// A client proposal was made to a node that is not the leader.
    ///
    /// Only the leader may accept proposals. `leader` carries the node's best
    /// knowledge of who the current leader is, so the caller can redirect the
    /// request; it is `None` when no leader is known (for example during an
    /// election). This is a normal, recoverable condition — retry against the
    /// indicated leader.
    NotLeader {
        /// The node believed to be the current leader, if known.
        leader: Option<NodeId>,
    },

    /// A [`RaftLog`](crate::RaftLog) backend operation failed.
    ///
    /// The in-memory log never produces this, but a durable backend (the
    /// `wal-db`-backed log arriving in `v0.4`) can fail to read, append, or
    /// flush. `context` names the operation that was attempted (for example
    /// `"append entries"` or `"sync log"`) so the message is actionable, and
    /// `detail` carries the backend's own description. The caller should treat
    /// a storage failure on the durability path as fatal to the node: a node
    /// that cannot persist its state must not continue participating.
    Storage {
        /// What the log was trying to do when the failure occurred.
        context: &'static str,
        /// The underlying backend error, rendered as text.
        detail: String,
    },

    /// A message failed to encode to or decode from its wire form.
    ///
    /// Produced by the `framing` module (the `framing` feature) when
    /// `pack-io` cannot serialize a message or a received byte string does not
    /// decode into a valid one. `context` names the operation and `detail`
    /// carries the codec's description. A decode failure is not fatal — the
    /// transport should drop the malformed message and carry on, exactly as Raft
    /// tolerates a lost one.
    Encoding {
        /// What the framing layer was doing when it failed.
        context: &'static str,
        /// The underlying codec error, rendered as text.
        detail: String,
    },

    /// A membership change was requested while a previous one is still in flight.
    ///
    /// Raft changes the configuration one server at a time, and the leader must
    /// not begin a new change until the previous configuration entry has
    /// committed. Retry once the in-flight change completes. This is a routine,
    /// retryable condition.
    ConfigInProgress,
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::NotLeader { leader: Some(id) } => {
                write!(f, "not the leader; current leader is node {id}")
            }
            Self::NotLeader { leader: None } => {
                write!(f, "not the leader; no leader is currently known")
            }
            Self::Storage { context, detail } => {
                write!(f, "log storage error while {context}: {detail}")
            }
            Self::Encoding { context, detail } => {
                write!(f, "message framing error while {context}: {detail}")
            }
            Self::ConfigInProgress => {
                write!(f, "a configuration change is already in progress")
            }
        }
    }
}

impl std::error::Error for Error {}

impl ForgeError for Error {
    fn kind(&self) -> &'static str {
        match self {
            Self::NotLeader { .. } => "NotLeader",
            Self::Storage { .. } => "Storage",
            Self::Encoding { .. } => "Encoding",
            Self::ConfigInProgress => "ConfigInProgress",
        }
    }

    fn caption(&self) -> &'static str {
        match self {
            Self::NotLeader { .. } => "Not the leader",
            Self::Storage { .. } => "Log storage failure",
            Self::Encoding { .. } => "Message framing failure",
            Self::ConfigInProgress => "Configuration change in progress",
        }
    }

    /// A `NotLeader` rejection is retryable against the indicated leader and a
    /// `ConfigInProgress` rejection is retryable once the change completes; a
    /// storage failure on the durability path is not.
    fn is_retryable(&self) -> bool {
        matches!(self, Self::NotLeader { .. } | Self::ConfigInProgress)
    }

    /// A storage failure means the node can no longer guarantee durability and
    /// should stop; a `NotLeader` rejection is a routine redirect.
    fn is_fatal(&self) -> bool {
        matches!(self, Self::Storage { .. })
    }
}

impl Error {
    /// Builds a [`Storage`](Error::Storage) error from any displayable backend
    /// error.
    ///
    /// Backends implementing [`RaftLog`](crate::RaftLog) use this to map their
    /// own error type into the crate's error without naming its fields.
    ///
    /// # Examples
    ///
    /// ```
    /// use raft_io::Error;
    ///
    /// let io = std::io::Error::new(std::io::ErrorKind::Other, "disk full");
    /// let err = Error::storage("append entries", io);
    /// assert!(err.to_string().contains("disk full"));
    /// ```
    #[must_use]
    pub fn storage(context: &'static str, source: impl fmt::Display) -> Self {
        Self::Storage {
            context,
            detail: source.to_string(),
        }
    }

    /// Builds an [`Encoding`](Error::Encoding) error from any displayable codec
    /// error. Used by the `framing` layer.
    ///
    /// # Examples
    ///
    /// ```
    /// use raft_io::Error;
    ///
    /// let err = Error::encoding("decode message", "unexpected end of input");
    /// assert!(err.to_string().contains("unexpected end of input"));
    /// ```
    #[must_use]
    pub fn encoding(context: &'static str, source: impl fmt::Display) -> Self {
        Self::Encoding {
            context,
            detail: source.to_string(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_not_leader_display_with_known_leader() {
        let e = Error::NotLeader { leader: Some(7) };
        assert_eq!(e.to_string(), "not the leader; current leader is node 7");
    }

    #[test]
    fn test_not_leader_display_without_leader() {
        let e = Error::NotLeader { leader: None };
        assert_eq!(
            e.to_string(),
            "not the leader; no leader is currently known"
        );
    }

    #[test]
    fn test_storage_constructor_captures_detail() {
        let e = Error::storage("sync log", "device busy");
        assert_eq!(
            e.to_string(),
            "log storage error while sync log: device busy"
        );
    }

    #[test]
    fn test_forge_metadata_distinguishes_variants() {
        let not_leader = Error::NotLeader { leader: None };
        let storage = Error::storage("append entries", "x");
        assert_eq!(not_leader.kind(), "NotLeader");
        assert_eq!(storage.kind(), "Storage");
        assert!(not_leader.is_retryable());
        assert!(!not_leader.is_fatal());
        assert!(!storage.is_retryable());
        assert!(storage.is_fatal());
    }
}
