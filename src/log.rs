//! The log-storage seam and its in-memory implementation.
//!
//! [`RaftLog`] is the boundary between the protocol and where the log actually
//! lives. The node reads through it (last index, term at an index, an entry)
//! and writes through it (append, truncate, hard state), and treats a returned
//! `Ok` from [`sync`](RaftLog::sync) as the durability point: everything
//! written before a successful `sync` will survive a crash. That contract is
//! what lets the same protocol run over a throwaway [`MemoryLog`] in tests and a
//! `wal-db`-backed store in production (arriving in `v0.4`) without the core
//! knowing the difference.
//!
//! Implementors map their own failures into [`Error::Storage`] via
//! [`Error::storage`](crate::Error::storage), so the trait's error type stays
//! the crate's own — no associated error type for callers to name.

use crate::error::{Error, Result};
use crate::types::{HardState, Index, LogEntry, Term};

/// Storage for a node's persistent state: its log entries and its
/// [`HardState`].
///
/// Indices are 1-based and contiguous. Index `0` is the sentinel "before the
/// first entry": [`term_at`](RaftLog::term_at) returns `Some(0)` for it so the
/// `prev_log_index` consistency check at the head of the log needs no special
/// case.
///
/// # Durability contract
///
/// A backend may buffer writes, but once [`sync`](RaftLog::sync) returns `Ok`,
/// every preceding [`append`](RaftLog::append),
/// [`truncate`](RaftLog::truncate), and
/// [`set_hard_state`](RaftLog::set_hard_state) must be durable. The node always
/// calls `sync` before emitting any message that depends on that state, which
/// is how it honours Raft's "persist before you respond" rule.
///
/// # Examples
///
/// Implementing a custom backend means forwarding to your store and mapping its
/// errors. The in-memory [`MemoryLog`] is the reference implementation; see its
/// source for the full shape. A read-through usage example:
///
/// ```
/// use raft_io::{LogEntry, MemoryLog, RaftLog};
///
/// let mut log = MemoryLog::new();
/// log.append(&[LogEntry::new(1, 1, b"a".to_vec())]).unwrap();
/// log.sync().unwrap();
///
/// assert_eq!(log.last_index(), 1);
/// assert_eq!(log.last_term(), 1);
/// assert_eq!(log.term_at(1), Some(1));
/// assert_eq!(log.term_at(0), Some(0)); // sentinel
/// assert_eq!(log.entry(1).unwrap().command, b"a");
/// ```
pub trait RaftLog {
    /// Returns the index of the last entry, or `0` if the log is empty.
    fn last_index(&self) -> Index;

    /// Returns the term of the last entry, or `0` if the log is empty.
    fn last_term(&self) -> Term;

    /// Returns the term of the entry at `index`.
    ///
    /// Returns `Some(0)` for the sentinel index `0`, `Some(term)` for an entry
    /// that exists, and `None` for an index past the end of the log.
    fn term_at(&self, index: Index) -> Option<Term>;

    /// Returns the entry at `index`, or `None` if there is none.
    fn entry(&self, index: Index) -> Option<LogEntry>;

    /// Appends `entries` to the end of the log.
    ///
    /// The first entry's index must be exactly `last_index() + 1` and the
    /// entries must be contiguous; an implementation must reject a gap or
    /// overlap rather than corrupt the log. Appending an empty slice is a no-op.
    ///
    /// # Errors
    ///
    /// Returns [`Error::Storage`] if the entries are not contiguous with the log
    /// or the backend fails to store them.
    fn append(&mut self, entries: &[LogEntry]) -> Result<()>;

    /// Removes every entry whose index is `>= from`.
    ///
    /// Used to resolve a conflict when a follower's log diverges from the
    /// leader's (the replication path, `v0.3`). `from` must be `>= 1`; the
    /// sentinel at index `0` cannot be removed.
    ///
    /// # Errors
    ///
    /// Returns [`Error::Storage`] if `from` is `0` or the backend fails.
    fn truncate(&mut self, from: Index) -> Result<()>;

    /// Returns the persisted [`HardState`] (current term and vote).
    fn hard_state(&self) -> HardState;

    /// Persists `state` as the new [`HardState`].
    ///
    /// # Errors
    ///
    /// Returns [`Error::Storage`] if the backend fails to store it.
    fn set_hard_state(&mut self, state: HardState) -> Result<()>;

    /// Flushes all preceding writes to durable storage.
    ///
    /// After this returns `Ok`, the durability contract holds for everything
    /// written so far. The in-memory log has nothing to flush and returns `Ok`
    /// immediately.
    ///
    /// # Errors
    ///
    /// Returns [`Error::Storage`] if the backend cannot make its writes durable.
    fn sync(&mut self) -> Result<()>;
}

/// An in-memory [`RaftLog`] backed by a `Vec`.
///
/// This is the default store and the one [`RaftNode::new`](crate::RaftNode::new)
/// uses. It keeps entries in a vector (entry at index `i` lives at slot `i - 1`)
/// and the hard state in a field. Nothing is durable across a process restart —
/// it is for tests, examples, and the single-node path, not production. Its
/// operations never fail except on a misuse that would corrupt the log
/// (a non-contiguous append or a `truncate(0)`).
///
/// # Examples
///
/// ```
/// use raft_io::{HardState, LogEntry, MemoryLog, RaftLog};
///
/// let mut log = MemoryLog::new();
/// assert_eq!(log.last_index(), 0);
///
/// log.append(&[LogEntry::new(1, 1, b"x".to_vec())]).unwrap();
/// log.set_hard_state(HardState { term: 1, voted_for: Some(1) }).unwrap();
/// log.sync().unwrap();
///
/// assert_eq!(log.last_index(), 1);
/// assert_eq!(log.hard_state().voted_for, Some(1));
/// ```
#[derive(Clone, Debug, Default)]
pub struct MemoryLog {
    entries: Vec<LogEntry>,
    hard: HardState,
}

impl MemoryLog {
    /// Creates an empty in-memory log.
    ///
    /// # Examples
    ///
    /// ```
    /// use raft_io::{MemoryLog, RaftLog};
    ///
    /// let log = MemoryLog::new();
    /// assert_eq!(log.last_index(), 0);
    /// ```
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Returns the number of entries currently stored.
    ///
    /// # Examples
    ///
    /// ```
    /// use raft_io::{LogEntry, MemoryLog, RaftLog};
    ///
    /// let mut log = MemoryLog::new();
    /// log.append(&[LogEntry::new(1, 1, vec![])]).unwrap();
    /// assert_eq!(log.len(), 1);
    /// ```
    #[inline]
    #[must_use]
    pub fn len(&self) -> usize {
        self.entries.len()
    }

    /// Returns `true` if the log holds no entries.
    ///
    /// # Examples
    ///
    /// ```
    /// use raft_io::MemoryLog;
    ///
    /// assert!(MemoryLog::new().is_empty());
    /// ```
    #[inline]
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }
}

impl RaftLog for MemoryLog {
    #[inline]
    fn last_index(&self) -> Index {
        self.entries.len() as Index
    }

    #[inline]
    fn last_term(&self) -> Term {
        self.entries.last().map_or(0, |e| e.term)
    }

    fn term_at(&self, index: Index) -> Option<Term> {
        if index == 0 {
            return Some(0);
        }
        self.entries.get((index - 1) as usize).map(|e| e.term)
    }

    fn entry(&self, index: Index) -> Option<LogEntry> {
        if index == 0 {
            return None;
        }
        self.entries.get((index - 1) as usize).cloned()
    }

    fn append(&mut self, entries: &[LogEntry]) -> Result<()> {
        if entries.is_empty() {
            return Ok(());
        }
        let expected = self.last_index() + 1;
        if entries[0].index != expected {
            return Err(Error::storage(
                "append entries",
                format!(
                    "non-contiguous append: expected index {expected}, got {}",
                    entries[0].index
                ),
            ));
        }
        // The slice itself must be internally contiguous too.
        for pair in entries.windows(2) {
            if pair[1].index != pair[0].index + 1 {
                return Err(Error::storage(
                    "append entries",
                    "entries within the batch are not contiguous",
                ));
            }
        }
        self.entries.extend_from_slice(entries);
        Ok(())
    }

    fn truncate(&mut self, from: Index) -> Result<()> {
        if from == 0 {
            return Err(Error::storage(
                "truncate log",
                "cannot truncate the sentinel at index 0",
            ));
        }
        let keep = (from - 1) as usize;
        if keep < self.entries.len() {
            self.entries.truncate(keep);
        }
        Ok(())
    }

    #[inline]
    fn hard_state(&self) -> HardState {
        self.hard
    }

    #[inline]
    fn set_hard_state(&mut self, state: HardState) -> Result<()> {
        self.hard = state;
        Ok(())
    }

    #[inline]
    fn sync(&mut self) -> Result<()> {
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used, clippy::expect_used)]

    use super::*;

    fn entry(term: Term, index: Index) -> LogEntry {
        LogEntry::new(term, index, vec![index as u8])
    }

    #[test]
    fn test_empty_log_reports_zero() {
        let log = MemoryLog::new();
        assert_eq!(log.last_index(), 0);
        assert_eq!(log.last_term(), 0);
        assert!(log.is_empty());
        assert_eq!(log.entry(0), None);
        assert_eq!(log.entry(1), None);
    }

    #[test]
    fn test_term_at_sentinel_is_zero() {
        assert_eq!(MemoryLog::new().term_at(0), Some(0));
    }

    #[test]
    fn test_append_and_read_back() {
        let mut log = MemoryLog::new();
        log.append(&[entry(1, 1), entry(1, 2)]).unwrap();
        log.append(&[entry(2, 3)]).unwrap();
        assert_eq!(log.last_index(), 3);
        assert_eq!(log.last_term(), 2);
        assert_eq!(log.term_at(2), Some(1));
        assert_eq!(log.term_at(3), Some(2));
        assert_eq!(log.term_at(4), None);
        assert_eq!(log.entry(3).unwrap().term, 2);
    }

    #[test]
    fn test_append_empty_is_noop() {
        let mut log = MemoryLog::new();
        log.append(&[]).unwrap();
        assert_eq!(log.last_index(), 0);
    }

    #[test]
    fn test_append_rejects_gap() {
        let mut log = MemoryLog::new();
        let err = log.append(&[entry(1, 2)]).unwrap_err();
        assert!(matches!(err, Error::Storage { .. }));
    }

    #[test]
    fn test_append_rejects_internally_noncontiguous_batch() {
        let mut log = MemoryLog::new();
        let err = log.append(&[entry(1, 1), entry(1, 3)]).unwrap_err();
        assert!(matches!(err, Error::Storage { .. }));
    }

    #[test]
    fn test_truncate_removes_tail() {
        let mut log = MemoryLog::new();
        log.append(&[entry(1, 1), entry(1, 2), entry(1, 3)])
            .unwrap();
        log.truncate(2).unwrap();
        assert_eq!(log.last_index(), 1);
        assert_eq!(log.entry(2), None);
    }

    #[test]
    fn test_truncate_past_end_is_noop() {
        let mut log = MemoryLog::new();
        log.append(&[entry(1, 1)]).unwrap();
        log.truncate(5).unwrap();
        assert_eq!(log.last_index(), 1);
    }

    #[test]
    fn test_truncate_zero_is_rejected() {
        let mut log = MemoryLog::new();
        assert!(log.truncate(0).is_err());
    }

    #[test]
    fn test_hard_state_round_trips() {
        let mut log = MemoryLog::new();
        let hs = HardState {
            term: 4,
            voted_for: Some(2),
        };
        log.set_hard_state(hs).unwrap();
        assert_eq!(log.hard_state(), hs);
    }

    #[test]
    fn test_sync_is_ok() {
        assert!(MemoryLog::new().sync().is_ok());
    }
}
