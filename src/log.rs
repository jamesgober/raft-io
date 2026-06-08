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
use crate::types::{HardState, Index, LogEntry, Snapshot, Term};

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

    /// Returns the entries in the inclusive index range `[from, to]`.
    ///
    /// Indices outside the log are skipped, and an empty range (`to < from`, or
    /// `from == 0`) yields an empty vector. The leader uses this to assemble a
    /// replication batch. The default implementation reads each index through
    /// [`entry`](RaftLog::entry); a backend that stores entries contiguously
    /// should override it with a single bulk read.
    fn entries(&self, from: Index, to: Index) -> Vec<LogEntry> {
        if from == 0 || to < from {
            return Vec::new();
        }
        let mut out = Vec::with_capacity((to - from + 1) as usize);
        let mut index = from;
        while index <= to {
            if let Some(entry) = self.entry(index) {
                out.push(entry);
            }
            index += 1;
        }
        out
    }

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

    /// Returns the index the log has been compacted up to — the last index a
    /// snapshot includes — or `0` if there is no snapshot.
    ///
    /// Entries at or below this index are no longer individually available;
    /// [`snapshot`](RaftLog::snapshot) covers them, and
    /// [`term_at`](RaftLog::term_at) still answers for the boundary index itself.
    /// Defaults to `0` for backends without snapshot support.
    fn snapshot_index(&self) -> Index {
        0
    }

    /// Returns the current snapshot, if one exists.
    ///
    /// A leader reads this to send a far-behind follower an `InstallSnapshot`
    /// instead of replaying entries it has already compacted away. Defaults to
    /// `None`.
    fn snapshot(&self) -> Option<Snapshot> {
        None
    }

    /// Installs `snapshot`, replacing the prefix it subsumes.
    ///
    /// Entries up to `snapshot.index` are discarded and the snapshot becomes the
    /// log's new base. A matching tail — an entry at `snapshot.index` whose term
    /// is `snapshot.term` — is preserved; otherwise the remaining entries are
    /// cleared because the snapshot supersedes them. A snapshot no newer than the
    /// current one is a no-op.
    ///
    /// The default implementation returns an error, so a backend that does not
    /// support snapshots fails loudly rather than silently dropping compaction.
    ///
    /// # Errors
    ///
    /// Returns [`Error::Storage`] if the backend does not support snapshots or
    /// fails to store it.
    fn apply_snapshot(&mut self, snapshot: &Snapshot) -> Result<()> {
        let _ = snapshot;
        Err(Error::storage(
            "apply snapshot",
            "this log does not support snapshots",
        ))
    }
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
/// After a snapshot is installed the log is compacted: entries up to the
/// snapshot's index are dropped and `base_index` / `base_term` become the log's
/// new starting boundary, so reads below the boundary return `None` while
/// [`term_at`](RaftLog::term_at) still answers for the boundary index itself.
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
    /// Entries with index in `(base_index, base_index + entries.len()]`.
    entries: Vec<LogEntry>,
    /// Index of the snapshot boundary (last included), or `0` if none.
    base_index: Index,
    /// Term at the snapshot boundary, or `0` if none.
    base_term: Term,
    /// Snapshot bytes, present once a snapshot has been installed.
    snapshot: Option<Vec<u8>>,
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

impl MemoryLog {
    /// Slot in `entries` for `index`, if it is in range `(base_index, last]`.
    #[inline]
    fn slot(&self, index: Index) -> Option<usize> {
        if index <= self.base_index || index > self.last_index() {
            None
        } else {
            Some((index - self.base_index - 1) as usize)
        }
    }
}

impl RaftLog for MemoryLog {
    #[inline]
    fn last_index(&self) -> Index {
        self.base_index + self.entries.len() as Index
    }

    #[inline]
    fn last_term(&self) -> Term {
        self.entries.last().map_or(self.base_term, |e| e.term)
    }

    fn term_at(&self, index: Index) -> Option<Term> {
        if index == self.base_index {
            return Some(self.base_term);
        }
        self.slot(index).map(|s| self.entries[s].term)
    }

    fn entry(&self, index: Index) -> Option<LogEntry> {
        self.slot(index).map(|s| self.entries[s].clone())
    }

    fn entries(&self, from: Index, to: Index) -> Vec<LogEntry> {
        if from == 0 {
            return Vec::new();
        }
        let from = from.max(self.base_index + 1);
        if to < from {
            return Vec::new();
        }
        let start = (from - self.base_index - 1) as usize;
        let end = ((to - self.base_index) as usize).min(self.entries.len());
        if start >= end {
            return Vec::new();
        }
        self.entries[start..end].to_vec()
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
        if from <= self.base_index {
            return Err(Error::storage(
                "truncate log",
                "cannot truncate into the snapshot",
            ));
        }
        let keep = (from - self.base_index - 1) as usize;
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

    #[inline]
    fn snapshot_index(&self) -> Index {
        self.base_index
    }

    fn snapshot(&self) -> Option<Snapshot> {
        self.snapshot
            .as_ref()
            .map(|data| Snapshot::new(self.base_index, self.base_term, data.clone()))
    }

    fn apply_snapshot(&mut self, snapshot: &Snapshot) -> Result<()> {
        // A snapshot no newer than the one we hold tells us nothing.
        if snapshot.index <= self.base_index {
            return Ok(());
        }
        // Keep the tail only if our log agrees with the snapshot at its boundary.
        if self.term_at(snapshot.index) == Some(snapshot.term) {
            let drop = ((snapshot.index - self.base_index) as usize).min(self.entries.len());
            let _ = self.entries.drain(0..drop);
        } else {
            self.entries.clear();
        }
        self.base_index = snapshot.index;
        self.base_term = snapshot.term;
        self.snapshot = Some(snapshot.data.clone());
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
    fn test_entries_range_inclusive() {
        let mut log = MemoryLog::new();
        log.append(&[entry(1, 1), entry(1, 2), entry(2, 3), entry(2, 4)])
            .unwrap();
        let mid = log.entries(2, 3);
        assert_eq!(mid.len(), 2);
        assert_eq!(mid[0].index, 2);
        assert_eq!(mid[1].index, 3);
    }

    #[test]
    fn test_entries_range_clamps_and_handles_empty() {
        let mut log = MemoryLog::new();
        log.append(&[entry(1, 1), entry(1, 2)]).unwrap();
        // Past the end is clamped.
        assert_eq!(log.entries(1, 99).len(), 2);
        // Empty / degenerate ranges yield nothing.
        assert!(log.entries(3, 2).is_empty());
        assert!(log.entries(0, 5).is_empty());
        assert!(log.entries(5, 9).is_empty());
    }

    #[test]
    fn test_default_entries_matches_override() {
        // Drive the trait's default `entries` impl through a wrapper that does
        // not override it, and confirm it agrees with `MemoryLog`'s bulk read.
        struct Wrap(MemoryLog);
        impl RaftLog for Wrap {
            fn last_index(&self) -> Index {
                self.0.last_index()
            }
            fn last_term(&self) -> Term {
                self.0.last_term()
            }
            fn term_at(&self, index: Index) -> Option<Term> {
                self.0.term_at(index)
            }
            fn entry(&self, index: Index) -> Option<LogEntry> {
                self.0.entry(index)
            }
            fn append(&mut self, entries: &[LogEntry]) -> Result<()> {
                self.0.append(entries)
            }
            fn truncate(&mut self, from: Index) -> Result<()> {
                self.0.truncate(from)
            }
            fn hard_state(&self) -> HardState {
                self.0.hard_state()
            }
            fn set_hard_state(&mut self, state: HardState) -> Result<()> {
                self.0.set_hard_state(state)
            }
            fn sync(&mut self) -> Result<()> {
                self.0.sync()
            }
        }
        let mut inner = MemoryLog::new();
        inner
            .append(&[entry(1, 1), entry(1, 2), entry(2, 3)])
            .unwrap();
        let wrap = Wrap(inner.clone());
        assert_eq!(wrap.entries(1, 3), inner.entries(1, 3));
        assert_eq!(wrap.entries(2, 2), inner.entries(2, 2));
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

    // ---- compaction / snapshots -------------------------------------------

    #[test]
    fn test_apply_snapshot_compacts_and_keeps_matching_tail() {
        let mut log = MemoryLog::new();
        log.append(&[entry(1, 1), entry(1, 2), entry(2, 3), entry(2, 4)])
            .unwrap();
        // Snapshot through index 2 (term 1) — our log matches there, keep the tail.
        log.apply_snapshot(&Snapshot::new(2, 1, b"state@2".to_vec()))
            .unwrap();

        assert_eq!(log.snapshot_index(), 2);
        assert_eq!(log.last_index(), 4);
        // Compacted entries are gone, the boundary term is still answerable.
        assert_eq!(log.entry(1), None);
        assert_eq!(log.entry(2), None);
        assert_eq!(log.term_at(2), Some(1)); // boundary
        assert_eq!(log.term_at(1), None); // below boundary
        // The tail survived.
        assert_eq!(log.entry(3).unwrap().term, 2);
        assert_eq!(log.entry(4).unwrap().index, 4);
        // The snapshot is retrievable.
        assert_eq!(log.snapshot().unwrap().data, b"state@2");
    }

    #[test]
    fn test_apply_snapshot_clears_log_on_mismatch() {
        let mut log = MemoryLog::new();
        log.append(&[entry(1, 1), entry(1, 2)]).unwrap();
        // A snapshot at index 5 our log cannot match supersedes everything.
        log.apply_snapshot(&Snapshot::new(5, 3, b"state@5".to_vec()))
            .unwrap();
        assert_eq!(log.snapshot_index(), 5);
        assert_eq!(log.last_index(), 5);
        assert_eq!(log.last_term(), 3);
        assert!(log.entries(1, 5).is_empty());
        assert_eq!(log.term_at(5), Some(3));
    }

    #[test]
    fn test_append_continues_after_snapshot() {
        let mut log = MemoryLog::new();
        log.apply_snapshot(&Snapshot::new(7, 2, b"base".to_vec()))
            .unwrap();
        assert_eq!(log.last_index(), 7);
        // Next append must be contiguous with the snapshot boundary.
        assert!(log.append(&[entry(2, 7)]).is_err()); // 7 already covered
        log.append(&[entry(3, 8), entry(3, 9)]).unwrap();
        assert_eq!(log.last_index(), 9);
        assert_eq!(log.entry(8).unwrap().term, 3);
        assert_eq!(log.term_at(7), Some(2)); // boundary term preserved
    }

    #[test]
    fn test_stale_snapshot_is_ignored() {
        let mut log = MemoryLog::new();
        log.apply_snapshot(&Snapshot::new(5, 2, b"new".to_vec()))
            .unwrap();
        log.apply_snapshot(&Snapshot::new(3, 1, b"old".to_vec()))
            .unwrap();
        assert_eq!(log.snapshot_index(), 5);
        assert_eq!(log.snapshot().unwrap().data, b"new");
    }

    #[test]
    fn test_truncate_into_snapshot_is_rejected() {
        let mut log = MemoryLog::new();
        log.apply_snapshot(&Snapshot::new(5, 2, b"s".to_vec()))
            .unwrap();
        assert!(log.truncate(5).is_err());
        assert!(log.truncate(3).is_err());
    }

    #[test]
    fn test_default_apply_snapshot_errors() {
        // The trait's default `apply_snapshot` rejects, so a snapshot-unaware
        // backend fails loudly.
        struct NoSnap(MemoryLog);
        impl RaftLog for NoSnap {
            fn last_index(&self) -> Index {
                self.0.last_index()
            }
            fn last_term(&self) -> Term {
                self.0.last_term()
            }
            fn term_at(&self, index: Index) -> Option<Term> {
                self.0.term_at(index)
            }
            fn entry(&self, index: Index) -> Option<LogEntry> {
                self.0.entry(index)
            }
            fn append(&mut self, entries: &[LogEntry]) -> Result<()> {
                self.0.append(entries)
            }
            fn truncate(&mut self, from: Index) -> Result<()> {
                self.0.truncate(from)
            }
            fn hard_state(&self) -> HardState {
                self.0.hard_state()
            }
            fn set_hard_state(&mut self, state: HardState) -> Result<()> {
                self.0.set_hard_state(state)
            }
            fn sync(&mut self) -> Result<()> {
                self.0.sync()
            }
        }
        let mut log = NoSnap(MemoryLog::new());
        assert_eq!(log.snapshot_index(), 0);
        assert!(log.snapshot().is_none());
        assert!(log.apply_snapshot(&Snapshot::new(1, 1, vec![])).is_err());
    }
}
