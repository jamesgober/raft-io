//! A durable [`RaftLog`] backed by `wal-db`.
//!
//! [`WalLog`] gives a node a log that survives a restart. Raft's safety rests on
//! `current_term`, `voted_for`, and the log entries being durable *before* the
//! node acts on them — a node that forgot it had voted, or lost an
//! acknowledged entry, could break consensus. This type provides that durability
//! while presenting the same [`RaftLog`] interface as the in-memory store, so
//! the protocol core is unchanged.
//!
//! # Design
//!
//! The store is log-structured. Every mutation — an appended entry, a hard-state
//! update, a truncation — is encoded as a record and appended to a `wal-db`
//! write-ahead log, which frames and checksums each record. An in-memory index
//! (a [`MemoryLog`]) mirrors the current state for fast reads. On
//! [`open`](WalLog::open) the records are replayed in order to rebuild that index
//! exactly. Installing a snapshot writes a snapshot record and then physically
//! drops every earlier record from the WAL (re-persisting the current hard state
//! first), so the file stays bounded as the log is compacted.
//!
//! [`RaftLog`]: crate::RaftLog

use wal_db::Wal;

use crate::error::{Error, Result};
use crate::log::{MemoryLog, RaftLog};
use crate::types::{EntryKind, HardState, Index, LogEntry, NodeId, Snapshot, Term};

/// Record tag for an appended [`LogEntry`].
const TAG_ENTRY: u8 = 1;
/// Record tag for a [`HardState`] update.
const TAG_HARD_STATE: u8 = 2;
/// Record tag for a truncation to a given index.
const TAG_TRUNCATE: u8 = 3;
/// Record tag for an installed [`Snapshot`].
const TAG_SNAPSHOT: u8 = 4;

/// A durable [`RaftLog`] whose entries and hard state survive a process restart.
///
/// Open it with [`open`](WalLog::open) and hand it to
/// [`RaftNode::with_log`](crate::RaftNode::with_log). Reads are served from an
/// in-memory index; writes are appended to the underlying `wal-db` log and
/// become durable when [`sync`](RaftLog::sync) returns `Ok`.
///
/// # Examples
///
/// ```no_run
/// use raft_io::{LogEntry, RaftLog, WalLog};
///
/// let mut log = WalLog::open("raft.wal")?;
/// log.append(&[LogEntry::new(1, 1, b"set x = 1".to_vec())])?;
/// log.sync()?; // durable from here
///
/// // After a restart, reopening the same path recovers the entry.
/// let recovered = WalLog::open("raft.wal")?;
/// assert_eq!(recovered.last_index(), 1);
/// # Ok::<(), raft_io::Error>(())
/// ```
#[cfg_attr(docsrs, doc(cfg(feature = "persistence")))]
pub struct WalLog {
    wal: Wal,
    index: MemoryLog,
}

impl WalLog {
    /// Opens the durable log at `path`, replaying any existing records to recover
    /// the log entries and hard state.
    ///
    /// Creates the file if it does not exist. Recovery is exact: the recovered
    /// state is the logical result of every mutation that was appended before the
    /// process stopped.
    ///
    /// # Errors
    ///
    /// Returns [`Error::Storage`] if the WAL cannot be opened or a record fails
    /// to decode (for example, a checksum mismatch reported by `wal-db`).
    pub fn open(path: impl AsRef<std::path::Path>) -> Result<Self> {
        let wal = Wal::open(path).map_err(|e| Error::storage("open durable log", e))?;
        let mut index = MemoryLog::new();
        let iter = wal
            .iter()
            .map_err(|e| Error::storage("read durable log", e))?;
        for record in iter {
            let record = record.map_err(|e| Error::storage("read durable log record", e))?;
            match decode(record.data())? {
                Decoded::Entry(entry) => index.append(&[entry])?,
                Decoded::HardState(hs) => index.set_hard_state(hs)?,
                Decoded::Truncate(from) => index.truncate(from)?,
                Decoded::Snapshot(snapshot) => index.apply_snapshot(&snapshot)?,
            }
        }
        Ok(Self { wal, index })
    }

    /// Appends `record` to the WAL, mapping any backend failure to a storage
    /// error tagged with `context`.
    fn write(&self, context: &'static str, record: &[u8]) -> Result<()> {
        self.wal
            .append(record)
            .map(|_lsn| ())
            .map_err(|e| Error::storage(context, e))
    }
}

impl RaftLog for WalLog {
    #[inline]
    fn last_index(&self) -> Index {
        self.index.last_index()
    }

    #[inline]
    fn last_term(&self) -> Term {
        self.index.last_term()
    }

    #[inline]
    fn term_at(&self, index: Index) -> Option<Term> {
        self.index.term_at(index)
    }

    #[inline]
    fn entry(&self, index: Index) -> Option<LogEntry> {
        self.index.entry(index)
    }

    #[inline]
    fn entries(&self, from: Index, to: Index) -> Vec<LogEntry> {
        self.index.entries(from, to)
    }

    fn append(&mut self, entries: &[LogEntry]) -> Result<()> {
        // Validate contiguity against the in-memory index first, so a bad batch
        // is rejected before any record reaches the durable log.
        self.index.append(entries)?;
        for entry in entries {
            self.write("append entry to durable log", &encode_entry(entry))?;
        }
        Ok(())
    }

    fn truncate(&mut self, from: Index) -> Result<()> {
        self.index.truncate(from)?;
        self.write("truncate durable log", &encode_truncate(from))
    }

    #[inline]
    fn hard_state(&self) -> HardState {
        self.index.hard_state()
    }

    fn set_hard_state(&mut self, state: HardState) -> Result<()> {
        self.index.set_hard_state(state)?;
        self.write("persist hard state", &encode_hard_state(&state))
    }

    fn sync(&mut self) -> Result<()> {
        self.wal
            .sync()
            .map_err(|e| Error::storage("sync durable log", e))
    }

    #[inline]
    fn snapshot_index(&self) -> Index {
        self.index.snapshot_index()
    }

    fn snapshot(&self) -> Option<Snapshot> {
        self.index.snapshot()
    }

    fn apply_snapshot(&mut self, snapshot: &Snapshot) -> Result<()> {
        if snapshot.index <= self.index.snapshot_index() {
            return Ok(()); // stale; nothing to persist
        }
        // Compact the in-memory index first.
        self.index.apply_snapshot(snapshot)?;
        // Persist the snapshot record, then re-write the current hard state so
        // the latest term/vote sits *after* the snapshot in the log.
        let lsn = self
            .wal
            .append(&encode_snapshot(snapshot))
            .map_err(|e| Error::storage("persist snapshot", e))?;
        self.write(
            "persist hard state",
            &encode_hard_state(&self.index.hard_state()),
        )?;
        // Physically drop every record before the snapshot. This is an
        // optimisation: if it fails, the WAL is merely larger — replay still
        // re-applies the snapshot record and reconstructs the same state — so the
        // outcome is deliberately ignored rather than turned into a fatal error.
        let _ = self.wal.truncate_before(lsn);
        Ok(())
    }
}

// ---- record codec --------------------------------------------------------

/// A decoded WAL record.
enum Decoded {
    Entry(LogEntry),
    HardState(HardState),
    Truncate(Index),
    Snapshot(Snapshot),
}

fn encode_snapshot(snapshot: &Snapshot) -> Vec<u8> {
    let mut buf =
        Vec::with_capacity(1 + 8 + 8 + 8 + snapshot.config.len() * 8 + 8 + snapshot.data.len());
    buf.push(TAG_SNAPSHOT);
    buf.extend_from_slice(&snapshot.index.to_le_bytes());
    buf.extend_from_slice(&snapshot.term.to_le_bytes());
    buf.extend_from_slice(&(snapshot.config.len() as u64).to_le_bytes());
    for &id in &snapshot.config {
        buf.extend_from_slice(&id.to_le_bytes());
    }
    buf.extend_from_slice(&(snapshot.data.len() as u64).to_le_bytes());
    buf.extend_from_slice(&snapshot.data);
    buf
}

/// On-disk byte for an [`EntryKind`].
fn kind_byte(kind: EntryKind) -> u8 {
    match kind {
        EntryKind::Normal => 0,
        EntryKind::Config => 1,
    }
}

/// Reads an [`EntryKind`] from its on-disk byte.
fn kind_from_byte(byte: u8) -> Result<EntryKind> {
    match byte {
        0 => Ok(EntryKind::Normal),
        1 => Ok(EntryKind::Config),
        other => Err(Error::storage(
            "decode durable log record",
            format!("unknown entry kind {other}"),
        )),
    }
}

fn encode_entry(entry: &LogEntry) -> Vec<u8> {
    let mut buf = Vec::with_capacity(1 + 8 + 8 + 1 + 8 + entry.command.len());
    buf.push(TAG_ENTRY);
    buf.extend_from_slice(&entry.term.to_le_bytes());
    buf.extend_from_slice(&entry.index.to_le_bytes());
    buf.push(kind_byte(entry.kind));
    buf.extend_from_slice(&(entry.command.len() as u64).to_le_bytes());
    buf.extend_from_slice(&entry.command);
    buf
}

fn encode_hard_state(state: &HardState) -> Vec<u8> {
    let mut buf = Vec::with_capacity(1 + 8 + 1 + 8);
    buf.push(TAG_HARD_STATE);
    buf.extend_from_slice(&state.term.to_le_bytes());
    match state.voted_for {
        Some(id) => {
            buf.push(1);
            buf.extend_from_slice(&id.to_le_bytes());
        }
        None => {
            buf.push(0);
            buf.extend_from_slice(&0u64.to_le_bytes());
        }
    }
    buf
}

fn encode_truncate(from: Index) -> Vec<u8> {
    let mut buf = Vec::with_capacity(1 + 8);
    buf.push(TAG_TRUNCATE);
    buf.extend_from_slice(&from.to_le_bytes());
    buf
}

/// Reads a little-endian `u64` at `offset`, bounds-checked.
fn read_u64(data: &[u8], offset: usize) -> Result<u64> {
    let end = offset
        .checked_add(8)
        .filter(|&e| e <= data.len())
        .ok_or_else(|| Error::storage("decode durable log record", "record truncated"))?;
    let mut bytes = [0u8; 8];
    bytes.copy_from_slice(&data[offset..end]);
    Ok(u64::from_le_bytes(bytes))
}

fn decode(data: &[u8]) -> Result<Decoded> {
    let (&tag, rest_at) = match data.split_first() {
        Some((tag, _)) => (tag, 1usize),
        None => return Err(Error::storage("decode durable log record", "empty record")),
    };
    match tag {
        TAG_ENTRY => {
            let term = read_u64(data, rest_at)?;
            let index = read_u64(data, rest_at + 8)?;
            let kind =
                kind_from_byte(*data.get(rest_at + 16).ok_or_else(|| {
                    Error::storage("decode durable log record", "entry truncated")
                })?)?;
            let len = read_u64(data, rest_at + 17)? as usize;
            let start = rest_at + 25;
            let end = start
                .checked_add(len)
                .filter(|&e| e == data.len())
                .ok_or_else(|| {
                    Error::storage("decode durable log record", "entry length mismatch")
                })?;
            Ok(Decoded::Entry(LogEntry {
                term,
                index,
                kind,
                command: data[start..end].to_vec(),
            }))
        }
        TAG_HARD_STATE => {
            let term = read_u64(data, rest_at)?;
            let flag = *data.get(rest_at + 8).ok_or_else(|| {
                Error::storage("decode durable log record", "hard-state truncated")
            })?;
            let vote = read_u64(data, rest_at + 9)?;
            let voted_for = if flag == 1 { Some(vote) } else { None };
            Ok(Decoded::HardState(HardState { term, voted_for }))
        }
        TAG_TRUNCATE => {
            let from = read_u64(data, rest_at)?;
            Ok(Decoded::Truncate(from))
        }
        TAG_SNAPSHOT => {
            let index = read_u64(data, rest_at)?;
            let term = read_u64(data, rest_at + 8)?;
            let config_count = read_u64(data, rest_at + 16)? as usize;
            let mut config = Vec::with_capacity(config_count);
            let mut off = rest_at + 24;
            for _ in 0..config_count {
                config.push(read_u64(data, off)? as NodeId);
                off += 8;
            }
            let len = read_u64(data, off)? as usize;
            let start = off + 8;
            let end = start
                .checked_add(len)
                .filter(|&e| e == data.len())
                .ok_or_else(|| {
                    Error::storage("decode durable log record", "snapshot length mismatch")
                })?;
            Ok(Decoded::Snapshot(Snapshot::with_config(
                index,
                term,
                config,
                data[start..end].to_vec(),
            )))
        }
        other => Err(Error::storage(
            "decode durable log record",
            format!("unknown record tag {other}"),
        )),
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used, clippy::expect_used)]

    use super::*;

    fn entry(term: Term, index: Index, cmd: &[u8]) -> LogEntry {
        LogEntry::new(term, index, cmd.to_vec())
    }

    fn temp_path() -> (tempfile::TempDir, std::path::PathBuf) {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("raft.wal");
        (dir, path)
    }

    #[test]
    fn test_entry_codec_round_trips() {
        let e = entry(3, 9, b"hello world");
        match decode(&encode_entry(&e)).unwrap() {
            Decoded::Entry(got) => assert_eq!(got, e),
            _ => panic!("wrong record"),
        }
    }

    #[test]
    fn test_hard_state_codec_round_trips() {
        for hs in [
            HardState {
                term: 7,
                voted_for: Some(4),
            },
            HardState {
                term: 0,
                voted_for: None,
            },
        ] {
            match decode(&encode_hard_state(&hs)).unwrap() {
                Decoded::HardState(got) => assert_eq!(got, hs),
                _ => panic!("wrong record"),
            }
        }
    }

    #[test]
    fn test_truncate_codec_round_trips() {
        match decode(&encode_truncate(5)).unwrap() {
            Decoded::Truncate(from) => assert_eq!(from, 5),
            _ => panic!("wrong record"),
        }
    }

    #[test]
    fn test_decode_rejects_malformed() {
        assert!(decode(&[]).is_err()); // empty
        assert!(decode(&[TAG_ENTRY, 1, 2, 3]).is_err()); // truncated entry
        assert!(decode(&[TAG_TRUNCATE, 0, 0]).is_err()); // short index
        assert!(decode(&[99]).is_err()); // unknown tag
        // Entry claiming a longer command than is present.
        let mut bad = encode_entry(&entry(1, 1, b"x"));
        let _ = bad.pop(); // drop the command byte; length now mismatches
        assert!(decode(&bad).is_err());
    }

    #[test]
    fn test_append_sync_recover() {
        let (_dir, path) = temp_path();
        {
            let mut log = WalLog::open(&path).unwrap();
            log.append(&[entry(1, 1, b"a"), entry(1, 2, b"b")]).unwrap();
            log.set_hard_state(HardState {
                term: 1,
                voted_for: Some(2),
            })
            .unwrap();
            log.sync().unwrap();
        }
        let recovered = WalLog::open(&path).unwrap();
        assert_eq!(recovered.last_index(), 2);
        assert_eq!(recovered.last_term(), 1);
        assert_eq!(recovered.entry(2).unwrap().command, b"b");
        assert_eq!(
            recovered.hard_state(),
            HardState {
                term: 1,
                voted_for: Some(2)
            }
        );
    }

    #[test]
    fn test_truncation_survives_recovery() {
        let (_dir, path) = temp_path();
        {
            let mut log = WalLog::open(&path).unwrap();
            log.append(&[entry(1, 1, b"a"), entry(1, 2, b"b"), entry(1, 3, b"c")])
                .unwrap();
            log.truncate(2).unwrap(); // drop indices >= 2
            log.append(&[entry(2, 2, b"B")]).unwrap(); // re-write index 2 in a new term
            log.sync().unwrap();
        }
        let recovered = WalLog::open(&path).unwrap();
        assert_eq!(recovered.last_index(), 2);
        assert_eq!(recovered.entry(2).unwrap().term, 2);
        assert_eq!(recovered.entry(2).unwrap().command, b"B");
        assert_eq!(recovered.entry(3), None);
    }

    #[test]
    fn test_latest_hard_state_wins_on_recovery() {
        let (_dir, path) = temp_path();
        {
            let mut log = WalLog::open(&path).unwrap();
            log.set_hard_state(HardState {
                term: 1,
                voted_for: Some(1),
            })
            .unwrap();
            log.set_hard_state(HardState {
                term: 2,
                voted_for: None,
            })
            .unwrap();
            log.set_hard_state(HardState {
                term: 3,
                voted_for: Some(2),
            })
            .unwrap();
            log.sync().unwrap();
        }
        let recovered = WalLog::open(&path).unwrap();
        assert_eq!(
            recovered.hard_state(),
            HardState {
                term: 3,
                voted_for: Some(2)
            }
        );
    }

    #[test]
    fn test_snapshot_compaction_survives_recovery() {
        let (_dir, path) = temp_path();
        {
            let mut log = WalLog::open(&path).unwrap();
            log.append(&[entry(1, 1, b"a"), entry(1, 2, b"b"), entry(2, 3, b"c")])
                .unwrap();
            log.apply_snapshot(&Snapshot::new(2, 1, b"state@2".to_vec()))
                .unwrap();
            log.append(&[entry(2, 4, b"d")]).unwrap();
            log.sync().unwrap();
        }
        let recovered = WalLog::open(&path).unwrap();
        // The snapshot boundary, the surviving tail, and the snapshot bytes all
        // came back; compacted entries did not.
        assert_eq!(recovered.snapshot_index(), 2);
        assert_eq!(recovered.last_index(), 4);
        assert_eq!(recovered.entry(1), None);
        assert_eq!(recovered.entry(2), None);
        assert_eq!(recovered.term_at(2), Some(1));
        assert_eq!(recovered.entry(3).unwrap().command, b"c");
        assert_eq!(recovered.entry(4).unwrap().command, b"d");
        assert_eq!(recovered.snapshot().unwrap().data, b"state@2");
    }

    #[test]
    fn test_snapshot_codec_round_trips() {
        let snap = Snapshot::with_config(9, 4, vec![1, 2, 3], b"payload".to_vec());
        match decode(&encode_snapshot(&snap)).unwrap() {
            Decoded::Snapshot(got) => assert_eq!(got, snap),
            _ => panic!("wrong record"),
        }
    }

    #[test]
    fn test_config_entry_and_snapshot_membership_survive_recovery() {
        let (_dir, path) = temp_path();
        {
            let mut log = WalLog::open(&path).unwrap();
            log.apply_snapshot(&Snapshot::with_config(2, 1, vec![1, 2, 3], b"s".to_vec()))
                .unwrap();
            log.append(&[LogEntry::config(2, 3, &[1, 2, 3, 4])])
                .unwrap();
            log.sync().unwrap();
        }
        let recovered = WalLog::open(&path).unwrap();
        assert_eq!(recovered.snapshot().unwrap().config, vec![1, 2, 3]);
        assert_eq!(
            recovered.entry(3).unwrap().members(),
            Some(vec![1, 2, 3, 4])
        );
    }

    #[test]
    fn test_empty_log_opens_clean() {
        let (_dir, path) = temp_path();
        let log = WalLog::open(&path).unwrap();
        assert_eq!(log.last_index(), 0);
        assert_eq!(log.hard_state(), HardState::default());
    }

    #[test]
    fn test_non_contiguous_append_is_rejected_before_write() {
        let (_dir, path) = temp_path();
        let mut log = WalLog::open(&path).unwrap();
        assert!(log.append(&[entry(1, 5, b"x")]).is_err());
        // The rejected batch left nothing behind.
        assert_eq!(log.last_index(), 0);
        drop(log);
        assert_eq!(WalLog::open(&path).unwrap().last_index(), 0);
    }
}
