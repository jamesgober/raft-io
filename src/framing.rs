//! Typed wire framing for protocol messages.
//!
//! The protocol core is transport-agnostic: it emits
//! [`Action::Send`](crate::node::Action::Send) carrying a [`Message`] and
//! lets the caller move the bytes. This module, behind the `framing` feature,
//! supplies that codec — [`encode`] turns a [`Message`] into a
//! byte string, [`decode`] reads one back — built on `pack-io`, the portfolio's
//! typed binary format. The message types derive `pack_io::Serialize` /
//! `pack_io::Deserialize` under the feature, so the encoding is compact and
//! versioned without any hand-written codec.
//!
//! Using it is optional: a transport that already frames messages another way
//! does not need it. A decode failure yields [`Error::Encoding`](crate::Error),
//! which a transport should treat like a dropped message rather than a fatal
//! error.
//!
//! [`decode`] is built for bytes from untrusted peers: it never panics, it
//! accepts only the canonical encoding of a message (one byte string per
//! message, so frames can be hashed or deduplicated), and the memory it builds
//! is capped by [`decode_limit`]: pack-io's network preset (16 MiB), widened to
//! twice the frame length for larger frames, so an `InstallSnapshot` that
//! carries a large state machine still decodes while a small frame cannot
//! expand into gigabytes. Use [`decode_with_config`] to set a fixed limit
//! instead. Since 1.1.0 the codec is pack-io 2, whose wire format is identical
//! to the pack-io 1 format raft-io 1.0 used, so nodes on any 1.x release can
//! share a cluster.

use crate::error::{Error, Result};
use crate::message::Message;

/// The memory budget [`decode`] applies to a frame of `frame_len` bytes:
/// [`pack_io::Config::NETWORK_MAX_ALLOC`] (16 MiB), or twice the frame length
/// when that is larger.
///
/// A decoded message is at most about this size in memory, whatever the frame
/// claims. Messages are nearly one to one with their encoding except for long
/// runs of tiny log entries, which this bounds: a frame of minimal entries
/// beyond the 16 MiB floor fails to decode instead of growing sixteen-fold.
///
/// # Examples
///
/// ```
/// use raft_io::framing;
///
/// assert_eq!(framing::decode_limit(100), 16 << 20);
/// assert_eq!(framing::decode_limit(64 << 20), 128 << 20);
/// ```
#[must_use]
pub fn decode_limit(frame_len: usize) -> usize {
    pack_io::Config::NETWORK_MAX_ALLOC.max(frame_len.saturating_mul(2))
}

/// The pack-io configuration [`decode`] uses for a frame of `frame_len` bytes:
/// [`pack_io::Config::network`] with canonical decoding on and both limits set
/// to [`decode_limit`].
#[must_use]
fn decode_config(frame_len: usize) -> pack_io::Config {
    let limit = decode_limit(frame_len);
    pack_io::Config::network()
        .with_max_alloc(limit)
        .with_max_total_alloc(limit)
        .with_canonical(true)
}

/// Encodes a [`Message`] into its wire bytes.
///
/// # Errors
///
/// Returns [`Error::Encoding`](crate::Error) if serialization fails.
///
/// # Examples
///
/// ```
/// use raft_io::{framing, Message, RequestVote};
///
/// let msg = Message::RequestVote(RequestVote {
///     term: 4, candidate: 2, last_log_index: 9, last_log_term: 3, force: false,
/// });
/// let bytes = framing::encode(&msg).unwrap();
/// assert_eq!(framing::decode(&bytes).unwrap(), msg);
/// ```
pub fn encode(message: &Message) -> Result<Vec<u8>> {
    pack_io::encode(message).map_err(|e| Error::encoding("encode message", e))
}

/// Decodes a [`Message`] from wire bytes produced by
/// [`encode`].
///
/// # Errors
///
/// Returns [`Error::Encoding`](crate::Error) if the bytes are not the canonical
/// encoding of a message, including when a length or count in the input runs
/// past the end of the input or the message would build more than
/// [`decode_limit`] bytes in memory. Decoding never panics and never allocates
/// in proportion to a length the input merely claims.
///
/// # Examples
///
/// ```
/// use raft_io::{framing, AppendEntries, Message};
///
/// let msg = Message::AppendEntries(AppendEntries {
///     term: 1, leader: 1, prev_log_index: 0, prev_log_term: 0,
///     entries: Vec::new(), leader_commit: 0,
/// });
/// let bytes = framing::encode(&msg).unwrap();
/// let back = framing::decode(&bytes).unwrap();
/// assert_eq!(back, msg);
/// ```
pub fn decode(bytes: &[u8]) -> Result<Message> {
    pack_io::decode_with_config(bytes, decode_config(bytes.len()))
        .map_err(|e| Error::encoding("decode message", e))
}

/// Decodes a [`Message`] under an explicit pack-io [`Config`](pack_io::Config).
///
/// Use it to cap decode memory at a fixed size instead of [`decode_limit`],
/// for example `pack_io::Config::network().with_canonical(true)` for a
/// transport that never carries snapshots larger than 16 MiB. The config is
/// used as given; turn canonical mode on to keep [`decode`]'s one-encoding
/// guarantee.
///
/// # Errors
///
/// As [`decode`], with the limits of `config`.
///
/// # Examples
///
/// ```
/// use raft_io::{framing, Message, TimeoutNow};
///
/// let msg = Message::TimeoutNow(TimeoutNow { term: 3, leader: 1 });
/// let bytes = framing::encode(&msg).unwrap();
/// let config = pack_io::Config::network().with_max_alloc(4096).with_canonical(true);
/// assert_eq!(framing::decode_with_config(&bytes, config).unwrap(), msg);
/// ```
pub fn decode_with_config(bytes: &[u8], config: pack_io::Config) -> Result<Message> {
    pack_io::decode_with_config(bytes, config).map_err(|e| Error::encoding("decode message", e))
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used, clippy::expect_used)]

    use super::*;
    use crate::message::{
        AppendEntries, AppendEntriesReply, InstallSnapshot, InstallSnapshotReply, PreVote,
        PreVoteReply, ReadProbe, ReadProbeReply, RequestVote, RequestVoteReply, TimeoutNow,
    };
    use crate::types::{LogEntry, Snapshot};

    fn round_trip(message: Message) {
        let bytes = encode(&message).unwrap();
        assert_eq!(decode(&bytes).unwrap(), message);
    }

    #[test]
    fn test_every_message_variant_round_trips() {
        round_trip(Message::PreVote(PreVote {
            term: 4,
            candidate: 2,
            last_log_index: 9,
            last_log_term: 3,
        }));
        round_trip(Message::PreVoteReply(PreVoteReply {
            term: 4,
            vote_granted: true,
            from: 3,
        }));
        round_trip(Message::RequestVote(RequestVote {
            term: 4,
            candidate: 2,
            last_log_index: 9,
            last_log_term: 3,
            force: false,
        }));
        round_trip(Message::RequestVoteReply(RequestVoteReply {
            term: 4,
            vote_granted: true,
            from: 3,
        }));
        round_trip(Message::AppendEntries(AppendEntries {
            term: 5,
            leader: 1,
            prev_log_index: 2,
            prev_log_term: 1,
            // Mix a normal command and a configuration entry to cover EntryKind.
            entries: vec![
                LogEntry::new(5, 3, b"cmd".to_vec()),
                LogEntry::config(5, 4, &[1, 2, 3]),
            ],
            leader_commit: 2,
        }));
        round_trip(Message::AppendEntriesReply(AppendEntriesReply {
            term: 5,
            success: false,
            from: 2,
            match_index: 0,
            conflict_index: 3,
            conflict_term: 2,
        }));
        round_trip(Message::InstallSnapshot(InstallSnapshot {
            term: 6,
            leader: 1,
            snapshot: Snapshot::new(10, 3, b"serialized state".to_vec()),
        }));
        round_trip(Message::InstallSnapshotReply(InstallSnapshotReply {
            term: 6,
            from: 2,
            last_index: 10,
        }));
        round_trip(Message::InstallSnapshot(InstallSnapshot {
            term: 6,
            leader: 1,
            snapshot: Snapshot::with_config(10, 3, vec![1, 2, 3], b"state".to_vec()),
        }));
        round_trip(Message::TimeoutNow(TimeoutNow { term: 7, leader: 1 }));
        round_trip(Message::ReadProbe(ReadProbe {
            term: 7,
            leader: 1,
            seq: 42,
        }));
        round_trip(Message::ReadProbeReply(ReadProbeReply {
            term: 7,
            from: 2,
            seq: 42,
        }));
    }

    #[test]
    fn test_decode_rejects_garbage() {
        // A truncated / nonsensical byte string must not decode to a message.
        assert!(decode(&[0xFF, 0xFF, 0xFF]).is_err());
    }

    #[test]
    fn test_decode_accepts_only_the_canonical_encoding() {
        let msg = Message::TimeoutNow(TimeoutNow { term: 1, leader: 1 });
        let canonical = encode(&msg).unwrap();
        assert_eq!(decode(&canonical).unwrap(), msg);
        // The same message with an overlong varint for `term`.
        let mut overlong = vec![canonical[0], 0x81, 0x00];
        overlong.extend_from_slice(&canonical[2..]);
        assert!(decode(&overlong).is_err());
    }

    #[test]
    fn test_decode_memory_is_capped_relative_to_the_frame() {
        // 400,000 minimal entries: a frame of about 2.4 MB that would build
        // more than 20 MiB of `LogEntry` values, above the 16 MiB floor.
        let count = 400_000u64;
        let mut entries = Vec::new();
        for i in 0..count {
            entries.push(LogEntry::new(1, i + 1, Vec::new()));
        }
        let msg = Message::AppendEntries(AppendEntries {
            term: 1,
            leader: 1,
            prev_log_index: 0,
            prev_log_term: 0,
            entries,
            leader_commit: 0,
        });
        let bytes = encode(&msg).unwrap();
        assert!(bytes.len() < decode_limit(bytes.len()));
        assert!(decode(&bytes).is_err());
        // An explicit, roomier config still reads it.
        let roomy = pack_io::Config::new().with_canonical(true);
        assert_eq!(decode_with_config(&bytes, roomy).unwrap(), msg);
    }

    #[test]
    fn test_large_snapshots_still_decode() {
        // 20 MiB of snapshot data: above the 16 MiB network floor, but one to
        // one with its frame, so it fits twice the frame length.
        let data = vec![0x5a; 20 << 20];
        let msg = Message::InstallSnapshot(InstallSnapshot {
            term: 2,
            leader: 1,
            snapshot: Snapshot::new(10, 2, data),
        });
        let bytes = encode(&msg).unwrap();
        assert_eq!(decode(&bytes).unwrap(), msg);
    }

    proptest::proptest! {
        /// Fuzz the decode path: arbitrary bytes must yield `Ok` or `Err`, never
        /// a panic — untrusted input off the wire cannot crash a node.
        #[test]
        fn decode_never_panics_on_arbitrary_bytes(
            bytes in proptest::collection::vec(proptest::prelude::any::<u8>(), 0..512)
        ) {
            let _ = decode(&bytes);
        }

        /// Anything that decodes re-encodes to the identical bytes (a decoded
        /// message is in canonical form).
        #[test]
        fn decoded_messages_re_encode_identically(
            bytes in proptest::collection::vec(proptest::prelude::any::<u8>(), 0..512)
        ) {
            if let Ok(message) = decode(&bytes) {
                let re = encode(&message).unwrap();
                proptest::prop_assert_eq!(decode(&re).unwrap(), message);
            }
        }
    }
}
