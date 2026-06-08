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

use crate::error::{Error, Result};
use crate::message::Message;

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
///     term: 4, candidate: 2, last_log_index: 9, last_log_term: 3,
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
/// Returns [`Error::Encoding`](crate::Error) if the bytes are not a valid
/// encoded message.
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
    pack_io::decode(bytes).map_err(|e| Error::encoding("decode message", e))
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used, clippy::expect_used)]

    use super::*;
    use crate::message::{
        AppendEntries, AppendEntriesReply, InstallSnapshot, InstallSnapshotReply, RequestVote,
        RequestVoteReply,
    };
    use crate::types::{LogEntry, Snapshot};

    fn round_trip(message: Message) {
        let bytes = encode(&message).unwrap();
        assert_eq!(decode(&bytes).unwrap(), message);
    }

    #[test]
    fn test_every_message_variant_round_trips() {
        round_trip(Message::RequestVote(RequestVote {
            term: 4,
            candidate: 2,
            last_log_index: 9,
            last_log_term: 3,
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
            entries: vec![
                LogEntry::new(5, 3, b"cmd".to_vec()),
                LogEntry::new(5, 4, vec![]),
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
    }

    #[test]
    fn test_decode_rejects_garbage() {
        // A truncated / nonsensical byte string must not decode to a message.
        assert!(decode(&[0xFF, 0xFF, 0xFF]).is_err());
    }
}
