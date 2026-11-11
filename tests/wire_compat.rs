//! Wire compatibility: `framing::encode` must keep producing the exact bytes
//! that raft-io 1.0.0 produced, and `framing::decode` must keep reading them,
//! so nodes on different 1.x releases can share a cluster.
//!
//! The golden vectors below were written by raft-io 1.0.0 (built on pack-io
//! 1.0). Do not regenerate them: a mismatch means the wire format changed.
#![cfg(feature = "framing")]
#![allow(clippy::unwrap_used, clippy::expect_used)]

use raft_io::{
    AppendEntries, AppendEntriesReply, InstallSnapshot, InstallSnapshotReply, LogEntry, Message,
    PreVote, PreVoteReply, ReadProbe, ReadProbeReply, RequestVote, RequestVoteReply, Snapshot,
    TimeoutNow, framing,
};

fn corpus() -> Vec<Message> {
    vec![
        Message::PreVote(PreVote {
            term: 4,
            candidate: 2,
            last_log_index: 9,
            last_log_term: 3,
        }),
        Message::PreVoteReply(PreVoteReply {
            term: u64::MAX,
            vote_granted: true,
            from: 3,
        }),
        Message::RequestVote(RequestVote {
            term: 300,
            candidate: 70_000,
            last_log_index: 1 << 40,
            last_log_term: 299,
            force: true,
        }),
        Message::RequestVoteReply(RequestVoteReply {
            term: 0,
            vote_granted: false,
            from: 0,
        }),
        Message::AppendEntries(AppendEntries {
            term: 5,
            leader: 1,
            prev_log_index: 2,
            prev_log_term: 1,
            entries: vec![
                LogEntry::new(5, 3, b"set x = 1".to_vec()),
                LogEntry::new(5, 4, Vec::new()),
                LogEntry::config(5, 5, &[1, 2, 3]),
                LogEntry::config_with_learners(5, 6, &[1, 2, 3], &[9]),
            ],
            leader_commit: 2,
        }),
        Message::AppendEntries(AppendEntries {
            term: 1,
            leader: 1,
            prev_log_index: 0,
            prev_log_term: 0,
            entries: Vec::new(),
            leader_commit: 0,
        }),
        Message::AppendEntriesReply(AppendEntriesReply {
            term: 5,
            success: false,
            from: 2,
            match_index: 0,
            conflict_index: 3,
            conflict_term: 2,
        }),
        Message::InstallSnapshot(InstallSnapshot {
            term: 6,
            leader: 1,
            snapshot: Snapshot::new(10, 3, b"serialized state".to_vec()),
        }),
        Message::InstallSnapshot(InstallSnapshot {
            term: 6,
            leader: 1,
            snapshot: Snapshot::with_config(10, 3, vec![1, 2, 3], vec![0xFF; 200]),
        }),
        Message::InstallSnapshotReply(InstallSnapshotReply {
            term: 6,
            from: 2,
            last_index: 10,
        }),
        Message::TimeoutNow(TimeoutNow { term: 7, leader: 1 }),
        Message::ReadProbe(ReadProbe {
            term: 7,
            leader: 1,
            seq: 42,
        }),
        Message::ReadProbeReply(ReadProbeReply {
            term: 7,
            from: 2,
            seq: u64::MAX - 1,
        }),
    ]
}

fn hex(bytes: &[u8]) -> String {
    use std::fmt::Write;
    bytes.iter().fold(String::new(), |mut s, b| {
        let _ = write!(s, "{b:02x}");
        s
    })
}

fn unhex(s: &str) -> Vec<u8> {
    (0..s.len())
        .step_by(2)
        .map(|i| u8::from_str_radix(&s[i..i + 2], 16).unwrap())
        .collect()
}

/// `framing::encode` output for each message of [`corpus`], as written by
/// raft-io 1.0.0.
const GOLDEN: [&str; 13] = [
    "0004020903",
    "01ffffffffffffffffff010103",
    "02ac02f0a204808080808020ab0201",
    "03000000",
    concat!(
        "040501020104050300097365742078203d2031050400000505011801000000000000000200000000",
        "000000030000000000000005060128010000000000000002000000000000000300000000000000ff",
        "ffffffffffffff090000000000000002",
    ),
    "04010100000000",
    "05050002000302",
    "0606010a03001073657269616c697a6564207374617465",
    concat!(
        "0606010a0303010203c801ffffffffffffffffffffffffffffffffffffffffffffffffffffffffff",
        "ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff",
        "ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff",
        "ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff",
        "ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff",
        "ffffffffffffffffffffff",
    ),
    "0706020a",
    "080701",
    "0907012a",
    "0a0702feffffffffffffffff01",
];

#[test]
fn encode_matches_the_1_0_wire_bytes() {
    for (message, golden) in corpus().iter().zip(GOLDEN) {
        let bytes = framing::encode(message).unwrap();
        assert_eq!(hex(&bytes), golden, "wire bytes changed for {message:?}");
    }
}

#[test]
fn decode_reads_the_1_0_wire_bytes() {
    for (message, golden) in corpus().into_iter().zip(GOLDEN) {
        let decoded = framing::decode(&unhex(golden)).unwrap();
        assert_eq!(decoded, message);
    }
}
