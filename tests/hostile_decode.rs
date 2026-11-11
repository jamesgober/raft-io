//! Adversarial decode: a few bytes from a peer must not make
//! `framing::decode` allocate memory in proportion to a length the peer
//! merely claims. A tracking allocator records the largest single
//! allocation made by the decoding thread.
#![cfg(feature = "framing")]
#![allow(clippy::unwrap_used, clippy::expect_used, unsafe_code)]

use std::alloc::{GlobalAlloc, Layout, System};
use std::cell::Cell;

use raft_io::framing;

struct Tracking;

thread_local! {
    static LARGEST: Cell<usize> = const { Cell::new(0) };
}

fn note(size: usize) {
    // `try_with` so allocations during thread teardown are ignored.
    let _ = LARGEST.try_with(|l| {
        if size > l.get() {
            l.set(size);
        }
    });
}

// SAFETY: every method forwards to the system allocator unchanged; the
// only addition is recording the requested size in a const-initialized
// thread-local `Cell`, which never allocates.
unsafe impl GlobalAlloc for Tracking {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        note(layout.size());
        // SAFETY: forwarded with the caller's layout.
        unsafe { System.alloc(layout) }
    }

    unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
        note(layout.size());
        // SAFETY: forwarded with the caller's layout.
        unsafe { System.alloc_zeroed(layout) }
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        // SAFETY: `ptr` came from `System` with this layout.
        unsafe { System.dealloc(ptr, layout) }
    }

    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        note(new_size);
        // SAFETY: forwarded with the caller's pointer and layout.
        unsafe { System.realloc(ptr, layout, new_size) }
    }
}

#[global_allocator]
static GLOBAL: Tracking = Tracking;

/// Largest single allocation `framing::decode(input)` makes, and whether
/// the decode failed.
fn largest_allocation(input: &[u8]) -> (usize, bool) {
    LARGEST.with(|l| l.set(0));
    let failed = framing::decode(input).is_err();
    (LARGEST.with(Cell::get), failed)
}

/// One mebibyte: far above anything a short input needs.
const LIMIT: usize = 1 << 20;

#[test]
fn snapshot_data_length_is_not_trusted() {
    // InstallSnapshot whose snapshot data claims 512 MiB but ends here.
    let input = [
        0x06, 0x06, 0x01, 0x0a, 0x03, 0x00, 0x80, 0x80, 0x80, 0x80, 0x02,
    ];
    let (largest, failed) = largest_allocation(&input);
    assert!(failed, "truncated input must not decode");
    assert!(
        largest < LIMIT,
        "decode allocated {largest} bytes for an 11-byte input"
    );
}

#[test]
fn snapshot_config_count_is_not_trusted() {
    // InstallSnapshot whose member list claims 2^28 node ids.
    let input = [0x06, 0x06, 0x01, 0x0a, 0x03, 0x80, 0x80, 0x80, 0x80, 0x01];
    let (largest, failed) = largest_allocation(&input);
    assert!(failed, "truncated input must not decode");
    assert!(
        largest < LIMIT,
        "decode allocated {largest} bytes for a 10-byte input"
    );
}

#[test]
fn entry_count_is_not_trusted() {
    // AppendEntries claiming 2^28 log entries.
    let input = [0x04, 0x05, 0x01, 0x02, 0x01, 0x80, 0x80, 0x80, 0x80, 0x01];
    let (largest, failed) = largest_allocation(&input);
    assert!(failed, "truncated input must not decode");
    assert!(
        largest < LIMIT,
        "decode allocated {largest} bytes for a 10-byte input"
    );
}

#[test]
fn entry_command_length_is_not_trusted() {
    // AppendEntries with one entry whose command claims 512 MiB.
    let input = [
        0x04, 0x05, 0x01, 0x02, 0x01, 0x01, 0x05, 0x03, 0x00, 0x80, 0x80, 0x80, 0x80, 0x02,
    ];
    let (largest, failed) = largest_allocation(&input);
    assert!(failed, "truncated input must not decode");
    assert!(
        largest < LIMIT,
        "decode allocated {largest} bytes for a 14-byte input"
    );
}

#[test]
fn every_truncation_of_valid_messages_fails_cleanly() {
    // A valid message cut at every byte must fail without a large
    // allocation; the full message must still decode.
    let message = raft_io::Message::InstallSnapshot(raft_io::InstallSnapshot {
        term: 6,
        leader: 1,
        snapshot: raft_io::Snapshot::with_config(10, 3, vec![1, 2, 3], vec![7; 300]),
    });
    let bytes = framing::encode(&message).unwrap();
    for cut in 0..bytes.len() {
        let (largest, failed) = largest_allocation(&bytes[..cut]);
        assert!(failed, "prefix of {cut} bytes decoded");
        assert!(largest < LIMIT);
    }
    assert_eq!(framing::decode(&bytes).unwrap(), message);
}
