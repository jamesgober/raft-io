//! Fuzz the message decode path: arbitrary bytes off the wire must never crash a
//! node, only decode to a valid [`Message`] or fail cleanly.
#![no_main]

use libfuzzer_sys::fuzz_target;

fuzz_target!(|data: &[u8]| {
    // Decoding untrusted input must return `Ok` or `Err`, never panic. When it
    // does decode, the result must re-encode to the same bytes (canonical form).
    if let Ok(message) = raft_io::framing::decode(data) {
        if let Ok(reencoded) = raft_io::framing::encode(&message) {
            let again = raft_io::framing::decode(&reencoded)
                .expect("a freshly encoded message must decode");
            assert_eq!(again, message, "decode/encode is not canonical");
        }
    }
});
