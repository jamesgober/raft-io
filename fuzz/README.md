# Fuzzing raft-io

The decode path is the only place `raft-io` touches untrusted input: bytes that
arrive over a transport and are turned back into a [`Message`] by
[`framing::decode`]. A malformed or hostile byte string must never crash a node —
it must decode to a valid message or fail cleanly with `Error::Encoding`.

This crate fuzzes that path with [`cargo-fuzz`] (libFuzzer). It is intentionally a
separate workspace so it does not affect the library's build, `Cargo.lock`, or
`cargo deny`/`cargo audit` results, and it is **not** run in the default CI
matrix: libFuzzer requires a nightly toolchain and does not run on Windows. The
same invariants are checked continuously and cross-platform by the `proptest`
no-panic tests in the library (`src/framing.rs`, `src/wal_log.rs`); this target is
for deeper, coverage-guided exploration on Linux/macOS.

## Running

```bash
# One-time, on a Linux/macOS host with a nightly toolchain:
cargo install cargo-fuzz

# From the crate root (the parent of this directory):
cargo +nightly fuzz run framing_decode
```

Add a time bound with `-- -max_total_time=60`, or replay a crash with
`cargo +nightly fuzz run framing_decode <artifact-path>`.

## Targets

- **`framing_decode`** — feeds arbitrary bytes to `framing::decode`. Asserts it
  never panics, and that anything which decodes re-encodes to identical bytes
  (the wire format is canonical).

[`cargo-fuzz`]: https://github.com/rust-fuzz/cargo-fuzz
