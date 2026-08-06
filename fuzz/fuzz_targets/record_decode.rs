//! Fuzz target: record/row decoding must never panic on malformed input.
//!
//! Exercises the engine's row decoding entry points with arbitrary bytes.
//! Any failure must surface as a typed `DbError`, not a panic (libFuzzer
//! reports panics as crashes).

#![no_main]

use libfuzzer_sys::fuzz_target;

fuzz_target!(|data: &[u8]| {
    let _ = decentdb::fuzzing::row_decode(data);
    let _ = decentdb::fuzzing::decode_varint_u64(data);

    if data.len() >= 2 {
        let column_index = u16::from_le_bytes([data[0], data[1]]) as usize % 64;
        let _ = decentdb::fuzzing::row_decode_int64_at(&data[2..], column_index);
    }
});
