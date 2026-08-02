//! Comparable index-key encoding.
//!
//! Implements:
//! - design/adr/0061-typed-index-key-encoding-text-blob.md

use std::cmp::Ordering;

use smallvec::SmallVec;

use crate::error::{DbError, Result};
use crate::record::value::{
    compare_cidr, compare_decimal, compare_interval, compare_ip_addr, compare_mac_addr,
    normalize_decimal, Value, IP_FAMILY_V4,
};

const TAG_NULL: u8 = 0;
const TAG_BOOL: u8 = 1;
const TAG_INT64: u8 = 2;
const TAG_FLOAT64: u8 = 3;
const TAG_DECIMAL: u8 = 4;
const TAG_TIMESTAMP: u8 = 5;
const TAG_UUID: u8 = 6;
const TAG_TEXT: u8 = 7;
const TAG_BLOB: u8 = 8;
const TAG_ENUM: u8 = 9;
const TAG_IPADDR: u8 = 10;
const TAG_CIDR: u8 = 11;
const TAG_DATE: u8 = 12;
const TAG_TIME: u8 = 13;
const TAG_TIMESTAMP_TZ: u8 = 14;
const TAG_INTERVAL: u8 = 15;
const TAG_MACADDR: u8 = 16;

/// Owned comparable key used only by in-memory runtime indexes.
///
/// The common single-column text keys fit in the inline buffer, avoiding one
/// allocation per runtime B-tree entry. Longer keys spill to the heap while
/// retaining the exact byte ordering of the persistent encoding.
///
/// The inline capacity is pointer-width specific so the owner never exceeds
/// the three-word footprint of the `Vec<u8>` it replaces. Sixteen bytes fit in
/// union-layout `SmallVec` on 64-bit targets; eight bytes is the corresponding
/// capacity on supported 32-bit targets such as `wasm32-unknown-unknown`.
#[cfg(target_pointer_width = "64")]
pub(crate) type RuntimeEncodedKey = SmallVec<[u8; 16]>;
#[cfg(target_pointer_width = "32")]
pub(crate) type RuntimeEncodedKey = SmallVec<[u8; 8]>;

#[cfg(all(test, target_pointer_width = "64"))]
const RUNTIME_ENCODED_KEY_INLINE_BYTES: usize = 16;
#[cfg(all(test, target_pointer_width = "32"))]
const RUNTIME_ENCODED_KEY_INLINE_BYTES: usize = 8;

// This is a production invariant, not only a native unit-test expectation.
// In particular, it fails compilation if a dependency/layout change grows
// runtime B-tree nodes on wasm32.
const _: () = assert!(std::mem::size_of::<RuntimeEncodedKey>() == std::mem::size_of::<Vec<u8>>());

pub(crate) fn encode_index_key(value: &Value) -> Result<Vec<u8>> {
    encode_runtime_index_key(value).map(|key| key.into_vec())
}

/// Encodes a value using the canonical comparable-key representation while
/// keeping short keys inline for runtime indexes.
pub(crate) fn encode_runtime_index_key(value: &Value) -> Result<RuntimeEncodedKey> {
    match value {
        Value::Null => Ok(SmallVec::from_slice(&[TAG_NULL])),
        Value::Bool(value) => Ok(SmallVec::from_slice(&[TAG_BOOL, u8::from(*value)])),
        Value::Int64(value) => {
            let mut encoded = RuntimeEncodedKey::with_capacity(9);
            encoded.push(TAG_INT64);
            encoded.extend_from_slice(&sortable_signed_bytes(*value));
            Ok(encoded)
        }
        Value::Float64(value) => {
            let mut encoded = RuntimeEncodedKey::with_capacity(9);
            encoded.push(TAG_FLOAT64);
            encoded.extend_from_slice(&sortable_float_bytes(*value));
            Ok(encoded)
        }
        Value::Decimal { scaled, scale } => {
            let decimal = sortable_decimal_bytes(*scaled, *scale);
            let mut encoded = RuntimeEncodedKey::with_capacity(1 + decimal.len());
            encoded.push(TAG_DECIMAL);
            encoded.extend_from_slice(&decimal);
            Ok(encoded)
        }
        Value::TimestampMicros(value) => {
            let mut encoded = RuntimeEncodedKey::with_capacity(9);
            encoded.push(TAG_TIMESTAMP);
            encoded.extend_from_slice(&sortable_signed_bytes(*value));
            Ok(encoded)
        }
        Value::Uuid(value) => {
            let mut encoded = RuntimeEncodedKey::with_capacity(17);
            encoded.push(TAG_UUID);
            encoded.extend_from_slice(value);
            Ok(encoded)
        }
        Value::Text(value) => {
            let mut encoded = RuntimeEncodedKey::with_capacity(1 + value.len());
            encoded.push(TAG_TEXT);
            encoded.extend_from_slice(value.as_bytes());
            Ok(encoded)
        }
        Value::Blob(value) => {
            let mut encoded = RuntimeEncodedKey::with_capacity(1 + value.len());
            encoded.push(TAG_BLOB);
            encoded.extend_from_slice(value);
            Ok(encoded)
        }
        Value::Enum {
            enum_type_id,
            label_id,
        } => {
            let mut encoded = RuntimeEncodedKey::with_capacity(17);
            encoded.push(TAG_ENUM);
            encoded.extend_from_slice(&sortable_unsigned_bytes(*enum_type_id));
            encoded.extend_from_slice(&sortable_unsigned_bytes(*label_id));
            Ok(encoded)
        }
        Value::IpAddr { family, addr } => {
            let mut encoded = RuntimeEncodedKey::with_capacity(18);
            encoded.push(TAG_IPADDR);
            encoded.extend_from_slice(&ip_addr_sortable_bytes(*family, addr)?);
            encoded.push(*family);
            Ok(encoded)
        }
        Value::Cidr {
            family,
            prefix_len,
            network,
        } => {
            let mut encoded = RuntimeEncodedKey::with_capacity(20);
            encoded.push(TAG_CIDR);
            encoded.push(*family);
            encoded.push(*prefix_len);
            if *family == IP_FAMILY_V4 {
                encoded.extend_from_slice(&network[..4]);
            } else {
                encoded.extend_from_slice(network);
            }
            Ok(encoded)
        }
        Value::MacAddr { len, bytes } => {
            compare_mac_addr(*len, bytes, *len, bytes)?;
            let len = usize::from(*len);
            let mut encoded = RuntimeEncodedKey::with_capacity(2 + len);
            encoded.push(TAG_MACADDR);
            encoded.extend_from_slice(&bytes[..len]);
            encoded.push(u8::try_from(len).expect("MACADDR length fits u8"));
            Ok(encoded)
        }
        Value::DateDays(value) => {
            let mut encoded = RuntimeEncodedKey::with_capacity(5);
            encoded.push(TAG_DATE);
            encoded.extend_from_slice(&sortable_i32_bytes(*value));
            Ok(encoded)
        }
        Value::TimeMicros(value) => {
            let mut encoded = RuntimeEncodedKey::with_capacity(9);
            encoded.push(TAG_TIME);
            encoded.extend_from_slice(&sortable_signed_bytes(*value));
            Ok(encoded)
        }
        Value::TimestampTzMicros(value) => {
            let mut encoded = RuntimeEncodedKey::with_capacity(9);
            encoded.push(TAG_TIMESTAMP_TZ);
            encoded.extend_from_slice(&sortable_signed_bytes(*value));
            Ok(encoded)
        }
        Value::Interval {
            months,
            days,
            micros,
        } => {
            let mut encoded = RuntimeEncodedKey::with_capacity(17);
            encoded.push(TAG_INTERVAL);
            encoded.extend_from_slice(&sortable_i32_bytes(*months));
            encoded.extend_from_slice(&sortable_i32_bytes(*days));
            encoded.extend_from_slice(&sortable_signed_bytes(*micros));
            Ok(encoded)
        }
        Value::Geometry(_) | Value::Geography(_) => Err(DbError::constraint(
            "spatial values cannot be encoded as generic BTREE index keys",
        )),
    }
}

pub(crate) fn compare_index_values(left: &Value, right: &Value) -> Result<Ordering> {
    match (left, right) {
        (Value::Null, Value::Null) => Ok(Ordering::Equal),
        (Value::Bool(left), Value::Bool(right)) => Ok(left.cmp(right)),
        (Value::Int64(left), Value::Int64(right)) => Ok(left.cmp(right)),
        (Value::Float64(left), Value::Float64(right)) => Ok(left.total_cmp(right)),
        (
            Value::Decimal {
                scaled: left_scaled,
                scale: left_scale,
            },
            Value::Decimal {
                scaled: right_scaled,
                scale: right_scale,
            },
        ) => Ok(compare_decimal(
            *left_scaled,
            *left_scale,
            *right_scaled,
            *right_scale,
        )),
        (Value::TimestampMicros(left), Value::TimestampMicros(right)) => Ok(left.cmp(right)),
        (Value::Uuid(left), Value::Uuid(right)) => Ok(left.cmp(right)),
        (Value::Text(left), Value::Text(right)) => Ok(left.as_bytes().cmp(right.as_bytes())),
        (Value::Blob(left), Value::Blob(right)) => Ok(left.cmp(right)),
        (
            Value::Enum {
                enum_type_id: left_type,
                label_id: left_label,
            },
            Value::Enum {
                enum_type_id: right_type,
                label_id: right_label,
            },
        ) => Ok(left_type
            .cmp(right_type)
            .then_with(|| left_label.cmp(right_label))),
        (
            Value::IpAddr {
                family: left_family,
                addr: left_addr,
            },
            Value::IpAddr {
                family: right_family,
                addr: right_addr,
            },
        ) => compare_ip_addr(*left_family, left_addr, *right_family, right_addr),
        (
            Value::Cidr {
                family: left_family,
                prefix_len: left_prefix,
                network: left_network,
            },
            Value::Cidr {
                family: right_family,
                prefix_len: right_prefix,
                network: right_network,
            },
        ) => compare_cidr(
            *left_family,
            *left_prefix,
            left_network,
            *right_family,
            *right_prefix,
            right_network,
        ),
        (
            Value::MacAddr {
                len: left_len,
                bytes: left_bytes,
            },
            Value::MacAddr {
                len: right_len,
                bytes: right_bytes,
            },
        ) => compare_mac_addr(*left_len, left_bytes, *right_len, right_bytes),
        (Value::DateDays(left), Value::DateDays(right)) => Ok(left.cmp(right)),
        (Value::TimeMicros(left), Value::TimeMicros(right)) => Ok(left.cmp(right)),
        (Value::TimestampTzMicros(left), Value::TimestampTzMicros(right)) => Ok(left.cmp(right)),
        (
            Value::Interval {
                months: left_months,
                days: left_days,
                micros: left_micros,
            },
            Value::Interval {
                months: right_months,
                days: right_days,
                micros: right_micros,
            },
        ) => Ok(compare_interval(
            *left_months,
            *left_days,
            *left_micros,
            *right_months,
            *right_days,
            *right_micros,
        )),
        (Value::Geometry(_), Value::Geometry(_)) | (Value::Geography(_), Value::Geography(_)) => {
            Err(DbError::constraint(
                "spatial values cannot use generic index-key comparison",
            ))
        }
        _ => Err(DbError::constraint(
            "index-key comparison requires values of the same indexed type",
        )),
    }
}

fn sortable_signed_bytes(value: i64) -> [u8; 8] {
    ((value as u64) ^ 0x8000_0000_0000_0000).to_be_bytes()
}

fn sortable_unsigned_bytes(value: u64) -> [u8; 8] {
    value.to_be_bytes()
}

fn sortable_i32_bytes(value: i32) -> [u8; 4] {
    ((value as u32) ^ 0x8000_0000).to_be_bytes()
}

fn ip_addr_sortable_bytes(family: u8, addr: &[u8; 16]) -> Result<[u8; 16]> {
    match family {
        IP_FAMILY_V4 => {
            let mut mapped = [0_u8; 16];
            mapped[10] = 0xff;
            mapped[11] = 0xff;
            mapped[12..16].copy_from_slice(&addr[..4]);
            Ok(mapped)
        }
        6 => Ok(*addr),
        _ => Err(DbError::constraint(format!(
            "invalid IPADDR family for generic key encoding: {family}"
        ))),
    }
}

fn sortable_float_bytes(value: f64) -> [u8; 8] {
    let bits = value.to_bits();
    let sortable = if bits & (1_u64 << 63) != 0 {
        !bits
    } else {
        bits ^ (1_u64 << 63)
    };
    sortable.to_be_bytes()
}

fn sortable_decimal_bytes(scaled: i64, scale: u8) -> Vec<u8> {
    let (scaled, scale) = normalize_decimal(scaled, scale);
    if scaled == 0 {
        return vec![1];
    }

    let digits = scaled.unsigned_abs().to_string();
    let adjusted_exp = digits.len() as i32 - i32::from(scale);
    let biased_exp = u16::try_from(adjusted_exp + 1024).expect("decimal exponent fits u16");
    let mut output = Vec::with_capacity(1 + 2 + digits.len());
    if scaled < 0 {
        output.push(0);
        let inverted = u16::MAX - biased_exp;
        output.extend_from_slice(&inverted.to_be_bytes());
        output.extend(digits.bytes().map(|byte| u8::MAX - byte));
    } else {
        output.push(2);
        output.extend_from_slice(&biased_exp.to_be_bytes());
        output.extend_from_slice(digits.as_bytes());
    }
    output
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use std::mem::size_of;
    use std::ops::Bound;

    use crate::record::value::Value;

    use super::{
        compare_index_values, encode_index_key, encode_runtime_index_key, RuntimeEncodedKey,
        RUNTIME_ENCODED_KEY_INLINE_BYTES,
    };

    fn assert_order(values: &[Value]) {
        let mut encoded = values
            .iter()
            .map(|value| encode_index_key(value).expect("encode"))
            .collect::<Vec<_>>();
        let mut expected = values.to_vec();
        encoded.sort();
        expected.sort_by(|left, right| compare_index_values(left, right).expect("compare"));

        let decoded_order = encoded.to_vec();
        let expected_order = expected
            .iter()
            .map(|value| encode_index_key(value).expect("encode"))
            .collect::<Vec<_>>();
        assert_eq!(decoded_order, expected_order);
    }

    #[test]
    fn scalar_keys_sort_correctly() {
        assert_order(&[
            Value::Int64(-9),
            Value::Int64(0),
            Value::Int64(7),
            Value::Int64(99),
        ]);
        assert_order(&[
            Value::Decimal {
                scaled: -150,
                scale: 2,
            },
            Value::Decimal {
                scaled: -14,
                scale: 1,
            },
            Value::Decimal {
                scaled: 119,
                scale: 2,
            },
            Value::Decimal {
                scaled: 12,
                scale: 1,
            },
            Value::Decimal {
                scaled: 120,
                scale: 2,
            },
        ]);
        assert_order(&[
            Value::TimestampMicros(-10),
            Value::TimestampMicros(0),
            Value::TimestampMicros(10),
        ]);
        assert_order(&[Value::Bool(false), Value::Bool(true)]);
        assert_order(&[
            Value::Float64(f64::NEG_INFINITY),
            Value::Float64(-1.5),
            Value::Float64(0.0),
            Value::Float64(2.0),
            Value::Float64(f64::INFINITY),
        ]);
        assert_order(&[
            Value::Uuid([0; 16]),
            Value::Uuid([1; 16]),
            Value::Uuid([255; 16]),
        ]);
        assert_order(&[
            Value::Text("Alpha".into()),
            Value::Text("Beta".into()),
            Value::Text("beta".into()),
        ]);
        assert_order(&[
            Value::Blob(vec![0x00]),
            Value::Blob(vec![0x00, 0x01]),
            Value::Blob(vec![0xFF]),
        ]);
        assert_order(&[
            Value::Enum {
                enum_type_id: 1,
                label_id: 1,
            },
            Value::Enum {
                enum_type_id: 1,
                label_id: 2,
            },
            Value::Enum {
                enum_type_id: 2,
                label_id: 0,
            },
        ]);
        assert_order(&[
            Value::IpAddr {
                family: 4,
                addr: [10, 1, 2, 3, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0],
            },
            Value::IpAddr {
                family: 6,
                addr: [0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0xff, 0xff, 10, 1, 2, 3],
            },
            Value::IpAddr {
                family: 6,
                addr: [0x20, 0x01, 0x0d, 0xb8, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1],
            },
        ]);
        assert_order(&[
            Value::Cidr {
                family: 4,
                prefix_len: 8,
                network: [10, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0],
            },
            Value::Cidr {
                family: 4,
                prefix_len: 24,
                network: [10, 1, 2, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0],
            },
            Value::Cidr {
                family: 6,
                prefix_len: 64,
                network: [0x20, 0x01, 0x0d, 0xb8, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0],
            },
        ]);
        assert_order(&[
            Value::MacAddr {
                len: 6,
                bytes: [0, 1, 2, 3, 4, 5, 0, 0],
            },
            Value::MacAddr {
                len: 8,
                bytes: [0, 1, 2, 3, 4, 5, 6, 7],
            },
            Value::MacAddr {
                len: 6,
                bytes: [8, 0, 0x2b, 1, 2, 3, 0, 0],
            },
        ]);
        assert_order(&[
            Value::DateDays(-1),
            Value::DateDays(0),
            Value::DateDays(1),
            Value::DateDays(8_000),
        ]);
        assert_order(&[
            Value::TimeMicros(0),
            Value::TimeMicros(1),
            Value::TimeMicros(86_399_999_999),
        ]);
        assert_order(&[
            Value::TimestampTzMicros(-10),
            Value::TimestampTzMicros(0),
            Value::TimestampTzMicros(10),
        ]);
        assert_order(&[
            Value::Interval {
                months: 0,
                days: 30,
                micros: 0,
            },
            Value::Interval {
                months: 1,
                days: 0,
                micros: 0,
            },
            Value::Interval {
                months: 1,
                days: 0,
                micros: 1,
            },
        ]);
    }

    #[test]
    fn text_and_blob_keys_are_not_hash_based() {
        let left = encode_index_key(&Value::Text("abcdef".into())).expect("encode");
        let right = encode_index_key(&Value::Text("abcdeg".into())).expect("encode");
        assert_ne!(left, right);
        assert!(left < right);

        let left = encode_index_key(&Value::Blob(vec![0x10, 0x20])).expect("encode");
        let right = encode_index_key(&Value::Blob(vec![0x10, 0x21])).expect("encode");
        assert_ne!(left, right);
        assert!(left < right);
    }

    #[test]
    fn runtime_and_canonical_encoders_match_frozen_golden_bytes() {
        // These vectors are deliberately independent of both encoder paths.
        // Changing a tag, byte order, normalization rule, or field layout must
        // therefore be an explicit compatibility decision.
        let cases = vec![
            (Value::Null, vec![0x00]),
            (Value::Bool(true), vec![0x01, 0x01]),
            (
                Value::Int64(-123),
                vec![0x02, 0x7F, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0x85],
            ),
            (
                Value::Float64(-12.5),
                vec![0x03, 0x3F, 0xD6, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF],
            ),
            (
                Value::Decimal {
                    scaled: -123_450,
                    scale: 3,
                },
                vec![0x04, 0x00, 0xFB, 0xFC, 0xCE, 0xCD, 0xCC, 0xCB, 0xCA],
            ),
            (
                Value::TimestampMicros(-456),
                vec![0x05, 0x7F, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFE, 0x38],
            ),
            {
                let mut expected = vec![0x06];
                expected.extend_from_slice(&[0xAB; 16]);
                (Value::Uuid([0xAB; 16]), expected)
            },
            (
                Value::Text("Artist 250000".into()),
                b"\x07Artist 250000".to_vec(),
            ),
            (
                Value::Blob(vec![0, 1, 2, 3]),
                vec![0x08, 0x00, 0x01, 0x02, 0x03],
            ),
            (
                Value::Enum {
                    enum_type_id: 9,
                    label_id: 4,
                },
                vec![0x09, 0, 0, 0, 0, 0, 0, 0, 9, 0, 0, 0, 0, 0, 0, 0, 4],
            ),
            (
                Value::IpAddr {
                    family: 4,
                    addr: [127, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0],
                },
                vec![
                    0x0A, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0xFF, 0xFF, 127, 0, 0, 1, 4,
                ],
            ),
            (
                Value::IpAddr {
                    family: 6,
                    addr: [0x20, 1, 0x0D, 0xB8, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1],
                },
                vec![
                    0x0A, 0x20, 1, 0x0D, 0xB8, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 6,
                ],
            ),
            (
                Value::Cidr {
                    family: 4,
                    prefix_len: 24,
                    network: [10, 1, 2, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0],
                },
                vec![0x0B, 4, 24, 10, 1, 2, 0],
            ),
            (
                Value::Cidr {
                    family: 6,
                    prefix_len: 64,
                    network: [0x20, 1, 0x0D, 0xB8, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0],
                },
                vec![
                    0x0B, 6, 64, 0x20, 1, 0x0D, 0xB8, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
                ],
            ),
            (
                Value::MacAddr {
                    len: 6,
                    bytes: [0, 1, 2, 3, 4, 5, 0, 0],
                },
                vec![0x10, 0, 1, 2, 3, 4, 5, 6],
            ),
            (Value::DateDays(-10), vec![0x0C, 0x7F, 0xFF, 0xFF, 0xF6]),
            (
                Value::TimeMicros(123_456),
                vec![0x0D, 0x80, 0, 0, 0, 0, 1, 0xE2, 0x40],
            ),
            (
                Value::TimestampTzMicros(-789),
                vec![0x0E, 0x7F, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFC, 0xEB],
            ),
            (
                Value::Interval {
                    months: -2,
                    days: 3,
                    micros: 4,
                },
                vec![
                    0x0F, 0x7F, 0xFF, 0xFF, 0xFE, 0x80, 0, 0, 3, 0x80, 0, 0, 0, 0, 0, 0, 4,
                ],
            ),
        ];

        for (value, expected) in cases {
            assert_eq!(
                encode_index_key(&value).expect("canonical encoding"),
                expected,
                "canonical value: {value:?}"
            );
            assert_eq!(
                encode_runtime_index_key(&value)
                    .expect("runtime encoding")
                    .as_slice(),
                expected.as_slice(),
                "runtime value: {value:?}"
            );
        }
    }

    #[test]
    fn runtime_encoding_preserves_spatial_errors() {
        let geometry = Value::Geometry(vec![1, 2, 3]);
        let geography = Value::Geography(vec![4, 5, 6]);
        for value in [geometry, geography] {
            let expected = "constraint violation: spatial values cannot be encoded as generic BTREE index keys";
            assert_eq!(
                encode_index_key(&value)
                    .expect_err("spatial key must fail")
                    .to_string(),
                expected
            );
            assert_eq!(
                encode_runtime_index_key(&value)
                    .expect_err("spatial runtime key must fail")
                    .to_string(),
                expected
            );
        }
    }

    #[test]
    fn benchmark_shaped_runtime_text_keys_stay_inline_and_long_keys_spill() {
        let inline_text = "a".repeat(RUNTIME_ENCODED_KEY_INLINE_BYTES - 1);
        let key = encode_runtime_index_key(&Value::Text(inline_text)).expect("encode inline key");
        assert!(!key.spilled());

        let spilled_text = "a".repeat(RUNTIME_ENCODED_KEY_INLINE_BYTES);
        let key = encode_runtime_index_key(&Value::Text(spilled_text)).expect("encode spilled key");
        assert!(key.spilled());

        #[cfg(target_pointer_width = "64")]
        for text in ["Artist 250000", "Album 2500000"] {
            let key = encode_runtime_index_key(&Value::Text(text.into())).expect("encode");
            assert_eq!(key.len(), text.len() + 1);
            assert!(!key.spilled(), "benchmark key unexpectedly spilled: {text}");
        }
    }

    #[test]
    fn runtime_key_has_vec_sized_inline_storage_and_borrowed_map_lookup() {
        assert_eq!(size_of::<RuntimeEncodedKey>(), size_of::<Vec<u8>>());

        let short = encode_runtime_index_key(&Value::Text("alpha".into())).expect("short key");
        let long =
            encode_runtime_index_key(&Value::Text("a long key beyond inline capacity".into()))
                .expect("long key");
        let short_bytes = short.as_slice().to_vec();
        let long_bytes = long.as_slice().to_vec();
        let mut map = BTreeMap::new();
        map.insert(short, 1);
        map.insert(long, 2);

        assert_eq!(map.get(short_bytes.as_slice()), Some(&1));
        assert_eq!(map.get(long_bytes.as_slice()), Some(&2));
        assert_eq!(map.remove(short_bytes.as_slice()), Some(1));
        assert_eq!(map.remove(long_bytes.as_slice()), Some(2));
    }

    #[test]
    fn runtime_key_borrowed_range_crosses_inline_and_spilled_entries() {
        let values = ["a", "inline", "a key that spills", "zzzzzzzzzzzzzzzzzzzz"];
        let mut map = BTreeMap::new();
        for (row_id, value) in values.into_iter().enumerate() {
            map.insert(
                encode_runtime_index_key(&Value::Text(value.into())).expect("encode key"),
                row_id,
            );
        }
        let lower = encode_index_key(&Value::Text("a".into())).expect("lower");
        let upper = encode_index_key(&Value::Text("zzzzzzzzzzzzzzzzzzzz".into())).expect("upper");
        let ranged = map
            .range::<[u8], _>((
                Bound::Included(lower.as_slice()),
                Bound::Included(upper.as_slice()),
            ))
            .map(|(_, row_id)| *row_id)
            .collect::<Vec<_>>();

        assert_eq!(ranged.len(), values.len());
        assert!(map.keys().any(|key| !key.spilled()));
        assert!(map.keys().any(|key| key.spilled()));
    }

    #[test]
    fn runtime_key_order_matches_canonical_order_across_inline_boundary() {
        let values = [
            Value::Text("short".into()),
            Value::Text("123456789012345".into()),
            Value::Text("1234567890123456".into()),
            Value::Text("a much longer text key".into()),
        ];
        let mut canonical = values
            .iter()
            .map(|value| encode_index_key(value).expect("canonical"))
            .collect::<Vec<_>>();
        let mut runtime = values
            .iter()
            .map(|value| encode_runtime_index_key(value).expect("runtime"))
            .collect::<Vec<_>>();
        canonical.sort();
        runtime.sort();

        assert_eq!(
            runtime
                .iter()
                .map(RuntimeEncodedKey::as_slice)
                .collect::<Vec<_>>(),
            canonical.iter().map(Vec::as_slice).collect::<Vec<_>>()
        );
    }
}
