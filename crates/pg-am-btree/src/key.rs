//! Order-preserving index-key encoding (tech-selection §13.1).
//!
//! M2b indexes are single-column; the supported key types are
//! [`ColumnType::Int4`], [`ColumnType::Int8`], [`ColumnType::Text`] and
//! [`ColumnType::Bytea`]. The encoding maps each type onto byte strings whose
//! **lexicographic order equals the type's native order**, so B+Tree pages
//! can compare encoded keys with plain `&[u8]` comparisons:
//!
//! - `Int4` / `Int8`: big-endian with the sign bit flipped (the standard
//!   order-preserving trick: `i32::MIN` maps to `0x00000000`, `i32::MAX` to
//!   `0xFFFFFFFF`).
//! - `Text` / `Bytea`: raw bytes. Rust's `str`/`[u8]` ordering is already
//!   byte-wise lexicographic, and UTF-8 byte order equals code-point order.
//!
//! Decoding back to a [`Datum`] is provided for the `AccessMethod::scan`
//! adaptation, which yields the key as a single decoded column.

use pg_am_heap::tuple::{ColumnType, Datum};

use crate::error::{BTreeError, Result};

/// Maximum encoded key size in bytes.
///
/// An index page must always be able to hold at least two entries after a
/// split plus the one being inserted; capping keys at roughly one third of
/// the usable page (PG uses a similar ~2712-byte limit for 8 KB pages)
/// guarantees that, so the split machinery never wedges.
pub const MAX_INDEX_KEY_BYTES: usize = (pg_storage::types::PAGE_SIZE - 32 - 16) / 3 - 16;

/// True if `key_type` is supported as an M2b index key.
pub fn is_supported_key_type(key_type: ColumnType) -> bool {
    matches!(
        key_type,
        ColumnType::Int4 | ColumnType::Int8 | ColumnType::Text | ColumnType::Bytea
    )
}

/// Encode a datum into its order-preserving byte form.
///
/// Returns [`BTreeError::InvalidArgument`] for datum types that are not
/// supported index keys (or do not match a supported type), and
/// [`BTreeError::KeyTooLarge`] if the encoded key exceeds
/// [`MAX_INDEX_KEY_BYTES`].
pub fn encode_key(datum: &Datum) -> Result<Vec<u8>> {
    let bytes = match datum {
        Datum::Int4(v) => encode_i32(*v).to_vec(),
        Datum::Int8(v) => encode_i64(*v).to_vec(),
        Datum::Text(s) => s.as_bytes().to_vec(),
        Datum::Bytea(b) => b.clone(),
        other => {
            return Err(BTreeError::InvalidArgument(format!(
                "unsupported index key datum: {other:?}"
            )));
        }
    };
    if bytes.len() > MAX_INDEX_KEY_BYTES {
        return Err(BTreeError::KeyTooLarge(bytes.len()));
    }
    Ok(bytes)
}

/// Decode order-preserving key bytes back into a datum of `key_type`.
///
/// Returns [`BTreeError::Corrupted`] if the byte length is inconsistent with
/// the type — on-disk bytes must never cause a panic.
pub fn decode_key(key_type: ColumnType, bytes: &[u8]) -> Result<Datum> {
    match key_type {
        ColumnType::Int4 => {
            let arr: [u8; 4] = bytes
                .try_into()
                .map_err(|_| BTreeError::Corrupted(format!("Int4 key of {} bytes", bytes.len())))?;
            Ok(Datum::Int4(decode_i32(arr)))
        }
        ColumnType::Int8 => {
            let arr: [u8; 8] = bytes
                .try_into()
                .map_err(|_| BTreeError::Corrupted(format!("Int8 key of {} bytes", bytes.len())))?;
            Ok(Datum::Int8(decode_i64(arr)))
        }
        ColumnType::Text => {
            let s = std::str::from_utf8(bytes)
                .map_err(|e| BTreeError::Corrupted(format!("Text key is not valid UTF-8: {e}")))?;
            Ok(Datum::Text(s.to_string()))
        }
        ColumnType::Bytea => Ok(Datum::Bytea(bytes.to_vec())),
        other => Err(BTreeError::InvalidArgument(format!(
            "unsupported index key type: {other:?}"
        ))),
    }
}

/// Order-preserving encoding of an `i32`: sign-bit-flipped big-endian.
pub fn encode_i32(v: i32) -> [u8; 4] {
    ((v as u32) ^ 0x8000_0000).to_be_bytes()
}

/// Inverse of [`encode_i32`].
pub fn decode_i32(bytes: [u8; 4]) -> i32 {
    (u32::from_be_bytes(bytes) ^ 0x8000_0000) as i32
}

/// Order-preserving encoding of an `i64`: sign-bit-flipped big-endian.
pub fn encode_i64(v: i64) -> [u8; 8] {
    ((v as u64) ^ 0x8000_0000_0000_0000).to_be_bytes()
}

/// Inverse of [`encode_i64`].
pub fn decode_i64(bytes: [u8; 8]) -> i64 {
    (u64::from_be_bytes(bytes) ^ 0x8000_0000_0000_0000) as i64
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;

    #[test]
    fn int4_encoding_boundaries() {
        assert_eq!(encode_i32(i32::MIN), [0x00, 0x00, 0x00, 0x00]);
        assert_eq!(encode_i32(-1), [0x7F, 0xFF, 0xFF, 0xFF]);
        assert_eq!(encode_i32(0), [0x80, 0x00, 0x00, 0x00]);
        assert_eq!(encode_i32(i32::MAX), [0xFF, 0xFF, 0xFF, 0xFF]);
    }

    #[test]
    fn int8_encoding_boundaries() {
        assert_eq!(encode_i64(i64::MIN), [0x00; 8]);
        assert_eq!(encode_i64(0), [0x80, 0, 0, 0, 0, 0, 0, 0]);
        assert_eq!(encode_i64(i64::MAX), [0xFF; 8]);
    }

    #[test]
    fn roundtrip_primitives() {
        for v in [i32::MIN, -1, 0, 1, i32::MAX] {
            assert_eq!(decode_i32(encode_i32(v)), v);
        }
        for v in [i64::MIN, -1, 0, 1, i64::MAX] {
            assert_eq!(decode_i64(encode_i64(v)), v);
        }
    }

    #[test]
    fn datum_roundtrip() {
        let cases = [
            (ColumnType::Int4, Datum::Int4(-42)),
            (ColumnType::Int8, Datum::Int8(1_000_000_000)),
            (ColumnType::Text, Datum::Text("héllo".to_string())),
            (ColumnType::Bytea, Datum::Bytea(vec![0, 1, 0xFF])),
        ];
        for (ty, datum) in cases {
            let bytes = encode_key(&datum).unwrap();
            assert_eq!(decode_key(ty, &bytes).unwrap(), datum);
        }
    }

    #[test]
    fn rejects_unsupported_types() {
        assert!(matches!(
            encode_key(&Datum::Timestamptz(0)),
            Err(BTreeError::InvalidArgument(_))
        ));
        assert!(!is_supported_key_type(ColumnType::Uuid));
        assert!(is_supported_key_type(ColumnType::Int4));
    }

    #[test]
    fn rejects_oversized_key() {
        let big = Datum::Bytea(vec![0xAB; MAX_INDEX_KEY_BYTES + 1]);
        assert!(matches!(encode_key(&big), Err(BTreeError::KeyTooLarge(_))));
    }

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(
            std::env::var("PROPTEST_CASES")
                .ok()
                .and_then(|s| s.parse().ok())
                .unwrap_or(1024)
        ))]

        /// Encoded byte order must equal native i32 order.
        #[test]
        fn int4_order_preserved(a in any::<i32>(), b in any::<i32>()) {
            prop_assert_eq!(a.cmp(&b), encode_i32(a).cmp(&encode_i32(b)));
        }

        /// Encoded byte order must equal native i64 order.
        #[test]
        fn int8_order_preserved(a in any::<i64>(), b in any::<i64>()) {
            prop_assert_eq!(a.cmp(&b), encode_i64(a).cmp(&encode_i64(b)));
        }

        /// Text keys use raw bytes; UTF-8 byte order equals str order.
        #[test]
        fn text_order_preserved(a in any::<String>(), b in any::<String>()) {
            prop_assert_eq!(
                a.cmp(&b),
                a.as_bytes().cmp(b.as_bytes())
            );
        }

        /// Bytea keys use raw bytes, preserving the native byte-slice order.
        #[test]
        fn bytea_order_preserved(a in any::<Vec<u8>>(), b in any::<Vec<u8>>()) {
            let ea = encode_key(&Datum::Bytea(a.clone())).unwrap();
            let eb = encode_key(&Datum::Bytea(b.clone())).unwrap();
            prop_assert_eq!(a.cmp(&b), ea.cmp(&eb));
        }

        /// Encode/decode round-trips for every supported datum.
        #[test]
        fn datum_encode_decode_roundtrip(
            v4 in any::<i32>(),
            v8 in any::<i64>(),
            s in any::<String>(),
            b in proptest::collection::vec(any::<u8>(), 0..64),
        ) {
            for (ty, datum) in [
                (ColumnType::Int4, Datum::Int4(v4)),
                (ColumnType::Int8, Datum::Int8(v8)),
                (ColumnType::Text, Datum::Text(s.clone())),
                (ColumnType::Bytea, Datum::Bytea(b.clone())),
            ] {
                let bytes = encode_key(&datum).unwrap();
                prop_assert_eq!(decode_key(ty, &bytes).unwrap(), datum);
            }
        }
    }
}
