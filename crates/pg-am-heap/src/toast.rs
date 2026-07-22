//! TOAST pointer encoding (tech-selection §四).
//!
//! M2 supports only **EXTERNAL** out-of-line storage (chunked into a hidden
//! `pg_toast_<oid>` table); compression is deferred to Phase 7b. An external
//! attribute is stored in the main tuple as a 20-byte TOAST pointer:
//!
//! ```text
//! vl_len_:       u32  (high 2 bits = 01 marks external)   offset  0..4
//! va_rawsize:    u32  (original uncompressed size)        offset  4..8
//! va_extsize:    u32  (stored size)                       offset  8..12
//! va_valueid:    u32  (chunk group id in the TOAST table) offset 12..16
//! va_toastrelid: u32  (TOAST table OID, low 32 bits)      offset 16..20
//! ```
//!
//! This stage only encodes/decodes the pointer; TOAST chunk table I/O is
//! Stage I.

use crate::error::{HeapError, Result};

/// Size of a TOAST pointer in bytes (5 × u32, §四).
pub const TOAST_POINTER_SIZE: usize = 20;

/// Serialized attribute size above which a value is moved out of line (§四).
pub const TOAST_TUPLE_THRESHOLD: usize = 2048;

/// Maximum payload bytes per TOAST chunk (§四).
pub const TOAST_MAX_CHUNK_SIZE: usize = 2000;

/// `vl_len_` bit pattern marking an external (out-of-line) attribute: the
/// high 2 bits are `01` (§四).
pub const VARLENA_EXTERNAL_FLAG: u32 = 0x4000_0000;

/// Mask covering the two high tag bits of a varlena header.
const VARLENA_TAG_MASK: u32 = 0xC000_0000;

/// Mask extracting the total-length field of a varlena header (tag bits
/// cleared).
pub const VARLENA_LEN_MASK: u32 = !VARLENA_TAG_MASK;

/// Return true if a varlena header marks the attribute as external
/// (high 2 bits = `01`, §四).
pub fn is_external(vl_len: u32) -> bool {
    vl_len & VARLENA_TAG_MASK == VARLENA_EXTERNAL_FLAG
}

/// A 20-byte TOAST pointer stored in place of an out-of-line attribute.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ToastPointer {
    /// Original uncompressed size of the attribute.
    pub va_rawsize: u32,
    /// Size as stored in the TOAST table (M2: always == `va_rawsize`, no
    /// compression).
    pub va_extsize: u32,
    /// Chunk group id (`va_valueid`) inside the TOAST table.
    pub va_valueid: u32,
    /// TOAST table OID, low 32 bits (§四: OIDs are u64 but M2 relation
    /// counts stay below 2^32).
    pub va_toastrelid: u32,
}

impl ToastPointer {
    /// Encode to the 20-byte on-disk layout. `vl_len_` is written as
    /// `TOAST_POINTER_SIZE` with the external tag bits set.
    pub fn encode(&self) -> [u8; TOAST_POINTER_SIZE] {
        let vl_len = (TOAST_POINTER_SIZE as u32) | VARLENA_EXTERNAL_FLAG;
        let mut out = [0u8; TOAST_POINTER_SIZE];
        out[0..4].copy_from_slice(&vl_len.to_le_bytes());
        out[4..8].copy_from_slice(&self.va_rawsize.to_le_bytes());
        out[8..12].copy_from_slice(&self.va_extsize.to_le_bytes());
        out[12..16].copy_from_slice(&self.va_valueid.to_le_bytes());
        out[16..20].copy_from_slice(&self.va_toastrelid.to_le_bytes());
        out
    }

    /// Decode from the 20-byte on-disk layout.
    ///
    /// Returns [`HeapError::Corrupted`] if the slice is not exactly 20
    /// bytes, the external tag is missing, or the length field disagrees
    /// with [`TOAST_POINTER_SIZE`].
    pub fn decode(bytes: &[u8]) -> Result<Self> {
        if bytes.len() != TOAST_POINTER_SIZE {
            return Err(HeapError::Corrupted(format!(
                "TOAST pointer must be {TOAST_POINTER_SIZE} bytes, got {}",
                bytes.len()
            )));
        }
        let vl_len = u32::from_le_bytes(bytes[0..4].try_into().unwrap());
        if !is_external(vl_len) {
            return Err(HeapError::Corrupted(format!(
                "TOAST pointer missing external tag: vl_len_ = {vl_len:#010x}"
            )));
        }
        if (vl_len & VARLENA_LEN_MASK) as usize != TOAST_POINTER_SIZE {
            return Err(HeapError::Corrupted(format!(
                "TOAST pointer length mismatch: vl_len_ = {:#010x}",
                vl_len & VARLENA_LEN_MASK
            )));
        }
        Ok(ToastPointer {
            va_rawsize: u32::from_le_bytes(bytes[4..8].try_into().unwrap()),
            va_extsize: u32::from_le_bytes(bytes[8..12].try_into().unwrap()),
            va_valueid: u32::from_le_bytes(bytes[12..16].try_into().unwrap()),
            va_toastrelid: u32::from_le_bytes(bytes[16..20].try_into().unwrap()),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn encode_decode_round_trip() {
        let ptr = ToastPointer {
            va_rawsize: 5000,
            va_extsize: 5000,
            va_valueid: 42,
            va_toastrelid: 16399,
        };
        let bytes = ptr.encode();
        // External tag present in the high 2 bits (01).
        assert_eq!(bytes[3] & 0xC0, 0x40);
        assert_eq!(ToastPointer::decode(&bytes).unwrap(), ptr);
    }

    #[test]
    fn is_external_checks_tag_bits() {
        assert!(!is_external(100));
        assert!(is_external(100 | VARLENA_EXTERNAL_FLAG));
        // Tag 10 / 11 are not the external marker.
        assert!(!is_external(0x8000_0000 | 100));
        assert!(!is_external(0xC000_0000 | 100));
    }

    #[test]
    fn decode_rejects_bad_input() {
        assert!(ToastPointer::decode(&[0u8; 19]).is_err());
        // Inline varlena header (no external tag) is rejected.
        let mut bytes = [0u8; TOAST_POINTER_SIZE];
        bytes[0..4].copy_from_slice(&20u32.to_le_bytes());
        assert!(matches!(
            ToastPointer::decode(&bytes),
            Err(HeapError::Corrupted(_))
        ));
    }
}
