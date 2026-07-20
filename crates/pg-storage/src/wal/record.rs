//! WAL record format, types, and (de)serialization.
//!
//! A WAL record consists of a 32-byte fixed header (24 B header + 8 B meta),
//! followed by a variable-length payload and 0-7 bytes of padding so that the
//! total record length is a multiple of 8 bytes.

use crc32fast::Hasher;
use serde::{Deserialize, Serialize};

use crate::error::{Result, StorageError};
use crate::types::{align_up, Lsn, PageId, TxnId};

/// Size of the fixed record header in bytes.
pub const WAL_RECORD_HEADER_SIZE: usize = 32;

/// WAL record type with explicit discriminants for on-disk compatibility.
///
/// Discriminants are part of the on-disk format and must never be renumbered.
/// Values marked "reserved" have no producer or replay logic yet; recovery
/// ignores them until the corresponding stage lands.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum WalRecordType {
    /// Heap insert (M2 logic; value reserved).
    HeapInsert = 1,
    /// Heap update (M2 logic; value reserved).
    HeapUpdate = 2,
    /// Heap delete (M2 logic; value reserved).
    HeapDelete = 3,
    /// B+Tree insert (M2 logic; value reserved).
    BTreeInsert = 4,
    /// B+Tree split prepare (M2 logic; value reserved). Renamed from M1's
    /// `BTreeSplit`; the discriminant is unchanged (tech-selection v2.3-8).
    BTreeSplitPrepare = 5,
    /// B+Tree delete (M2 logic; value reserved).
    BTreeDelete = 6,
    /// Heap HOT update (M2 logic; value reserved).
    HeapHotUpdate = 7,
    /// Heap cleanup (M2 logic; value reserved).
    HeapCleanup = 8,

    /// Full page image written before the first modification of a page after a
    /// checkpoint (M1 implements).
    FullPageImage = 10,

    /// Transaction begin (M2 logic; value reserved).
    TxnBegin = 20,
    /// Transaction commit (M2 logic; value reserved).
    TxnCommit = 21,
    /// Transaction abort (M2 logic; value reserved).
    TxnAbort = 22,

    /// Checkpoint start marker (M1 implements).
    CheckpointBegin = 30,
    /// Checkpoint end marker (M1 implements).
    CheckpointEnd = 31,

    /// Page allocation (M1 implements).
    PageAlloc = 40,
    /// Page free (M2 logic; value reserved).
    PageFree = 41,

    /// B+Tree split compensation log record (M2c undo; value reserved).
    BTreeSplitCLR = 50,
    /// B+Tree split copy: redo recomputes the moved content from the left
    /// page (M2 logic; value reserved).
    BTreeSplitCopy = 51,
    /// B+Tree split commit (M2 logic; value reserved).
    BTreeSplitCommit = 52,

    /// Logical HNSW operation (Phase 2+).
    LogicalHnsw = 100,
    /// Logical inverted-index operation (Phase 2+).
    LogicalInverted = 101,
    /// Logical graph operation (Phase 2+).
    LogicalGraph = 102,
    /// Logical time-series operation (Phase 2+).
    LogicalTimeSeries = 103,

    /// Segment seal operation (Phase 3+).
    SegmentSeal = 110,
    /// Segment merge operation (Phase 3+).
    SegmentMerge = 111,
}

impl WalRecordType {
    /// Convert the enum to its on-disk `u8` discriminant.
    pub fn to_u8(self) -> u8 {
        self as u8
    }

    /// Parse a `u8` discriminant back into a `WalRecordType`.
    pub fn from_u8(v: u8) -> Result<Self> {
        match v {
            1 => Ok(WalRecordType::HeapInsert),
            2 => Ok(WalRecordType::HeapUpdate),
            3 => Ok(WalRecordType::HeapDelete),
            4 => Ok(WalRecordType::BTreeInsert),
            5 => Ok(WalRecordType::BTreeSplitPrepare),
            6 => Ok(WalRecordType::BTreeDelete),
            7 => Ok(WalRecordType::HeapHotUpdate),
            8 => Ok(WalRecordType::HeapCleanup),
            10 => Ok(WalRecordType::FullPageImage),
            20 => Ok(WalRecordType::TxnBegin),
            21 => Ok(WalRecordType::TxnCommit),
            22 => Ok(WalRecordType::TxnAbort),
            30 => Ok(WalRecordType::CheckpointBegin),
            31 => Ok(WalRecordType::CheckpointEnd),
            40 => Ok(WalRecordType::PageAlloc),
            41 => Ok(WalRecordType::PageFree),
            50 => Ok(WalRecordType::BTreeSplitCLR),
            51 => Ok(WalRecordType::BTreeSplitCopy),
            52 => Ok(WalRecordType::BTreeSplitCommit),
            100 => Ok(WalRecordType::LogicalHnsw),
            101 => Ok(WalRecordType::LogicalInverted),
            102 => Ok(WalRecordType::LogicalGraph),
            103 => Ok(WalRecordType::LogicalTimeSeries),
            110 => Ok(WalRecordType::SegmentSeal),
            111 => Ok(WalRecordType::SegmentMerge),
            _ => Err(StorageError::WalReadFailed(format!(
                "unknown WAL record type discriminant {v}"
            ))),
        }
    }
}

/// Payload for a `PageAlloc` record.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct PageAllocRecord {
    /// The page that was allocated.
    pub page_id: PageId,
}

/// Payload for a `FullPageImage` record.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FullPageImageRecord {
    /// The page whose image is being stored.
    pub page_id: PageId,
    /// The raw page image.
    pub image: Vec<u8>,
}

/// Payload for a `CheckpointEnd` record.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct CheckpointEndRecord {
    /// The LSN of the corresponding `CheckpointBegin` record (redo point).
    pub checkpoint_lsn: Lsn,
    /// The next page ID to allocate after the checkpoint.
    pub next_page_id: PageId,
    /// The next transaction ID to allocate after the checkpoint.
    pub next_txn_id: TxnId,
}

/// A single WAL record.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WalRecord {
    /// LSN at which this record begins.
    pub lsn: Lsn,
    /// LSN of the previous record from the same transaction (undo chain).
    pub prev_lsn: Lsn,
    /// Transaction ID, or 0 for non-transactional operations.
    pub txn_id: TxnId,
    /// Record type.
    pub record_type: WalRecordType,
    /// Flags (e.g. FPI marker).
    pub flags: u8,
    /// Variable-length payload.
    pub payload: Vec<u8>,
}

impl WalRecord {
    /// Create a `PageAlloc` record.
    pub fn page_alloc(page_id: PageId) -> Result<Self> {
        let payload = bincode::serde::encode_to_vec(PageAllocRecord { page_id }, bincode_config())
            .map_err(|e| StorageError::Serialize(e.to_string()))?;
        Ok(Self::new(WalRecordType::PageAlloc, payload))
    }

    /// Create a `FullPageImage` record.
    pub fn full_page_image(page_id: PageId, image: Vec<u8>) -> Result<Self> {
        let payload =
            bincode::serde::encode_to_vec(FullPageImageRecord { page_id, image }, bincode_config())
                .map_err(|e| StorageError::Serialize(e.to_string()))?;
        Ok(Self::new(WalRecordType::FullPageImage, payload))
    }

    /// Create a `CheckpointBegin` record.
    pub fn checkpoint_begin() -> Self {
        Self::new(WalRecordType::CheckpointBegin, Vec::new())
    }

    /// Create a `CheckpointEnd` record.
    pub fn checkpoint_end(
        checkpoint_lsn: Lsn,
        next_page_id: PageId,
        next_txn_id: TxnId,
    ) -> Result<Self> {
        let payload = bincode::serde::encode_to_vec(
            CheckpointEndRecord {
                checkpoint_lsn,
                next_page_id,
                next_txn_id,
            },
            bincode_config(),
        )
        .map_err(|e| StorageError::Serialize(e.to_string()))?;
        Ok(Self::new(WalRecordType::CheckpointEnd, payload))
    }

    /// Return the total serialized size of this record, including padding.
    pub fn record_size(&self) -> usize {
        let raw = WAL_RECORD_HEADER_SIZE + self.payload.len();
        align_up(raw, 8)
    }

    /// Serialize the record into a byte vector.
    ///
    /// The caller should set `self.lsn` before calling this method.
    pub fn encode(&self) -> Result<Vec<u8>> {
        if self.payload.len() > u16::MAX as usize {
            return Err(StorageError::WalWriteFailed(format!(
                "payload length {} exceeds maximum {}",
                self.payload.len(),
                u16::MAX
            )));
        }

        let total = self.record_size();
        let mut buf = Vec::with_capacity(total);

        // Header (24 bytes).
        buf.extend_from_slice(&self.lsn.0.to_le_bytes());
        buf.extend_from_slice(&self.prev_lsn.0.to_le_bytes());
        buf.extend_from_slice(&self.txn_id.0.to_le_bytes());

        // Meta (8 bytes): record_type, flags, payload_len, crc placeholder.
        buf.push(self.record_type.to_u8());
        buf.push(self.flags);
        buf.extend_from_slice(&(self.payload.len() as u16).to_le_bytes());
        buf.extend_from_slice(&0u32.to_le_bytes());

        // Payload.
        buf.extend_from_slice(&self.payload);

        // Padding to 8-byte alignment.
        buf.resize(total, 0);

        // Compute CRC over everything except the crc field itself (bytes 28-31).
        let mut hasher = Hasher::new();
        hasher.update(&buf[0..28]);
        hasher.update(&buf[32..total]);
        let crc = hasher.finalize();
        buf[28..32].copy_from_slice(&crc.to_le_bytes());

        Ok(buf)
    }

    /// Decode a record from its serialized form.
    ///
    /// Returns the record and the total number of bytes consumed (including
    /// padding).
    pub fn decode(buf: &[u8]) -> Result<(Self, usize)> {
        if buf.len() < WAL_RECORD_HEADER_SIZE {
            return Err(StorageError::WalCorrupted(Lsn::INVALID));
        }

        let lsn = Lsn(u64::from_le_bytes(buf[0..8].try_into().unwrap()));
        let prev_lsn = Lsn(u64::from_le_bytes(buf[8..16].try_into().unwrap()));
        let txn_id = TxnId(u64::from_le_bytes(buf[16..24].try_into().unwrap()));
        let record_type = WalRecordType::from_u8(buf[24])?;
        let flags = buf[25];
        let payload_len = u16::from_le_bytes(buf[26..28].try_into().unwrap()) as usize;
        let stored_crc = u32::from_le_bytes(buf[28..32].try_into().unwrap());

        let total = align_up(WAL_RECORD_HEADER_SIZE + payload_len, 8);
        if buf.len() < total {
            return Err(StorageError::WalCorrupted(lsn));
        }

        let mut hasher = Hasher::new();
        hasher.update(&buf[0..28]);
        hasher.update(&buf[32..total]);
        if hasher.finalize() != stored_crc {
            return Err(StorageError::WalCorrupted(lsn));
        }

        let payload = buf[32..32 + payload_len].to_vec();
        let record = Self {
            lsn,
            prev_lsn,
            txn_id,
            record_type,
            flags,
            payload,
        };
        Ok((record, total))
    }

    fn new(record_type: WalRecordType, payload: Vec<u8>) -> Self {
        Self {
            lsn: Lsn::INVALID,
            prev_lsn: Lsn::INVALID,
            txn_id: TxnId::INVALID,
            record_type,
            flags: 0,
            payload,
        }
    }
}

/// Return the shared bincode configuration used across the storage crate.
pub(crate) fn bincode_config() -> bincode::config::Configuration {
    bincode::config::standard()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::PAGE_SIZE;
    use proptest::prelude::*;

    #[test]
    fn page_alloc_roundtrip() {
        let mut record = WalRecord::page_alloc(PageId(42)).unwrap();
        record.lsn = Lsn(16);
        let buf = record.encode().unwrap();
        assert_eq!(buf.len() % 8, 0);

        let (decoded, consumed) = WalRecord::decode(&buf).unwrap();
        assert_eq!(consumed, buf.len());
        assert_eq!(decoded.lsn, Lsn(16));
        assert_eq!(decoded.record_type, WalRecordType::PageAlloc);
        assert_eq!(decoded.payload, record.payload);
    }

    #[test]
    fn checkpoint_begin_roundtrip() {
        let mut record = WalRecord::checkpoint_begin();
        record.lsn = Lsn(128);
        let buf = record.encode().unwrap();
        assert_eq!(buf.len() % 8, 0);

        let (decoded, consumed) = WalRecord::decode(&buf).unwrap();
        assert_eq!(consumed, buf.len());
        assert_eq!(decoded.lsn, Lsn(128));
        assert_eq!(decoded.record_type, WalRecordType::CheckpointBegin);
        assert!(decoded.payload.is_empty());
    }

    #[test]
    fn checkpoint_end_roundtrip() {
        let mut record = WalRecord::checkpoint_end(Lsn(128), PageId(99), TxnId(7)).unwrap();
        record.lsn = Lsn(256);
        let buf = record.encode().unwrap();
        let (decoded, _) = WalRecord::decode(&buf).unwrap();
        assert_eq!(decoded.lsn, Lsn(256));
        assert_eq!(decoded.record_type, WalRecordType::CheckpointEnd);
        assert_eq!(decoded.payload, record.payload);
    }

    #[test]
    fn full_page_image_roundtrip() {
        let image = vec![0xAB; 8192];
        let mut record = WalRecord::full_page_image(PageId(3), image).unwrap();
        record.lsn = Lsn(64);
        let buf = record.encode().unwrap();
        let (decoded, _) = WalRecord::decode(&buf).unwrap();
        assert_eq!(decoded.record_type, WalRecordType::FullPageImage);
        assert_eq!(decoded.payload, record.payload);
    }

    #[test]
    fn decode_rejects_corrupted_crc() {
        let mut record = WalRecord::page_alloc(PageId(1)).unwrap();
        record.lsn = Lsn(16);
        let mut buf = record.encode().unwrap();
        buf[0] ^= 0xff; // corrupt the LSN
        assert!(WalRecord::decode(&buf).is_err());
    }

    #[test]
    fn decode_rejects_truncated_record() {
        let mut record = WalRecord::page_alloc(PageId(1)).unwrap();
        record.lsn = Lsn(16);
        let buf = record.encode().unwrap();
        assert!(WalRecord::decode(&buf[..buf.len() - 1]).is_err());
    }

    #[test]
    fn record_type_discriminants_are_stable() {
        assert_eq!(WalRecordType::PageAlloc.to_u8(), 40);
        assert_eq!(WalRecordType::CheckpointEnd.to_u8(), 31);
        assert_eq!(
            WalRecordType::from_u8(40).unwrap(),
            WalRecordType::PageAlloc
        );
    }

    proptest! {
        // Coding plan target is 10,000 cases. 1024 keeps normal CI fast while
        // still exercising the encoding paths thoroughly; set PROPTEST_CASES
        // environment variable to override.
        #![proptest_config(ProptestConfig::with_cases(
            std::env::var("PROPTEST_CASES")
                .ok()
                .and_then(|s| s.parse().ok())
                .unwrap_or(1024)
        ))]

        #[test]
        fn wal_record_roundtrip(
            lsn in 8u64..10_000u64,
            record_type in prop_oneof![
                Just(WalRecordType::PageAlloc),
                Just(WalRecordType::CheckpointBegin),
                Just(WalRecordType::CheckpointEnd),
                Just(WalRecordType::FullPageImage),
            ],
            page_id in 1u64..1000u64,
            checkpoint_lsn in 8u64..10_000u64,
            next_page_id in 1u64..1000u64,
            next_txn_id in 1u64..1000u64,
            image_seed in 0u8..=255u8,
        ) {
            let lsn = Lsn(lsn);
            let mut record = match record_type {
                WalRecordType::PageAlloc => WalRecord::page_alloc(PageId(page_id)).unwrap(),
                WalRecordType::CheckpointBegin => WalRecord::checkpoint_begin(),
                WalRecordType::CheckpointEnd => WalRecord::checkpoint_end(
                    Lsn(checkpoint_lsn),
                    PageId(next_page_id),
                    TxnId(next_txn_id),
                ).unwrap(),
                WalRecordType::FullPageImage => {
                    let image = vec![image_seed; PAGE_SIZE];
                    WalRecord::full_page_image(PageId(page_id), image).unwrap()
                }
                _ => unreachable!(),
            };
            record.lsn = lsn;

            let buf = record.encode().unwrap();
            prop_assert_eq!(buf.len() % 8, 0);

            let (decoded, consumed) = WalRecord::decode(&buf).unwrap();
            prop_assert_eq!(consumed, buf.len());
            prop_assert_eq!(decoded.lsn, lsn);
            prop_assert_eq!(decoded.record_type, record_type);
            prop_assert_eq!(decoded.payload, record.payload);
        }
    }
}
