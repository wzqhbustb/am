//! Integration tests for the superblock v1 → v2 migration (Stage C).
//!
//! v2 inserts `next_oid` at offset 40..48, moving `created_at` to 48..56 and
//! the CRC to 56..60. `Superblock::read` must transparently migrate a v1
//! file (initializing `next_oid` to `Oid::FIRST_USER`) and write the v2
//! layout back to disk so the migration runs only once.

use std::path::Path;

use pg_storage::superblock::{
    Superblock, SUPERBLOCK_MAGIC, SUPERBLOCK_SIZE, SUPERBLOCK_VERSION, SUPERBLOCK_VERSION_V1,
};
use pg_storage::types::{Lsn, Oid, PageId, TxnId, PAGE_SIZE};

/// Encode a legacy v1 superblock copy into `buf` (created_at at 40..48,
/// crc32 at 48..52, no next_oid).
fn encode_v1_copy(
    buf: &mut [u8],
    page_size: u32,
    checkpoint_lsn: u64,
    next_page_id: u64,
    next_txn_id: u64,
    created_at: u64,
) {
    assert_eq!(buf.len(), SUPERBLOCK_SIZE);
    buf[0..4].copy_from_slice(&SUPERBLOCK_MAGIC.to_le_bytes());
    buf[4..8].copy_from_slice(&SUPERBLOCK_VERSION_V1.to_le_bytes());
    buf[8..12].copy_from_slice(&page_size.to_le_bytes());
    // 12..16 padding stays zero.
    buf[16..24].copy_from_slice(&checkpoint_lsn.to_le_bytes());
    buf[24..32].copy_from_slice(&next_page_id.to_le_bytes());
    buf[32..40].copy_from_slice(&next_txn_id.to_le_bytes());
    buf[40..48].copy_from_slice(&created_at.to_le_bytes());

    let mut hasher = crc32fast::Hasher::new();
    hasher.update(&buf[0..48]);
    hasher.update(&buf[52..SUPERBLOCK_SIZE]);
    let crc = hasher.finalize();
    buf[48..52].copy_from_slice(&crc.to_le_bytes());
}

fn write_v1_file(path: &Path, copy_a: &[u8; SUPERBLOCK_SIZE], copy_b: &[u8; SUPERBLOCK_SIZE]) {
    let mut bytes = Vec::with_capacity(SUPERBLOCK_SIZE * 2);
    bytes.extend_from_slice(copy_a);
    bytes.extend_from_slice(copy_b);
    std::fs::write(path, bytes).unwrap();
}

/// Assert that a raw on-disk copy is valid v2 with the expected field values.
fn assert_v2_copy(copy: &[u8], expected_next_oid: u64, expected_created_at: u64) {
    assert_eq!(
        u32::from_le_bytes(copy[4..8].try_into().unwrap()),
        SUPERBLOCK_VERSION,
        "on-disk copy is not v2"
    );
    assert_eq!(
        u64::from_le_bytes(copy[40..48].try_into().unwrap()),
        expected_next_oid,
        "next_oid must live at offset 40..48"
    );
    assert_eq!(
        u64::from_le_bytes(copy[48..56].try_into().unwrap()),
        expected_created_at,
        "created_at must live at offset 48..56"
    );
    let stored_crc = u32::from_le_bytes(copy[56..60].try_into().unwrap());
    let mut hasher = crc32fast::Hasher::new();
    hasher.update(&copy[0..56]);
    hasher.update(&copy[60..SUPERBLOCK_SIZE]);
    assert_eq!(
        hasher.finalize(),
        stored_crc,
        "v2 CRC must cover 0..56|60..512"
    );
}

#[test]
fn test_superblock_v1_to_v2_migration() {
    let tmp = tempfile::TempDir::new().unwrap();
    let path = Superblock::path(tmp.path());

    // Craft a v1 file: copy A older (checkpoint 1024), copy B newer (2048),
    // so the reader must select B and migrate it.
    let created_at = 1_700_000_000_000_000_000u64;
    let mut copy_a = [0u8; SUPERBLOCK_SIZE];
    encode_v1_copy(&mut copy_a, PAGE_SIZE as u32, 1024, 7, 3, created_at);
    let mut copy_b = [0u8; SUPERBLOCK_SIZE];
    encode_v1_copy(&mut copy_b, PAGE_SIZE as u32, 2048, 9, 4, created_at);
    write_v1_file(&path, &copy_a, &copy_b);

    // Read: migrate, selecting the newer copy (B).
    let sb = Superblock::read(&path).unwrap();
    assert_eq!(sb.version, SUPERBLOCK_VERSION);
    assert_eq!(sb.page_size, PAGE_SIZE as u32);
    assert_eq!(sb.checkpoint_lsn, Lsn(2048));
    assert_eq!(sb.next_page_id, PageId(9));
    assert_eq!(sb.next_txn_id, TxnId(4));
    assert_eq!(sb.next_oid, Oid::FIRST_USER);
    assert_eq!(sb.created_at, created_at);

    // The migration must be persisted: both copies are v2 on disk now.
    let raw = std::fs::read(&path).unwrap();
    assert_v2_copy(&raw[0..SUPERBLOCK_SIZE], Oid::FIRST_USER.0, created_at);
    assert_v2_copy(&raw[SUPERBLOCK_SIZE..], Oid::FIRST_USER.0, created_at);

    // A second read is native v2 and returns the same content.
    let sb2 = Superblock::read(&path).unwrap();
    assert_eq!(sb2, sb);

    // Normal monotonic writes still work on the migrated file.
    let mut sb3 = sb2;
    sb3.checkpoint_lsn = Lsn(4096);
    sb3.write(&path).unwrap();
    let sb4 = Superblock::read(&path).unwrap();
    assert_eq!(sb4.checkpoint_lsn, Lsn(4096));
    assert_eq!(sb4.next_oid, Oid::FIRST_USER);
}

#[test]
fn fresh_database_creates_v2_superblock() {
    let tmp = tempfile::TempDir::new().unwrap();
    let path = Superblock::path(tmp.path());

    let sb = Superblock::create(&path, PAGE_SIZE as u32).unwrap();
    assert_eq!(sb.version, SUPERBLOCK_VERSION);
    assert_eq!(sb.next_oid, Oid::FIRST_USER);

    // The on-disk bytes are already v2; reopening needs no migration and
    // returns identical content.
    let raw = std::fs::read(&path).unwrap();
    assert_v2_copy(&raw[0..SUPERBLOCK_SIZE], Oid::FIRST_USER.0, sb.created_at);
    assert_v2_copy(&raw[SUPERBLOCK_SIZE..], Oid::FIRST_USER.0, sb.created_at);

    let read = Superblock::read(&path).unwrap();
    assert_eq!(read, sb);
}

#[test]
fn v1_corrupt_copy_falls_back_to_valid_copy_and_migrates() {
    let tmp = tempfile::TempDir::new().unwrap();
    let path = Superblock::path(tmp.path());

    // Copy A is a valid v1 copy; copy B has a valid v1 layout but a corrupted
    // payload byte (created_at flipped without fixing the CRC).
    let created_at = 1_700_000_000_000_000_000u64;
    let mut copy_a = [0u8; SUPERBLOCK_SIZE];
    encode_v1_copy(&mut copy_a, PAGE_SIZE as u32, 1024, 7, 3, created_at);
    let mut copy_b = [0u8; SUPERBLOCK_SIZE];
    encode_v1_copy(&mut copy_b, PAGE_SIZE as u32, 2048, 9, 4, created_at);
    copy_b[40] ^= 0xff; // corrupt created_at; the stored CRC no longer matches
    write_v1_file(&path, &copy_a, &copy_b);

    // The reader must fall back to the only valid copy (A) and migrate it,
    // even though B carried a higher checkpoint_lsn.
    let sb = Superblock::read(&path).unwrap();
    assert_eq!(sb.version, SUPERBLOCK_VERSION);
    assert_eq!(sb.checkpoint_lsn, Lsn(1024));
    assert_eq!(sb.next_page_id, PageId(7));
    assert_eq!(sb.next_txn_id, TxnId(3));
    assert_eq!(sb.next_oid, Oid::FIRST_USER);
    assert_eq!(sb.created_at, created_at);

    // The migration write-back also heals the corrupted copy: both copies are
    // valid v2 on disk now.
    let raw = std::fs::read(&path).unwrap();
    assert_v2_copy(&raw[0..SUPERBLOCK_SIZE], Oid::FIRST_USER.0, created_at);
    assert_v2_copy(&raw[SUPERBLOCK_SIZE..], Oid::FIRST_USER.0, created_at);
}

#[test]
fn mixed_v1_and_v2_copies_converge_to_v2() {
    // Simulates a crash in the middle of the migration write-back: one copy
    // already v2, the other still v1. The read must pick the newer copy and
    // converge both copies to v2.
    let tmp = tempfile::TempDir::new().unwrap();

    // Produce genuine v2 bytes via the public API: create + one monotonic
    // write, so the second copy carries checkpoint_lsn = 2048.
    let seed_path = Superblock::path(&tmp.path().join("seed"));
    let mut seed = Superblock::create(&seed_path, PAGE_SIZE as u32).unwrap();
    seed.checkpoint_lsn = Lsn(2048);
    seed.write(&seed_path).unwrap();
    let seed_raw = std::fs::read(&seed_path).unwrap();
    let mut v2_copy = [0u8; SUPERBLOCK_SIZE];
    v2_copy.copy_from_slice(&seed_raw[SUPERBLOCK_SIZE..]); // the updated copy

    let created_at = 1_700_000_000_000_000_000u64;
    let mut v1_copy = [0u8; SUPERBLOCK_SIZE];
    encode_v1_copy(&mut v1_copy, PAGE_SIZE as u32, 1024, 7, 3, created_at);

    let path = tmp.path().join("mixed").join("superblock");
    std::fs::create_dir_all(path.parent().unwrap()).unwrap();
    let mut bytes = Vec::new();
    bytes.extend_from_slice(&v2_copy);
    bytes.extend_from_slice(&v1_copy);
    std::fs::write(&path, bytes).unwrap();

    // The v2 copy is newer (2048 > 1024) and must win; the v1 copy marks the
    // file for migration write-back.
    let sb = Superblock::read(&path).unwrap();
    assert_eq!(sb.version, SUPERBLOCK_VERSION);
    assert_eq!(sb.checkpoint_lsn, Lsn(2048));
    assert_eq!(sb.next_oid, Oid::FIRST_USER);
    assert_eq!(sb.created_at, seed.created_at);

    let raw = std::fs::read(&path).unwrap();
    for copy in [&raw[0..SUPERBLOCK_SIZE], &raw[SUPERBLOCK_SIZE..]] {
        assert_v2_copy(copy, Oid::FIRST_USER.0, seed.created_at);
        assert_eq!(
            u64::from_le_bytes(copy[16..24].try_into().unwrap()),
            2048,
            "both copies must carry the newer checkpoint_lsn"
        );
    }

    // Idempotent: a second read returns the same content.
    assert_eq!(Superblock::read(&path).unwrap(), sb);
}
