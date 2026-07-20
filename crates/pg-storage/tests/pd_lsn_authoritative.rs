//! End-to-end test for the `pd_lsn` authority contract (Stage D).
//!
//! After `pin_mut` writes a full-page image, the page's own `pd_lsn`
//! (`page[0..8]`) must equal the FPI record's LSN — and the value must
//! survive a flush → evict → reload round trip through the data file.

use std::sync::Arc;

use parking_lot::Mutex;

use pg_storage::buffer_pool::BufferPool;
use pg_storage::config::StorageConfig;
use pg_storage::page::{page_pd_lsn, PageHeader, PAGE_HEADER_SIZE};
use pg_storage::page_allocator::PageAllocator;
use pg_storage::types::Lsn;
use pg_storage::wal::reader::WalReader;
use pg_storage::wal::record::{FullPageImageRecord, WalRecordType};
use pg_storage::wal::writer::WalWriter;

/// Read the WAL from the beginning and return the LSN of the newest
/// FullPageImage record for `page_id`.
fn newest_fpi_lsn(
    data_dir: &std::path::Path,
    segment_size: u64,
    page_id: pg_storage::types::PageId,
) -> Option<Lsn> {
    let mut reader = WalReader::open_at(data_dir.join("wal"), segment_size, Lsn::FIRST).unwrap();
    let mut found = None;
    while let Some(record) = reader.next_record().unwrap() {
        if record.record_type != WalRecordType::FullPageImage {
            continue;
        }
        let decoded: FullPageImageRecord =
            bincode::serde::decode_from_slice(&record.payload, bincode::config::standard())
                .unwrap()
                .0;
        if decoded.page_id == page_id {
            found = Some(record.lsn);
        }
    }
    found
}

#[test]
fn test_pd_lsn_authoritative() {
    let tmp = tempfile::TempDir::new().unwrap();
    let mut cfg = StorageConfig::new(tmp.path());
    // Small pool (32 frames) so the eviction loops below stay fast.
    cfg.buffer_pool_size = 256 * 1024;
    let cfg = cfg;
    let wal = Arc::new(WalWriter::open(tmp.path(), &cfg).unwrap());
    let allocator = Arc::new(Mutex::new(
        PageAllocator::open(tmp.path(), &cfg, Arc::clone(&wal)).unwrap(),
    ));
    let pool =
        BufferPool::open(tmp.path(), &cfg, Arc::clone(&allocator), Arc::clone(&wal)).unwrap();

    // Create and flush a page, then evict it so the next pin_mut starts a
    // new (FPI-eligible) residency.
    let page_id = {
        let mut guard = pool.new_page().unwrap();
        guard.page_mut()[PAGE_HEADER_SIZE] = 0x42;
        guard.page_id()
    };
    pool.flush(page_id).unwrap();
    for _ in 0..pool.frame_count() + 2 {
        let _ = pool.new_page().unwrap();
    }

    // Simulate a checkpoint: the next modification must write an FPI.
    pool.set_checkpoint_lsn(wal.synced_lsn());

    let (pd_in_memory, fpi_lsn) = {
        let mut guard = pool.pin_mut(page_id).unwrap();
        guard.page_mut()[PAGE_HEADER_SIZE] = 0x43;
        let pd = page_pd_lsn(guard.page());
        let fpi = newest_fpi_lsn(tmp.path(), cfg.wal_segment_size, page_id)
            .expect("pin_mut must have written an FPI for the page");
        (pd, fpi)
    };

    // The page's own pd_lsn is the FPI record's LSN — the authority contract.
    assert!(pd_in_memory.is_valid());
    assert_eq!(
        pd_in_memory, fpi_lsn,
        "page[0..8] must carry the FPI record's LSN"
    );
    // The 32-byte header decodes to the same value.
    {
        let guard = pool.pin(page_id).unwrap();
        assert_eq!(PageHeader::read_from(guard.page()).pd_lsn, fpi_lsn);
    }

    // Flush + evict + reload: pd_lsn survives the round trip through the
    // data file, and the reloaded page carries the same authoritative LSN.
    pool.flush(page_id).unwrap();
    for _ in 0..pool.frame_count() + 2 {
        let _ = pool.new_page().unwrap();
    }
    let pd_after_reload = {
        let guard = pool.pin(page_id).unwrap();
        page_pd_lsn(guard.page())
    };
    assert_eq!(pd_after_reload, fpi_lsn);
}
