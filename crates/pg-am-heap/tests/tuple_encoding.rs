//! Integration tests for tuple encoding/decoding (tech-selection §三 / §四).
//!
//! Acceptance command: `cargo test -p pg-am-heap --test tuple_encoding`.

use pg_am_heap::toast::{ToastPointer, TOAST_POINTER_SIZE};
use pg_am_heap::tuple::{
    decode_tuple, encode_tuple, ColumnType, Datum, TupleHeader, HEAP_HASEXTERNAL, HEAP_HASNULL,
    HEAP_HASVARWIDTH, HEAP_ONLY_TUPLE, HEAP_UPDATED, HEAP_XMIN_COMMITTED,
};
use pg_am_heap::SlottedPage;
use pg_storage::types::{PageId, Tid, TxnId, PAGE_SIZE};

fn header(xmin: u64) -> TupleHeader {
    TupleHeader::new(
        TxnId(xmin),
        TxnId::INVALID,
        0xA6E7,
        [0x11; 16],
        Tid {
            page_id: PageId(42),
            slot_id: 7,
        },
        0,
    )
}

/// Stage G acceptance: every header field lands at the exact byte offsets of
/// the tech-selection §三 table.
#[test]
fn test_tuple_header_offsets() {
    let mut h = TupleHeader::new(
        TxnId(0x0102_0304_0506_0708),
        TxnId(0x1112_1314_1516_1718),
        0x2122_2324_2526_2728,
        [
            0x30, 0x31, 0x32, 0x33, 0x34, 0x35, 0x36, 0x37, //
            0x38, 0x39, 0x3A, 0x3B, 0x3C, 0x3D, 0x3E, 0x3F,
        ],
        Tid {
            page_id: PageId(0x4142_4344_4546_4748),
            slot_id: 0x5152,
        },
        0xDEAD_BEEF,
    );
    h.t_infomask = HEAP_HASNULL | HEAP_XMIN_COMMITTED | HEAP_UPDATED; // 0x2101
    h.t_infomask2 = HEAP_ONLY_TUPLE; // natts filled by encode_tuple
    h.t_flags = 0x00F0; // high bits preserved; low 4 bits = version, reset by encode

    // One NULL column so HASNULL and the bitmap survive encoding.
    let columns = [ColumnType::Int4, ColumnType::Int4];
    let values = vec![Some(Datum::Int4(1)), None];
    let bytes = encode_tuple(h, &columns, &values).unwrap();

    // §三 offset table, field by field. t_hoff = 72; the only non-null
    // column (Int4) occupies bytes 72..76.
    assert_eq!(bytes.len(), 76);
    assert_eq!(&bytes[0..8], &0x0102_0304_0506_0708u64.to_le_bytes()); // t_xmin
    assert_eq!(&bytes[8..16], &0x1112_1314_1516_1718u64.to_le_bytes()); // t_xmax
    assert_eq!(&bytes[16..24], &0x2122_2324_2526_2728u64.to_le_bytes()); // t_agent_id
    assert_eq!(&bytes[24..40], &(0x30u8..=0x3F).collect::<Vec<_>>()[..]); // t_trace_id
    assert_eq!(&bytes[40..48], &0x4142_4344_4546_4748u64.to_le_bytes()); // t_ctid.page_id
    assert_eq!(&bytes[48..50], &0x5152u16.to_le_bytes()); // t_ctid.slot_id
    assert_eq!(&bytes[50..52], &[0, 0]); // t_ctid pad u16: always 0
    assert_eq!(&bytes[52..54], &0x2101u16.to_le_bytes()); // t_infomask
    assert_eq!(&bytes[54..56], &(HEAP_ONLY_TUPLE | 2).to_le_bytes()); // t_infomask2 (natts=2)
    assert_eq!(&bytes[56..58], &72u16.to_le_bytes()); // t_hoff = align8(64 + 1 bitmap byte)
    assert_eq!(&bytes[58..60], &0x00F0u16.to_le_bytes()); // t_flags (version = 0)
    assert_eq!(&bytes[60..64], &0xDEAD_BEEFu32.to_le_bytes()); // t_cid

    // Null bitmap: column 1 is NULL → bit 1 set, at offset 64.
    assert_eq!(bytes[64], 0b0000_0010);
    // Bitmap padding up to t_hoff is zeroed.
    assert!(bytes[65..72].iter().all(|&b| b == 0));
    // Attribute data starts exactly at t_hoff.
    assert_eq!(&bytes[72..76], &1i32.to_le_bytes());

    // Full decode agrees with the encoded header.
    let (decoded_header, decoded_values) = decode_tuple(&bytes, &columns).unwrap();
    assert_eq!(decoded_header.t_cid, 0xDEAD_BEEF);
    assert_eq!(decoded_header.t_ctid, h.t_ctid);
    assert_eq!(decoded_header.natts(), 2);
    assert_eq!(decoded_values, values);
}

/// Round-trip scenario 1: small tuple, fixed-width columns only.
#[test]
fn round_trip_small_fixed_tuple() {
    let columns = [
        ColumnType::Int4,
        ColumnType::Int8,
        ColumnType::Timestamptz,
        ColumnType::Uuid,
    ];
    let values = vec![
        Some(Datum::Int4(-7)),
        Some(Datum::Int8(i64::MIN)),
        Some(Datum::Timestamptz(1_752_000_000_000_000)),
        Some(Datum::Uuid(uuid::Uuid::from_bytes([0x5A; 16]))),
    ];
    let bytes = encode_tuple(header(100), &columns, &values).unwrap();

    // No nulls and no varlena: t_hoff = 64, both hint bits clear.
    let h = TupleHeader::read_from(&bytes).unwrap();
    assert_eq!(h.t_hoff, 64);
    assert_eq!(
        h.t_infomask & (HEAP_HASNULL | HEAP_HASVARWIDTH | HEAP_HASEXTERNAL),
        0
    );
    assert_eq!(h.t_flags & 0x000F, 0, "M2 encoding version");

    let (decoded_header, decoded) = decode_tuple(&bytes, &columns).unwrap();
    assert_eq!(decoded_header, h);
    assert_eq!(decoded, values);
}

/// Round-trip scenario 2: large tuple with varlena columns and NULLs.
#[test]
fn round_trip_large_varlena_null_tuple() {
    let long_text = "agent-trace-payload·中文·".repeat(100);
    let long_blob: Vec<u8> = (0..3000u32).map(|i| (i % 251) as u8).collect();
    let columns = [
        ColumnType::Int8,
        ColumnType::Text,
        ColumnType::Bytea,
        ColumnType::Int4,
        ColumnType::Text,
    ];
    let values = vec![
        Some(Datum::Int8(42)),
        Some(Datum::Text(long_text.clone())),
        Some(Datum::Bytea(long_blob.clone())),
        None,
        None,
    ];
    let bytes = encode_tuple(header(200), &columns, &values).unwrap();

    let h = TupleHeader::read_from(&bytes).unwrap();
    assert_ne!(h.t_infomask & HEAP_HASNULL, 0);
    assert_ne!(h.t_infomask & HEAP_HASVARWIDTH, 0);
    assert_eq!(h.t_infomask & HEAP_HASEXTERNAL, 0);
    // 5 natts → 1 bitmap byte; t_hoff = align8(65) = 72.
    assert_eq!(h.t_hoff, 72);

    let (_, decoded) = decode_tuple(&bytes, &columns).unwrap();
    assert_eq!(decoded, values);

    // The encoded tuple must fit on a slotted page and come back intact.
    let mut page = [0u8; PAGE_SIZE];
    SlottedPage::init(&mut page);
    let slot = SlottedPage::add_tuple(&mut page, &bytes).unwrap();
    let stored = SlottedPage::tuple(&page, slot).unwrap().unwrap();
    let (_, decoded_from_page) = decode_tuple(stored, &columns).unwrap();
    assert_eq!(decoded_from_page, values);
}

/// Trailing bytes after the last column indicate corruption (e.g. a stale
/// lp_len); decode must reject them.
#[test]
fn decode_rejects_trailing_garbage() {
    let columns = [ColumnType::Int8];
    let values = vec![Some(Datum::Int8(5))];
    let bytes = encode_tuple(header(1), &columns, &values).unwrap();
    let mut padded = bytes.clone();
    padded.extend_from_slice(&[0u8; 8]);
    assert!(matches!(
        decode_tuple(&padded, &columns),
        Err(pg_am_heap::HeapError::Corrupted(_))
    ));
}

/// Round-trip scenario 3: TOAST out-of-line column (§四). The main tuple
/// stores only the 20-byte ToastPointer as the attribute payload.
#[test]
fn round_trip_toast_out_of_line() {
    let pointer = ToastPointer {
        va_rawsize: 10_000,
        va_extsize: 10_000,
        va_valueid: 77,
        va_toastrelid: 16_401,
    };
    let columns = [ColumnType::Int4, ColumnType::Bytea];
    let values = vec![Some(Datum::Int4(1)), Some(Datum::External(pointer))];
    let bytes = encode_tuple(header(300), &columns, &values).unwrap();

    let h = TupleHeader::read_from(&bytes).unwrap();
    assert_ne!(h.t_infomask & HEAP_HASEXTERNAL, 0);
    assert_ne!(h.t_infomask & HEAP_HASVARWIDTH, 0);
    assert_eq!(h.t_infomask & HEAP_HASNULL, 0);

    // The external attribute occupies exactly the 20-byte pointer; its first
    // 4 bytes carry the external tag (high 2 bits = 01).
    let attr_start = h.t_hoff as usize + 4; // after the Int4 column
    assert_eq!(bytes.len(), attr_start + TOAST_POINTER_SIZE);
    assert_eq!(bytes[attr_start + 3] & 0xC0, 0x40);

    let (_, decoded) = decode_tuple(&bytes, &columns).unwrap();
    assert_eq!(decoded, values);
    match &decoded[1] {
        Some(Datum::External(p)) => assert_eq!(*p, pointer),
        other => panic!("expected external datum, got {other:?}"),
    }
}
