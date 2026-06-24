//! Pure in-memory benchmarks for slotted-page tuple operations (Stage G
//! acceptance: >= 5M ops/s for add_tuple / tuple lookup).

use criterion::{black_box, criterion_group, criterion_main, Criterion};
use pg_am_heap::SlottedPage;
use pg_storage::types::PAGE_SIZE;

/// 128-byte tuple: 64-byte header + a few columns, a typical small row.
const TUPLE_LEN: usize = 128;

fn bench_add_tuple(c: &mut Criterion) {
    let mut page = [0u8; PAGE_SIZE];
    SlottedPage::init(&mut page);
    let tuple = vec![0xABu8; TUPLE_LEN];
    c.bench_function("add_tuple_128B", |b| {
        b.iter(|| {
            match SlottedPage::add_tuple(&mut page, black_box(&tuple)) {
                Ok(slot) => slot,
                Err(_) => {
                    // Page full: reset and keep measuring steady-state inserts.
                    SlottedPage::init(&mut page);
                    SlottedPage::add_tuple(&mut page, black_box(&tuple)).unwrap()
                }
            }
        })
    });
}

fn bench_tuple_lookup(c: &mut Criterion) {
    let mut page = [0u8; PAGE_SIZE];
    SlottedPage::init(&mut page);
    let tuple = vec![0xCDu8; TUPLE_LEN];
    let mut slots = Vec::new();
    while let Ok(slot) = SlottedPage::add_tuple(&mut page, &tuple) {
        slots.push(slot);
    }
    assert!(!slots.is_empty(), "page must hold at least one tuple");
    let mut next = 0usize;
    c.bench_function("tuple_lookup_128B", |b| {
        b.iter(|| {
            let slot = slots[next % slots.len()];
            next = next.wrapping_add(1);
            black_box(SlottedPage::tuple(black_box(&page), slot))
        })
    });
}

criterion_group!(benches, bench_add_tuple, bench_tuple_lookup);
criterion_main!(benches);
