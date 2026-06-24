//! CRC32 throughput benchmark for freelist.meta encode/decode.
//!
//! The acceptance criterion from the Stage E coding plan is < 1μs per 4KB
//! freelist chunk. `crc32fast` is SIMD-accelerated on most platforms, so we
//! expect to be well under budget.

use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use pg_storage::freelist_meta::FreelistMeta;
use pg_storage::types::{Lsn, PageId};

/// Build a freelist snapshot whose encoded body is approximately `target_bytes`.
fn make_snapshot(target_bytes: usize) -> FreelistMeta {
    // Each page_id is 8 bytes; body = 4 (CRC) + 16 (header) + N*8.
    let n = target_bytes.saturating_sub(20) / 8;
    let page_ids: Vec<PageId> = (1..=n as u64).map(PageId).collect();
    FreelistMeta {
        checkpoint_lsn: Lsn(1024),
        page_ids,
    }
}

fn bench_crc(c: &mut Criterion) {
    let sizes: &[(usize, &str)] = &[
        (256, "256 B"),
        (1024, "1 KB"),
        (4096, "4 KB"),
        (16384, "16 KB"),
    ];

    let mut group = c.benchmark_group("freelist_crc");
    for &(size, label) in sizes {
        let meta = make_snapshot(size);
        let encoded = meta.encode();
        group.throughput(Throughput::Bytes(encoded.len() as u64));

        group.bench_with_input(BenchmarkId::new("encode", label), &meta, |b, m| {
            b.iter(|| black_box(m.encode()));
        });

        group.bench_with_input(BenchmarkId::new("decode", label), &encoded, |b, data| {
            b.iter(|| FreelistMeta::decode(black_box(data)).unwrap());
        });
    }
    group.finish();
}

criterion_group!(benches, bench_crc);
criterion_main!(benches);
