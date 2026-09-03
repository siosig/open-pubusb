//! Criterion micro-benchmark for `open_pubusb_core::store::keys` — the
//! big-endian fixed-width key builders/parsers every hot-path
//! `KvStore` operation (Publish's log append, `DeliveryEngine::lease_next`'s
//! per-message scan, Ack) goes through at least once.

use criterion::{black_box, criterion_group, criterion_main, Criterion};
use open_pubusb_core::store::keys;

fn bench_msg_key(c: &mut Criterion) {
    let mut group = c.benchmark_group("msg_key");
    group.bench_function("build", |b| {
        b.iter(|| keys::msg_key(black_box(42), black_box(1_234_567)))
    });
    let key = keys::msg_key(42, 1_234_567);
    group.bench_function("parse", |b| b.iter(|| keys::parse_msg_key(black_box(&key))));
    group.finish();
}

fn bench_delivery_key(c: &mut Criterion) {
    let mut group = c.benchmark_group("delivery_key");
    group.bench_function("build", |b| {
        b.iter(|| keys::delivery_key(black_box(7), black_box(99)))
    });
    let key = keys::delivery_key(7, 99);
    group.bench_function("parse", |b| {
        b.iter(|| keys::parse_delivery_key(black_box(&key)))
    });
    group.finish();
}

fn bench_name_key(c: &mut Criterion) {
    let mut group = c.benchmark_group("name_key");
    group.bench_function("topic", |b| {
        b.iter(|| {
            keys::name_key(
                black_box(keys::NameKind::Topic),
                black_box("projects/bench-project/topics/bench-topic"),
            )
        })
    });
    group.finish();
}

criterion_group!(benches, bench_msg_key, bench_delivery_key, bench_name_key);
criterion_main!(benches);
