//! Criterion micro-benchmark for the subscription-filter subsystem
//! (`open_pubusb_core::filter`), which sits in the delivery hot path
//! (`DeliveryEngine::lease_next` evaluates a subscription's compiled
//! filter against every scanned message).

#![allow(clippy::unwrap_used, clippy::expect_used)]

use std::collections::HashMap;

use criterion::{black_box, criterion_group, criterion_main, Criterion};
use open_pubusb_core::filter::compile;

fn attrs(pairs: &[(&str, &str)]) -> HashMap<String, String> {
    pairs
        .iter()
        .map(|(k, v)| (k.to_string(), v.to_string()))
        .collect()
}

fn bench_compile(c: &mut Criterion) {
    let mut group = c.benchmark_group("filter_compile");
    group.bench_function("simple_equality", |b| {
        b.iter(|| compile(black_box(r#"attributes.kind = "greeting""#)))
    });
    group.bench_function("and_chain", |b| {
        b.iter(|| {
            compile(black_box(
                r#"attributes.a = "1" AND attributes.b = "2" AND attributes.c = "3""#,
            ))
        })
    });
    group.bench_function("or_chain", |b| {
        b.iter(|| {
            compile(black_box(
                r#"attributes.a = "1" OR attributes.b = "2" OR attributes.c = "3""#,
            ))
        })
    });
    group.finish();
}

fn bench_matches(c: &mut Criterion) {
    let mut group = c.benchmark_group("filter_matches");

    let simple = compile(r#"attributes.kind = "greeting""#)
        .ok()
        .flatten()
        .expect("simple filter should compile");
    let matching = attrs(&[("kind", "greeting"), ("origin", "compat-test")]);
    let non_matching = attrs(&[("kind", "other")]);
    group.bench_function("simple_equality_match", |b| {
        b.iter(|| simple.matches(black_box(&matching)))
    });
    group.bench_function("simple_equality_no_match", |b| {
        b.iter(|| simple.matches(black_box(&non_matching)))
    });

    let and_chain = compile(r#"attributes.a = "1" AND attributes.b = "2" AND attributes.c = "3""#)
        .ok()
        .flatten()
        .expect("and-chain filter should compile");
    let and_match = attrs(&[("a", "1"), ("b", "2"), ("c", "3")]);
    group.bench_function("and_chain_match", |b| {
        b.iter(|| and_chain.matches(black_box(&and_match)))
    });

    let has_attribute = compile("attributes:origin")
        .ok()
        .flatten()
        .expect("has-attribute filter should compile");
    let large_attrs: HashMap<String, String> = (0..20)
        .map(|i| (format!("k{i}"), format!("v{i}")))
        .chain(std::iter::once(("origin".to_string(), "x".to_string())))
        .collect();
    group.bench_function("has_attribute_among_many", |b| {
        b.iter(|| has_attribute.matches(black_box(&large_attrs)))
    });

    group.finish();
}

criterion_group!(benches, bench_compile, bench_matches);
criterion_main!(benches);
