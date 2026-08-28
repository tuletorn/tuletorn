//! Plan §8, Scenario 4: `matchit` vs `route-recognizer`.

use criterion::{BenchmarkId, Criterion, criterion_group, criterion_main};
use lb_core::{BackendEndpoint, RouteFilters, RouteTableBuilder};
use std::hint::black_box;

/// Route-table sizes to sweep, spanning a small ingress to a large cluster.
const SIZES: [usize; 4] = [10, 100, 1_000, 5_000];

fn build_lb_table(size: usize) -> lb_core::RouteTable {
    let mut builder = RouteTableBuilder::new();
    for i in 0..size {
        builder.add_route(
            Some("api.example.com"),
            format!("/v1/resource/{i}"),
            vec![BackendEndpoint::new("10.0.0.1:8080", 1)],
            RouteFilters::default(),
            format!("route-{i}"),
        );
    }
    builder.add_route(
        Some("*.example.com"),
        "/v1",
        vec![BackendEndpoint::new("10.0.2.1:8080", 1)],
        RouteFilters::default(),
        "wildcard",
    );
    builder.build().expect("table builds")
}

fn build_route_recognizer(size: usize) -> route_recognizer::Router<String> {
    let mut router = route_recognizer::Router::new();
    for i in 0..size {
        router.add(&format!("/v1/resource/{i}/*rest"), format!("route-{i}"));
    }
    router
}

fn bench_routing(c: &mut Criterion) {
    let mut group = c.benchmark_group("routing_lookup");

    for size in SIZES {
        let lb_table = build_lb_table(size);
        let recognizer = build_route_recognizer(size);
        // Look up a route in the middle of the table, so neither structure is
        // favoured by hitting its first or last entry.
        let path = format!("/v1/resource/{}", size / 2);
        let deep_path = format!("/v1/resource/{}/details/extra", size / 2);

        group.bench_with_input(BenchmarkId::new("matchit_exact", size), &size, |b, _| {
            b.iter(|| {
                let hit = lb_table.lookup(black_box(Some("api.example.com")), black_box(&path));
                debug_assert!(hit.is_some());
                black_box(hit)
            });
        });

        group.bench_with_input(
            BenchmarkId::new("route_recognizer_exact", size),
            &size,
            |b, _| {
                b.iter(|| black_box(recognizer.recognize(black_box(&deep_path)).is_ok()));
            },
        );

        group.bench_with_input(
            BenchmarkId::new("matchit_wildcard_host", size),
            &size,
            |b, _| {
                b.iter(|| {
                    black_box(
                        lb_table.lookup(black_box(Some("sub.example.com")), black_box("/v1/any")),
                    )
                });
            },
        );

        group.bench_with_input(BenchmarkId::new("matchit_miss", size), &size, |b, _| {
            b.iter(|| {
                black_box(lb_table.lookup(black_box(Some("api.example.com")), black_box("/absent")))
            });
        });
    }

    group.finish();
}

/// Isolate the hostname-matching cost: SIMD lowercase + FxHash lookup vs the
/// allocating `to_lowercase()` + SipHash path it replaced.
fn bench_host_matching(c: &mut Criterion) {
    let table = build_lb_table(100);
    let mut group = c.benchmark_group("host_matching");

    group.bench_function("exact_lowercase", |b| {
        b.iter(|| black_box(table.lookup(black_box(Some("api.example.com")), "/v1/resource/50")));
    });
    group.bench_function("exact_mixed_case_with_port", |b| {
        b.iter(|| {
            black_box(table.lookup(black_box(Some("API.Example.COM:8443")), "/v1/resource/50"))
        });
    });
    group.bench_function("wildcard_three_labels", |b| {
        b.iter(|| black_box(table.lookup(black_box(Some("a.b.example.com")), "/v1/x")));
    });

    group.finish();
}

criterion_group!(benches, bench_routing, bench_host_matching);
criterion_main!(benches);
