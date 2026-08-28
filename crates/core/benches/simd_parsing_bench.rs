//! Plan §8, Scenario 4: SIMD accelerations vs their scalar equivalents.
//!
//! Every vector routine in `lb_core::simd` is benchmarked against the std
//! implementation it replaces, at input lengths that bracket the SIMD block
//! size, so a win that only exists on long inputs is visible as such.

use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use lb_core::simd;
use std::hint::black_box;

/// Realistic proxy inputs.
const PATH: &str = "/api/v1/workloads/deployments/production-cluster/services/analytics/metrics?format=json&range=24h";
const HOST: &str = "api.production.example.com";

fn bench_utf8_validation(c: &mut Criterion) {
    let mut group = c.benchmark_group("simd_utf8_validation");

    for len in [16usize, 64, 256, 4096] {
        // Pure ASCII, the common case for HTTP paths and headers.
        let ascii: Vec<u8> = PATH.bytes().cycle().take(len).collect();
        group.throughput(Throughput::Bytes(len as u64));

        group.bench_with_input(BenchmarkId::new("simdutf8", len), &len, |b, _| {
            b.iter(|| black_box(simd::validate_utf8(black_box(&ascii))));
        });
        group.bench_with_input(BenchmarkId::new("std_from_utf8", len), &len, |b, _| {
            b.iter(|| black_box(std::str::from_utf8(black_box(&ascii)).is_ok()));
        });
    }

    // Multibyte input, where simdutf8's advantage is largest.
    let multibyte: Vec<u8> = "/api/v1/データ/測定/🚀"
        .bytes()
        .cycle()
        .take(4096)
        .collect();
    group.throughput(Throughput::Bytes(4096));
    group.bench_function("simdutf8_multibyte_4096", |b| {
        b.iter(|| black_box(simd::validate_utf8(black_box(&multibyte))));
    });
    group.bench_function("std_from_utf8_multibyte_4096", |b| {
        b.iter(|| black_box(std::str::from_utf8(black_box(&multibyte)).is_ok()));
    });

    group.finish();
}

fn bench_delimiter_search(c: &mut Criterion) {
    let mut group = c.benchmark_group("simd_delimiter_search");
    let bytes = PATH.as_bytes();
    group.throughput(Throughput::Bytes(bytes.len() as u64));

    group.bench_function("memchr", |b| {
        b.iter(|| black_box(simd::find_byte(black_box(b'?'), black_box(bytes))));
    });
    group.bench_function("std_iterator_position", |b| {
        b.iter(|| black_box(bytes.iter().position(|&x| x == black_box(b'?'))));
    });

    group.finish();
}

fn bench_case_insensitive_compare(c: &mut Criterion) {
    let mut group = c.benchmark_group("simd_ascii_case_compare");

    // 4 bytes exercises the scalar tail only; 16/32 hit one vector block;
    // 128 exercises the steady-state loop.
    for (label, a, b_str) in [
        ("4B_host", "Host", "host"),
        ("15B_xff", "X-Forwarded-For", "x-forwarded-for"),
        ("26B_hostname", HOST, "API.PRODUCTION.EXAMPLE.COM"),
        (
            "128B_long_header",
            "X-Very-Long-Custom-Header-Name-For-Benchmarking-Vectorised-Comparison-Paths-In-The-Proxy-Data-Plane-Aaaaaaaaaaaaaaaaaaaaaaaa",
            "x-very-long-custom-header-name-for-benchmarking-vectorised-comparison-paths-in-the-proxy-data-plane-aaaaaaaaaaaaaaaaaaaaaaaa",
        ),
    ] {
        let left = a.as_bytes();
        let right = b_str.as_bytes();
        group.throughput(Throughput::Bytes(left.len() as u64));

        group.bench_with_input(BenchmarkId::new("simd", label), &label, |bencher, _| {
            bencher.iter(|| {
                black_box(simd::eq_ignore_ascii_case(
                    black_box(left),
                    black_box(right),
                ))
            });
        });
        group.bench_with_input(BenchmarkId::new("scalar", label), &label, |bencher, _| {
            bencher.iter(|| {
                black_box(simd::eq_ignore_ascii_case_scalar(
                    black_box(left),
                    black_box(right),
                ))
            });
        });
        group.bench_with_input(
            BenchmarkId::new("std_eq_ignore", label),
            &label,
            |bencher, _| {
                bencher.iter(|| black_box(black_box(left).eq_ignore_ascii_case(black_box(right))));
            },
        );
    }

    group.finish();
}

fn bench_lowercase(c: &mut Criterion) {
    let mut group = c.benchmark_group("simd_ascii_lowercase");

    for len in [16usize, 64, 256] {
        let src: Vec<u8> = HOST.bytes().cycle().take(len).collect();
        let mut dst = vec![0u8; len];
        group.throughput(Throughput::Bytes(len as u64));

        group.bench_with_input(BenchmarkId::new("simd", len), &len, |b, _| {
            b.iter(|| {
                let written = simd::lowercase_ascii_into(black_box(&src), &mut dst).is_some();
                black_box(written);
            });
        });
        group.bench_with_input(BenchmarkId::new("std_make_ascii", len), &len, |b, _| {
            b.iter(|| {
                dst.copy_from_slice(black_box(&src));
                dst.make_ascii_lowercase();
                black_box(&dst);
            });
        });
        group.bench_with_input(BenchmarkId::new("to_lowercase_alloc", len), &len, |b, _| {
            // What the route table used to do: allocate a String per request.
            let text = String::from_utf8(src.clone()).expect("ascii");
            b.iter(|| black_box(black_box(&text).to_lowercase()));
        });
    }

    group.finish();
}

fn bench_host_port_split(c: &mut Criterion) {
    let mut group = c.benchmark_group("simd_host_port_split");

    for input in [
        "api.production.example.com",
        "api.production.example.com:8443",
        "[fe80::1%25eth0]:8080",
    ] {
        group.bench_with_input(BenchmarkId::from_parameter(input), &input, |b, &input| {
            b.iter(|| black_box(simd::host_without_port(black_box(input))));
        });
    }

    group.finish();
}

fn bench_hashing(c: &mut Criterion) {
    use std::hash::{BuildHasher, BuildHasherDefault, Hasher, RandomState};

    let mut group = c.benchmark_group("hostname_hashing");
    let host = HOST.as_bytes();
    group.throughput(Throughput::Bytes(host.len() as u64));

    let fx: BuildHasherDefault<lb_core::FxHasher> = BuildHasherDefault::default();
    group.bench_function("fxhash", |b| {
        b.iter(|| {
            let mut h = fx.build_hasher();
            h.write(black_box(host));
            black_box(h.finish())
        });
    });

    let sip = RandomState::new();
    group.bench_function("siphash_std_default", |b| {
        b.iter(|| {
            let mut h = sip.build_hasher();
            h.write(black_box(host));
            black_box(h.finish())
        });
    });

    group.finish();
}

/// The inline-asm cycle counter vs `Instant::now()`, which is what makes it
/// worth using in the load generator's hot loop.
fn bench_timestamp(c: &mut Criterion) {
    let mut group = c.benchmark_group("timestamp_source");

    group.bench_function("cycles_timestamp_asm", |b| {
        b.iter(|| black_box(lb_core::cycles::timestamp()));
    });
    group.bench_function("instant_now", |b| {
        b.iter(|| black_box(std::time::Instant::now()));
    });

    group.finish();
}

criterion_group!(
    benches,
    bench_utf8_validation,
    bench_delimiter_search,
    bench_case_insensitive_compare,
    bench_lowercase,
    bench_host_port_split,
    bench_hashing,
    bench_timestamp
);
criterion_main!(benches);
