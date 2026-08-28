//! Plan §8, Scenario 4: `ArcSwap` vs `parking_lot::RwLock` vs
//! `tokio::sync::RwLock` vs `std::sync::RwLock` under control-plane churn.

use criterion::{Criterion, criterion_group, criterion_main};
use lb_core::{BackendEndpoint, RouteFilters, RouteTable, RouteTableBuilder, SharedRouteTable};
use std::hint::black_box;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, RwLock as StdRwLock};
use std::thread;

const HOST: &str = "api.example.com";
const PATH: &str = "/v1/users";

fn table(generation: usize) -> RouteTable {
    let mut builder = RouteTableBuilder::new();
    builder.add_route(
        Some(HOST),
        PATH,
        vec![BackendEndpoint::new(
            format!("10.0.0.{}:8080", generation % 250 + 1),
            1,
        )],
        RouteFilters::default(),
        format!("route-{generation}"),
    );
    builder.build().expect("table builds")
}

/// Spawn a writer that republishes the table as fast as it can, and return a
/// stop flag plus its join handle.
fn spawn_writer<F>(mut publish: F) -> (Arc<AtomicBool>, thread::JoinHandle<()>)
where
    F: FnMut(RouteTable) + Send + 'static,
{
    let stop = Arc::new(AtomicBool::new(false));
    let flag = stop.clone();
    let handle = thread::spawn(move || {
        let mut generation = 0usize;
        while !flag.load(Ordering::Relaxed) {
            generation += 1;
            publish(table(generation));
            thread::yield_now();
        }
    });
    (stop, handle)
}

fn bench_read_under_churn(c: &mut Criterion) {
    let mut group = c.benchmark_group("route_swap_read_under_churn");

    group.bench_function("arc_swap", |b| {
        let shared = Arc::new(SharedRouteTable::from_table(table(0)));
        let writer = shared.clone();
        let (stop, handle) = spawn_writer(move |t| writer.store(t));

        b.iter(|| {
            let guard = shared.load();
            black_box(guard.lookup(black_box(Some(HOST)), black_box(PATH))).expect("route present");
        });

        stop.store(true, Ordering::Relaxed);
        handle.join().expect("writer thread");
    });

    group.bench_function("parking_lot_rwlock", |b| {
        let shared = Arc::new(parking_lot::RwLock::new(table(0)));
        let writer = shared.clone();
        let (stop, handle) = spawn_writer(move |t| *writer.write() = t);

        b.iter(|| {
            let guard = shared.read();
            black_box(guard.lookup(black_box(Some(HOST)), black_box(PATH))).expect("route present");
        });

        stop.store(true, Ordering::Relaxed);
        handle.join().expect("writer thread");
    });

    group.bench_function("std_rwlock", |b| {
        let shared = Arc::new(StdRwLock::new(table(0)));
        let writer = shared.clone();
        let (stop, handle) = spawn_writer(move |t| {
            if let Ok(mut guard) = writer.write() {
                *guard = t;
            }
        });

        b.iter(|| {
            let guard = shared.read().expect("lock not poisoned");
            black_box(guard.lookup(black_box(Some(HOST)), black_box(PATH))).expect("route present");
        });

        stop.store(true, Ordering::Relaxed);
        handle.join().expect("writer thread");
    });

    group.bench_function("tokio_rwlock", |b| {
        // `tokio::sync::RwLock` is async, so it needs a runtime to be read at
        // all; that overhead is part of what this comparison measures.
        let runtime = tokio::runtime::Builder::new_current_thread()
            .build()
            .expect("runtime builds");
        let shared = Arc::new(tokio::sync::RwLock::new(table(0)));
        let writer = shared.clone();
        let stop = Arc::new(AtomicBool::new(false));
        let flag = stop.clone();
        let handle = thread::spawn(move || {
            let rt = tokio::runtime::Builder::new_current_thread()
                .build()
                .expect("runtime builds");
            rt.block_on(async move {
                let mut generation = 0usize;
                while !flag.load(Ordering::Relaxed) {
                    generation += 1;
                    *writer.write().await = table(generation);
                    tokio::task::yield_now().await;
                }
            });
        });

        b.iter(|| {
            runtime.block_on(async {
                let guard = shared.read().await;
                black_box(guard.lookup(black_box(Some(HOST)), black_box(PATH)))
                    .expect("route present");
            });
        });

        stop.store(true, Ordering::Relaxed);
        handle.join().expect("writer thread");
    });

    group.finish();
}

/// Uncontended reads, to isolate the per-read cost from the churn effect.
fn bench_read_uncontended(c: &mut Criterion) {
    let mut group = c.benchmark_group("route_swap_read_uncontended");

    let arc_swap = SharedRouteTable::from_table(table(0));
    group.bench_function("arc_swap", |b| {
        b.iter(|| {
            let guard = arc_swap.load();
            black_box(guard.lookup(black_box(Some(HOST)), black_box(PATH)));
        });
    });

    let parking = parking_lot::RwLock::new(table(0));
    group.bench_function("parking_lot_rwlock", |b| {
        b.iter(|| {
            let guard = parking.read();
            black_box(guard.lookup(black_box(Some(HOST)), black_box(PATH)));
        });
    });

    let std_lock = StdRwLock::new(table(0));
    group.bench_function("std_rwlock", |b| {
        b.iter(|| {
            let guard = std_lock.read().expect("lock not poisoned");
            black_box(guard.lookup(black_box(Some(HOST)), black_box(PATH)));
        });
    });

    group.finish();
}

/// The write side: how expensive is publishing a new table?
fn bench_publish(c: &mut Criterion) {
    let mut group = c.benchmark_group("route_swap_publish");

    let shared = SharedRouteTable::from_table(table(0));
    let mut generation = 0usize;
    group.bench_function("arc_swap_store", |b| {
        b.iter(|| {
            generation += 1;
            shared.store(table(generation));
        });
    });

    let parking = parking_lot::RwLock::new(table(0));
    group.bench_function("parking_lot_write", |b| {
        b.iter(|| {
            generation += 1;
            *parking.write() = table(generation);
        });
    });

    group.finish();
}

criterion_group!(
    benches,
    bench_read_under_churn,
    bench_read_uncontended,
    bench_publish
);
criterion_main!(benches);
