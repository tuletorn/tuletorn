//! `lb-proxy-native` — a completion-native io_uring reverse proxy.
//!
//! The other candidates in this workspace run Hyper over some reactor. This one
//! has no async runtime at all: each core owns a ring, a registered buffer
//! pool, and a slab of connection state machines, and advances them directly
//! from completion events.
//!
//! That is the whole point. Adapting io_uring to a poll-based executor costs a
//! copy and a task wakeup on every read and every write, and gives the runtime
//! no place to batch submissions. Removing the adapter removes all three.

mod http;
mod ring;
mod worker;

use clap::Parser;
use lb_core::{RouteConfig, SharedRouteTable, alloc};
use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::atomic::Ordering;
use tracing::info;
use worker::{Worker, WorkerConfig, WorkerStats};

// Same allocator as every other candidate, so the allocator is not a second
// uncontrolled variable.
#[global_allocator]
static GLOBAL: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;

#[derive(Parser, Debug)]
#[command(author, version, about = "Completion-native io_uring reverse proxy")]
struct Args {
    #[arg(short, long, default_value = "0.0.0.0:8080")]
    listen: SocketAddr,

    /// YAML route configuration.
    #[arg(short, long)]
    config: Option<String>,

    /// Fallback upstream when no --config is given.
    #[arg(short, long, default_value = "127.0.0.1:9090")]
    default_upstream: String,

    /// Worker threads / cores. Defaults to the logical CPU count.
    #[arg(long)]
    workers: Option<usize>,

    /// Ring size in entries.
    #[arg(long, default_value_t = 4096)]
    ring_entries: u32,

    /// Registered buffers per worker.
    ///
    /// Each connection holds one for its lifetime and a second while a request
    /// is in flight, so this caps concurrent connections per worker at roughly
    /// half the count.
    #[arg(long, default_value_t = 2048)]
    buf_count: usize,

    /// Size of each registered buffer.
    ///
    /// This sets how much of a response crosses per read/write round trip. At
    /// 16 KiB a 1 MiB body costs 64 serialized round trips and the proxy loses
    /// to Hyper on large payloads; at 64 KiB it wins at every size measured.
    #[arg(long, default_value_t = 65536)]
    buf_size: usize,

    /// Connection slots per worker.
    #[arg(long, default_value_t = 8192)]
    max_conns: usize,

    /// Pin each worker to its own core.
    #[arg(long, default_value_t = true, action = clap::ArgAction::Set)]
    pin: bool,

    /// Log ring counters every N seconds. 0 disables.
    #[arg(long, default_value_t = 0)]
    stats_secs: u64,
}

fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .init();
    alloc::register("jemalloc");

    let args = Args::parse();
    let workers = args.workers.unwrap_or_else(num_cpus::get).max(1);

    let seed = match &args.config {
        Some(path) => RouteConfig::from_path(path)?,
        None => RouteConfig::single_upstream(&args.default_upstream),
    };
    let routes = Arc::new(SharedRouteTable::from_table(seed.compile()));

    info!(
        allocator = alloc::allocator_name(),
        listen = %args.listen,
        workers,
        buf_count = args.buf_count,
        buf_size = args.buf_size,
        "starting lb-proxy-native"
    );

    let stats = Arc::new(WorkerStats::default());
    if args.stats_secs > 0 {
        let stats = stats.clone();
        let secs = args.stats_secs;
        std::thread::spawn(move || {
            loop {
                std::thread::sleep(std::time::Duration::from_secs(secs));
                let sqes = stats.sqes.load(Ordering::Relaxed);
                let enters = stats.enters.load(Ordering::Relaxed);
                let cqes = stats.cqes.load(Ordering::Relaxed);
                info!(
                    requests = stats.requests.load(Ordering::Relaxed),
                    sqes,
                    enters,
                    cqes,
                    // The batching factor: how many operations one syscall
                    // carried. This is the number the whole design turns on.
                    sqes_per_enter = ratio(sqes, enters),
                    cqes_per_enter = ratio(cqes, enters),
                    "ring stats"
                );
            }
        });
    }

    let mut handles = Vec::with_capacity(workers);
    for core in 0..workers {
        let cfg = WorkerConfig {
            core,
            listen: args.listen,
            ring_entries: args.ring_entries,
            buf_count: args.buf_count,
            buf_size: args.buf_size,
            max_conns: args.max_conns,
            pin: args.pin,
        };
        let routes = routes.clone();
        let stats = stats.clone();
        handles.push(
            std::thread::Builder::new()
                .name(format!("lb-native-{core}"))
                .spawn(move || -> anyhow::Result<()> {
                    let mut worker = Worker::new(&cfg, routes, stats)?;
                    worker.run()?;
                    Ok(())
                })?,
        );
    }

    for h in handles {
        match h.join() {
            Ok(Ok(())) => {}
            Ok(Err(err)) => tracing::error!(%err, "worker exited with an error"),
            Err(_) => tracing::error!("worker panicked"),
        }
    }
    Ok(())
}

fn ratio(n: u64, d: u64) -> f64 {
    if d == 0 { 0.0 } else { n as f64 / d as f64 }
}
