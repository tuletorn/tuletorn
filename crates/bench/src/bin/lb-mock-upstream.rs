//! Standalone mock upstream backend.
//!
//! Containerised as `lb-mock-upstream:latest` and deployed as the benchmark's
//! backend pods, so every candidate is measured against the same upstream
//! implementation rather than against a third-party echo server whose own
//! performance would sit inside every measurement.

use clap::Parser;
use lb_bench::{MockUpstream, MockUpstreamConfig};
use std::net::SocketAddr;
use tokio::sync::watch;
use tracing::info;

#[global_allocator]
static GLOBAL: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;

#[derive(Parser, Debug)]
#[command(author, version, about = "Mock upstream backend for the lb benchmark")]
struct Args {
    #[arg(short, long, default_value = "0.0.0.0:9090")]
    listen: SocketAddr,

    /// Artificial per-request delay in ms (plan §1: 0 / 1 / 5 profiles).
    #[arg(long, default_value_t = 0)]
    delay_ms: u64,

    /// Worker threads. Defaults to the logical CPU count.
    #[arg(long)]
    workers: Option<usize>,
}

fn main() -> Result<(), anyhow::Error> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("warn")),
        )
        .init();

    let args = Args::parse();
    let workers = args.workers.unwrap_or_else(num_cpus::get).max(1);

    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(workers)
        .enable_all()
        .thread_name("lb-mock")
        .build()?;

    runtime.block_on(async move {
        info!(listen = %args.listen, delay_ms = args.delay_ms, workers, "mock upstream starting");

        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        tokio::spawn(async move {
            if tokio::signal::ctrl_c().await.is_ok() {
                let _ = shutdown_tx.send(true);
            }
        });

        MockUpstream::new(MockUpstreamConfig {
            listen_addr: args.listen,
            delay_ms: args.delay_ms,
            ..Default::default()
        })
        .run(shutdown_rx)
        .await
    })
}
