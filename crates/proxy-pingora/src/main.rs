//! `lb-proxy-pingora` binary: standalone or in-cluster entry point.

use clap::Parser;
use lb_core::{RouteConfig, SharedRouteTable, alloc};
use lb_proxy_pingora::k8s_controller::{ControllerConfig, spawn_k8s_reconciler};
use lb_proxy_pingora::{ProxyPingora, ServiceConfig};
use std::net::SocketAddr;
use std::sync::Arc;
use tracing::info;

#[global_allocator]
static GLOBAL: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;

#[derive(Parser, Debug)]
#[command(
    author,
    version,
    about = "Cloudflare Pingora reverse proxy (lb benchmark candidate)"
)]
struct Args {
    #[arg(short, long, default_value = "0.0.0.0:8081")]
    listen: SocketAddr,

    #[arg(long, default_value = "standalone")]
    mode: String,

    #[arg(short, long)]
    config: Option<String>,

    #[arg(short, long, default_value = "127.0.0.1:9090")]
    default_upstream: String,

    /// Worker threads. Defaults to the logical CPU count, not Pingora's own
    /// single-threaded `ServerConf` default.
    #[arg(long)]
    threads: Option<usize>,

    #[arg(long, default_value = "lb-gateway")]
    gateway_name: String,

    #[arg(long)]
    namespace: Option<String>,
}

fn main() -> Result<(), anyhow::Error> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .init();
    alloc::register("jemalloc");

    let args = Args::parse();
    let threads = args.threads.unwrap_or_else(num_cpus::get).max(1);

    info!(
        allocator = alloc::allocator_name(),
        listen = %args.listen,
        mode = %args.mode,
        threads,
        "starting lb-proxy-pingora"
    );

    let seed = match &args.config {
        Some(path) => RouteConfig::from_path(path)?,
        None => RouteConfig::single_upstream(&args.default_upstream),
    };
    let routes = Arc::new(SharedRouteTable::from_table(seed.compile()));

    if args.mode == "k8s" {
        // Pingora's `run_forever` never returns, so the reconciler gets its own
        // thread and its own current-thread runtime.
        spawn_k8s_reconciler(
            routes.clone(),
            ControllerConfig {
                gateway_name: Some(args.gateway_name.clone()),
                namespace: args.namespace.clone(),
            },
        );
    }

    let config = ServiceConfig {
        threads,
        ..ServiceConfig::new(args.listen)
    };
    ProxyPingora::new(routes)
        .run_server(&config)
        .map_err(|e| anyhow::anyhow!("pingora server failed: {e}"))
}
