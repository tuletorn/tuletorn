//! `lb-proxy-hyper` binary: standalone or in-cluster entry point.

use clap::Parser;
use lb_core::{RouteConfig, SharedRouteTable, alloc};
use lb_proxy_hyper::client::{ClientConfig, UpstreamClient};
use lb_proxy_hyper::k8s_controller::{ControllerConfig, start_k8s_reconciler};
use lb_proxy_hyper::{ProxyHyper, ServerConfig};
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::sync::watch;
use tracing::{info, warn};

// This binary owns its allocator. Declaring it here rather than in `lb-core`
// means Cargo feature unification cannot silently swap it for another.
#[global_allocator]
static GLOBAL: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;

#[derive(Parser, Debug)]
#[command(
    author,
    version,
    about = "Hyper 1.x reverse proxy (lb benchmark candidate)"
)]
struct Args {
    /// Address to listen on.
    #[arg(short, long, default_value = "0.0.0.0:8080")]
    listen: SocketAddr,

    /// Route source: `standalone` reads --config, `k8s` watches Gateway API.
    #[arg(long, default_value = "standalone")]
    mode: String,

    /// YAML route configuration (standalone mode).
    #[arg(short, long)]
    config: Option<String>,

    /// Fallback upstream when no --config is given.
    #[arg(short, long, default_value = "127.0.0.1:9090")]
    default_upstream: String,

    /// Accept loops / worker threads. Defaults to the logical CPU count.
    #[arg(long)]
    workers: Option<usize>,

    /// Gateway name to reconcile (k8s mode).
    #[arg(long, default_value = "lb-gateway")]
    gateway_name: String,

    /// Restrict the reconciler to one namespace (k8s mode).
    #[arg(long)]
    namespace: Option<String>,

    /// Speak HTTP/2 to upstream backends.
    #[arg(long)]
    http2_upstream: bool,
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
    let workers = args.workers.unwrap_or_else(num_cpus::get).max(1);

    // Build the runtime explicitly so the worker count is pinned rather than
    // left to the default. Every candidate must get the same number of cores or
    // the comparison in plan §8 is meaningless.
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(workers)
        .enable_all()
        .thread_name("lb-hyper")
        .build()?;

    runtime.block_on(run(args, workers))
}

async fn run(args: Args, workers: usize) -> Result<(), anyhow::Error> {
    info!(
        allocator = alloc::allocator_name(),
        listen = %args.listen,
        mode = %args.mode,
        workers,
        "starting lb-proxy-hyper"
    );

    // Seed the table before serving so there is never a window where requests
    // 404 because the control plane has not caught up.
    let seed = match &args.config {
        Some(path) => RouteConfig::from_path(path)?,
        None => RouteConfig::single_upstream(&args.default_upstream),
    };
    let routes = Arc::new(SharedRouteTable::from_table(seed.compile()));

    if args.mode == "k8s" {
        let routes = routes.clone();
        let cfg = ControllerConfig {
            gateway_name: Some(args.gateway_name.clone()),
            namespace: args.namespace.clone(),
        };
        tokio::spawn(async move {
            if let Err(err) = start_k8s_reconciler(routes, cfg).await {
                warn!(%err, "reconciler stopped");
            }
        });
    }

    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    tokio::spawn(async move {
        if tokio::signal::ctrl_c().await.is_ok() {
            info!("shutdown signal received");
            let _ = shutdown_tx.send(true);
        }
    });

    let upstream = UpstreamClient::new(&ClientConfig {
        http2_upstream: args.http2_upstream,
        ..Default::default()
    });
    let server_config = ServerConfig {
        workers,
        ..ServerConfig::new(args.listen)
    };

    ProxyHyper::with_config(routes, upstream, server_config)
        .run(shutdown_rx)
        .await
}
