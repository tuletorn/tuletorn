//! `lb-proxy-monoio` binary — hybrid Tokio control plane + Monoio data plane.

use clap::Parser;
use lb_core::{RouteConfig, SharedRouteTable, alloc};
use lb_proxy_monoio::k8s_controller::{ControllerConfig, spawn_control_plane};
use lb_proxy_monoio::{ProxyMonoio, WorkerConfig};
use std::net::SocketAddr;
use std::sync::Arc;
use tracing::{error, info};

#[global_allocator]
static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;

#[derive(Parser, Debug)]
#[command(
    author,
    version,
    about = "Monoio thread-per-core reverse proxy (lb benchmark candidate)"
)]
struct Args {
    #[arg(short, long, default_value = "0.0.0.0:8082")]
    listen: SocketAddr,

    #[arg(long, default_value = "standalone")]
    mode: String,

    #[arg(short, long)]
    config: Option<String>,

    #[arg(short, long, default_value = "127.0.0.1:9090")]
    default_upstream: String,

    /// Thread-per-core workers. Defaults to the logical CPU count so this
    /// candidate gets the same share of the machine as the others.
    #[arg(short, long)]
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
    alloc::register("mimalloc");

    let args = Args::parse();
    let threads = args.threads.unwrap_or_else(num_cpus::get).max(1);

    info!(
        allocator = alloc::allocator_name(),
        listen = %args.listen,
        mode = %args.mode,
        threads,
        "starting lb-proxy-monoio (hybrid runtime)"
    );

    let seed = match &args.config {
        Some(path) => RouteConfig::from_path(path)?,
        None => RouteConfig::single_upstream(&args.default_upstream),
    };
    let routes = Arc::new(SharedRouteTable::from_table(seed.compile()));

    // 1. Control plane on its own Tokio thread.
    if args.mode == "k8s" {
        spawn_control_plane(
            routes.clone(),
            ControllerConfig {
                gateway_name: Some(args.gateway_name.clone()),
                namespace: args.namespace.clone(),
            },
        );
    }

    // 2. Data plane: one Monoio runtime per core, each with its own
    //    SO_REUSEPORT listener so the kernel shards the accept queue.
    let config = WorkerConfig {
        workers: threads,
        ..WorkerConfig::new(args.listen)
    };

    let mut handles = Vec::with_capacity(threads);
    for worker_id in 0..threads {
        let routes = routes.clone();
        let config = config.clone();
        handles.push(
            std::thread::Builder::new()
                .name(format!("lb-monoio-{worker_id}"))
                .spawn(move || {
                    let mut runtime = match monoio::RuntimeBuilder::<monoio::FusionDriver>::new()
                        .enable_all()
                        .with_entries(4096)
                        .build()
                    {
                        Ok(rt) => rt,
                        Err(err) => {
                            error!(worker = worker_id, %err, "failed to build the Monoio runtime");
                            return;
                        }
                    };
                    runtime.block_on(async move {
                        let proxy = ProxyMonoio::with_config(routes, config);
                        if let Err(err) = proxy.run_worker(worker_id).await {
                            error!(worker = worker_id, %err, "worker exited");
                        }
                    });
                })?,
        );
    }

    for handle in handles {
        let _ = handle.join();
    }
    Ok(())
}
