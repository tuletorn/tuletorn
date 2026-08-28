//! Hybrid-runtime Kubernetes controller for the Monoio data plane (plan §6).
//!
//! `kube-rs` is built on Tokio and Monoio is a different, thread-per-core
//! runtime, so the two cannot share an executor. The control plane therefore
//! gets one dedicated OS thread running a Tokio current-thread runtime, and the
//! only thing crossing the boundary is the `ArcSwap<RouteTable>` — no channels,
//! no locks, and no Tokio types on the data path.

use lb_core::SharedRouteTable;
use std::sync::Arc;
use tracing::{error, info, warn};

#[cfg(feature = "k8s")]
pub use lb_core::gateway::ControllerConfig;

/// Configuration stand-in when built without the `k8s` feature.
#[cfg(not(feature = "k8s"))]
#[derive(Debug, Clone, Default)]
pub struct ControllerConfig {
    pub gateway_name: Option<String>,
    pub namespace: Option<String>,
}

/// Spawn the control plane on its own Tokio thread.
pub fn spawn_control_plane(
    shared_routes: Arc<SharedRouteTable>,
    config: ControllerConfig,
) -> std::thread::JoinHandle<()> {
    std::thread::Builder::new()
        .name("lb-monoio-k8s".into())
        .spawn(move || {
            let runtime = match tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
            {
                Ok(rt) => rt,
                Err(err) => {
                    error!(%err, "failed to build the control-plane runtime");
                    return;
                }
            };
            runtime.block_on(start_k8s_reconciler_on_tokio(shared_routes, config));
        })
        .expect("spawning the control-plane thread")
}

/// Run the reconciler, restarting it with backoff on failure.
pub async fn start_k8s_reconciler_on_tokio(
    shared_routes: Arc<SharedRouteTable>,
    config: ControllerConfig,
) {
    #[cfg(feature = "k8s")]
    {
        info!("starting Gateway API reconciler on the dedicated Tokio thread");
        let mut backoff = std::time::Duration::from_secs(1);
        loop {
            match lb_core::gateway::run(shared_routes.clone(), config.clone()).await {
                Ok(()) => warn!("Gateway API reconciler exited cleanly, restarting"),
                Err(err) => error!(%err, "Gateway API reconciler failed, restarting"),
            }
            tokio::time::sleep(backoff).await;
            backoff = (backoff * 2).min(std::time::Duration::from_secs(30));
        }
    }
    #[cfg(not(feature = "k8s"))]
    {
        let _ = (shared_routes, config);
        warn!("built without the `k8s` feature; routes come from --config only");
        std::future::pending::<()>().await
    }
}
