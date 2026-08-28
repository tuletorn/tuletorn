//! Kubernetes Gateway API reconciler for the Pingora data plane.
//!
//! Pingora owns its own runtime and `run_forever` never returns, so the
//! reconciler runs on a dedicated Tokio current-thread runtime on its own OS
//! thread and publishes into the shared `ArcSwap`.

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

/// Spawn the reconciler on its own thread. Returns the join handle.
pub fn spawn_k8s_reconciler(
    shared_routes: Arc<SharedRouteTable>,
    config: ControllerConfig,
) -> std::thread::JoinHandle<()> {
    std::thread::Builder::new()
        .name("lb-pingora-k8s".into())
        .spawn(move || {
            let runtime = match tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
            {
                Ok(rt) => rt,
                Err(err) => {
                    error!(%err, "failed to build reconciler runtime");
                    return;
                }
            };
            runtime.block_on(start_k8s_reconciler(shared_routes, config));
        })
        .expect("spawning the reconciler thread")
}

/// Run the reconciler, restarting it with backoff if the watch stack fails.
pub async fn start_k8s_reconciler(shared_routes: Arc<SharedRouteTable>, config: ControllerConfig) {
    #[cfg(feature = "k8s")]
    {
        info!("starting Gateway API reconciler for lb-proxy-pingora");
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
