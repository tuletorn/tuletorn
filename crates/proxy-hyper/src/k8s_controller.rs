//! Kubernetes Gateway API reconciler for the Hyper data plane.
//!
//! A thin adapter over [`lb_core::gateway`]; all three candidates share one
//! reconciler so a benchmark difference between them cannot come from a
//! difference in how an `HTTPRoute` was interpreted.

use lb_core::SharedRouteTable;
use std::sync::Arc;
use tracing::{error, info, warn};

#[cfg(feature = "k8s")]
pub use lb_core::gateway::ControllerConfig;

/// Configuration stand-in when the crate is built without the `k8s` feature.
#[cfg(not(feature = "k8s"))]
#[derive(Debug, Clone, Default)]
pub struct ControllerConfig {
    pub gateway_name: Option<String>,
    pub namespace: Option<String>,
}

/// Start the reconciler, restarting it if the watch stack fails.
///
/// Returns only when the process is shutting down. Errors are logged and
/// retried: losing the API server must degrade routing to "last known good",
/// not take the data plane down with it.
pub async fn start_k8s_reconciler(
    shared_routes: Arc<SharedRouteTable>,
    config: ControllerConfig,
) -> Result<(), anyhow::Error> {
    #[cfg(feature = "k8s")]
    {
        info!("starting Gateway API reconciler for lb-proxy-hyper");
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
        std::future::pending::<()>().await;
        Ok(())
    }
}
