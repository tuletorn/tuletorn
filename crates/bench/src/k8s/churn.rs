//! HTTPRoute churn injector (plan §8, Scenario 3).
//!
//! Mutates the live `HTTPRoute` at a fixed frequency while the load generator
//! holds a steady offered rate, so the report can attribute p99.99 spikes and
//! dropped requests to control-plane churn rather than to load.

use futures_util::StreamExt;
use kube::api::{Api, Patch, PatchParams};
use kube::core::{ApiResource, DynamicObject, GroupVersionKind};
use serde_json::json;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::Duration;
use tracing::{debug, info, warn};

/// Churn configuration.
#[derive(Debug, Clone)]
pub struct ChurnConfig {
    pub namespace: String,
    pub route_name: String,
    /// Mutations per second. Plan §8 sweeps 10-500 Hz.
    pub rate_hz: u32,
    /// Backend service names cycled through on each mutation.
    pub backends: Vec<String>,
    pub backend_port: i32,
}

impl Default for ChurnConfig {
    fn default() -> Self {
        Self {
            namespace: "default".to_string(),
            route_name: "benchmark-route".to_string(),
            rate_hz: 50,
            backends: vec!["mock-upstream".to_string(), "mock-upstream-b".to_string()],
            backend_port: 80,
        }
    }
}

/// Counters describing what the injector actually did.
#[derive(Debug, Default)]
pub struct ChurnStats {
    pub mutations_attempted: AtomicU64,
    pub mutations_succeeded: AtomicU64,
    pub mutations_failed: AtomicU64,
}

impl ChurnStats {
    /// A plain snapshot for reporting.
    #[must_use]
    pub fn snapshot(&self) -> (u64, u64, u64) {
        (
            self.mutations_attempted.load(Ordering::Relaxed),
            self.mutations_succeeded.load(Ordering::Relaxed),
            self.mutations_failed.load(Ordering::Relaxed),
        )
    }
}

/// Mutates an HTTPRoute at a fixed rate until stopped.
pub struct ChurnInjector {
    config: ChurnConfig,
    running: Arc<AtomicBool>,
    stats: Arc<ChurnStats>,
}

impl ChurnInjector {
    #[must_use]
    pub fn new(config: ChurnConfig) -> Self {
        Self {
            config,
            running: Arc::new(AtomicBool::new(true)),
            stats: Arc::new(ChurnStats::default()),
        }
    }

    /// Shared stats handle, readable while the injector runs.
    #[must_use]
    pub fn stats(&self) -> Arc<ChurnStats> {
        self.stats.clone()
    }

    /// Stop handle.
    #[must_use]
    pub fn stop_handle(&self) -> Arc<AtomicBool> {
        self.running.clone()
    }

    /// Run until stopped, patching the route `rate_hz` times per second.
    pub async fn run(&self) -> Result<(), anyhow::Error> {
        if self.config.rate_hz == 0 {
            info!("churn rate is 0 Hz, injector idle");
            return Ok(());
        }
        let client = kube::Client::try_default().await?;
        let ar = ApiResource::from_gvk(&GroupVersionKind::gvk(
            "gateway.networking.k8s.io",
            "v1",
            "HTTPRoute",
        ));
        let api: Api<DynamicObject> = Api::namespaced_with(client, &self.config.namespace, &ar);

        let interval = Duration::from_secs_f64(1.0 / f64::from(self.config.rate_hz));
        info!(
            rate_hz = self.config.rate_hz,
            route = %self.config.route_name,
            "starting HTTPRoute churn injector"
        );

        // `Patch::Merge` rather than a read-modify-write: a full GET+PUT at
        // 500 Hz would measure the API server's round trip, not the proxy's
        // reaction to a change.
        let params = PatchParams::apply("lb-bench-churn").force();
        let mut ticker = tokio::time::interval(interval);
        ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        let mut generation = 0u64;

        while self.running.load(Ordering::Relaxed) {
            ticker.tick().await;
            generation += 1;
            let backend = &self.config.backends[generation as usize % self.config.backends.len()];
            let patch = json!({
                "apiVersion": "gateway.networking.k8s.io/v1",
                "kind": "HTTPRoute",
                "metadata": {
                    "name": self.config.route_name,
                    "annotations": {"lb-bench/churn-generation": generation.to_string()}
                },
                "spec": {
                    "rules": [{
                        "matches": [{"path": {"type": "PathPrefix", "value": "/"}}],
                        "backendRefs": [{
                            "name": backend,
                            "port": self.config.backend_port,
                            "weight": 1
                        }]
                    }]
                }
            });

            self.stats
                .mutations_attempted
                .fetch_add(1, Ordering::Relaxed);
            match api
                .patch(&self.config.route_name, &params, &Patch::Apply(&patch))
                .await
            {
                Ok(_) => {
                    self.stats
                        .mutations_succeeded
                        .fetch_add(1, Ordering::Relaxed);
                    debug!(generation, backend, "route mutated");
                }
                Err(err) => {
                    self.stats.mutations_failed.fetch_add(1, Ordering::Relaxed);
                    warn!(%err, "route mutation failed");
                }
            }
        }

        let (attempted, ok, failed) = self.stats.snapshot();
        info!(attempted, ok, failed, "churn injector stopped");
        Ok(())
    }

    /// Watch the route and report how quickly mutations become visible.
    ///
    /// Used to sanity-check that the injector is actually reaching the API
    /// server before attributing latency spikes to it.
    pub async fn observe_propagation(
        &self,
        samples: usize,
    ) -> Result<Vec<Duration>, anyhow::Error> {
        let client = kube::Client::try_default().await?;
        let ar = ApiResource::from_gvk(&GroupVersionKind::gvk(
            "gateway.networking.k8s.io",
            "v1",
            "HTTPRoute",
        ));
        let api: Api<DynamicObject> = Api::namespaced_with(client, &self.config.namespace, &ar);

        let mut latencies = Vec::with_capacity(samples);
        let mut stream =
            kube::runtime::watcher(api, kube::runtime::watcher::Config::default()).boxed();
        let start = std::time::Instant::now();
        let mut seen = 0usize;

        while seen < samples {
            match tokio::time::timeout(Duration::from_secs(10), stream.next()).await {
                Ok(Some(Ok(kube::runtime::watcher::Event::Apply(_)))) => {
                    latencies.push(start.elapsed());
                    seen += 1;
                }
                Ok(Some(_)) => {}
                Ok(None) | Err(_) => break,
            }
        }
        Ok(latencies)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_config_matches_the_shipped_manifests() {
        let c = ChurnConfig::default();
        // Must line up with k8s/routes/benchmark-httproute.yaml and
        // k8s/routes/churn-test-routes.yaml, or the injector patches nothing.
        assert_eq!(c.route_name, "benchmark-route");
        assert_eq!(c.backends, ["mock-upstream", "mock-upstream-b"]);
        assert_eq!(c.namespace, "default");
        assert!(
            c.backends.len() >= 2,
            "churn needs something to alternate between"
        );
    }

    #[test]
    fn churn_interval_is_the_reciprocal_of_the_rate() {
        for hz in [10u32, 50, 100, 500] {
            let interval = Duration::from_secs_f64(1.0 / f64::from(hz));
            assert!(
                (interval.as_secs_f64() * f64::from(hz) - 1.0).abs() < 1e-9,
                "interval for {hz} Hz is wrong"
            );
        }
    }

    #[tokio::test]
    async fn zero_rate_is_a_no_op_and_needs_no_cluster() {
        let injector = ChurnInjector::new(ChurnConfig {
            rate_hz: 0,
            ..Default::default()
        });
        injector
            .run()
            .await
            .expect("0 Hz must not require a cluster");
        assert_eq!(injector.stats().snapshot(), (0, 0, 0));
    }

    #[test]
    fn stop_handle_stops_the_loop() {
        let injector = ChurnInjector::new(ChurnConfig::default());
        let handle = injector.stop_handle();
        assert!(handle.load(Ordering::Relaxed));
        handle.store(false, Ordering::Relaxed);
        assert!(!injector.running.load(Ordering::Relaxed));
    }

    #[test]
    fn stats_snapshot_reflects_recorded_counts() {
        let stats = ChurnStats::default();
        stats.mutations_attempted.fetch_add(10, Ordering::Relaxed);
        stats.mutations_succeeded.fetch_add(9, Ordering::Relaxed);
        stats.mutations_failed.fetch_add(1, Ordering::Relaxed);
        assert_eq!(stats.snapshot(), (10, 9, 1));
    }
}
