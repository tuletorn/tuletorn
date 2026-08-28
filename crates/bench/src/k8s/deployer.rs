//! Deploys candidates and Traefik into the `kind` cluster (plan §5).

use crate::harness::Candidate;
use crate::k8s::kind_cluster::{KindCluster, run_checked};
use std::path::Path;
use std::time::Duration;
use tracing::{info, warn};

/// Manifest paths, relative to the repository root.
pub struct Manifests;

impl Manifests {
    pub const MOCK_BACKEND: &'static str = "k8s/mock-backend/mock-upstream-deployment.yaml";
    pub const TRAEFIK: &'static str = "k8s/traefik/deployment.yaml";
    pub const TRAEFIK_GATEWAY: &'static str = "k8s/traefik/gateway.yaml";
    pub const BENCHMARK_ROUTE: &'static str = "k8s/routes/benchmark-httproute.yaml";
    pub const CHURN_ROUTES: &'static str = "k8s/routes/churn-test-routes.yaml";

    /// Manifest for a Rust candidate's Deployment, Service and Gateway.
    #[must_use]
    pub fn for_candidate(candidate: Candidate) -> Option<&'static str> {
        match candidate {
            Candidate::Hyper => Some("k8s/rust-proxies/hyper-deployment.yaml"),
            Candidate::Pingora => Some("k8s/rust-proxies/pingora-deployment.yaml"),
            Candidate::Monoio => Some("k8s/rust-proxies/monoio-deployment.yaml"),
            Candidate::Traefik => Some(Self::TRAEFIK),
        }
    }
}

/// Applies manifests and waits for rollouts.
pub struct Deployer<'a> {
    cluster: &'a KindCluster,
}

impl<'a> Deployer<'a> {
    #[must_use]
    pub fn new(cluster: &'a KindCluster) -> Self {
        Self { cluster }
    }

    /// Apply a manifest file.
    pub async fn apply(&self, manifest: &str) -> Result<(), anyhow::Error> {
        if !Path::new(manifest).exists() {
            anyhow::bail!("manifest not found: {manifest}");
        }
        info!(manifest, "applying manifest");
        run_checked(
            self.cluster.kubectl().arg("apply").arg("-f").arg(manifest),
            &format!("kubectl apply -f {manifest}"),
        )
        .await
    }

    /// Delete the resources in a manifest, ignoring "not found".
    pub async fn delete(&self, manifest: &str) -> Result<(), anyhow::Error> {
        if !Path::new(manifest).exists() {
            return Ok(());
        }
        let output = self
            .cluster
            .kubectl()
            .arg("delete")
            .arg("-f")
            .arg(manifest)
            .arg("--ignore-not-found")
            .output()
            .await?;
        if !output.status.success() {
            warn!(
                manifest,
                stderr = %String::from_utf8_lossy(&output.stderr).trim(),
                "delete reported an error"
            );
        }
        Ok(())
    }

    /// Wait for a Deployment to become available.
    pub async fn wait_for_rollout(
        &self,
        namespace: &str,
        deployment: &str,
        timeout: Duration,
    ) -> Result<(), anyhow::Error> {
        info!(namespace, deployment, "waiting for rollout");
        run_checked(
            self.cluster
                .kubectl()
                .arg("-n")
                .arg(namespace)
                .arg("rollout")
                .arg("status")
                .arg(format!("deployment/{deployment}"))
                .arg(format!("--timeout={}s", timeout.as_secs())),
            &format!("rollout status {namespace}/{deployment}"),
        )
        .await
    }

    /// Stand up the shared benchmark fixtures: CRDs and mock backends.
    pub async fn deploy_baseline(&self) -> Result<(), anyhow::Error> {
        self.cluster.install_gateway_api().await?;
        // The CRDs need a moment to register before dependent objects apply.
        tokio::time::sleep(Duration::from_secs(3)).await;
        self.apply(Manifests::MOCK_BACKEND).await?;
        self.wait_for_rollout("default", "mock-upstream", Duration::from_secs(180))
            .await
    }

    /// Deploy one candidate and wait for it to be ready.
    pub async fn deploy_candidate(&self, candidate: Candidate) -> Result<(), anyhow::Error> {
        let Some(manifest) = Manifests::for_candidate(candidate) else {
            anyhow::bail!("no manifest for {}", candidate.display_name());
        };
        self.apply(manifest).await?;

        let (namespace, deployment) = deployment_ref(candidate);
        self.wait_for_rollout(namespace, deployment, Duration::from_secs(180))
            .await?;

        if candidate == Candidate::Traefik {
            self.apply(Manifests::TRAEFIK_GATEWAY).await?;
        }
        self.apply(Manifests::BENCHMARK_ROUTE).await
    }

    /// Tear down one candidate, leaving the shared fixtures in place.
    pub async fn remove_candidate(&self, candidate: Candidate) -> Result<(), anyhow::Error> {
        if let Some(manifest) = Manifests::for_candidate(candidate) {
            self.delete(manifest).await?;
        }
        Ok(())
    }

    /// The node port a candidate is reachable on from the host.
    #[must_use]
    pub fn host_port(candidate: Candidate) -> u16 {
        match candidate {
            // Matches the extraPortMappings in k8s/kind-cluster-config.yaml.
            Candidate::Traefik => 8000,
            Candidate::Hyper => 8001,
            Candidate::Pingora => 8002,
            Candidate::Monoio => 8003,
        }
    }
}

/// The Gateway object a candidate attaches to.
///
/// Each candidate owns a distinct Gateway so that all of them can be deployed
/// at once; the shared `benchmark-route` lists every one as a parentRef.
#[must_use]
pub fn gateway_name(candidate: Candidate) -> &'static str {
    match candidate {
        Candidate::Hyper => "lb-gateway-hyper",
        Candidate::Pingora => "lb-gateway-pingora",
        Candidate::Monoio => "lb-gateway-monoio",
        Candidate::Traefik => "traefik-gateway",
    }
}

/// `(namespace, deployment-name)` for a candidate.
#[must_use]
pub fn deployment_ref(candidate: Candidate) -> (&'static str, &'static str) {
    match candidate {
        Candidate::Hyper => ("default", "lb-proxy-hyper"),
        Candidate::Pingora => ("default", "lb-proxy-pingora"),
        Candidate::Monoio => ("default", "lb-proxy-monoio"),
        Candidate::Traefik => ("traefik", "traefik"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn every_candidate_has_a_manifest() {
        for c in Candidate::all() {
            assert!(
                Manifests::for_candidate(c).is_some(),
                "{} has no manifest",
                c.display_name()
            );
        }
    }

    #[test]
    fn manifests_referenced_by_the_deployer_exist_in_the_repo() {
        // Run from the workspace root, where cargo test executes.
        let root = std::env::var("CARGO_MANIFEST_DIR")
            .map(|d| Path::new(&d).join("../..").to_path_buf())
            .expect("CARGO_MANIFEST_DIR is set under cargo test");
        for manifest in [
            Manifests::MOCK_BACKEND,
            Manifests::TRAEFIK,
            Manifests::TRAEFIK_GATEWAY,
            Manifests::BENCHMARK_ROUTE,
            Manifests::CHURN_ROUTES,
        ] {
            assert!(root.join(manifest).exists(), "missing manifest: {manifest}");
        }
        for c in Candidate::all() {
            let manifest = Manifests::for_candidate(c).unwrap();
            assert!(
                root.join(manifest).exists(),
                "missing manifest for {}: {manifest}",
                c.display_name()
            );
        }
    }

    #[test]
    fn host_ports_are_unique() {
        let ports: Vec<u16> = Candidate::all()
            .iter()
            .map(|c| Deployer::host_port(*c))
            .collect();
        let mut sorted = ports.clone();
        sorted.sort_unstable();
        sorted.dedup();
        assert_eq!(sorted.len(), ports.len(), "host ports collide: {ports:?}");
    }

    #[test]
    fn each_candidate_owns_a_distinct_gateway() {
        // A shared Gateway name would mean applying all three manifests leaves
        // only the last one's Gateway, silently detaching the others.
        let names: Vec<&str> = Candidate::all().iter().map(|c| gateway_name(*c)).collect();
        let mut sorted = names.clone();
        sorted.sort_unstable();
        sorted.dedup();
        assert_eq!(
            sorted.len(),
            names.len(),
            "gateway names collide: {names:?}"
        );
    }

    #[test]
    fn manifests_declare_the_gateway_each_candidate_expects() {
        let root = std::env::var("CARGO_MANIFEST_DIR")
            .map(|d| Path::new(&d).join("../..").to_path_buf())
            .expect("CARGO_MANIFEST_DIR is set under cargo test");
        for candidate in Candidate::rust_candidates() {
            let manifest = root.join(Manifests::for_candidate(candidate).unwrap());
            let text = std::fs::read_to_string(&manifest).expect("manifest reads");
            let expected = gateway_name(candidate);
            assert!(
                text.contains(&format!("name: {expected}")),
                "{} does not declare Gateway {expected}",
                manifest.display()
            );
            assert!(
                text.contains(&format!(
                    "--gateway-name
            - {expected}"
                )),
                "{} does not pass --gateway-name {expected} to the binary",
                manifest.display()
            );
        }
    }

    #[test]
    fn the_shared_route_names_every_candidate_gateway() {
        let root = std::env::var("CARGO_MANIFEST_DIR")
            .map(|d| Path::new(&d).join("../..").to_path_buf())
            .expect("CARGO_MANIFEST_DIR is set under cargo test");
        let route = std::fs::read_to_string(root.join(Manifests::BENCHMARK_ROUTE))
            .expect("benchmark route reads");
        for candidate in Candidate::all() {
            let gateway = gateway_name(candidate);
            assert!(
                route.contains(gateway),
                "benchmark-httproute.yaml does not list {gateway} as a parentRef"
            );
        }
    }

    #[test]
    fn traefik_lives_in_its_own_namespace() {
        assert_eq!(deployment_ref(Candidate::Traefik).0, "traefik");
        for c in Candidate::rust_candidates() {
            assert_eq!(deployment_ref(c).0, "default");
        }
    }
}
