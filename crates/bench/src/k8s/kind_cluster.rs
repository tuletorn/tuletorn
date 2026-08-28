//! `kind` cluster lifecycle (plan §5, §7.1).

use serde::{Deserialize, Serialize};
use std::path::Path;
use std::process::Output;
use tokio::process::Command;
use tracing::{info, warn};

/// Pinned infrastructure versions from plan §7.1.
pub const KIND_NODE_IMAGE: &str = "kindest/node:v1.31.6";
pub const GATEWAY_API_VERSION: &str = "v1.2.1";
pub const GATEWAY_API_CRD_URL: &str =
    "https://github.com/kubernetes-sigs/gateway-api/releases/download/v1.2.1/standard-install.yaml";
pub const TRAEFIK_IMAGE: &str = "traefik:v3.7.12";

/// Cluster configuration.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct KindConfig {
    pub cluster_name: String,
    pub config_path: String,
}

impl Default for KindConfig {
    fn default() -> Self {
        Self {
            cluster_name: "lb-bench".to_string(),
            config_path: "k8s/kind-cluster-config.yaml".to_string(),
        }
    }
}

/// Manages a `kind` cluster.
pub struct KindCluster {
    config: KindConfig,
}

impl KindCluster {
    #[must_use]
    pub fn new(config: KindConfig) -> Self {
        Self { config }
    }

    /// Whether the `kind` binary is on PATH.
    pub async fn is_available() -> bool {
        Command::new("kind")
            .arg("--version")
            .output()
            .await
            .is_ok_and(|o| o.status.success())
    }

    /// Whether this cluster already exists.
    pub async fn exists(&self) -> Result<bool, anyhow::Error> {
        let out = Command::new("kind")
            .arg("get")
            .arg("clusters")
            .output()
            .await?;
        let listing = String::from_utf8_lossy(&out.stdout);
        Ok(listing
            .lines()
            .any(|l| l.trim() == self.config.cluster_name))
    }

    /// Create the cluster if it does not exist. Idempotent.
    pub async fn create(&self) -> Result<(), anyhow::Error> {
        if self.exists().await? {
            info!(cluster = %self.config.cluster_name, "kind cluster already exists");
            return Ok(());
        }
        if !Path::new(&self.config.config_path).exists() {
            anyhow::bail!("kind config not found at {}", self.config.config_path);
        }
        info!(
            cluster = %self.config.cluster_name,
            image = KIND_NODE_IMAGE,
            "creating kind cluster"
        );
        run_checked(
            Command::new("kind")
                .arg("create")
                .arg("cluster")
                .arg("--name")
                .arg(&self.config.cluster_name)
                .arg("--config")
                .arg(&self.config.config_path)
                .arg("--wait")
                .arg("120s"),
            "kind create cluster",
        )
        .await
    }

    /// Install the pinned Gateway API CRDs.
    pub async fn install_gateway_api(&self) -> Result<(), anyhow::Error> {
        info!(version = GATEWAY_API_VERSION, "installing Gateway API CRDs");
        // Prefer the vendored copy so a benchmark run is reproducible offline.
        let local = Path::new("k8s/gateway-api-crds.yaml");
        let source = if local.exists() {
            local.to_string_lossy().into_owned()
        } else {
            warn!("vendored CRDs not found, fetching from upstream");
            GATEWAY_API_CRD_URL.to_string()
        };
        run_checked(
            self.kubectl().arg("apply").arg("-f").arg(&source),
            "kubectl apply gateway-api-crds",
        )
        .await
    }

    /// Load a locally built image into the cluster's nodes.
    pub async fn load_image(&self, image: &str) -> Result<(), anyhow::Error> {
        info!(image, cluster = %self.config.cluster_name, "loading image into kind");
        run_checked(
            Command::new("kind")
                .arg("load")
                .arg("docker-image")
                .arg(image)
                .arg("--name")
                .arg(&self.config.cluster_name),
            "kind load docker-image",
        )
        .await
    }

    /// Delete the cluster.
    pub async fn destroy(&self) -> Result<(), anyhow::Error> {
        info!(cluster = %self.config.cluster_name, "deleting kind cluster");
        run_checked(
            Command::new("kind")
                .arg("delete")
                .arg("cluster")
                .arg("--name")
                .arg(&self.config.cluster_name),
            "kind delete cluster",
        )
        .await
    }

    /// A `kubectl` command bound to this cluster's context.
    #[must_use]
    pub fn kubectl(&self) -> Command {
        let mut cmd = Command::new("kubectl");
        cmd.arg("--context")
            .arg(format!("kind-{}", self.config.cluster_name));
        cmd
    }

    /// The kubeconfig context name for this cluster.
    #[must_use]
    pub fn context(&self) -> String {
        format!("kind-{}", self.config.cluster_name)
    }
}

/// Run a command, turning a non-zero exit into an error carrying stderr.
pub(crate) async fn run_checked(cmd: &mut Command, what: &str) -> Result<(), anyhow::Error> {
    let output: Output = cmd.output().await?;
    if output.status.success() {
        return Ok(());
    }
    anyhow::bail!(
        "{what} failed ({}): {}",
        output.status,
        String::from_utf8_lossy(&output.stderr).trim()
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn pinned_versions_match_the_plan() {
        assert_eq!(KIND_NODE_IMAGE, "kindest/node:v1.31.6");
        assert_eq!(GATEWAY_API_VERSION, "v1.2.1");
        assert_eq!(TRAEFIK_IMAGE, "traefik:v3.7.12");
        assert!(GATEWAY_API_CRD_URL.contains(GATEWAY_API_VERSION));
    }

    #[test]
    fn context_name_follows_the_kind_convention() {
        let c = KindCluster::new(KindConfig::default());
        assert_eq!(c.context(), "kind-lb-bench");
    }

    #[tokio::test]
    async fn creating_without_a_config_file_reports_the_path() {
        let cluster = KindCluster::new(KindConfig {
            cluster_name: "lb-bench-test-nonexistent".into(),
            config_path: "/nonexistent/kind.yaml".into(),
        });
        // `exists` shells out to kind; skip when kind is absent.
        if !KindCluster::is_available().await {
            return;
        }
        let err = cluster.create().await.unwrap_err();
        assert!(err.to_string().contains("/nonexistent/kind.yaml"));
    }
}
