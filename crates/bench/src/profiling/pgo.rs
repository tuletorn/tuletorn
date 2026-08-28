//! PGO build pipeline orchestration (plan §4.2).
//!
//! The critical detail is the profile-collection pass. A pipeline that starts
//! the instrumented binary and then runs a benchmark which spins up its *own*
//! proxy collects an empty profile and silently produces a non-PGO build. The
//! [`PgoPipeline::collect_profile`] step here drives load at the instrumented
//! binary's own address.

use crate::harness::Candidate;
use std::path::{Path, PathBuf};
use std::time::Duration;
use tokio::process::Command;
use tracing::{info, warn};

/// PGO pipeline configuration.
#[derive(Debug, Clone)]
pub struct PgoConfig {
    /// Where `.profraw` files are written.
    pub profile_dir: PathBuf,
    /// Where the instrumented and optimised binaries are built.
    pub target_dir: PathBuf,
    /// Route config used for the profiling workload.
    pub route_config: PathBuf,
    /// How long to drive load during profile collection.
    pub collect_duration: Duration,
    /// Concurrency levels used during collection.
    pub concurrencies: Vec<usize>,
    /// `target-cpu` for both passes.
    pub target_cpu: String,
}

impl Default for PgoConfig {
    fn default() -> Self {
        Self {
            profile_dir: PathBuf::from("target/pgo-profiles"),
            target_dir: PathBuf::from("target"),
            route_config: PathBuf::from("examples/pgo_routes.yaml"),
            collect_duration: Duration::from_secs(10),
            concurrencies: vec![100, 500, 1_000],
            target_cpu: "native".to_string(),
        }
    }
}

/// Drives the three-pass PGO build.
pub struct PgoPipeline {
    config: PgoConfig,
}

impl PgoPipeline {
    #[must_use]
    pub fn new(config: PgoConfig) -> Self {
        Self { config }
    }

    /// Locate `llvm-profdata`.
    ///
    /// The one that matches rustc's LLVM ships in the `llvm-tools` rustup
    /// component; a Homebrew or distro `llvm-profdata` is frequently a
    /// different LLVM major version and will reject rustc's `.profraw` format.
    pub async fn find_llvm_profdata() -> Option<PathBuf> {
        // Preferred: the rustup component, which is version-matched by design.
        if let Ok(output) = Command::new("rustc")
            .arg("--print")
            .arg("sysroot")
            .output()
            .await
            && output.status.success()
            && let Ok(sysroot) = String::from_utf8(output.stdout)
        {
            let root = Path::new(sysroot.trim());
            if let Ok(entries) = std::fs::read_dir(root.join("lib/rustlib")) {
                for entry in entries.filter_map(Result::ok) {
                    let candidate = entry.path().join("bin/llvm-profdata");
                    if candidate.exists() {
                        return Some(candidate);
                    }
                }
            }
        }
        // Fallback: whatever is on PATH, with the version caveat above.
        Command::new("which")
            .arg("llvm-profdata")
            .output()
            .await
            .ok()
            .filter(|o| o.status.success())
            .and_then(|o| String::from_utf8(o.stdout).ok())
            .map(|s| PathBuf::from(s.trim()))
            .filter(|p| p.exists())
    }

    /// Pass 1: build with `-Cprofile-generate`.
    pub async fn build_instrumented(&self, candidate: Candidate) -> Result<(), anyhow::Error> {
        let binary = candidate
            .binary_name()
            .ok_or_else(|| anyhow::anyhow!("{} cannot be PGO built", candidate.display_name()))?;

        let _ = std::fs::remove_dir_all(&self.config.profile_dir);
        std::fs::create_dir_all(&self.config.profile_dir)?;

        info!(binary, "PGO pass 1/3: instrumented build");
        let flags = format!(
            "-C target-cpu={} -C profile-generate={}",
            self.config.target_cpu,
            std::path::absolute(&self.config.profile_dir)?.display()
        );
        run_cargo(&["build", "--release", "--bin", binary], &flags).await
    }

    /// Pass 2: run a representative workload against the instrumented binary.
    ///
    /// Returns the number of requests actually driven at it, so a caller can
    /// refuse to proceed on an empty profile.
    pub async fn collect_profile(&self, candidate: Candidate) -> Result<u64, anyhow::Error> {
        use crate::harness::{Candidate as C, LaunchSpec, RunningCandidate};
        use crate::load::{LoadConfig, LoadGenerator};
        use crate::metrics::LatencyRecorder;
        use crate::mock::{MockUpstream, MockUpstreamConfig};
        use std::sync::Arc;

        info!("PGO pass 2/3: profile collection");

        // Mock upstream on an ephemeral port.
        let probe = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
        let upstream_addr = probe.local_addr()?;
        drop(probe);
        let (shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);
        let mock = MockUpstream::new(MockUpstreamConfig {
            listen_addr: upstream_addr,
            ..Default::default()
        });
        tokio::spawn(async move {
            let _ = mock.run(shutdown_rx).await;
        });

        // Launch the *instrumented* binary and drive load at its own address.
        // This is the step that makes the profile representative.
        let listen: std::net::SocketAddr =
            format!("127.0.0.1:{}", candidate.default_port()).parse()?;
        let spec = LaunchSpec {
            binary_dir: self.config.target_dir.join("release"),
            config_path: self
                .config
                .route_config
                .exists()
                .then(|| self.config.route_config.clone()),
            ..LaunchSpec::new(candidate, listen, upstream_addr)
        };
        let running = RunningCandidate::launch(&spec).await?;
        debug_assert_ne!(candidate, C::Traefik);

        let recorder = Arc::new(LatencyRecorder::new());
        for &concurrency in &self.config.concurrencies {
            // Two payload sizes, so both the small-response and the streaming
            // paths appear in the profile.
            for size in [1024usize, 65_536] {
                let url = format!("{}/bench?size={size}", running.base_url());
                LoadGenerator::new(
                    LoadConfig::new(url, concurrency, self.config.collect_duration),
                    recorder.clone(),
                )
                .run()
                .await?;
            }
        }

        let driven = recorder.completed();
        running.stop().await;
        let _ = shutdown_tx.send(true);

        if driven == 0 {
            anyhow::bail!(
                "profile collection drove 0 requests at the instrumented binary; \
                 the resulting PGO build would be no better than a plain release build"
            );
        }
        info!(requests = driven, "profile collection complete");
        Ok(driven)
    }

    /// Merge `.profraw` files into a single `.profdata`.
    pub async fn merge_profiles(&self) -> Result<PathBuf, anyhow::Error> {
        let profdata = Self::find_llvm_profdata().await.ok_or_else(|| {
            anyhow::anyhow!(
                "llvm-profdata not found. Install the version-matched tool with: \
                 rustup component add llvm-tools"
            )
        })?;
        let merged = self.config.profile_dir.join("merged.profdata");

        let raw_count = std::fs::read_dir(&self.config.profile_dir)
            .map(|entries| {
                entries
                    .filter_map(Result::ok)
                    .filter(|e| e.path().extension().is_some_and(|x| x == "profraw"))
                    .count()
            })
            .unwrap_or(0);
        if raw_count == 0 {
            anyhow::bail!(
                "no .profraw files in {}; pass 2 did not exercise the instrumented binary",
                self.config.profile_dir.display()
            );
        }

        info!(raw_count, tool = %profdata.display(), "merging PGO profiles");
        let output = Command::new(&profdata)
            .arg("merge")
            .arg("-o")
            .arg(&merged)
            .arg(&self.config.profile_dir)
            .output()
            .await?;
        if !output.status.success() {
            anyhow::bail!(
                "llvm-profdata merge failed: {}",
                String::from_utf8_lossy(&output.stderr).trim()
            );
        }
        Ok(merged)
    }

    /// Pass 3: rebuild with `-Cprofile-use`.
    pub async fn build_optimized(
        &self,
        candidate: Candidate,
        profdata: &Path,
    ) -> Result<(), anyhow::Error> {
        let binary = candidate
            .binary_name()
            .ok_or_else(|| anyhow::anyhow!("{} cannot be PGO built", candidate.display_name()))?;
        info!(binary, "PGO pass 3/3: optimised build");
        let flags = format!(
            "-C target-cpu={} -C profile-use={} -C llvm-args=-pgo-warn-missing-function",
            self.config.target_cpu,
            std::path::absolute(profdata)?.display()
        );
        run_cargo(&["build", "--release", "--bin", binary], &flags).await
    }

    /// Run all three passes.
    pub async fn run(&self, candidate: Candidate) -> Result<PathBuf, anyhow::Error> {
        self.build_instrumented(candidate).await?;
        self.collect_profile(candidate).await?;
        let profdata = self.merge_profiles().await?;
        self.build_optimized(candidate, &profdata).await?;

        let binary = self
            .config
            .target_dir
            .join("release")
            .join(candidate.binary_name().expect("checked above"));
        info!(binary = %binary.display(), "PGO build complete");
        Ok(binary)
    }
}

/// Run cargo with explicit `RUSTFLAGS`.
async fn run_cargo(args: &[&str], rustflags: &str) -> Result<(), anyhow::Error> {
    let output = Command::new("cargo")
        .args(args)
        .env("RUSTFLAGS", rustflags)
        .output()
        .await?;
    if output.status.success() {
        return Ok(());
    }
    let stderr = String::from_utf8_lossy(&output.stderr);
    warn!(%stderr, "cargo build failed");
    anyhow::bail!("cargo {} failed", args.join(" "))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_config_points_at_the_plan_paths() {
        let c = PgoConfig::default();
        assert_eq!(c.profile_dir, PathBuf::from("target/pgo-profiles"));
        assert_eq!(c.route_config, PathBuf::from("examples/pgo_routes.yaml"));
        assert_eq!(c.concurrencies, vec![100, 500, 1_000]);
    }

    #[tokio::test]
    async fn merging_without_profraw_files_is_an_explicit_error() {
        let dir = std::env::temp_dir().join("lb-pgo-empty-test");
        let _ = std::fs::create_dir_all(&dir);
        let pipeline = PgoPipeline::new(PgoConfig {
            profile_dir: dir.clone(),
            ..Default::default()
        });
        let err = pipeline.merge_profiles().await.unwrap_err();
        let message = err.to_string();
        assert!(
            message.contains("no .profraw") || message.contains("llvm-profdata not found"),
            "unexpected error: {message}"
        );
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[tokio::test]
    async fn traefik_cannot_be_pgo_built() {
        let pipeline = PgoPipeline::new(PgoConfig::default());
        let err = pipeline
            .build_optimized(Candidate::Traefik, Path::new("/tmp/x.profdata"))
            .await
            .unwrap_err();
        assert!(err.to_string().contains("cannot be PGO built"));
    }

    #[tokio::test]
    async fn llvm_profdata_lookup_prefers_the_rustup_component() {
        // Only asserts the shape of the result; the tool may not be installed.
        if let Some(path) = PgoPipeline::find_llvm_profdata().await {
            assert!(path.exists(), "returned a path that does not exist");
            assert!(path.ends_with("llvm-profdata"));
        }
    }
}
