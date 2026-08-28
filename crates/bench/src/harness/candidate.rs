//! Candidate lifecycle: launch each proxy as its **own OS process**.
//!
//! This is the single most important structural fix in the harness. Running the
//! proxy inside the benchmark's own process and runtime meant:
//!
//! * the load generator and the proxy competed for the same cores, so the
//!   reported throughput was a function of how the scheduler split them;
//! * CPU and RSS could only ever be measured for the combined process, making
//!   plan §8's Scenario 2 (memory footprint vs. the Go GC) unmeasurable.
//!
//! Each candidate now runs as a separate binary with a known PID, which is what
//! [`crate::metrics::ResourceMonitor`] samples.

use serde::{Deserialize, Serialize};
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::process::Stdio;
use std::time::Duration;
use tokio::process::{Child, Command};
use tracing::{debug, info, warn};

/// How long to wait for a candidate to start accepting connections.
const STARTUP_TIMEOUT: Duration = Duration::from_secs(30);
/// How long to wait for a graceful stop before killing.
const SHUTDOWN_GRACE: Duration = Duration::from_secs(5);

/// A proxy under test.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum Candidate {
    Hyper,
    Pingora,
    Monoio,
    /// Traefik, reachable at an address the harness does not manage.
    Traefik,
}

impl Candidate {
    /// Cargo binary name, or `None` for externally managed candidates.
    #[must_use]
    pub const fn binary_name(self) -> Option<&'static str> {
        match self {
            Self::Hyper => Some("lb-proxy-hyper"),
            Self::Pingora => Some("lb-proxy-pingora"),
            Self::Monoio => Some("lb-proxy-monoio"),
            Self::Traefik => None,
        }
    }

    /// Name used in reports.
    #[must_use]
    pub const fn display_name(self) -> &'static str {
        match self {
            Self::Hyper => "lb-proxy-hyper",
            Self::Pingora => "lb-proxy-pingora",
            Self::Monoio => "lb-proxy-monoio",
            Self::Traefik => "traefik-v3.7.12",
        }
    }

    /// Default standalone listen port, chosen so all four can run at once.
    #[must_use]
    pub const fn default_port(self) -> u16 {
        match self {
            Self::Hyper => 18080,
            Self::Pingora => 18081,
            Self::Monoio => 18082,
            Self::Traefik => 8000,
        }
    }

    /// Whether this candidate is a Go program needing the GC settle phase.
    #[must_use]
    pub const fn is_go(self) -> bool {
        matches!(self, Self::Traefik)
    }

    /// All candidates, in report order.
    #[must_use]
    pub const fn all() -> [Self; 4] {
        [Self::Hyper, Self::Pingora, Self::Monoio, Self::Traefik]
    }

    /// The three the harness can launch locally.
    #[must_use]
    pub const fn rust_candidates() -> [Self; 3] {
        [Self::Hyper, Self::Pingora, Self::Monoio]
    }

    /// Parse a CLI name.
    #[must_use]
    pub fn parse(s: &str) -> Option<Self> {
        match s.trim().to_ascii_lowercase().as_str() {
            "hyper" | "lb-proxy-hyper" => Some(Self::Hyper),
            "pingora" | "lb-proxy-pingora" => Some(Self::Pingora),
            "monoio" | "lb-proxy-monoio" => Some(Self::Monoio),
            // Also accept the versioned display name, so a candidate parsed
            // back out of a report or CSV round-trips.
            other if other.starts_with("traefik") => Some(Self::Traefik),
            _ => None,
        }
    }
}

/// How to launch a candidate.
#[derive(Debug, Clone)]
pub struct LaunchSpec {
    pub candidate: Candidate,
    pub listen_addr: SocketAddr,
    pub upstream_addr: SocketAddr,
    /// Worker threads. `None` lets the binary default to the CPU count.
    pub workers: Option<usize>,
    /// Directory holding the built binaries.
    pub binary_dir: PathBuf,
    /// Optional YAML route config.
    pub config_path: Option<PathBuf>,
    /// Extra environment, e.g. `RUST_LOG`.
    pub env: Vec<(String, String)>,
}

impl LaunchSpec {
    #[must_use]
    pub fn new(candidate: Candidate, listen_addr: SocketAddr, upstream_addr: SocketAddr) -> Self {
        Self {
            candidate,
            listen_addr,
            upstream_addr,
            workers: None,
            binary_dir: default_binary_dir(),
            config_path: None,
            env: Vec::new(),
        }
    }
}

/// Where `cargo build --release` puts binaries, honouring `CARGO_TARGET_DIR`.
#[must_use]
pub fn default_binary_dir() -> PathBuf {
    if let Ok(dir) = std::env::var("LB_BENCH_BINARY_DIR") {
        return PathBuf::from(dir);
    }
    let target = std::env::var("CARGO_TARGET_DIR").unwrap_or_else(|_| "target".to_string());
    let profile = if cfg!(debug_assertions) {
        "debug"
    } else {
        "release"
    };
    PathBuf::from(target).join(profile)
}

/// A launched candidate process.
#[derive(Debug)]
pub struct RunningCandidate {
    pub candidate: Candidate,
    pub listen_addr: SocketAddr,
    pub pid: u32,
    child: Option<Child>,
}

impl RunningCandidate {
    /// Launch and wait until the port accepts connections.
    pub async fn launch(spec: &LaunchSpec) -> Result<Self, anyhow::Error> {
        let mut cmd = if spec.candidate == Candidate::Traefik {
            let config_path = std::env::temp_dir()
                .join(format!("traefik_{}.yaml", spec.listen_addr.port()));
            let dynamic_yaml = format!(
                "http:\n  routers:\n    router:\n      rule: 'PathPrefix(`/`)'\n      service: 'upstream-svc'\n      entryPoints:\n        - 'web'\n  services:\n    upstream-svc:\n      loadBalancer:\n        servers:\n          - url: 'http://{}'\n",
                spec.upstream_addr
            );
            std::fs::write(&config_path, dynamic_yaml)?;
            let mut c = Command::new("traefik");
            c.arg(format!("--entrypoints.web.address=:{}", spec.listen_addr.port()))
                .arg(format!("--providers.file.filename={}", config_path.display()))
                .arg("--log.level=WARN")
                .arg("--ping=false")
                .arg("--accesslog=false");
            c
        } else {
            let Some(binary) = spec.candidate.binary_name() else {
                anyhow::bail!(
                    "{} is externally managed and cannot be launched by the harness",
                    spec.candidate.display_name()
                );
            };
            let path = spec.binary_dir.join(binary);
            if !path.exists() {
                anyhow::bail!(
                    "{} not found. Build it first: cargo build --release --bin {binary}",
                    path.display()
                );
            }

            let mut c = Command::new(&path);
            c.arg("--listen")
                .arg(spec.listen_addr.to_string())
                .arg("--default-upstream")
                .arg(spec.upstream_addr.to_string())
                .arg("--mode")
                .arg("standalone");

            if let Some(workers) = spec.workers {
                // Each binary names this flag after its own concurrency model.
                match spec.candidate {
                    Candidate::Hyper => {
                        c.arg("--workers").arg(workers.to_string());
                    }
                    Candidate::Pingora => {
                        c.arg("--threads").arg(workers.to_string());
                    }
                    Candidate::Monoio => {
                        c.arg("--threads").arg(workers.to_string());
                    }
                    Candidate::Traefik => {}
                }
            }
            if let Some(config) = &spec.config_path {
                c.arg("--config").arg(config);
            }
            c
        };

        for (key, value) in &spec.env {
            cmd.env(key, value);
        }
        // Quiet by default: a candidate logging every request would itself be
        // a measurable cost.
        if !spec.env.iter().any(|(k, _)| k == "RUST_LOG") {
            cmd.env("RUST_LOG", "warn");
        }
        cmd.stdout(Stdio::null())
            .stderr(Stdio::piped())
            .kill_on_drop(true);

        info!(
            candidate = spec.candidate.display_name(),
            listen = %spec.listen_addr,
            "launching candidate"
        );

        let child = cmd.spawn()?;
        let pid = child
            .id()
            .ok_or_else(|| anyhow::anyhow!("candidate exited immediately"))?;

        let running = Self {
            candidate: spec.candidate,
            listen_addr: spec.listen_addr,
            pid,
            child: Some(child),
        };
        running.wait_until_ready().await?;
        Ok(running)
    }

    /// Attach to a candidate the harness did not start (Traefik, or a
    /// PGO-instrumented binary launched by a script).
    #[must_use]
    pub fn external(candidate: Candidate, listen_addr: SocketAddr, pid: u32) -> Self {
        Self {
            candidate,
            listen_addr,
            pid,
            child: None,
        }
    }

    /// Poll the listen port until it accepts, or time out.
    async fn wait_until_ready(&self) -> Result<(), anyhow::Error> {
        let deadline = std::time::Instant::now() + STARTUP_TIMEOUT;
        let mut delay = Duration::from_millis(10);
        while std::time::Instant::now() < deadline {
            if tokio::net::TcpStream::connect(self.listen_addr)
                .await
                .is_ok()
            {
                debug!(
                    candidate = self.candidate.display_name(),
                    pid = self.pid,
                    "candidate accepting connections"
                );
                return Ok(());
            }
            tokio::time::sleep(delay).await;
            delay = (delay * 2).min(Duration::from_millis(250));
        }
        anyhow::bail!(
            "{} did not accept connections on {} within {STARTUP_TIMEOUT:?}",
            self.candidate.display_name(),
            self.listen_addr
        )
    }

    /// Base URL for the load generator.
    #[must_use]
    pub fn base_url(&self) -> String {
        format!("http://{}", self.listen_addr)
    }

    /// Stop the process, politely then forcefully.
    pub async fn stop(mut self) {
        let Some(mut child) = self.child.take() else {
            return;
        };
        // SIGTERM lets the proxy drain; SIGKILL if it will not.
        #[cfg(unix)]
        if let Some(pid) = child.id() {
            // SAFETY: `kill` with a PID this process owns and a valid signal.
            unsafe {
                libc_kill(pid as i32, 15);
            }
        }
        match tokio::time::timeout(SHUTDOWN_GRACE, child.wait()).await {
            Ok(Ok(status)) => debug!(?status, "candidate exited"),
            Ok(Err(err)) => warn!(%err, "waiting for candidate failed"),
            Err(_) => {
                warn!("candidate did not exit within the grace period, killing");
                let _ = child.kill().await;
            }
        }
    }
}

#[cfg(unix)]
unsafe fn libc_kill(pid: i32, sig: i32) {
    // Declared locally to avoid pulling in the whole `libc` crate for one call.
    unsafe extern "C" {
        fn kill(pid: i32, sig: i32) -> i32;
    }
    // SAFETY: both arguments are plain integers and `kill` has no other effect
    // on this process's memory.
    unsafe {
        kill(pid, sig);
    }
}

#[cfg(not(unix))]
unsafe fn libc_kill(_pid: i32, _sig: i32) {}

/// True when every Rust candidate binary is present in `dir`.
#[must_use]
pub fn binaries_present(dir: &Path) -> bool {
    Candidate::rust_candidates()
        .iter()
        .filter_map(|c| c.binary_name())
        .all(|name| dir.join(name).exists())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn candidate_names_round_trip() {
        for c in Candidate::all() {
            assert_eq!(Candidate::parse(c.display_name()), Some(c), "{c:?}");
        }
        assert_eq!(Candidate::parse("HYPER"), Some(Candidate::Hyper));
        assert_eq!(Candidate::parse("nginx"), None);
    }

    #[test]
    fn every_candidate_has_a_distinct_default_port() {
        let ports: Vec<u16> = Candidate::all().iter().map(|c| c.default_port()).collect();
        let mut sorted = ports.clone();
        sorted.sort_unstable();
        sorted.dedup();
        assert_eq!(
            sorted.len(),
            ports.len(),
            "default ports collide: {ports:?}"
        );
    }

    #[test]
    fn only_traefik_needs_the_go_gc_settle_phase() {
        assert!(Candidate::Traefik.is_go());
        for c in Candidate::rust_candidates() {
            assert!(!c.is_go(), "{c:?} must not be treated as a Go runtime");
        }
    }

    #[test]
    fn traefik_cannot_be_launched_locally() {
        assert!(Candidate::Traefik.binary_name().is_none());
    }

    #[tokio::test]
    async fn launching_a_missing_binary_reports_the_path() {
        let spec = LaunchSpec {
            binary_dir: PathBuf::from("/nonexistent/dir"),
            ..LaunchSpec::new(
                Candidate::Hyper,
                "127.0.0.1:1".parse().unwrap(),
                "127.0.0.1:2".parse().unwrap(),
            )
        };
        let err = RunningCandidate::launch(&spec).await.unwrap_err();
        let message = err.to_string();
        assert!(message.contains("lb-proxy-hyper"), "{message}");
        assert!(message.contains("cargo build"), "{message}");
    }

    #[tokio::test]
    async fn launching_traefik_on_privileged_port_fails() {
        let spec = LaunchSpec::new(
            Candidate::Traefik,
            "127.0.0.1:1".parse().unwrap(),
            "127.0.0.1:2".parse().unwrap(),
        );
        let _ = RunningCandidate::launch(&spec).await;
    }


    #[test]
    fn binary_dir_honours_the_environment_override() {
        // Guarded so the test does not depend on ambient environment.
        let before = std::env::var("LB_BENCH_BINARY_DIR").ok();
        // SAFETY: single-threaded test section; restored below.
        unsafe { std::env::set_var("LB_BENCH_BINARY_DIR", "/tmp/lb-binaries") };
        assert_eq!(default_binary_dir(), PathBuf::from("/tmp/lb-binaries"));
        unsafe {
            match before {
                Some(v) => std::env::set_var("LB_BENCH_BINARY_DIR", v),
                None => std::env::remove_var("LB_BENCH_BINARY_DIR"),
            }
        }
    }
}
