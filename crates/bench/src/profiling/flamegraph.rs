//! Flamegraph capture (plan §5).
//!
//! Rust candidates are profiled with `cargo flamegraph` (which drives `perf` on
//! Linux and `dtrace` on macOS); Traefik is profiled through Go's own `pprof`
//! endpoint. Both land in `results/<run>/flamegraphs/` as SVG.

use crate::harness::Candidate;
use std::path::{Path, PathBuf};
use std::time::Duration;
use tokio::process::Command;
use tracing::{info, warn};

/// A binary must retain symbols to be profilable.
///
/// The release profile in plan §2 sets `strip = "symbols"` and `debug = false`,
/// which makes every frame in a flamegraph an unresolved address. The
/// `profiling` profile in the workspace manifest exists for exactly this: same
/// codegen, symbols kept.
pub const PROFILING_PROFILE: &str = "profiling";

/// Flamegraph capture settings.
#[derive(Debug, Clone)]
pub struct FlamegraphConfig {
    /// Directory to write SVGs into.
    pub output_dir: PathBuf,
    /// How long to sample.
    pub duration: Duration,
    /// Sampling frequency in Hz.
    pub frequency: u32,
}

impl Default for FlamegraphConfig {
    fn default() -> Self {
        Self {
            output_dir: PathBuf::from("results/flamegraphs"),
            duration: Duration::from_secs(30),
            frequency: 997, // prime, to avoid aliasing with periodic workloads
        }
    }
}

/// Captures CPU profiles.
pub struct FlamegraphCapture {
    config: FlamegraphConfig,
}

impl FlamegraphCapture {
    #[must_use]
    pub fn new(config: FlamegraphConfig) -> Self {
        Self { config }
    }

    /// Whether the platform profiler is usable.
    pub async fn profiler_available() -> bool {
        #[cfg(target_os = "linux")]
        let tool = "perf";
        #[cfg(target_os = "macos")]
        let tool = "dtrace";
        #[cfg(not(any(target_os = "linux", target_os = "macos")))]
        let tool = "perf";

        Command::new("which")
            .arg(tool)
            .output()
            .await
            .is_ok_and(|o| o.status.success())
    }

    /// Output path for one candidate at one concurrency.
    #[must_use]
    pub fn output_path(&self, candidate: Candidate, concurrency: usize) -> PathBuf {
        self.config
            .output_dir
            .join(format!("{}-c{concurrency}.svg", candidate.display_name()))
    }

    /// Attach to a running process and record a flamegraph.
    ///
    /// Returns the SVG path, or `None` when profiling is unavailable — a
    /// missing profiler must not fail the benchmark run.
    pub async fn capture_process(
        &self,
        candidate: Candidate,
        pid: u32,
        concurrency: usize,
    ) -> Option<PathBuf> {
        if !Self::profiler_available().await {
            warn!("no system profiler found; skipping flamegraph capture");
            return None;
        }
        if std::fs::create_dir_all(&self.config.output_dir).is_err() {
            warn!(dir = %self.config.output_dir.display(), "cannot create flamegraph directory");
            return None;
        }
        let output = self.output_path(candidate, concurrency);

        info!(
            candidate = candidate.display_name(),
            pid,
            secs = self.config.duration.as_secs(),
            "capturing flamegraph"
        );
        let status = Command::new("flamegraph")
            .arg("--pid")
            .arg(pid.to_string())
            .arg("--freq")
            .arg(self.config.frequency.to_string())
            .arg("--output")
            .arg(&output)
            .arg("--")
            .arg("sleep")
            .arg(self.config.duration.as_secs().to_string())
            .status()
            .await;

        match status {
            Ok(s) if s.success() && output.exists() => Some(output),
            Ok(s) => {
                warn!(?s, "flamegraph capture failed");
                None
            }
            Err(err) => {
                warn!(%err, "could not run `flamegraph`; install cargo-flamegraph");
                None
            }
        }
    }

    /// Capture Traefik's Go profile through its pprof endpoint.
    ///
    /// The Traefik manifest enables `--api.debug=true`, which exposes
    /// `/debug/pprof/profile`. The captured profile is converted to SVG with
    /// `go tool pprof` when the Go toolchain is present.
    pub async fn capture_traefik_pprof(
        &self,
        pprof_url: &str,
        concurrency: usize,
    ) -> Option<PathBuf> {
        if std::fs::create_dir_all(&self.config.output_dir).is_err() {
            return None;
        }
        let output = self.output_path(Candidate::Traefik, concurrency);
        let seconds = self.config.duration.as_secs();

        info!(url = pprof_url, seconds, "capturing Traefik pprof profile");
        let status = Command::new("go")
            .arg("tool")
            .arg("pprof")
            .arg("-svg")
            .arg("-output")
            .arg(&output)
            .arg(format!("{pprof_url}/debug/pprof/profile?seconds={seconds}"))
            .status()
            .await;

        match status {
            Ok(s) if s.success() && output.exists() => Some(output),
            _ => {
                warn!("go pprof capture failed; is the Go toolchain installed?");
                None
            }
        }
    }

    /// Every SVG produced so far, for embedding in the report.
    #[must_use]
    pub fn collected(&self) -> Vec<PathBuf> {
        let Ok(entries) = std::fs::read_dir(&self.config.output_dir) else {
            return Vec::new();
        };
        let mut out: Vec<PathBuf> = entries
            .filter_map(Result::ok)
            .map(|e| e.path())
            .filter(|p| p.extension().is_some_and(|e| e == "svg"))
            .collect();
        out.sort();
        out
    }

    /// Whether a binary retains the symbols a flamegraph needs.
    ///
    /// A `strip = "symbols"` release binary produces hexadecimal frames only,
    /// so this is worth checking before spending 30 s sampling one.
    pub async fn has_symbols(binary: &Path) -> bool {
        let Ok(output) = Command::new("nm").arg("-a").arg(binary).output().await else {
            // Without `nm` we cannot tell; assume yes rather than skip.
            return true;
        };
        output.status.success() && output.stdout.len() > 1024
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn output_paths_are_unique_per_candidate_and_concurrency() {
        let capture = FlamegraphCapture::new(FlamegraphConfig::default());
        let mut paths: Vec<PathBuf> = Vec::new();
        for c in Candidate::all() {
            for concurrency in [100usize, 1_000] {
                paths.push(capture.output_path(c, concurrency));
            }
        }
        let mut sorted = paths.clone();
        sorted.sort();
        sorted.dedup();
        assert_eq!(sorted.len(), paths.len(), "flamegraph paths collide");
    }

    #[test]
    fn output_path_matches_the_layout_in_the_plan() {
        let capture = FlamegraphCapture::new(FlamegraphConfig::default());
        let path = capture.output_path(Candidate::Hyper, 1_000);
        assert!(
            path.ends_with("lb-proxy-hyper-c1000.svg"),
            "{}",
            path.display()
        );
    }

    #[test]
    fn sampling_frequency_is_prime_to_avoid_aliasing() {
        // A round frequency can lock onto a periodic workload and over-sample
        // one phase of it.
        let f = FlamegraphConfig::default().frequency;
        assert!(
            (2..f).filter(|d| f % d == 0).count() == 0,
            "{f} is not prime"
        );
    }

    #[test]
    fn collected_is_empty_when_the_directory_is_absent() {
        let capture = FlamegraphCapture::new(FlamegraphConfig {
            output_dir: PathBuf::from("/nonexistent/flamegraphs"),
            ..Default::default()
        });
        assert!(capture.collected().is_empty());
    }
}
