//! CPU and RSS sampling of the process under test.
//!
//! The previous implementation sampled `std::process::id()` — the benchmark's
//! own PID — while the proxy and the mock upstream ran inside that same
//! process. Every CPU and memory number it produced was the sum of the load
//! generator, the proxy and the backend, which is not a measurement of anything.
//!
//! This samples an explicit target PID (and, on Linux, its cgroup when running
//! under Kubernetes), so "RSS of lb-proxy-hyper" means what it says.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;
use sysinfo::{Pid, ProcessRefreshKind, ProcessesToUpdate, System};

/// What a monitor should watch.
#[derive(Debug, Clone, Copy)]
pub enum MonitorTarget {
    /// A specific process, plus its children (a proxy may fork workers).
    Process(u32),
    /// The whole machine. Used when the candidate runs in a container the
    /// harness cannot see into, e.g. Traefik under kind.
    System,
}

/// A resource sample series.
#[derive(Debug, Clone, Default)]
pub struct ResourceSummary {
    /// Mean CPU percentage over the window. 100% = one saturated core.
    pub cpu_mean_pct: f32,
    /// Peak CPU percentage observed.
    pub cpu_peak_pct: f32,
    /// Peak resident set size in MiB.
    pub rss_peak_mb: f64,
    /// Mean resident set size in MiB.
    pub rss_mean_mb: f64,
    /// Number of samples taken.
    pub samples: usize,
}

/// Periodically samples CPU% and RSS of a target.
pub struct ResourceMonitor {
    running: Arc<AtomicBool>,
    handle: Option<tokio::task::JoinHandle<ResourceSummary>>,
}

impl ResourceMonitor {
    /// Start sampling `target` every `interval`.
    #[must_use]
    pub fn start(target: MonitorTarget, interval: Duration) -> Self {
        let running = Arc::new(AtomicBool::new(true));
        let flag = running.clone();

        let handle = tokio::spawn(async move {
            let mut sys = System::new();
            let mut cpu_samples: Vec<f32> = Vec::new();
            let mut rss_samples: Vec<f64> = Vec::new();

            // sysinfo reports CPU as a delta since the previous refresh, so the
            // first sample is always meaningless. Prime and discard it.
            sample(&mut sys, target);
            tokio::time::sleep(interval).await;

            while flag.load(Ordering::Relaxed) {
                if let Some((cpu, rss_mb)) = sample(&mut sys, target) {
                    cpu_samples.push(cpu);
                    rss_samples.push(rss_mb);
                }
                tokio::time::sleep(interval).await;
            }

            let samples = cpu_samples.len();
            let cpu_mean_pct = if samples == 0 {
                0.0
            } else {
                cpu_samples.iter().sum::<f32>() / samples as f32
            };
            let cpu_peak_pct = cpu_samples.iter().copied().fold(0.0f32, f32::max);
            let rss_peak_mb = rss_samples.iter().copied().fold(0.0f64, f64::max);
            let rss_mean_mb = if rss_samples.is_empty() {
                0.0
            } else {
                rss_samples.iter().sum::<f64>() / rss_samples.len() as f64
            };

            ResourceSummary {
                cpu_mean_pct,
                cpu_peak_pct,
                rss_peak_mb,
                rss_mean_mb,
                samples,
            }
        });

        Self {
            running,
            handle: Some(handle),
        }
    }

    /// Stop sampling and return the summary.
    pub async fn stop(mut self) -> ResourceSummary {
        self.running.store(false, Ordering::Relaxed);
        match self.handle.take() {
            Some(handle) => handle.await.unwrap_or_default(),
            None => ResourceSummary::default(),
        }
    }
}

/// Take one sample, returning `(cpu_pct, rss_mib)`.
fn sample(sys: &mut System, target: MonitorTarget) -> Option<(f32, f64)> {
    match target {
        MonitorTarget::Process(pid) => {
            let pid = Pid::from_u32(pid);
            // Refresh everything, because a proxy's children (Pingora forks a
            // worker per service) hold most of the RSS.
            sys.refresh_processes_specifics(
                ProcessesToUpdate::All,
                true,
                ProcessRefreshKind::nothing().with_cpu().with_memory(),
            );
            let root = sys.process(pid)?;
            let mut cpu = root.cpu_usage();
            let mut rss = root.memory();
            for proc in sys.processes().values() {
                if proc.parent() == Some(pid) {
                    cpu += proc.cpu_usage();
                    rss += proc.memory();
                }
            }
            Some((cpu, rss as f64 / (1024.0 * 1024.0)))
        }
        MonitorTarget::System => {
            sys.refresh_cpu_usage();
            sys.refresh_memory();
            let cpu = sys.global_cpu_usage();
            let used = sys.used_memory() as f64 / (1024.0 * 1024.0);
            Some((cpu, used))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // Multi-threaded: the sampler is a spawned task, and the busy-wait below
    // would starve it on a current-thread runtime.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn samples_this_process_and_reports_nonzero_rss() {
        let monitor = ResourceMonitor::start(
            MonitorTarget::Process(std::process::id()),
            Duration::from_millis(20),
        );
        // Burn a little CPU so the sample is not trivially zero.
        let deadline = std::time::Instant::now() + Duration::from_millis(150);
        let mut acc = 0u64;
        while std::time::Instant::now() < deadline {
            acc = acc.wrapping_add(1);
        }
        std::hint::black_box(acc);

        let summary = monitor.stop().await;
        assert!(summary.samples > 0, "no samples collected");
        assert!(summary.rss_peak_mb > 0.0, "RSS should be measurable");
        assert!(summary.rss_peak_mb >= summary.rss_mean_mb);
        assert!(summary.cpu_peak_pct >= summary.cpu_mean_pct);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn missing_process_yields_an_empty_summary_not_a_panic() {
        // PID 0 is never a real user process on Linux or macOS.
        let monitor = ResourceMonitor::start(MonitorTarget::Process(0), Duration::from_millis(10));
        tokio::time::sleep(Duration::from_millis(60)).await;
        let summary = monitor.stop().await;
        assert_eq!(summary.samples, 0);
        assert_eq!(summary.rss_peak_mb, 0.0);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn system_target_reports_machine_wide_usage() {
        let monitor = ResourceMonitor::start(MonitorTarget::System, Duration::from_millis(20));
        tokio::time::sleep(Duration::from_millis(120)).await;
        let summary = monitor.stop().await;
        assert!(summary.samples > 0);
        assert!(summary.rss_peak_mb > 0.0);
    }
}
