//! Benchmark execution engine: orchestrates start -> warm-up -> measure -> stop.

use crate::harness::candidate::{Candidate, LaunchSpec, RunningCandidate, default_binary_dir};
use crate::harness::scenario::ScenarioConfig;
use crate::harness::warmup::WarmupProtocol;
use crate::load::{HttpVersion, LoadConfig, LoadGenerator, PayloadSize};
use crate::metrics::{BenchmarkResult, LatencyRecorder, MonitorTarget, ResourceMonitor};
use crate::mock::{MockUpstream, MockUpstreamConfig};
use http::Method;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::watch;
use tracing::{info, warn};

/// Sampling interval for the CPU/RSS monitor.
const SAMPLE_INTERVAL: Duration = Duration::from_millis(100);
/// Idle gap between measurement windows, so one window's queue drain does not
/// land inside the next window's histogram.
const INTER_RUN_SETTLE: Duration = Duration::from_secs(2);

/// Where the harness should send traffic and what it should measure.
#[derive(Debug, Clone)]
pub enum Deployment {
    /// The harness launches the candidate locally as a child process.
    Local {
        binary_dir: PathBuf,
        workers: Option<usize>,
    },
    /// The candidate is already running (Kubernetes, or a PGO script).
    External {
        addr: SocketAddr,
        /// PID to sample, when the process is visible to this machine.
        pid: Option<u32>,
    },
}

impl Default for Deployment {
    fn default() -> Self {
        Self::Local {
            binary_dir: default_binary_dir(),
            workers: None,
        }
    }
}

/// Runner configuration.
#[derive(Debug, Clone, Default)]
pub struct RunnerConfig {
    pub warmup: WarmupProtocol,
    pub deployment: Deployment,
    /// Mock upstream address. `None` starts one on an ephemeral port.
    pub upstream_addr: Option<SocketAddr>,
    /// Upstream response delay profile, in milliseconds (plan §1: 0/1/5 ms).
    pub upstream_delay_ms: u64,
    /// Route config handed to locally launched candidates.
    pub config_path: Option<PathBuf>,
}

/// Orchestrates candidate lifecycle and measurement.
pub struct BenchmarkRunner {
    config: RunnerConfig,
}

impl Default for BenchmarkRunner {
    fn default() -> Self {
        Self::new()
    }
}

impl BenchmarkRunner {
    #[must_use]
    pub fn new() -> Self {
        Self {
            config: RunnerConfig::default(),
        }
    }

    #[must_use]
    pub fn with_config(config: RunnerConfig) -> Self {
        Self { config }
    }

    /// Run `scenario` against `candidate`, returning one result per
    /// (concurrency x payload x HTTP version) combination.
    pub async fn run_candidate(
        &self,
        candidate: Candidate,
        scenario: &ScenarioConfig,
    ) -> Result<Vec<BenchmarkResult>, anyhow::Error> {
        // 1. Mock upstream. Started only for locally deployed candidates; a
        //    Kubernetes run has its own backend pods.
        let (upstream_addr, mock_shutdown) = match &self.config.deployment {
            Deployment::Local { .. } => {
                let (addr, shutdown) = self.start_mock_upstream().await?;
                (addr, Some(shutdown))
            }
            Deployment::External { .. } => (
                self.config
                    .upstream_addr
                    .unwrap_or_else(|| "127.0.0.1:9090".parse().expect("literal")),
                None,
            ),
        };

        // 2. Bring up the candidate.
        let running = match &self.config.deployment {
            Deployment::Local {
                binary_dir,
                workers,
            } => {
                let listen: SocketAddr =
                    format!("127.0.0.1:{}", candidate.default_port()).parse()?;
                let spec = LaunchSpec {
                    workers: *workers,
                    binary_dir: binary_dir.clone(),
                    config_path: self.config.config_path.clone(),
                    ..LaunchSpec::new(candidate, listen, upstream_addr)
                };
                RunningCandidate::launch(&spec).await?
            }
            Deployment::External { addr, pid } => {
                // In k8s mode each candidate answers on its own host port
                // (the kind extraPortMappings), so a single --target address
                // would silently send every candidate's load to one proxy.
                let addr = if addr.port() == 0 {
                    #[cfg(feature = "k8s")]
                    {
                        format!("127.0.0.1:{}", crate::k8s::Deployer::host_port(candidate))
                            .parse()?
                    }
                    #[cfg(not(feature = "k8s"))]
                    {
                        *addr
                    }
                } else {
                    *addr
                };
                RunningCandidate::external(candidate, addr, pid.unwrap_or(0))
            }
        };

        // The proxy runs in its own process, so CPU and RSS are attributable.
        let monitor_target = if running.pid > 0 {
            MonitorTarget::Process(running.pid)
        } else {
            // A candidate in a container we cannot see into: fall back to
            // machine-wide sampling and say so in the report.
            warn!(
                candidate = candidate.display_name(),
                "no PID available; sampling machine-wide resource usage"
            );
            MonitorTarget::System
        };

        let results = self
            .measure_all(candidate, scenario, &running, monitor_target)
            .await;

        // 3. Tear down in reverse order.
        running.stop().await;
        if let Some(shutdown) = mock_shutdown {
            let _ = shutdown.send(true);
        }

        results
    }

    /// Run every combination in the scenario against an already-running candidate.
    async fn measure_all(
        &self,
        candidate: Candidate,
        scenario: &ScenarioConfig,
        running: &RunningCandidate,
        monitor_target: MonitorTarget,
    ) -> Result<Vec<BenchmarkResult>, anyhow::Error> {
        let base_url = running.base_url();
        let mut results = Vec::with_capacity(scenario.measurement_count());

        for &version in &scenario.http_versions {
            for &payload in &scenario.payloads {
                for &concurrency in &scenario.concurrencies {
                    info!(
                        candidate = candidate.display_name(),
                        concurrency,
                        payload = payload.label(),
                        version = version.label(),
                        "measurement window starting"
                    );

                    self.config
                        .warmup
                        .execute(candidate, &format!("{base_url}/bench"), concurrency)
                        .await?;

                    let result = self
                        .measure_one(
                            candidate,
                            scenario,
                            &base_url,
                            concurrency,
                            payload,
                            version,
                            monitor_target,
                        )
                        .await?;

                    info!(
                        candidate = candidate.display_name(),
                        rps = format_args!("{:.0}", result.rps),
                        p99_us = result.latency_p99_us,
                        rss_mb = format_args!("{:.1}", result.rss_memory_mb),
                        "measurement window complete"
                    );
                    results.push(result);

                    tokio::time::sleep(INTER_RUN_SETTLE).await;
                }
            }
        }
        Ok(results)
    }

    /// One measurement window.
    #[allow(clippy::too_many_arguments)]
    async fn measure_one(
        &self,
        candidate: Candidate,
        scenario: &ScenarioConfig,
        base_url: &str,
        concurrency: usize,
        payload: PayloadSize,
        version: HttpVersion,
        monitor_target: MonitorTarget,
    ) -> Result<BenchmarkResult, anyhow::Error> {
        let monitor = ResourceMonitor::start(monitor_target, SAMPLE_INTERVAL);
        let recorder = Arc::new(LatencyRecorder::new());

        // Payload semantics differ by scenario: the throughput sweep asks the
        // upstream for a body of the given size (GET with a sized path), while
        // an upload sweep would POST it. The mock upstream honours the size hint
        // in the path, so a single GET path drives both directions.
        let url = format!("{base_url}/bench?size={}", payload.bytes());
        let config = LoadConfig {
            method: Method::GET,
            payload: None,
            http_version: version,
            target_rps: scenario.target_rps,
            host_header: "localhost".to_string(),
            ..LoadConfig::new(url, concurrency, scenario.duration)
        };

        let elapsed = LoadGenerator::new(config, recorder.clone()).run().await?;
        let resources = monitor.stop().await;

        Ok(recorder.summarize(
            candidate.display_name(),
            &scenario.name,
            concurrency,
            payload.bytes(),
            version.label(),
            elapsed,
            resources.cpu_mean_pct,
            resources.rss_peak_mb,
        ))
    }

    /// Start the mock upstream on an ephemeral port.
    async fn start_mock_upstream(
        &self,
    ) -> Result<(SocketAddr, watch::Sender<bool>), anyhow::Error> {
        let addr = match self.config.upstream_addr {
            Some(addr) => addr,
            None => {
                // Bind then release, so the mock can claim the port with
                // SO_REUSEPORT without racing another test.
                let probe = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
                let addr = probe.local_addr()?;
                drop(probe);
                addr
            }
        };

        let (tx, rx) = watch::channel(false);
        let mock = MockUpstream::new(MockUpstreamConfig {
            listen_addr: addr,
            delay_ms: self.config.upstream_delay_ms,
            ..Default::default()
        });
        tokio::spawn(async move {
            if let Err(err) = mock.run(rx).await {
                warn!(%err, "mock upstream stopped");
            }
        });

        // Wait for it to accept before returning.
        for _ in 0..200 {
            if tokio::net::TcpStream::connect(addr).await.is_ok() {
                return Ok((addr, tx));
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        anyhow::bail!("mock upstream did not start on {addr}")
    }

    /// Run a scenario against every candidate in turn.
    ///
    /// Candidates are run strictly one at a time: two proxies sharing the
    /// machine would each be measured under the other's load.
    pub async fn run_all(
        &self,
        candidates: &[Candidate],
        scenario: &ScenarioConfig,
    ) -> Vec<BenchmarkResult> {
        let mut all = Vec::new();
        for &candidate in candidates {
            match self.run_candidate(candidate, scenario).await {
                Ok(results) => all.extend(results),
                Err(err) => warn!(
                    candidate = candidate.display_name(),
                    %err,
                    "candidate run failed, continuing with the rest"
                ),
            }
        }
        all
    }

    /// Compare a standard build against a PGO build (plan §8, Scenario 5).
    ///
    /// Runs the same scenario twice from two binary directories and pairs the
    /// results, so the delta is attributable to the build and nothing else.
    pub async fn run_pgo_delta(
        &self,
        candidate: Candidate,
        standard_dir: PathBuf,
        pgo_dir: PathBuf,
    ) -> Result<Vec<BenchmarkResult>, anyhow::Error> {
        let scenario = ScenarioConfig::pgo_delta();
        let mut results = Vec::new();

        for (label, dir) in [("standard", standard_dir), ("pgo", pgo_dir)] {
            let workers = match &self.config.deployment {
                Deployment::Local { workers, .. } => *workers,
                Deployment::External { .. } => None,
            };
            let runner = Self::with_config(RunnerConfig {
                deployment: Deployment::Local {
                    binary_dir: dir,
                    workers,
                },
                ..self.config.clone()
            });
            let mut batch = runner.run_candidate(candidate, &scenario).await?;
            for result in &mut batch {
                result.scenario_name = format!("{} [{label}]", scenario.name);
                result.candidate_name = format!("{} [{label}]", candidate.display_name());
            }
            results.extend(batch);
        }
        Ok(results)
    }

    /// The scenario kind this runner is configured for, if any.
    #[must_use]
    pub fn deployment(&self) -> &Deployment {
        &self.config.deployment
    }
}

/// Percentage improvement of `pgo` over `standard`, for the PGO delta report.
#[must_use]
pub fn pgo_improvement_pct(standard: &BenchmarkResult, pgo: &BenchmarkResult) -> f64 {
    if standard.rps <= 0.0 {
        return 0.0;
    }
    (pgo.rps - standard.rps) / standard.rps * 100.0
}

/// Pair standard and PGO results from [`BenchmarkRunner::run_pgo_delta`].
#[must_use]
pub fn pair_pgo_results(
    results: &[BenchmarkResult],
) -> Vec<(&BenchmarkResult, &BenchmarkResult, f64)> {
    let standard: Vec<_> = results
        .iter()
        .filter(|r| r.candidate_name.ends_with("[standard]"))
        .collect();
    let pgo: Vec<_> = results
        .iter()
        .filter(|r| r.candidate_name.ends_with("[pgo]"))
        .collect();

    standard
        .into_iter()
        .filter_map(|s| {
            pgo.iter()
                .find(|p| {
                    p.concurrency == s.concurrency
                        && p.payload_bytes == s.payload_bytes
                        && p.http_version == s.http_version
                })
                .map(|p| (s, *p, pgo_improvement_pct(s, p)))
        })
        .collect()
}

/// Deliberately re-exported so `ScenarioKind` stays reachable from the runner.
pub use crate::harness::scenario::ScenarioKind as RunnerScenarioKind;

#[cfg(test)]
mod tests {
    use super::*;

    fn result(name: &str, concurrency: usize, rps: f64) -> BenchmarkResult {
        BenchmarkResult {
            candidate_name: name.into(),
            scenario_name: "s".into(),
            concurrency,
            payload_bytes: 1024,
            http_version: "HTTP/1.1".into(),
            total_requests: 0,
            successful_requests: 0,
            failed_requests: 0,
            error_responses: 0,
            bytes_received: 0,
            duration_secs: 1.0,
            rps,
            throughput_mib_s: 0.0,
            latency_p50_us: 0,
            latency_p90_us: 0,
            latency_p95_us: 0,
            latency_p99_us: 0,
            latency_p999_us: 0,
            latency_p9999_us: 0,
            latency_max_us: 0,
            latency_mean_us: 0.0,
            latency_stdev_us: 0.0,
            co_corrected_p99_us: 0,
            co_corrected_p999_us: 0,
            cpu_usage_pct: 0.0,
            rss_memory_mb: 0.0,
        }
    }

    #[test]
    fn pgo_improvement_is_a_percentage_of_the_baseline() {
        let standard = result("x [standard]", 100, 1_000.0);
        let pgo = result("x [pgo]", 100, 1_150.0);
        assert!((pgo_improvement_pct(&standard, &pgo) - 15.0).abs() < 1e-9);
    }

    #[test]
    fn pgo_improvement_handles_a_zero_baseline() {
        let standard = result("x [standard]", 100, 0.0);
        let pgo = result("x [pgo]", 100, 500.0);
        assert_eq!(pgo_improvement_pct(&standard, &pgo), 0.0);
    }

    #[test]
    fn pgo_results_pair_by_workload_not_by_order() {
        let results = vec![
            result("h [standard]", 100, 1_000.0),
            result("h [standard]", 500, 2_000.0),
            // Deliberately reversed relative to the standard runs.
            result("h [pgo]", 500, 2_400.0),
            result("h [pgo]", 100, 1_100.0),
        ];
        let pairs = pair_pgo_results(&results);
        assert_eq!(pairs.len(), 2);
        for (s, p, delta) in pairs {
            assert_eq!(s.concurrency, p.concurrency, "paired across concurrencies");
            assert!(delta > 0.0);
        }
    }

    #[test]
    fn default_deployment_is_local_with_cpu_default_workers() {
        match BenchmarkRunner::new().deployment() {
            Deployment::Local { workers, .. } => assert!(workers.is_none()),
            Deployment::External { .. } => panic!("default must be Local"),
        }
    }

    #[tokio::test]
    async fn mock_upstream_starts_on_an_ephemeral_port() {
        let runner = BenchmarkRunner::new();
        let (addr, shutdown) = runner.start_mock_upstream().await.expect("mock starts");
        assert!(addr.port() > 0);
        assert!(tokio::net::TcpStream::connect(addr).await.is_ok());
        let _ = shutdown.send(true);
    }
}
