//! Async HTTP load generator.
//!
//! Design points that matter for the validity of the numbers:
//!
//! * **Per-worker recorders.** No shared mutex on the request path; each worker
//!   owns an `hdrhistogram` recorder that is merged only at the end.
//! * **Cycle-counter timing.** `Instant::now()` costs a vDSO call per sample;
//!   at these rates that is measurable inside the measurement. The inline-asm
//!   counter in [`lb_core::cycles`] costs a few cycles instead.
//! * **Open-loop pacing option.** With `target_rps` set, workers pace to a
//!   fixed interval and feed that interval to `record_correct`, which is what
//!   makes the coordinated-omission correction meaningful.
//! * **A separate runtime.** The generator never shares a runtime with a proxy
//!   under test; [`crate::harness`] enforces that by running candidates as
//!   separate processes.

use crate::metrics::LatencyRecorder;
use bytes::Bytes;
use http::{Method, Request, Uri};
use http_body_util::combinators::BoxBody;
use http_body_util::{BodyExt, Empty, Full};
use hyper_util::client::legacy::Client;
use hyper_util::client::legacy::connect::HttpConnector;
use hyper_util::rt::{TokioExecutor, TokioTimer};
use lb_core::cycles::{self, Calibration};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};

pub type BoxedBody = BoxBody<Bytes, hyper::Error>;

/// Which HTTP version the generator should speak to the proxy.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HttpVersion {
    /// HTTP/1.1 with keep-alive.
    Http11,
    /// HTTP/2 over cleartext, prior knowledge (no upgrade dance).
    Http2,
}

impl HttpVersion {
    #[must_use]
    pub const fn label(self) -> &'static str {
        match self {
            Self::Http11 => "HTTP/1.1",
            Self::Http2 => "HTTP/2",
        }
    }

    /// Parse a CLI value such as `h1`, `http1`, `h2`.
    #[must_use]
    pub fn parse(s: &str) -> Option<Self> {
        match s.trim().to_ascii_lowercase().as_str() {
            "h1" | "http1" | "http1.1" | "1.1" => Some(Self::Http11),
            "h2" | "http2" | "2" => Some(Self::Http2),
            _ => None,
        }
    }
}

/// Load generator configuration.
#[derive(Debug, Clone)]
pub struct LoadConfig {
    pub target_url: String,
    pub concurrency: usize,
    pub duration: Duration,
    pub method: Method,
    /// Request body. Empty for GET sweeps, sized for upload sweeps.
    pub payload: Option<Bytes>,
    pub http_version: HttpVersion,
    /// Aggregate requests per second to pace at. `None` runs closed-loop
    /// (as fast as responses allow).
    pub target_rps: Option<u64>,
    /// `Host` header sent with every request.
    pub host_header: String,
}

impl LoadConfig {
    #[must_use]
    pub fn new(target_url: impl Into<String>, concurrency: usize, duration: Duration) -> Self {
        Self {
            target_url: target_url.into(),
            concurrency: concurrency.max(1),
            duration,
            method: Method::GET,
            payload: None,
            http_version: HttpVersion::Http11,
            target_rps: None,
            host_header: "localhost".to_string(),
        }
    }
}

/// Concurrent HTTP load generator.
pub struct LoadGenerator {
    config: LoadConfig,
    recorder: Arc<LatencyRecorder>,
}

impl LoadGenerator {
    #[must_use]
    pub fn new(config: LoadConfig, recorder: Arc<LatencyRecorder>) -> Self {
        Self { config, recorder }
    }

    /// Convenience constructor matching the simple closed-loop case.
    #[must_use]
    pub fn simple(
        target_url: impl Into<String>,
        concurrency: usize,
        duration: Duration,
        recorder: Arc<LatencyRecorder>,
    ) -> Self {
        Self::new(LoadConfig::new(target_url, concurrency, duration), recorder)
    }

    /// Run the load phase. Returns the wall time actually spent measuring,
    /// which is what throughput must be divided by — not the nominal duration.
    pub async fn run(&self) -> Result<Duration, anyhow::Error> {
        let uri: Uri = self.config.target_url.parse()?;
        let running = Arc::new(AtomicBool::new(true));
        let calibration = Arc::new(Calibration::default());

        let mut connector = HttpConnector::new();
        connector.set_nodelay(true);
        connector.set_keepalive(Some(Duration::from_secs(60)));
        connector.set_reuse_address(true);

        let mut builder = Client::builder(TokioExecutor::new());
        builder
            .pool_timer(TokioTimer::new())
            .pool_idle_timeout(Duration::from_secs(90))
            // One idle slot per worker, so a worker never has to reconnect
            // between requests and the measurement excludes handshake cost.
            .pool_max_idle_per_host(self.config.concurrency.saturating_mul(2).max(64));
        if self.config.http_version == HttpVersion::Http2 {
            builder
                .http2_only(true)
                .http2_initial_stream_window_size(1024 * 1024)
                .http2_initial_connection_window_size(4 * 1024 * 1024);
        }
        let client = Arc::new(builder.build::<_, BoxedBody>(connector));

        // Per-worker pacing interval, in microseconds, for CO correction.
        let expected_interval_us = match self.config.target_rps {
            Some(rps) if rps > 0 => {
                (1_000_000.0 * self.config.concurrency as f64 / rps as f64) as u64
            }
            _ => 0,
        };

        let started = Instant::now();
        let stop_flag = running.clone();
        let duration = self.config.duration;
        let timer = tokio::spawn(async move {
            tokio::time::sleep(duration).await;
            stop_flag.store(false, Ordering::Relaxed);
        });

        let mut handles = Vec::with_capacity(self.config.concurrency);
        for _ in 0..self.config.concurrency {
            let client = client.clone();
            let running = running.clone();
            let recorder = self.recorder.clone();
            let calibration = calibration.clone();
            let uri = uri.clone();
            let method = self.config.method.clone();
            let payload = self.config.payload.clone();
            let host = self.config.host_header.clone();
            let version = self.config.http_version;

            handles.push(tokio::spawn(async move {
                let mut worker = recorder.worker();
                let mut next_send = Instant::now();

                while running.load(Ordering::Relaxed) {
                    if expected_interval_us > 0 {
                        // Open loop: hold the schedule even if the previous
                        // request was slow, which is what CO correction assumes.
                        next_send += Duration::from_micros(expected_interval_us);
                        let now = Instant::now();
                        if next_send > now {
                            tokio::time::sleep(next_send - now).await;
                        }
                    }

                    let body: BoxedBody = match &payload {
                        Some(bytes) => Full::new(bytes.clone())
                            .map_err(|never| match never {})
                            .boxed(),
                        None => Empty::<Bytes>::new()
                            .map_err(|never| match never {})
                            .boxed(),
                    };
                    let mut builder = Request::builder()
                        .method(method.clone())
                        .uri(uri.clone())
                        .header(http::header::HOST, host.as_str());
                    if version == HttpVersion::Http2 {
                        builder = builder.version(http::Version::HTTP_2);
                    }
                    let Ok(request) = builder.body(body) else {
                        worker.record_failure();
                        continue;
                    };

                    let start = cycles::timestamp();
                    match client.request(request).await {
                        Ok(response) => {
                            let status = response.status();
                            // The body must be drained or the connection cannot
                            // be reused, and the latency is not complete until
                            // the last byte arrives.
                            match response.into_body().collect().await {
                                Ok(collected) => {
                                    let bytes = collected.to_bytes().len() as u64;
                                    let elapsed = calibration
                                        .ticks_to_micros(cycles::timestamp().wrapping_sub(start));
                                    if status.is_success() || status.is_redirection() {
                                        worker.record_success(elapsed, bytes, expected_interval_us);
                                    } else {
                                        worker.record_error_response(elapsed, expected_interval_us);
                                    }
                                }
                                Err(_) => worker.record_failure(),
                            }
                        }
                        Err(_) => worker.record_failure(),
                    }
                }
            }));
        }

        for handle in handles {
            let _ = handle.await;
        }
        timer.abort();
        Ok(started.elapsed())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::mock::{MockUpstream, MockUpstreamConfig};
    use http_body_util::Empty;
    use tokio::sync::watch;

    async fn spawn_mock() -> (std::net::SocketAddr, watch::Sender<bool>) {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        drop(listener);
        let (tx, rx) = watch::channel(false);
        let mock = MockUpstream::new(MockUpstreamConfig {
            listen_addr: addr,
            ..Default::default()
        });
        tokio::spawn(async move {
            let _ = mock.run(rx).await;
        });
        // Readiness must be an actual HTTP round trip: a bare TCP connect can
        // succeed against a socket that is not yet serving, and the load window
        // is short enough that a slow start would show up as "no requests".
        let client: hyper_util::client::legacy::Client<HttpConnector, Empty<Bytes>> =
            Client::builder(TokioExecutor::new()).build_http();
        for _ in 0..200 {
            let uri: Uri = format!("http://{addr}/healthz").parse().expect("valid uri");
            if client.get(uri).await.is_ok() {
                return (addr, tx);
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        panic!("mock upstream did not become ready on {addr}");
    }

    #[test]
    fn http_version_labels_round_trip() {
        assert_eq!(HttpVersion::parse("h2"), Some(HttpVersion::Http2));
        assert_eq!(HttpVersion::parse("HTTP1.1"), Some(HttpVersion::Http11));
        assert_eq!(HttpVersion::parse("h3"), None);
        assert_eq!(HttpVersion::Http2.label(), "HTTP/2");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn generates_load_and_records_successes() {
        let (addr, shutdown) = spawn_mock().await;
        let recorder = Arc::new(LatencyRecorder::new());
        let generator = LoadGenerator::simple(
            format!("http://{addr}/test"),
            4,
            Duration::from_secs(1),
            recorder.clone(),
        );
        let elapsed = generator.run().await.expect("load run");
        let _ = shutdown.send(true);

        assert!(elapsed >= Duration::from_millis(950));
        let result = recorder.summarize("mock", "test", 4, 0, "HTTP/1.1", elapsed, 0.0, 0.0);
        assert!(result.successful_requests > 0, "no requests completed");
        assert_eq!(result.failed_requests, 0, "unexpected transport failures");
        assert!(
            result.bytes_received > 0,
            "response bodies were not counted"
        );
        assert!(result.latency_p50_us > 0);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn open_loop_pacing_respects_the_target_rate() {
        let (addr, shutdown) = spawn_mock().await;
        let recorder = Arc::new(LatencyRecorder::new());
        let mut config = LoadConfig::new(format!("http://{addr}/test"), 2, Duration::from_secs(1));
        config.target_rps = Some(200);
        let elapsed = LoadGenerator::new(config, recorder.clone())
            .run()
            .await
            .expect("load run");
        let _ = shutdown.send(true);

        let result = recorder.summarize("mock", "paced", 2, 0, "HTTP/1.1", elapsed, 0.0, 0.0);
        // Pacing is best-effort; assert the order of magnitude, not the exact
        // rate, so this stays stable on a loaded CI machine.
        assert!(
            (60..=400).contains(&(result.rps as u64)),
            "paced run produced {} rps, expected ~200",
            result.rps
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn unreachable_target_records_failures_not_panics() {
        // Port 1 on loopback is reserved and never listening.
        let recorder = Arc::new(LatencyRecorder::new());
        let generator = LoadGenerator::simple(
            "http://127.0.0.1:1/test",
            2,
            Duration::from_millis(200),
            recorder.clone(),
        );
        let elapsed = generator
            .run()
            .await
            .expect("run completes despite failures");
        let result = recorder.summarize("none", "fail", 2, 0, "HTTP/1.1", elapsed, 0.0, 0.0);
        assert!(result.failed_requests > 0);
        assert_eq!(result.successful_requests, 0);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn payload_bodies_are_sent() {
        let (addr, shutdown) = spawn_mock().await;
        let recorder = Arc::new(LatencyRecorder::new());
        let mut config =
            LoadConfig::new(format!("http://{addr}/echo"), 2, Duration::from_millis(300));
        config.method = Method::POST;
        config.payload = Some(crate::load::StandardPayloads::small_1kb());
        let elapsed = LoadGenerator::new(config, recorder.clone())
            .run()
            .await
            .expect("load run");
        let _ = shutdown.send(true);
        let result = recorder.summarize("mock", "post", 2, 1024, "HTTP/1.1", elapsed, 0.0, 0.0);
        assert!(result.successful_requests > 0);
    }
}
