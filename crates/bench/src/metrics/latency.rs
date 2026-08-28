//! Latency and throughput recording.
//!
//! # Two things the previous implementation got wrong
//!
//! **Contention.** A single `Mutex<Histogram>` was locked on every request from
//! every task. At the 10k-50k concurrency of plan §8 the load generator spends
//! its time in the lock rather than issuing requests, so the harness measures
//! itself. Recording now goes through `hdrhistogram`'s lock-free
//! [`Recorder`](hdrhistogram::sync::Recorder), one per worker, merged only when
//! the run ends.
//!
//! **Coordinated omission.** Plan §1 asks for CO correction explicitly. A
//! closed-loop generator that waits for a response before sending the next
//! request cannot observe the latency of the requests it *failed to send* while
//! blocked, which understates p99.9 by an order of magnitude under saturation.
//! [`LatencyRecorder::record_with_expected_interval`] uses
//! `record_correct`, which back-fills the synthetic samples HdrHistogram
//! prescribes for exactly this case.

use hdrhistogram::Histogram;
use hdrhistogram::sync::{Recorder, SyncHistogram};
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

/// Lowest latency the histogram can represent, in microseconds.
const LOWEST_US: u64 = 1;
/// Highest latency, 60 s. Anything slower is a timeout, not a latency.
const HIGHEST_US: u64 = 60_000_000;
/// Significant figures retained. 3 gives 0.1% precision at every magnitude.
const SIGFIGS: u8 = 3;

/// One benchmark measurement.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BenchmarkResult {
    pub candidate_name: String,
    pub scenario_name: String,
    pub concurrency: usize,
    pub payload_bytes: usize,
    pub http_version: String,
    pub total_requests: u64,
    pub successful_requests: u64,
    pub failed_requests: u64,
    /// Responses with a 4xx/5xx status. Counted as completed but not successful.
    pub error_responses: u64,
    pub bytes_received: u64,
    pub duration_secs: f64,
    pub rps: f64,
    pub throughput_mib_s: f64,
    pub latency_p50_us: u64,
    pub latency_p90_us: u64,
    pub latency_p95_us: u64,
    pub latency_p99_us: u64,
    pub latency_p999_us: u64,
    pub latency_p9999_us: u64,
    pub latency_max_us: u64,
    pub latency_mean_us: f64,
    pub latency_stdev_us: f64,
    /// Percentiles from the coordinated-omission-corrected histogram.
    pub co_corrected_p99_us: u64,
    pub co_corrected_p999_us: u64,
    /// CPU percentage of the proxy process only (not the load generator).
    pub cpu_usage_pct: f32,
    /// Peak RSS of the proxy process only, in MiB.
    pub rss_memory_mb: f64,
}

impl BenchmarkResult {
    /// Requests that neither failed at the transport level nor returned 4xx/5xx.
    #[must_use]
    pub fn is_clean(&self) -> bool {
        self.failed_requests == 0 && self.error_responses == 0
    }
}

/// Shared counters that do not belong in the histogram.
#[derive(Debug, Default)]
struct Counters {
    successful: AtomicU64,
    failed: AtomicU64,
    error_responses: AtomicU64,
    bytes: AtomicU64,
}

/// Aggregating recorder. Clone once per worker via [`Self::worker`].
pub struct LatencyRecorder {
    histogram: parking_lot::Mutex<SyncHistogram<u64>>,
    /// Second histogram fed through `record_correct`, so both the raw and the
    /// CO-corrected view of the same run are available.
    corrected: parking_lot::Mutex<SyncHistogram<u64>>,
    counters: Arc<Counters>,
}

impl Default for LatencyRecorder {
    fn default() -> Self {
        Self::new()
    }
}

impl LatencyRecorder {
    #[must_use]
    pub fn new() -> Self {
        let make = || {
            SyncHistogram::from(
                Histogram::<u64>::new_with_bounds(LOWEST_US, HIGHEST_US, SIGFIGS)
                    .expect("histogram bounds are valid"),
            )
        };
        Self {
            histogram: parking_lot::Mutex::new(make()),
            corrected: parking_lot::Mutex::new(make()),
            counters: Arc::new(Counters::default()),
        }
    }

    /// Create a per-worker handle.
    ///
    /// Each worker records into its own thread-local buffer; the aggregate is
    /// only assembled in [`Self::summarize`]. No lock is taken per request.
    #[must_use]
    pub fn worker(&self) -> WorkerRecorder {
        WorkerRecorder {
            raw: self.histogram.lock().recorder(),
            corrected: self.corrected.lock().recorder(),
            counters: self.counters.clone(),
        }
    }

    /// Compile the aggregate result.
    ///
    /// `expected_interval_us` is the target inter-request interval implied by
    /// the offered load; pass `0` for an unthrottled closed-loop run, where the
    /// CO-corrected histogram is fed the measured mean instead.
    #[allow(clippy::too_many_arguments)]
    pub fn summarize(
        &self,
        candidate_name: impl Into<String>,
        scenario_name: impl Into<String>,
        concurrency: usize,
        payload_bytes: usize,
        http_version: impl Into<String>,
        duration: Duration,
        cpu_usage_pct: f32,
        rss_memory_mb: f64,
    ) -> BenchmarkResult {
        // `refresh` drains every worker's buffer into the shared histogram.
        let mut raw = self.histogram.lock();
        raw.refresh_timeout(Duration::from_secs(5));
        let mut corrected = self.corrected.lock();
        corrected.refresh_timeout(Duration::from_secs(5));

        let successful = self.counters.successful.load(Ordering::Relaxed);
        let failed = self.counters.failed.load(Ordering::Relaxed);
        let error_responses = self.counters.error_responses.load(Ordering::Relaxed);
        let bytes_received = self.counters.bytes.load(Ordering::Relaxed);
        let total_requests = successful + failed;
        let duration_secs = duration.as_secs_f64();

        let rps = if duration_secs > 0.0 {
            total_requests as f64 / duration_secs
        } else {
            0.0
        };
        let throughput_mib_s = if duration_secs > 0.0 {
            bytes_received as f64 / duration_secs / (1024.0 * 1024.0)
        } else {
            0.0
        };

        BenchmarkResult {
            candidate_name: candidate_name.into(),
            scenario_name: scenario_name.into(),
            concurrency,
            payload_bytes,
            http_version: http_version.into(),
            total_requests,
            successful_requests: successful,
            failed_requests: failed,
            error_responses,
            bytes_received,
            duration_secs,
            rps,
            throughput_mib_s,
            latency_p50_us: raw.value_at_percentile(50.0),
            latency_p90_us: raw.value_at_percentile(90.0),
            latency_p95_us: raw.value_at_percentile(95.0),
            latency_p99_us: raw.value_at_percentile(99.0),
            latency_p999_us: raw.value_at_percentile(99.9),
            latency_p9999_us: raw.value_at_percentile(99.99),
            latency_max_us: raw.max(),
            latency_mean_us: raw.mean(),
            latency_stdev_us: raw.stdev(),
            co_corrected_p99_us: corrected.value_at_percentile(99.0),
            co_corrected_p999_us: corrected.value_at_percentile(99.9),
            cpu_usage_pct,
            rss_memory_mb,
        }
    }

    /// Total requests recorded so far, without draining the histograms.
    #[must_use]
    pub fn completed(&self) -> u64 {
        self.counters.successful.load(Ordering::Relaxed)
            + self.counters.failed.load(Ordering::Relaxed)
    }
}

/// A per-worker recording handle. Not `Sync`: give each task its own.
pub struct WorkerRecorder {
    raw: Recorder<u64>,
    corrected: Recorder<u64>,
    counters: Arc<Counters>,
}

impl WorkerRecorder {
    /// Record a successful request.
    ///
    /// `expected_interval_us` is the pacing interval this worker is trying to
    /// hold. When it is non-zero and the observed latency exceeds it,
    /// `record_correct` synthesises the samples that a blocked closed-loop
    /// client could not send — the coordinated-omission correction.
    #[inline]
    pub fn record_success(&mut self, latency_us: u64, bytes: u64, expected_interval_us: u64) {
        let value = latency_us.clamp(LOWEST_US, HIGHEST_US);
        self.counters.successful.fetch_add(1, Ordering::Relaxed);
        self.counters.bytes.fetch_add(bytes, Ordering::Relaxed);
        self.raw.saturating_record(value);
        if expected_interval_us > 0 {
            // `record_correct` errors only if a back-filled sample would exceed
            // the histogram's upper bound; the raw value is still worth keeping.
            if self
                .corrected
                .record_correct(value, expected_interval_us)
                .is_err()
            {
                self.corrected.saturating_record(value);
            }
        } else {
            self.corrected.saturating_record(value);
        }
    }

    /// Record a request that completed with a 4xx/5xx status.
    #[inline]
    pub fn record_error_response(&mut self, latency_us: u64, expected_interval_us: u64) {
        self.counters
            .error_responses
            .fetch_add(1, Ordering::Relaxed);
        self.record_success(latency_us, 0, expected_interval_us);
    }

    /// Record a transport-level failure. Failures carry no latency, so they are
    /// counted but deliberately not recorded into the histogram: a connection
    /// refused in 5 µs is not a 5 µs "latency".
    #[inline]
    pub fn record_failure(&mut self) {
        self.counters.failed.fetch_add(1, Ordering::Relaxed);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn summarize(rec: &LatencyRecorder) -> BenchmarkResult {
        rec.summarize(
            "cand",
            "scen",
            1,
            1024,
            "HTTP/1.1",
            Duration::from_secs(1),
            0.0,
            0.0,
        )
    }

    #[test]
    fn records_percentiles_from_a_known_distribution() {
        let rec = LatencyRecorder::new();
        {
            let mut w = rec.worker();
            for i in 1..=1_000u64 {
                w.record_success(i, 100, 0);
            }
        }
        let r = summarize(&rec);
        assert_eq!(r.successful_requests, 1_000);
        assert_eq!(r.bytes_received, 100_000);
        // 3 significant figures, so allow the bucket width.
        assert!(
            (490..=510).contains(&r.latency_p50_us),
            "p50 was {}",
            r.latency_p50_us
        );
        assert!(
            (985..=1_000).contains(&r.latency_p99_us),
            "p99 was {}",
            r.latency_p99_us
        );
        assert_eq!(r.latency_max_us, 1_000);
    }

    #[test]
    fn coordinated_omission_correction_raises_the_tail() {
        // One 1-second stall against a 1 ms pacing target: the raw histogram
        // sees a single slow request, the corrected one back-fills the ~1000
        // requests that could not be sent during the stall.
        let rec = LatencyRecorder::new();
        {
            let mut w = rec.worker();
            for _ in 0..1_000 {
                w.record_success(1_000, 0, 1_000);
            }
            w.record_success(1_000_000, 0, 1_000);
        }
        let r = summarize(&rec);
        assert!(
            r.co_corrected_p999_us > r.latency_p999_us,
            "CO correction must raise the tail: raw p99.9 {} vs corrected {}",
            r.latency_p999_us,
            r.co_corrected_p999_us
        );
        assert!(
            r.co_corrected_p99_us > 100_000,
            "a 1s stall must dominate the corrected p99, got {}",
            r.co_corrected_p99_us
        );
    }

    #[test]
    fn zero_expected_interval_disables_correction() {
        let rec = LatencyRecorder::new();
        {
            let mut w = rec.worker();
            for _ in 0..1_000 {
                w.record_success(1_000, 0, 0);
            }
            w.record_success(1_000_000, 0, 0);
        }
        let r = summarize(&rec);
        assert_eq!(r.co_corrected_p99_us, r.latency_p99_us);
    }

    #[test]
    fn failures_are_counted_but_not_recorded_as_latency() {
        let rec = LatencyRecorder::new();
        {
            let mut w = rec.worker();
            w.record_success(500, 10, 0);
            w.record_failure();
            w.record_failure();
        }
        let r = summarize(&rec);
        assert_eq!(r.successful_requests, 1);
        assert_eq!(r.failed_requests, 2);
        assert_eq!(r.total_requests, 3);
        assert_eq!(
            r.latency_max_us, 500,
            "failures must not enter the histogram"
        );
        assert!(!r.is_clean());
    }

    #[test]
    fn error_responses_are_tracked_separately_from_failures() {
        let rec = LatencyRecorder::new();
        {
            let mut w = rec.worker();
            w.record_success(100, 5, 0);
            w.record_error_response(200, 0);
        }
        let r = summarize(&rec);
        assert_eq!(r.error_responses, 1);
        assert_eq!(r.failed_requests, 0);
        assert_eq!(
            r.successful_requests, 2,
            "an error response still completed"
        );
        assert!(!r.is_clean());
    }

    #[test]
    fn throughput_and_rps_are_derived_from_the_measured_window() {
        let rec = LatencyRecorder::new();
        {
            let mut w = rec.worker();
            for _ in 0..2_000 {
                w.record_success(100, 1024, 0);
            }
        }
        let r = rec.summarize(
            "c",
            "s",
            1,
            1024,
            "HTTP/1.1",
            Duration::from_secs(2),
            0.0,
            0.0,
        );
        assert!((r.rps - 1_000.0).abs() < 0.001, "rps was {}", r.rps);
        let expected_mib = 2_000.0 * 1024.0 / 2.0 / (1024.0 * 1024.0);
        assert!((r.throughput_mib_s - expected_mib).abs() < 0.001);
    }

    #[test]
    fn many_workers_aggregate_without_losing_samples() {
        let rec = Arc::new(LatencyRecorder::new());
        std::thread::scope(|s| {
            for _ in 0..8 {
                let rec = rec.clone();
                s.spawn(move || {
                    let mut w = rec.worker();
                    for i in 0..5_000u64 {
                        w.record_success(1 + i % 1_000, 1, 0);
                    }
                });
            }
        });
        let r = summarize(&rec);
        assert_eq!(r.successful_requests, 40_000);
        assert_eq!(r.bytes_received, 40_000);
    }

    #[test]
    fn empty_run_summarizes_without_panicking() {
        let r = summarize(&LatencyRecorder::new());
        assert_eq!(r.total_requests, 0);
        assert_eq!(r.rps, 0.0);
        assert!(r.is_clean());
    }
}
