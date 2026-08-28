//! Fair warm-up protocol (plan §7.3).
//!
//! | Phase | Duration | Purpose |
//! | --- | --- | --- |
//! | Cold start | 3 s idle | runtime init, lazy statics, page faults |
//! | Warm-up traffic | 15 s at 50% concurrency | connection pools, Go heap |
//! | GC settle | 5 s idle | Go sweep completion (Go candidates only) |
//!
//! The Go settle phase is applied on the basis of the candidate's *runtime*,
//! not a substring search of its name — matching `"traefik"` in a display name
//! silently skips the phase the moment a candidate is renamed.

use crate::harness::candidate::Candidate;
use crate::load::{LoadConfig, LoadGenerator};
use crate::metrics::LatencyRecorder;
use std::sync::Arc;
use std::time::Duration;
use tracing::info;

/// Warm-up timings.
#[derive(Debug, Clone)]
pub struct WarmupProtocol {
    pub cold_start: Duration,
    pub traffic: Duration,
    pub gc_settle: Duration,
}

impl Default for WarmupProtocol {
    fn default() -> Self {
        Self {
            cold_start: Duration::from_secs(3),
            traffic: Duration::from_secs(15),
            gc_settle: Duration::from_secs(5),
        }
    }
}

impl WarmupProtocol {
    /// A shortened protocol for smoke tests.
    #[must_use]
    pub fn quick() -> Self {
        Self {
            cold_start: Duration::from_millis(500),
            traffic: Duration::from_secs(2),
            gc_settle: Duration::from_millis(500),
        }
    }

    /// Total wall time this protocol costs for `candidate`.
    #[must_use]
    pub fn total(&self, candidate: Candidate) -> Duration {
        let mut total = self.cold_start + self.traffic;
        if candidate.is_go() {
            total += self.gc_settle;
        }
        total
    }

    /// Execute the warm-up. Latency during warm-up is recorded into a throwaway
    /// recorder so it can never contaminate the measurement window.
    pub async fn execute(
        &self,
        candidate: Candidate,
        target_url: &str,
        target_concurrency: usize,
    ) -> Result<(), anyhow::Error> {
        info!(
            candidate = candidate.display_name(),
            secs = self.cold_start.as_secs(),
            "warm-up 1/3: cold start"
        );
        tokio::time::sleep(self.cold_start).await;

        let warmup_concurrency = (target_concurrency / 2).max(1);
        info!(
            candidate = candidate.display_name(),
            concurrency = warmup_concurrency,
            secs = self.traffic.as_secs(),
            "warm-up 2/3: traffic"
        );
        let throwaway = Arc::new(LatencyRecorder::new());
        LoadGenerator::new(
            LoadConfig::new(target_url, warmup_concurrency, self.traffic),
            throwaway,
        )
        .run()
        .await?;

        if candidate.is_go() {
            info!(
                candidate = candidate.display_name(),
                secs = self.gc_settle.as_secs(),
                "warm-up 3/3: Go GC settle"
            );
            tokio::time::sleep(self.gc_settle).await;
        }

        info!(candidate = candidate.display_name(), "warm-up complete");
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_timings_match_the_plan() {
        let w = WarmupProtocol::default();
        assert_eq!(w.cold_start, Duration::from_secs(3));
        assert_eq!(w.traffic, Duration::from_secs(15));
        assert_eq!(w.gc_settle, Duration::from_secs(5));
    }

    #[test]
    fn only_go_candidates_pay_the_gc_settle_phase() {
        let w = WarmupProtocol::default();
        assert_eq!(w.total(Candidate::Traefik), Duration::from_secs(23));
        for c in Candidate::rust_candidates() {
            assert_eq!(w.total(c), Duration::from_secs(18), "{c:?}");
        }
    }

    #[tokio::test]
    async fn warmup_against_a_dead_target_still_completes() {
        // Warm-up must not abort a run just because the target refuses:
        // the measurement phase reports the failure with real numbers.
        let w = WarmupProtocol {
            cold_start: Duration::from_millis(10),
            traffic: Duration::from_millis(50),
            gc_settle: Duration::from_millis(10),
        };
        w.execute(Candidate::Hyper, "http://127.0.0.1:1/", 2)
            .await
            .expect("warm-up should tolerate an unreachable target");
    }
}
