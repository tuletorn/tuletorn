//! Benchmark scenario definitions (plan §8).

use crate::load::{HttpVersion, PayloadSize};
use serde::{Deserialize, Serialize};
use std::time::Duration;

/// Which of the plan's five scenarios a run is executing.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ScenarioKind {
    /// §8.1 — throughput and tail latency across concurrency and payload size.
    Throughput,
    /// §8.2 — C10K-C50K connection density and memory footprint.
    ConnectionDensity,
    /// §8.3 — steady load with HTTPRoute churn, measuring p99.99 jitter.
    RouteChurn,
    /// §8.5 — the same workload as Throughput, against a PGO build.
    PgoDelta,
}

impl ScenarioKind {
    #[must_use]
    pub const fn label(self) -> &'static str {
        match self {
            Self::Throughput => "throughput",
            Self::ConnectionDensity => "connection-density",
            Self::RouteChurn => "route-churn",
            Self::PgoDelta => "pgo-delta",
        }
    }

    #[must_use]
    pub fn parse(s: &str) -> Option<Self> {
        match s.trim().to_ascii_lowercase().as_str() {
            "throughput" | "1" => Some(Self::Throughput),
            "connection-density" | "density" | "2" => Some(Self::ConnectionDensity),
            "route-churn" | "churn" | "3" => Some(Self::RouteChurn),
            "pgo-delta" | "pgo" | "5" => Some(Self::PgoDelta),
            _ => None,
        }
    }
}

/// A single scenario's parameters.
#[derive(Debug, Clone)]
pub struct ScenarioConfig {
    pub name: String,
    pub kind: ScenarioKind,
    pub concurrencies: Vec<usize>,
    pub payloads: Vec<PayloadSize>,
    pub http_versions: Vec<HttpVersion>,
    pub duration: Duration,
    /// Offered load for open-loop runs; `None` runs closed-loop.
    pub target_rps: Option<u64>,
    /// HTTPRoute mutations per second (Scenario 3).
    pub route_churn_rate_hz: u32,
    /// Fraction of connections kept idle (Scenario 2).
    pub idle_connection_ratio: f64,
}

impl ScenarioConfig {
    /// §8.1 — full sweep: 100 to 25 000 connections, all three payload sizes.
    #[must_use]
    pub fn throughput() -> Self {
        Self {
            name: "Maximum Throughput & Tail Latency Sweep".into(),
            kind: ScenarioKind::Throughput,
            concurrencies: vec![100, 1_000, 5_000, 10_000, 25_000],
            payloads: PayloadSize::all().to_vec(),
            http_versions: vec![HttpVersion::Http11, HttpVersion::Http2],
            duration: Duration::from_secs(30),
            target_rps: None,
            route_churn_rate_hz: 0,
            idle_connection_ratio: 0.0,
        }
    }

    /// §8.2 — connection density: 10k-50k persistent connections, 95% idle.
    #[must_use]
    pub fn connection_density() -> Self {
        Self {
            name: "Connection Density & Memory Footprint (C10K-C50K)".into(),
            kind: ScenarioKind::ConnectionDensity,
            concurrencies: vec![10_000, 25_000, 50_000],
            payloads: vec![PayloadSize::Small1Kb],
            http_versions: vec![HttpVersion::Http11],
            duration: Duration::from_secs(60),
            target_rps: None,
            route_churn_rate_hz: 0,
            idle_connection_ratio: 0.95,
        }
    }

    /// §8.3 — steady 20 000 RPS with route mutations at 10-500 Hz.
    #[must_use]
    pub fn route_churn(churn_hz: u32) -> Self {
        Self {
            name: format!("Route Churn & Tail Latency Jitter ({churn_hz} Hz)"),
            kind: ScenarioKind::RouteChurn,
            concurrencies: vec![1_000],
            payloads: vec![PayloadSize::Small1Kb],
            http_versions: vec![HttpVersion::Http11],
            duration: Duration::from_secs(30),
            // Open loop: the point is to see the tail move, which requires a
            // fixed offered rate and coordinated-omission correction.
            target_rps: Some(20_000),
            route_churn_rate_hz: churn_hz,
            idle_connection_ratio: 0.0,
        }
    }

    /// §8.5 — identical workload to §8.1, run against a PGO build.
    #[must_use]
    pub fn pgo_delta() -> Self {
        Self {
            name: "PGO Impact Delta".into(),
            kind: ScenarioKind::PgoDelta,
            concurrencies: vec![100, 1_000, 5_000],
            payloads: vec![PayloadSize::Small1Kb, PayloadSize::Medium64Kb],
            http_versions: vec![HttpVersion::Http11],
            duration: Duration::from_secs(30),
            target_rps: None,
            route_churn_rate_hz: 0,
            idle_connection_ratio: 0.0,
        }
    }

    /// A short smoke-test sweep for CI and local iteration.
    #[must_use]
    pub fn quick() -> Self {
        Self {
            name: "Quick Sweep".into(),
            kind: ScenarioKind::Throughput,
            concurrencies: vec![50, 200],
            payloads: vec![PayloadSize::Small1Kb],
            http_versions: vec![HttpVersion::Http11],
            duration: Duration::from_secs(5),
            target_rps: None,
            route_churn_rate_hz: 0,
            idle_connection_ratio: 0.0,
        }
    }

    /// Number of measurement windows this scenario will run per candidate.
    #[must_use]
    pub fn measurement_count(&self) -> usize {
        self.concurrencies.len() * self.payloads.len() * self.http_versions.len()
    }

    /// Total measurement time, excluding warm-up.
    #[must_use]
    pub fn total_measurement_time(&self) -> Duration {
        self.duration * self.measurement_count() as u32
    }
}

impl Default for ScenarioConfig {
    fn default() -> Self {
        Self::quick()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn plan_scenarios_have_the_documented_shape() {
        let t = ScenarioConfig::throughput();
        assert_eq!(t.concurrencies, [100, 1_000, 5_000, 10_000, 25_000]);
        assert_eq!(t.payloads.len(), 3);
        assert_eq!(
            t.http_versions.len(),
            2,
            "plan §1 requires HTTP/1.1 and HTTP/2"
        );

        let d = ScenarioConfig::connection_density();
        assert_eq!(d.concurrencies, [10_000, 25_000, 50_000]);
        assert!((d.idle_connection_ratio - 0.95).abs() < f64::EPSILON);

        let c = ScenarioConfig::route_churn(100);
        assert_eq!(c.route_churn_rate_hz, 100);
        assert_eq!(
            c.target_rps,
            Some(20_000),
            "churn must be measured under a fixed offered rate"
        );
    }

    #[test]
    fn measurement_count_is_the_full_cross_product() {
        let t = ScenarioConfig::throughput();
        assert_eq!(t.measurement_count(), 5 * 3 * 2);
        assert_eq!(t.total_measurement_time(), Duration::from_secs(30) * 30);
    }

    #[test]
    fn scenario_kinds_round_trip() {
        for kind in [
            ScenarioKind::Throughput,
            ScenarioKind::ConnectionDensity,
            ScenarioKind::RouteChurn,
            ScenarioKind::PgoDelta,
        ] {
            assert_eq!(ScenarioKind::parse(kind.label()), Some(kind));
        }
        assert_eq!(
            ScenarioKind::parse("2"),
            Some(ScenarioKind::ConnectionDensity)
        );
        assert_eq!(ScenarioKind::parse("nope"), None);
    }
}
