//! `lb-bench` — benchmark harness for the `lb` proxy candidates.
//!
//! | Module | Responsibility |
//! | --- | --- |
//! | [`harness`] | Candidate lifecycle, scenarios, warm-up protocol |
//! | [`load`] | HTTP/1.1 and HTTP/2 load generation, standard payloads |
//! | [`metrics`] | HdrHistogram latency with CO correction, CPU/RSS sampling |
//! | [`mock`] | Mock upstream backend with selectable payload sizes |
//! | [`k8s`] | kind lifecycle, deployment, HTTPRoute churn injection |
//! | [`profiling`] | Flamegraph capture and the three-pass PGO pipeline |
//! | [`report`] | Markdown, CSV and hardware-spec output |
//!
//! Every candidate runs as its own OS process, so CPU and memory are
//! attributable and the load generator never shares a runtime with the proxy
//! it is measuring.

pub mod harness;
pub mod load;
pub mod metrics;
pub mod mock;
pub mod report;

#[cfg(feature = "k8s")]
pub mod k8s;

pub mod profiling;

pub use harness::{
    BenchmarkRunner, Candidate, Deployment, RunnerConfig, ScenarioConfig, ScenarioKind,
    WarmupProtocol,
};
pub use load::{HttpVersion, LoadConfig, LoadGenerator, PayloadSize, StandardPayloads};
pub use metrics::{BenchmarkResult, HardwareSpec, LatencyRecorder, MonitorTarget, ResourceMonitor};
pub use mock::{MockUpstream, MockUpstreamConfig};
pub use profiling::{FlamegraphCapture, FlamegraphConfig, PgoConfig, PgoPipeline};
pub use report::{
    RunDirectory, export_csv, generate_markdown_report, pgo_delta_section, write_hardware_spec,
};
