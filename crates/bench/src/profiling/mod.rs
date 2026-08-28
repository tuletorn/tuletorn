//! Flamegraph capture and PGO build orchestration.

pub mod flamegraph;
pub mod pgo;

pub use flamegraph::{FlamegraphCapture, FlamegraphConfig, PROFILING_PROFILE};
pub use pgo::{PgoConfig, PgoPipeline};
