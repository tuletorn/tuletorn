//! `lb-proxy-monoio` — thread-per-core data plane on Monoio.
//!
//! Hybrid runtime (plan §6): Monoio workers own the data plane, one dedicated
//! Tokio thread owns the `kube-rs` control plane, and the two communicate only
//! through an `ArcSwap<RouteTable>`.

pub mod http1;
pub mod http2;
pub mod k8s_controller;
pub mod pool;
pub mod server;

pub use server::{ProxyMonoio, WorkerConfig};
