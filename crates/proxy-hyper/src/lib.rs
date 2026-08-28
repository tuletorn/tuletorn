//! `lb-proxy-hyper` — Hyper 1.x + Tokio data plane.
//!
//! Multi-threaded work-stealing runtime, one `SO_REUSEPORT` listener per worker
//! so the kernel shards the accept queue instead of funnelling every connection
//! through a single accept loop.

pub mod client;
pub mod k8s_controller;
pub mod server;

pub use client::UpstreamClient;
pub use server::{ProxyHyper, ServerConfig};
