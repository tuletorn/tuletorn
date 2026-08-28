//! `lb-proxy-pingora` — Cloudflare Pingora data plane.

pub mod k8s_controller;
pub mod proxy_service;

pub use proxy_service::{ProxyPingora, ServiceConfig};
