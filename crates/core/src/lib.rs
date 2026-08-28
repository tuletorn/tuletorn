//! `lb-core` — shared data-plane primitives for the `lb` proxy candidates.
//!
//! Everything that all three data planes (Hyper, Pingora, Monoio) must agree on
//! lives here, so that a benchmark difference between them reflects the runtime
//! and I/O model rather than an accidental divergence in routing or filtering.
//!
//! | Module | Responsibility |
//! | --- | --- |
//! | [`route_table`] | Lock-free `ArcSwap<RouteTable>` over matchit radix tries |
//! | [`target`] | Upstream endpoints, round-robin and O(1) weighted balancing |
//! | [`filter`] | Gateway API header modifiers and URL rewrites, pre-compiled |
//! | [`simd`] | NEON / AVX2 / SSE2 byte primitives used on the request path |
//! | [`hash`] | FxHash for hostname maps, replacing SipHash on the hot path |
//! | [`cycles`] | Inline-asm cycle counter for low-overhead latency sampling |
//! | [`config`] | YAML route config for standalone and PGO profiling runs |
//! | [`gateway`] | Kubernetes Gateway API reconciler (feature `k8s`) |
//! | [`alloc`] | Allocator name registry; binaries declare their own allocator |
//! | [`proxy`] | Hop-by-hop header hygiene and forwarding headers |
//! | [`forward`] | The shared per-request preparation every candidate runs |

pub mod alloc;
pub mod config;
pub mod cycles;
pub mod filter;
pub mod forward;
pub mod hash;
pub mod proxy;
pub mod route_table;
pub mod simd;
pub mod target;

#[cfg(feature = "k8s")]
pub mod gateway;

pub use alloc::allocator_name;
pub use config::{BackendEntry, PathType, RouteConfig, RouteEntry};
pub use cycles::Calibration;
pub use filter::{HeaderModifier, RouteFilters, UrlRewrite};
pub use forward::{Prepared, UpstreamProtocol, finish_response, prepare};
pub use hash::{FxHashMap, FxHasher};
pub use proxy::{FORWARDED_FOR, HOP_BY_HOP, connection_nominated, strip_hop_by_hop};
pub use route_table::{
    HostRouter, PathMatch, RouteAction, RouteSpec, RouteTable, RouteTableBuilder, SharedRouteTable,
};
pub use target::{BackendEndpoint, TargetGroup};
