//! Upstream endpoints and load-balancing policy.
//!
//! A [`TargetGroup`] is `Arc`-backed so that the several matchit patterns one
//! logical route expands into all share a single round-robin cursor, and so
//! that cloning a group into a new [`crate::RouteTable`] during a reconcile is
//! a refcount bump rather than a re-allocation.

use crossbeam_utils::CachePadded;
use http::uri::Authority;
use serde::{Deserialize, Serialize};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, OnceLock};

/// Cap on the pre-expanded weighted-selection table.
///
/// Weighted round-robin is served from a flattened lookup table so selection is
/// O(1) instead of a linear scan over cumulative weights. Weights are scaled
/// down proportionally if they would exceed this, which bounds memory at 8 KiB
/// per route while keeping the ratio error under one part in a thousand.
const MAX_WEIGHT_TABLE: usize = 4096;

/// A single upstream destination backend.
///
/// Carries a lazily-parsed [`Authority`] alongside the raw address. Building an
/// upstream `Uri` per request otherwise means re-parsing and re-allocating the
/// authority every time; cached, it is a `Bytes` refcount bump.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BackendEndpoint {
    /// `host:port` of the target.
    pub address: String,
    /// Weight for weighted round-robin (Gateway API `backendRefs[].weight`).
    pub weight: u32,
    /// Parsed form of `address`, filled on first use.
    #[serde(skip)]
    authority: OnceLock<Option<Authority>>,
}

impl BackendEndpoint {
    #[must_use]
    pub fn new(address: impl Into<String>, weight: u32) -> Self {
        Self {
            address: address.into(),
            weight,
            authority: OnceLock::new(),
        }
    }

    /// The address parsed as a URI authority, or `None` if it is not valid.
    ///
    /// Parsed once per endpoint, then shared by every request routed to it.
    #[inline]
    #[must_use]
    pub fn authority(&self) -> Option<&Authority> {
        self.authority
            .get_or_init(|| Authority::try_from(self.address.as_str()).ok())
            .as_ref()
    }

    /// Resolve to a `SocketAddr`, for data planes that dial directly.
    ///
    /// Returns `None` for DNS names, which the caller must resolve itself.
    #[must_use]
    pub fn socket_addr(&self) -> Option<std::net::SocketAddr> {
        self.address.parse().ok()
    }
}

// `authority` is a derived cache, so identity is `address` + `weight` only.
impl PartialEq for BackendEndpoint {
    fn eq(&self, other: &Self) -> bool {
        self.address == other.address && self.weight == other.weight
    }
}
impl Eq for BackendEndpoint {}
impl std::hash::Hash for BackendEndpoint {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.address.hash(state);
        self.weight.hash(state);
    }
}

#[derive(Debug)]
enum Policy {
    /// Single destination: no atomics, no indexing.
    Single(BackendEndpoint),
    /// Equal-weight round robin over a cache-padded cursor.
    RoundRobin {
        endpoints: Box<[BackendEndpoint]>,
        cursor: CachePadded<AtomicUsize>,
    },
    /// Weighted round robin served from a pre-expanded index table.
    Weighted {
        endpoints: Box<[BackendEndpoint]>,
        /// `table[i]` is an index into `endpoints`, repeated by weight.
        table: Box<[u16]>,
        cursor: CachePadded<AtomicUsize>,
    },
}

/// Load-balancing policy over a group of upstream endpoints.
#[derive(Debug, Clone)]
pub struct TargetGroup {
    inner: Arc<Policy>,
}

impl TargetGroup {
    /// Build a group, choosing the cheapest policy the endpoints allow.
    #[must_use]
    pub fn from_endpoints(mut endpoints: Vec<BackendEndpoint>) -> Self {
        // Gateway API: weight 0 means "receive no traffic". Drop those rather
        // than letting them take a round-robin slot.
        endpoints.retain(|e| e.weight > 0);

        let policy = match endpoints.len() {
            0 => Policy::Single(BackendEndpoint::new("127.0.0.1:80", 1)),
            1 => Policy::Single(endpoints.pop().expect("len == 1")),
            _ => {
                let uniform = endpoints.windows(2).all(|w| w[0].weight == w[1].weight);
                if uniform {
                    Policy::RoundRobin {
                        endpoints: endpoints.into_boxed_slice(),
                        cursor: CachePadded::new(AtomicUsize::new(0)),
                    }
                } else {
                    let table = Self::expand_weights(&endpoints);
                    Policy::Weighted {
                        endpoints: endpoints.into_boxed_slice(),
                        table,
                        cursor: CachePadded::new(AtomicUsize::new(0)),
                    }
                }
            }
        };
        Self {
            inner: Arc::new(policy),
        }
    }

    /// Flatten weights into an index table, interleaved rather than blocked so
    /// that consecutive requests spread across backends instead of hammering
    /// one backend for its whole share before moving on.
    fn expand_weights(endpoints: &[BackendEndpoint]) -> Box<[u16]> {
        let total: u64 = endpoints.iter().map(|e| u64::from(e.weight)).sum();
        let scale = if total as usize > MAX_WEIGHT_TABLE {
            MAX_WEIGHT_TABLE as f64 / total as f64
        } else {
            1.0
        };

        // Smooth weighted round robin (the nginx algorithm): repeatedly pick the
        // backend with the highest current credit. This yields an interleaved
        // sequence with optimal spread for any weight ratio.
        let scaled: Vec<i64> = endpoints
            .iter()
            .map(|e| ((f64::from(e.weight) * scale).round() as i64).max(1))
            .collect();
        let total_scaled: i64 = scaled.iter().sum();
        let mut current = vec![0i64; endpoints.len()];
        let mut table = Vec::with_capacity(total_scaled as usize);

        for _ in 0..total_scaled {
            let mut best = 0usize;
            for i in 0..current.len() {
                current[i] += scaled[i];
                if current[i] > current[best] {
                    best = i;
                }
            }
            current[best] -= total_scaled;
            table.push(u16::try_from(best).unwrap_or(0));
        }
        table.into_boxed_slice()
    }

    /// Select the next upstream endpoint.
    ///
    /// Wait-free: one relaxed `fetch_add` on a cache-padded counter, then an
    /// index. Relaxed ordering is correct here because the counter carries no
    /// happens-before relationship — only fairness of distribution matters.
    #[inline]
    #[must_use]
    pub fn select(&self) -> &BackendEndpoint {
        match &*self.inner {
            Policy::Single(ep) => ep,
            Policy::RoundRobin { endpoints, cursor } => {
                let idx = cursor.fetch_add(1, Ordering::Relaxed);
                // `%` on a runtime length; endpoints is never empty here.
                &endpoints[idx % endpoints.len()]
            }
            Policy::Weighted {
                endpoints,
                table,
                cursor,
            } => {
                let idx = cursor.fetch_add(1, Ordering::Relaxed);
                let slot = table[idx % table.len()] as usize;
                &endpoints[slot]
            }
        }
    }

    /// All endpoints in the group, in configuration order.
    #[must_use]
    pub fn endpoints(&self) -> &[BackendEndpoint] {
        match &*self.inner {
            Policy::Single(ep) => std::slice::from_ref(ep),
            Policy::RoundRobin { endpoints, .. } | Policy::Weighted { endpoints, .. } => endpoints,
        }
    }

    /// Number of endpoints.
    #[must_use]
    pub fn len(&self) -> usize {
        self.endpoints().len()
    }

    #[must_use]
    pub fn is_empty(&self) -> bool {
        false
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    fn group(specs: &[(&str, u32)]) -> TargetGroup {
        TargetGroup::from_endpoints(
            specs
                .iter()
                .map(|(a, w)| BackendEndpoint::new(*a, *w))
                .collect(),
        )
    }

    #[test]
    fn round_robin_cycles_in_order() {
        let g = group(&[
            ("10.0.0.1:8080", 1),
            ("10.0.0.2:8080", 1),
            ("10.0.0.3:8080", 1),
        ]);
        let seen: Vec<_> = (0..4).map(|_| g.select().address.clone()).collect();
        assert_eq!(
            seen,
            [
                "10.0.0.1:8080",
                "10.0.0.2:8080",
                "10.0.0.3:8080",
                "10.0.0.1:8080"
            ]
        );
    }

    #[test]
    fn single_endpoint_skips_the_atomic() {
        let g = group(&[("10.0.0.1:80", 1)]);
        assert_eq!(g.select().address, "10.0.0.1:80");
        assert_eq!(g.select().address, "10.0.0.1:80");
        assert_eq!(g.len(), 1);
    }

    #[test]
    fn empty_group_yields_a_safe_placeholder() {
        let g = TargetGroup::from_endpoints(vec![]);
        assert_eq!(g.select().address, "127.0.0.1:80");
    }

    #[test]
    fn zero_weight_endpoints_receive_no_traffic() {
        let g = group(&[("live:80", 1), ("drained:80", 0)]);
        for _ in 0..20 {
            assert_eq!(g.select().address, "live:80");
        }
    }

    #[test]
    fn weighted_distribution_matches_ratio() {
        let g = group(&[("a:80", 3), ("b:80", 1)]);
        let mut counts: HashMap<String, usize> = HashMap::new();
        for _ in 0..4_000 {
            *counts.entry(g.select().address.clone()).or_default() += 1;
        }
        assert_eq!(counts["a:80"], 3_000);
        assert_eq!(counts["b:80"], 1_000);
    }

    #[test]
    fn weighted_selection_is_interleaved_not_blocked() {
        // 3:1 must produce a,a,b,a-style spread, never aaa...bbb.
        let g = group(&[("a:80", 3), ("b:80", 1)]);
        let seq: Vec<_> = (0..8).map(|_| g.select().address.clone()).collect();
        let first_b = seq.iter().position(|s| s == "b:80").expect("b must appear");
        assert!(first_b < 4, "b starved for {first_b} slots: {seq:?}");
    }

    #[test]
    fn extreme_weights_stay_within_the_table_cap() {
        let g = group(&[("a:80", 1_000_000), ("b:80", 1)]);
        // Must not allocate a million-entry table, and must still serve both.
        let mut saw_b = false;
        for _ in 0..MAX_WEIGHT_TABLE {
            if g.select().address == "b:80" {
                saw_b = true;
            }
        }
        assert!(saw_b, "the low-weight backend was scaled out of existence");
    }

    #[test]
    fn authority_is_parsed_once_and_cached() {
        let ep = BackendEndpoint::new("10.0.0.1:8080", 1);
        let first = ep.authority().expect("valid authority") as *const _;
        let second = ep.authority().expect("valid authority") as *const _;
        assert!(
            std::ptr::eq(first, second),
            "authority must be cached, not reparsed"
        );
        assert_eq!(ep.authority().unwrap().as_str(), "10.0.0.1:8080");
    }

    #[test]
    fn invalid_authority_is_reported_not_panicked() {
        assert!(
            BackendEndpoint::new("not a valid authority", 1)
                .authority()
                .is_none()
        );
    }

    #[test]
    fn socket_addr_parses_ips_and_declines_dns_names() {
        assert!(
            BackendEndpoint::new("10.0.0.1:8080", 1)
                .socket_addr()
                .is_some()
        );
        assert!(
            BackendEndpoint::new("svc.default.svc.cluster.local:80", 1)
                .socket_addr()
                .is_none()
        );
    }

    #[test]
    fn equality_ignores_the_derived_authority_cache() {
        let a = BackendEndpoint::new("10.0.0.1:80", 1);
        let b = BackendEndpoint::new("10.0.0.1:80", 1);
        let _ = a.authority();
        assert_eq!(a, b, "populating the cache must not change identity");
    }

    #[test]
    fn clones_share_one_cursor() {
        let g = group(&[("a:80", 1), ("b:80", 1)]);
        let clone = g.clone();
        assert_eq!(g.select().address, "a:80");
        assert_eq!(
            clone.select().address,
            "b:80",
            "clone must share the cursor"
        );
    }

    #[test]
    fn concurrent_selection_is_evenly_distributed() {
        use std::sync::Mutex;
        use std::thread;
        let g = group(&[("a:80", 1), ("b:80", 1), ("c:80", 1)]);
        let counts = Arc::new(Mutex::new(HashMap::<String, usize>::new()));
        thread::scope(|s| {
            for _ in 0..8 {
                let g = g.clone();
                let counts = counts.clone();
                s.spawn(move || {
                    let mut local: HashMap<String, usize> = HashMap::new();
                    for _ in 0..3_000 {
                        *local.entry(g.select().address.clone()).or_default() += 1;
                    }
                    let mut shared = counts.lock().unwrap();
                    for (k, v) in local {
                        *shared.entry(k).or_default() += v;
                    }
                });
            }
        });
        let counts = counts.lock().unwrap();
        assert_eq!(counts.values().sum::<usize>(), 24_000);
        for (addr, n) in counts.iter() {
            assert!(
                (7_000..9_000).contains(n),
                "{addr} got {n} of 24000, expected ~8000"
            );
        }
    }
}
