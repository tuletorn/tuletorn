//! Lock-free, SIMD-accelerated Gateway API route table.
//!
//! Layout mirrors how a request is actually matched:
//!
//! ```text
//!   Host header ──SIMD lowercase+port strip──> exact map ──> matchit trie ──> RouteAction
//!                                           └> wildcard suffix walk ──┘
//!                                           └> default trie ──────────┘
//! ```
//!
//! Reads are wait-free ([`ArcSwap`]); the control plane publishes a whole new
//! table on every reconcile, so a route churn burst (plan §8, Scenario 3) never
//! blocks or even slows a request thread.

use crate::filter::RouteFilters;
use crate::hash::FxHashMap;
use crate::simd;
use crate::target::{BackendEndpoint, TargetGroup};
use arc_swap::ArcSwap;
use matchit::Router;
use std::sync::Arc;

/// Longest hostname we will lowercase on the stack. RFC 1035 caps a DNS name at
/// 253 octets, so this never spills in practice; longer values fall back to the
/// borrowed bytes and match case-sensitively rather than allocating per request.
const MAX_HOST_LEN: usize = 256;

/// A matched routing action.
#[derive(Debug)]
pub struct RouteAction {
    /// Upstream load balancing target group.
    pub target_group: TargetGroup,
    /// Pipeline of request and response filters, pre-compiled at build time.
    pub filters: RouteFilters,
    /// Route identifier, for logging and churn attribution.
    pub route_name: String,
}

/// A per-host URL router using a matchit radix trie.
#[derive(Default, Debug)]
pub struct HostRouter {
    /// matchit radix trie router.
    pub router: Router<RouteAction>,
}

impl HostRouter {
    #[inline]
    fn at(&self, path: &str) -> Option<&RouteAction> {
        self.router.at(path).ok().map(|m| m.value)
    }
}

/// The compiled routing table. Immutable once built; replaced wholesale.
#[derive(Default, Debug)]
pub struct RouteTable {
    /// Exact hostname matching, keys already ASCII-lowercased.
    pub exact_hosts: FxHashMap<Box<str>, HostRouter>,
    /// Wildcard hostname matching (`".example.com"` serves `*.example.com`).
    pub wildcard_hosts: FxHashMap<Box<str>, HostRouter>,
    /// Fallback router used when no hostname matches.
    pub default_router: HostRouter,
    /// Number of routes across all hosts, for reconcile logging.
    pub route_count: usize,
}

impl RouteTable {
    #[must_use]
    pub fn empty() -> Self {
        Self::default()
    }

    /// Look up a route for a `Host` header and request path.
    ///
    /// Precedence follows the Gateway API spec: exact hostname, then the
    /// longest matching wildcard, then the hostname-less default. A host that
    /// matches but whose path does not falls through to the next tier rather
    /// than 404-ing, so a catch-all default route still applies.
    #[inline]
    #[must_use]
    pub fn lookup(&self, host: Option<&str>, path: &str) -> Option<&RouteAction> {
        let clean_path = if path.is_empty() { "/" } else { path };

        if let Some(raw_host) = host {
            // `Host` is case-insensitive; normalise on the stack, no allocation.
            let stripped = simd::host_without_port(raw_host);
            let mut lower = [0u8; MAX_HOST_LEN];
            let normalized: &str = match simd::lowercase_ascii_into(stripped.as_bytes(), &mut lower)
            {
                // SAFETY: ASCII-lowercasing maps each byte to itself or to
                // `b | 0x20` only for `A-Z`, so UTF-8 structure is preserved.
                Some(buf) => unsafe { std::str::from_utf8_unchecked(buf) },
                None => stripped,
            };

            // 1. Exact hostname.
            if let Some(action) = self
                .exact_hosts
                .get(normalized)
                .and_then(|hr| hr.at(clean_path))
            {
                return Some(action);
            }

            // 2. Wildcard suffixes, most specific first:
            //    "a.b.example.com" tries ".b.example.com", then ".example.com",
            //    then ".com".
            if !self.wildcard_hosts.is_empty() {
                let mut current = normalized;
                while let Some(dot) = simd::find_byte(b'.', current.as_bytes()) {
                    let suffix = &current[dot..];
                    if let Some(action) = self
                        .wildcard_hosts
                        .get(suffix)
                        .and_then(|hr| hr.at(clean_path))
                    {
                        return Some(action);
                    }
                    if suffix.len() <= 1 {
                        break;
                    }
                    current = &suffix[1..];
                }
            }
        }

        // 3. Hostname-less default routes.
        self.default_router.at(clean_path)
    }

    /// Total number of routes installed.
    #[must_use]
    pub fn len(&self) -> usize {
        self.route_count
    }

    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.route_count == 0
    }
}

/// Wait-free atomic container for the active [`RouteTable`].
#[derive(Default, Debug)]
pub struct SharedRouteTable {
    inner: ArcSwap<RouteTable>,
}

impl SharedRouteTable {
    #[must_use]
    pub fn new() -> Self {
        Self {
            inner: ArcSwap::from_pointee(RouteTable::empty()),
        }
    }

    #[must_use]
    pub fn from_table(table: RouteTable) -> Self {
        Self {
            inner: ArcSwap::from_pointee(table),
        }
    }

    /// Wait-free load of the active table.
    #[inline]
    #[must_use]
    pub fn load(&self) -> arc_swap::Guard<Arc<RouteTable>> {
        self.inner.load()
    }

    /// Load a full `Arc` when the guard would outlive the borrow (needed by
    /// Monoio's per-connection tasks, which are `'static`).
    #[inline]
    #[must_use]
    pub fn load_full(&self) -> Arc<RouteTable> {
        self.inner.load_full()
    }

    /// Atomically publish a new table. Readers in flight finish on the old one.
    #[inline]
    pub fn store(&self, new_table: RouteTable) {
        self.inner.store(Arc::new(new_table));
    }

    /// Publish a pre-shared table without re-wrapping it.
    #[inline]
    pub fn store_arc(&self, new_table: Arc<RouteTable>) {
        self.inner.store(new_table);
    }
}

/// How a path pattern from the control plane should be matched.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PathMatch {
    /// Gateway API `PathPrefix`: matches the prefix and everything below it.
    Prefix,
    /// Gateway API `Exact`: matches only this path.
    Exact,
}

/// Translate a control-plane path pattern into the matchit patterns it needs.
///
/// A `PathPrefix` of `/api` must match `/api`, `/api/`, and `/api/v1/x`, which
/// in matchit takes *two* inserts: the bare path and a catch-all below it.
/// Returning both is what makes `PathPrefix: /` serve `/` — the single
/// `/{*catchall}` pattern used previously does not, because matchit catch-alls
/// require at least one segment.
fn expand_pattern(path: &str, kind: PathMatch) -> Vec<String> {
    // Accept the historical shorthands (`/*`, `/*path`, `/*catchall`) as
    // prefix matches so existing configs keep working.
    let (base, forced_prefix) = if path == "/*" {
        ("/", true)
    } else if let Some(stripped) = path
        .strip_suffix("/*catchall")
        .or_else(|| path.strip_suffix("/*path"))
        .or_else(|| path.strip_suffix("/*"))
    {
        (if stripped.is_empty() { "/" } else { stripped }, true)
    } else {
        (path, false)
    };

    let base = if base.is_empty() { "/" } else { base };

    if kind == PathMatch::Exact && !forced_prefix {
        return vec![base.to_string()];
    }

    let trimmed = base.trim_end_matches('/');
    if trimmed.is_empty() {
        // Root prefix: the bare "/" plus everything under it.
        vec!["/".to_string(), "/{*catchall}".to_string()]
    } else {
        // Three patterns, because matchit treats all three as distinct and a
        // catch-all requires at least one segment to match:
        //   /api               the bare prefix
        //   /api/              the prefix with a trailing slash
        //   /api/{*catchall}   everything below it
        vec![
            trimmed.to_string(),
            format!("{trimmed}/"),
            format!("{trimmed}/{{*catchall}}"),
        ]
    }
}

/// One route as supplied by the control plane, before compilation.
#[derive(Debug)]
pub struct RouteSpec {
    pub hostname: Option<String>,
    pub path: String,
    pub path_match: PathMatch,
    pub endpoints: Vec<BackendEndpoint>,
    pub filters: RouteFilters,
    pub route_name: String,
}

/// Builder that compiles [`RouteSpec`]s into an immutable [`RouteTable`].
#[derive(Default)]
pub struct RouteTableBuilder {
    exact: FxHashMap<Box<str>, Vec<(String, RouteAction)>>,
    wildcard: FxHashMap<Box<str>, Vec<(String, RouteAction)>>,
    default_routes: Vec<(String, RouteAction)>,
    route_count: usize,
}

impl RouteTableBuilder {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Add a prefix-matched route. Convenience wrapper over [`Self::add`].
    pub fn add_route(
        &mut self,
        hostname: Option<&str>,
        path_pattern: impl AsRef<str>,
        endpoints: Vec<BackendEndpoint>,
        filters: RouteFilters,
        route_name: impl Into<String>,
    ) -> &mut Self {
        self.add(RouteSpec {
            hostname: hostname.map(str::to_owned),
            path: path_pattern.as_ref().to_owned(),
            path_match: PathMatch::Prefix,
            endpoints,
            filters,
            route_name: route_name.into(),
        })
    }

    /// Add a fully specified route.
    pub fn add(&mut self, spec: RouteSpec) -> &mut Self {
        let patterns = expand_pattern(&spec.path, spec.path_match);
        let RouteSpec {
            hostname,
            endpoints,
            filters,
            route_name,
            ..
        } = spec;

        // A route expands to 1-2 matchit patterns that share one action. Both
        // `TargetGroup` and `RouteFilters` are `Arc`-backed, so cloning them
        // shares the round-robin cursor and the pre-compiled header set rather
        // than duplicating either.
        let group = TargetGroup::from_endpoints(endpoints);

        for pattern in patterns {
            let action = RouteAction {
                target_group: group.clone(),
                filters: filters.clone(),
                route_name: route_name.clone(),
            };
            match hostname.as_deref() {
                None | Some("*") => self.default_routes.push((pattern, action)),
                Some(h) if h.starts_with("*.") => {
                    let suffix: Box<str> = h[1..].to_ascii_lowercase().into_boxed_str();
                    self.wildcard
                        .entry(suffix)
                        .or_default()
                        .push((pattern, action));
                }
                Some(h) => {
                    let key: Box<str> = h.to_ascii_lowercase().into_boxed_str();
                    self.exact.entry(key).or_default().push((pattern, action));
                }
            }
        }
        self.route_count += 1;
        self
    }

    /// Compile into an immutable table.
    ///
    /// Duplicate patterns within one hostname are resolved first-wins rather
    /// than failing the whole reconcile: a single malformed `HTTPRoute` must not
    /// take down routing for every other route in the cluster.
    pub fn build(self) -> Result<RouteTable, matchit::InsertError> {
        fn compile(routes: Vec<(String, RouteAction)>) -> Router<RouteAction> {
            let mut router = Router::new();
            for (pattern, action) in routes {
                if let Err(err) = router.insert(&pattern, action) {
                    tracing::debug!(%pattern, %err, "skipping conflicting route pattern");
                }
            }
            router
        }

        let mut table = RouteTable {
            route_count: self.route_count,
            ..RouteTable::default()
        };
        for (host, routes) in self.exact {
            table.exact_hosts.insert(
                host,
                HostRouter {
                    router: compile(routes),
                },
            );
        }
        for (suffix, routes) in self.wildcard {
            table.wildcard_hosts.insert(
                suffix,
                HostRouter {
                    router: compile(routes),
                },
            );
        }
        table.default_router = HostRouter {
            router: compile(self.default_routes),
        };
        Ok(table)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ep(addr: &str) -> Vec<BackendEndpoint> {
        vec![BackendEndpoint::new(addr, 1)]
    }

    fn sample_table() -> RouteTable {
        let mut b = RouteTableBuilder::new();
        b.add_route(
            Some("api.example.com"),
            "/v1/users",
            ep("10.0.1.1:80"),
            RouteFilters::default(),
            "exact-api",
        );
        b.add_route(
            Some("*.example.com"),
            "/v1",
            ep("10.0.2.1:80"),
            RouteFilters::default(),
            "wildcard-api",
        );
        b.add_route(
            None,
            "/health",
            ep("127.0.0.1:8080"),
            RouteFilters::default(),
            "default-health",
        );
        b.build().expect("build table")
    }

    #[test]
    fn precedence_exact_then_wildcard_then_default() {
        let t = sample_table();
        assert_eq!(
            t.lookup(Some("api.example.com"), "/v1/users")
                .unwrap()
                .route_name,
            "exact-api"
        );
        assert_eq!(
            t.lookup(Some("foo.example.com"), "/v1/orders")
                .unwrap()
                .route_name,
            "wildcard-api"
        );
        assert_eq!(
            t.lookup(Some("random.org"), "/health").unwrap().route_name,
            "default-health"
        );
        assert!(t.lookup(Some("random.org"), "/not-found").is_none());
    }

    /// Regression: a `PathPrefix: /` catch-all must serve the root path.
    /// The previous `/*path` -> `/{*catchall}` translation 404'd on "/", because
    /// matchit catch-alls require at least one segment.
    #[test]
    fn root_catch_all_matches_root_and_below() {
        for pattern in ["/*path", "/*", "/*catchall", "/"] {
            let mut b = RouteTableBuilder::new();
            b.add_route(
                None,
                pattern,
                ep("127.0.0.1:9090"),
                RouteFilters::default(),
                "catchall",
            );
            let t = b.build().unwrap();
            for path in ["/", "/test", "/a/b/c", ""] {
                assert!(
                    t.lookup(Some("localhost"), path).is_some(),
                    "pattern {pattern:?} failed to match path {path:?}"
                );
            }
        }
    }

    #[test]
    fn prefix_matches_bare_path_and_descendants() {
        let mut b = RouteTableBuilder::new();
        b.add_route(
            None,
            "/api",
            ep("10.0.0.1:80"),
            RouteFilters::default(),
            "api",
        );
        let t = b.build().unwrap();
        for path in ["/api", "/api/", "/api/v1", "/api/v1/deep/nesting"] {
            assert!(t.lookup(None, path).is_some(), "prefix miss on {path}");
        }
        assert!(
            t.lookup(None, "/apiary").is_none(),
            "prefix must be segment-aligned"
        );
    }

    #[test]
    fn exact_match_does_not_match_descendants() {
        let mut b = RouteTableBuilder::new();
        b.add(RouteSpec {
            hostname: None,
            path: "/exact".into(),
            path_match: PathMatch::Exact,
            endpoints: ep("10.0.0.1:80"),
            filters: RouteFilters::default(),
            route_name: "exact".into(),
        });
        let t = b.build().unwrap();
        assert!(t.lookup(None, "/exact").is_some());
        assert!(t.lookup(None, "/exact/more").is_none());
    }

    #[test]
    fn host_matching_is_case_insensitive_and_port_agnostic() {
        let t = sample_table();
        for host in [
            "API.EXAMPLE.COM",
            "Api.Example.Com",
            "api.example.com:8443",
            "API.EXAMPLE.COM:80",
        ] {
            assert_eq!(
                t.lookup(Some(host), "/v1/users").unwrap().route_name,
                "exact-api",
                "host {host} did not match"
            );
        }
    }

    #[test]
    fn wildcard_prefers_most_specific_suffix() {
        let mut b = RouteTableBuilder::new();
        b.add_route(
            Some("*.example.com"),
            "/",
            ep("10.0.0.1:80"),
            RouteFilters::default(),
            "narrow",
        );
        b.add_route(
            Some("*.com"),
            "/",
            ep("10.0.0.2:80"),
            RouteFilters::default(),
            "broad",
        );
        let t = b.build().unwrap();
        assert_eq!(
            t.lookup(Some("a.example.com"), "/x").unwrap().route_name,
            "narrow"
        );
        assert_eq!(
            t.lookup(Some("a.other.com"), "/x").unwrap().route_name,
            "broad"
        );
    }

    /// A host that matches but whose path does not must fall through, not 404.
    #[test]
    fn host_hit_with_path_miss_falls_through_to_default() {
        let mut b = RouteTableBuilder::new();
        b.add_route(
            Some("api.example.com"),
            "/v1",
            ep("10.0.1.1:80"),
            RouteFilters::default(),
            "v1",
        );
        b.add_route(
            None,
            "/",
            ep("10.0.9.9:80"),
            RouteFilters::default(),
            "fallback",
        );
        let t = b.build().unwrap();
        assert_eq!(
            t.lookup(Some("api.example.com"), "/v1/x")
                .unwrap()
                .route_name,
            "v1"
        );
        assert_eq!(
            t.lookup(Some("api.example.com"), "/other")
                .unwrap()
                .route_name,
            "fallback"
        );
    }

    #[test]
    fn ipv6_host_literals_are_handled() {
        let mut b = RouteTableBuilder::new();
        b.add_route(
            Some("[::1]"),
            "/",
            ep("10.0.0.1:80"),
            RouteFilters::default(),
            "v6",
        );
        let t = b.build().unwrap();
        assert_eq!(t.lookup(Some("[::1]:8080"), "/x").unwrap().route_name, "v6");
    }

    #[test]
    fn conflicting_patterns_do_not_fail_the_reconcile() {
        let mut b = RouteTableBuilder::new();
        b.add_route(
            None,
            "/dup",
            ep("10.0.0.1:80"),
            RouteFilters::default(),
            "first",
        );
        b.add_route(
            None,
            "/dup",
            ep("10.0.0.2:80"),
            RouteFilters::default(),
            "second",
        );
        let t = b
            .build()
            .expect("duplicate patterns must not abort the build");
        assert_eq!(t.lookup(None, "/dup").unwrap().route_name, "first");
    }

    #[test]
    fn shared_target_group_round_robins_across_expanded_patterns() {
        // "/api" expands to two matchit patterns; both must share one cursor,
        // otherwise load balancing state resets depending on the request path.
        let mut b = RouteTableBuilder::new();
        b.add_route(
            None,
            "/api",
            vec![
                BackendEndpoint::new("10.0.0.1:80", 1),
                BackendEndpoint::new("10.0.0.2:80", 1),
            ],
            RouteFilters::default(),
            "api",
        );
        let t = b.build().unwrap();
        let a = t
            .lookup(None, "/api")
            .unwrap()
            .target_group
            .select()
            .address
            .clone();
        let b2 = t
            .lookup(None, "/api/deep")
            .unwrap()
            .target_group
            .select()
            .address
            .clone();
        assert_ne!(a, b2, "cursor is not shared between expanded patterns");
    }

    #[test]
    fn atomic_swap_publishes_new_table() {
        let shared = SharedRouteTable::new();
        assert!(shared.load().lookup(None, "/x").is_none());
        let mut b = RouteTableBuilder::new();
        b.add_route(None, "/", ep("10.0.0.1:80"), RouteFilters::default(), "new");
        shared.store(b.build().unwrap());
        assert_eq!(shared.load().lookup(None, "/x").unwrap().route_name, "new");
    }

    #[test]
    fn empty_path_is_treated_as_root() {
        let mut b = RouteTableBuilder::new();
        b.add_route(
            None,
            "/",
            ep("10.0.0.1:80"),
            RouteFilters::default(),
            "root",
        );
        let t = b.build().unwrap();
        assert!(t.lookup(None, "").is_some());
    }

    #[test]
    fn overlong_hostname_does_not_panic() {
        let t = sample_table();
        let long = "a".repeat(MAX_HOST_LEN * 2);
        assert!(
            t.lookup(Some(&long), "/health").is_some(),
            "must fall through to default"
        );
    }
}
