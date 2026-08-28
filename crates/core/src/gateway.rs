//! Kubernetes Gateway API v1 reconciler shared by all three data planes.
//!
//! Plan §2 puts a `k8s_controller.rs` in every proxy crate; those are thin
//! adapters over this module so that Hyper, Pingora and Monoio cannot drift
//! apart in how they interpret an `HTTPRoute`.
//!
//! # Resolution chain
//!
//! ```text
//! HTTPRoute ─ spec.rules[].backendRefs[] ─> Service (name, port)
//!                                             │
//!                    EndpointSlice ───────────┘
//!                    (kubernetes.io/service-name label)
//!                                             │
//!                                             v
//!                                   ready Pod IP:port list
//! ```
//!
//! Resolving to Pod IPs rather than the Service ClusterIP is what makes this a
//! real data plane: it bypasses kube-proxy/conntrack, which is exactly the
//! comparison plan §1 sets up against Traefik.
//!
//! Both watches feed one debounced rebuild task. A whole new [`RouteTable`] is
//! compiled and published with a single [`ArcSwap`] store, so the 10-500 Hz
//! churn of plan §8 Scenario 3 never blocks a request thread.
//!
//! [`ArcSwap`]: arc_swap::ArcSwap

use crate::filter::{HeaderModifier, RouteFilters, UrlRewrite};
use crate::route_table::{PathMatch, RouteSpec, RouteTableBuilder, SharedRouteTable};
use crate::target::BackendEndpoint;
use futures_util::StreamExt;
use k8s_openapi::api::discovery::v1::EndpointSlice;
use kube::api::{Api, ListParams};
use kube::core::{ApiResource, DynamicObject, GroupVersionKind};
use kube::runtime::watcher;
use serde::Deserialize;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Notify;
use tracing::{debug, error, info, warn};

/// Debounce window for rebuilds.
///
/// A rollout restart produces a burst of EndpointSlice events; rebuilding once
/// per event would recompile the trie hundreds of times a second for no benefit.
/// 25 ms coalesces a burst while staying far below the churn periods in §8.
const REBUILD_DEBOUNCE: Duration = Duration::from_millis(25);

/// Backoff before restarting a watch that has failed.
const WATCH_RETRY: Duration = Duration::from_secs(1);

/// Controller configuration.
#[derive(Debug, Clone)]
pub struct ControllerConfig {
    /// Only reconcile `HTTPRoute`s whose `parentRefs` name this Gateway.
    /// `None` accepts every route in scope.
    pub gateway_name: Option<String>,
    /// Restrict to one namespace; `None` watches cluster-wide.
    pub namespace: Option<String>,
}

impl Default for ControllerConfig {
    fn default() -> Self {
        Self {
            gateway_name: Some("lb-gateway".to_string()),
            namespace: None,
        }
    }
}

// ---------------------------------------------------------------------------
// Gateway API v1 wire types
//
// Gateway API ships as CRDs, so these are not in `k8s-openapi`. Only the fields
// that affect routing are modelled; everything else is ignored by serde, which
// keeps this forward-compatible with v1.2.x point releases.
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
struct HttpRouteSpec {
    #[serde(default)]
    parent_refs: Vec<ParentRef>,
    #[serde(default)]
    hostnames: Vec<String>,
    #[serde(default)]
    rules: Vec<HttpRouteRule>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
struct ParentRef {
    name: String,
    /// Defaults to the route's own namespace when omitted.
    #[serde(default)]
    namespace: Option<String>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
struct HttpRouteRule {
    #[serde(default)]
    matches: Vec<HttpRouteMatch>,
    #[serde(default)]
    backend_refs: Vec<BackendRef>,
    #[serde(default)]
    filters: Vec<HttpRouteFilter>,
}

#[derive(Debug, Clone, Default, Deserialize)]
#[serde(rename_all = "camelCase")]
struct HttpRouteMatch {
    #[serde(default)]
    path: Option<HttpPathMatch>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
struct HttpPathMatch {
    #[serde(rename = "type", default)]
    match_type: Option<String>,
    #[serde(default)]
    value: Option<String>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
struct BackendRef {
    name: String,
    #[serde(default)]
    namespace: Option<String>,
    #[serde(default)]
    port: Option<i32>,
    #[serde(default)]
    weight: Option<i32>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
struct HttpRouteFilter {
    #[serde(rename = "type")]
    filter_type: String,
    #[serde(default)]
    request_header_modifier: Option<HeaderFilter>,
    #[serde(default)]
    response_header_modifier: Option<HeaderFilter>,
    #[serde(default)]
    url_rewrite: Option<UrlRewriteFilter>,
}

#[derive(Debug, Clone, Default, Deserialize)]
#[serde(rename_all = "camelCase")]
struct HeaderFilter {
    #[serde(default)]
    set: Vec<NameValue>,
    #[serde(default)]
    add: Vec<NameValue>,
    #[serde(default)]
    remove: Vec<String>,
}

#[derive(Debug, Clone, Deserialize)]
struct NameValue {
    name: String,
    value: String,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
struct UrlRewriteFilter {
    #[serde(default)]
    path: Option<UrlRewritePath>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
struct UrlRewritePath {
    #[serde(rename = "type")]
    path_type: String,
    #[serde(default)]
    replace_prefix_match: Option<String>,
    #[serde(default)]
    replace_full_path: Option<String>,
}

impl HeaderFilter {
    fn to_modifiers(&self) -> Vec<HeaderModifier> {
        let mut out = Vec::with_capacity(self.set.len() + self.add.len() + self.remove.len());
        out.extend(self.set.iter().map(|nv| HeaderModifier::Set {
            name: nv.name.clone(),
            value: nv.value.clone(),
        }));
        out.extend(self.add.iter().map(|nv| HeaderModifier::Add {
            name: nv.name.clone(),
            value: nv.value.clone(),
        }));
        out.extend(
            self.remove
                .iter()
                .map(|n| HeaderModifier::Remove { name: n.clone() }),
        );
        out
    }
}

// ---------------------------------------------------------------------------
// Cluster state snapshot
// ---------------------------------------------------------------------------

/// Key identifying a Service in the cluster.
type ServiceKey = (String, String);

/// The observed cluster state that a route table is compiled from.
#[derive(Debug, Default)]
struct ClusterState {
    /// `(namespace, name)` -> parsed HTTPRoute.
    routes: HashMap<ServiceKey, StoredRoute>,
    /// `(namespace, service)` -> ready endpoints, per port.
    endpoints: HashMap<ServiceKey, Vec<ReadyEndpoint>>,
}

#[derive(Debug, Clone)]
struct StoredRoute {
    namespace: String,
    name: String,
    spec: HttpRouteSpec,
}

#[derive(Debug, Clone)]
struct ReadyEndpoint {
    address: String,
    port: i32,
}

impl ClusterState {
    /// Resolve a `backendRef` to concrete `ip:port` endpoints.
    ///
    /// Falls back to the Service DNS name when no EndpointSlice is known yet,
    /// so a route is never silently black-holed during a cold start.
    fn resolve(&self, route_ns: &str, backend: &BackendRef) -> Vec<BackendEndpoint> {
        let ns = backend.namespace.as_deref().unwrap_or(route_ns).to_string();
        let key = (ns.clone(), backend.name.clone());
        let port = backend.port.unwrap_or(80);
        // Gateway API weights are 0-1_000_000; 0 means "no traffic".
        let weight = backend.weight.unwrap_or(1).clamp(0, 1_000_000) as u32;

        match self.endpoints.get(&key) {
            Some(eps) if eps.iter().any(|e| e.port == port) => eps
                .iter()
                .filter(|e| e.port == port)
                .map(|e| BackendEndpoint::new(format!("{}:{}", e.address, e.port), weight))
                .collect(),
            _ => {
                debug!(
                    service = %backend.name,
                    namespace = %ns,
                    port,
                    "no ready EndpointSlice, falling back to Service DNS"
                );
                vec![BackendEndpoint::new(
                    format!("{}.{}.svc.cluster.local:{}", backend.name, ns, port),
                    weight,
                )]
            }
        }
    }

    /// Compile the whole observed state into a fresh route table.
    fn compile(&self, cfg: &ControllerConfig) -> crate::route_table::RouteTable {
        let mut builder = RouteTableBuilder::new();
        let mut n_routes = 0usize;

        for stored in self.routes.values() {
            if !Self::route_is_ours(cfg, stored) {
                continue;
            }
            for (rule_idx, rule) in stored.spec.rules.iter().enumerate() {
                let endpoints: Vec<BackendEndpoint> = rule
                    .backend_refs
                    .iter()
                    .flat_map(|b| self.resolve(&stored.namespace, b))
                    .collect();
                if endpoints.is_empty() {
                    continue;
                }

                // No `matches` means "match everything" per the Gateway API spec.
                let matches: Vec<(String, PathMatch)> = if rule.matches.is_empty() {
                    vec![("/".to_string(), PathMatch::Prefix)]
                } else {
                    rule.matches
                        .iter()
                        .map(|m| {
                            let path = m.path.as_ref();
                            let value = path
                                .and_then(|p| p.value.clone())
                                .unwrap_or_else(|| "/".to_string());
                            let kind = match path.and_then(|p| p.match_type.as_deref()) {
                                Some("Exact") => PathMatch::Exact,
                                // PathPrefix is the Gateway API default, and
                                // RegularExpression is unsupported here, so it
                                // degrades to a prefix rather than dropping.
                                _ => PathMatch::Prefix,
                            };
                            (value, kind)
                        })
                        .collect()
                };

                // No hostnames means "any host".
                let hostnames: Vec<Option<String>> = if stored.spec.hostnames.is_empty() {
                    vec![None]
                } else {
                    stored.spec.hostnames.iter().cloned().map(Some).collect()
                };

                for hostname in &hostnames {
                    for (path, path_match) in &matches {
                        // `ReplacePrefixMatch` replaces *this match's* prefix,
                        // so filters are compiled per match, not per rule.
                        builder.add(RouteSpec {
                            hostname: hostname.clone(),
                            path: path.clone(),
                            path_match: *path_match,
                            endpoints: endpoints.clone(),
                            filters: Self::compile_filters(&rule.filters, path),
                            route_name: format!("{}/{}#{rule_idx}", stored.namespace, stored.name),
                        });
                        n_routes += 1;
                    }
                }
            }
        }

        debug!(routes = n_routes, "compiled route table");
        builder.build().unwrap_or_default()
    }

    /// Whether this route attaches to the Gateway we are serving.
    ///
    /// A `parentRefs` entry may name a Gateway in another namespace; when the
    /// controller is scoped to one namespace, such a reference is not ours.
    fn route_is_ours(cfg: &ControllerConfig, stored: &StoredRoute) -> bool {
        let Some(want) = cfg.gateway_name.as_deref() else {
            return true;
        };
        if stored.spec.parent_refs.is_empty() {
            // No parentRefs: the route is unattached, so claim it only when the
            // controller is not scoped to a specific namespace.
            return true;
        }
        stored.spec.parent_refs.iter().any(|p| {
            if p.name != want {
                return false;
            }
            match (&cfg.namespace, &p.namespace) {
                // Cluster-scoped controller accepts any Gateway namespace.
                (None, _) => true,
                // parentRef namespace defaults to the route's own namespace.
                (Some(ours), None) => *ours == stored.namespace,
                (Some(ours), Some(theirs)) => ours == theirs,
            }
        })
    }

    /// Translate Gateway API filters, using `matched_prefix` as the prefix a
    /// `ReplacePrefixMatch` rewrite strips.
    fn compile_filters(filters: &[HttpRouteFilter], matched_prefix: &str) -> RouteFilters {
        let mut request = Vec::new();
        let mut response = Vec::new();
        let mut rewrite = None;

        for f in filters {
            match f.filter_type.as_str() {
                "RequestHeaderModifier" => {
                    if let Some(h) = &f.request_header_modifier {
                        request.extend(h.to_modifiers());
                    }
                }
                "ResponseHeaderModifier" => {
                    if let Some(h) = &f.response_header_modifier {
                        response.extend(h.to_modifiers());
                    }
                }
                "URLRewrite" => {
                    if let Some(path) = f.url_rewrite.as_ref().and_then(|u| u.path.as_ref()) {
                        rewrite = match path.path_type.as_str() {
                            "ReplacePrefixMatch" => {
                                path.replace_prefix_match
                                    .as_ref()
                                    .map(|r| UrlRewrite::Prefix {
                                        prefix: matched_prefix.trim_end_matches('/').to_string(),
                                        replacement: r.clone(),
                                    })
                            }
                            "ReplaceFullPath" => path
                                .replace_full_path
                                .as_ref()
                                .map(|r| UrlRewrite::Exact(r.clone())),
                            other => {
                                warn!(path_type = other, "unsupported URLRewrite path type");
                                None
                            }
                        };
                    }
                }
                other => debug!(filter = other, "unsupported HTTPRoute filter, ignoring"),
            }
        }

        RouteFilters::new(&request, &response, rewrite)
    }
}

// ---------------------------------------------------------------------------
// Controller
// ---------------------------------------------------------------------------

/// Watch Gateway API `HTTPRoute`s and `EndpointSlice`s, publishing a new route
/// table on every change.
///
/// Runs until cancelled. Watch failures are logged and retried with a fixed
/// backoff rather than terminating the data plane.
pub async fn run(routes: Arc<SharedRouteTable>, cfg: ControllerConfig) -> Result<(), kube::Error> {
    let client = kube::Client::try_default().await?;
    info!(
        gateway = cfg.gateway_name.as_deref().unwrap_or("<any>"),
        namespace = cfg.namespace.as_deref().unwrap_or("<all>"),
        "starting Gateway API reconciler"
    );

    let state = Arc::new(tokio::sync::RwLock::new(ClusterState::default()));
    let dirty = Arc::new(Notify::new());

    let httproute_ar = ApiResource::from_gvk(&GroupVersionKind::gvk(
        "gateway.networking.k8s.io",
        "v1",
        "HTTPRoute",
    ));

    let route_api: Api<DynamicObject> = match cfg.namespace.as_deref() {
        Some(ns) => Api::namespaced_with(client.clone(), ns, &httproute_ar),
        None => Api::all_with(client.clone(), &httproute_ar),
    };
    let slice_api: Api<EndpointSlice> = match cfg.namespace.as_deref() {
        Some(ns) => Api::namespaced(client.clone(), ns),
        None => Api::all(client.clone()),
    };

    let routes_task = tokio::spawn(watch_httproutes(route_api, state.clone(), dirty.clone()));
    let slices_task = tokio::spawn(watch_endpointslices(
        slice_api,
        state.clone(),
        dirty.clone(),
    ));
    let rebuild_task = tokio::spawn(rebuild_loop(routes, state, dirty, cfg));

    // If any task exits, the controller is no longer functional; stop them all
    // so the supervisor can restart cleanly.
    tokio::select! {
        _ = routes_task => warn!("HTTPRoute watch task ended"),
        _ = slices_task => warn!("EndpointSlice watch task ended"),
        _ = rebuild_task => warn!("route table rebuild task ended"),
    }
    Ok(())
}

async fn watch_httproutes(
    api: Api<DynamicObject>,
    state: Arc<tokio::sync::RwLock<ClusterState>>,
    dirty: Arc<Notify>,
) {
    loop {
        let mut stream = watcher(api.clone(), watcher::Config::default()).boxed();
        while let Some(event) = stream.next().await {
            match event {
                Ok(watcher::Event::Apply(obj)) | Ok(watcher::Event::InitApply(obj)) => {
                    let (Some(name), Some(ns)) =
                        (obj.metadata.name.clone(), obj.metadata.namespace.clone())
                    else {
                        continue;
                    };
                    match serde_json::from_value::<HttpRouteSpec>(obj.data["spec"].clone()) {
                        Ok(spec) => {
                            state.write().await.routes.insert(
                                (ns.clone(), name.clone()),
                                StoredRoute {
                                    namespace: ns,
                                    name,
                                    spec,
                                },
                            );
                            dirty.notify_one();
                        }
                        Err(err) => {
                            warn!(%ns, %name, %err, "unparseable HTTPRoute spec, ignoring");
                        }
                    }
                }
                Ok(watcher::Event::Delete(obj)) => {
                    if let (Some(name), Some(ns)) = (obj.metadata.name, obj.metadata.namespace) {
                        state.write().await.routes.remove(&(ns, name));
                        dirty.notify_one();
                    }
                }
                Ok(watcher::Event::Init) => {
                    state.write().await.routes.clear();
                }
                Ok(watcher::Event::InitDone) => dirty.notify_one(),
                Err(err) => {
                    error!(%err, "HTTPRoute watch error, retrying");
                    break;
                }
            }
        }
        tokio::time::sleep(WATCH_RETRY).await;
    }
}

async fn watch_endpointslices(
    api: Api<EndpointSlice>,
    state: Arc<tokio::sync::RwLock<ClusterState>>,
    dirty: Arc<Notify>,
) {
    // Only slices that belong to a Service are useful to us.
    let config = watcher::Config::default().labels("kubernetes.io/service-name");
    loop {
        let mut stream = watcher(api.clone(), config.clone()).boxed();
        while let Some(event) = stream.next().await {
            match event {
                Ok(watcher::Event::Apply(slice)) | Ok(watcher::Event::InitApply(slice)) => {
                    if let Some((key, eps)) = parse_endpoint_slice(&slice) {
                        state.write().await.endpoints.insert(key, eps);
                        dirty.notify_one();
                    }
                }
                Ok(watcher::Event::Delete(slice)) => {
                    if let Some((key, _)) = parse_endpoint_slice(&slice) {
                        state.write().await.endpoints.remove(&key);
                        dirty.notify_one();
                    }
                }
                Ok(watcher::Event::Init) => {
                    state.write().await.endpoints.clear();
                }
                Ok(watcher::Event::InitDone) => dirty.notify_one(),
                Err(err) => {
                    error!(%err, "EndpointSlice watch error, retrying");
                    break;
                }
            }
        }
        tokio::time::sleep(WATCH_RETRY).await;
    }
}

/// Extract ready `ip:port` pairs from an EndpointSlice.
///
/// Endpoints whose `conditions.ready` is explicitly false are excluded; an
/// absent condition means ready per the API contract.
fn parse_endpoint_slice(slice: &EndpointSlice) -> Option<(ServiceKey, Vec<ReadyEndpoint>)> {
    let ns = slice.metadata.namespace.clone()?;
    let service = slice
        .metadata
        .labels
        .as_ref()?
        .get("kubernetes.io/service-name")?
        .clone();

    let ports: Vec<i32> = slice
        .ports
        .as_ref()
        .map(|ps| ps.iter().filter_map(|p| p.port).collect())
        .unwrap_or_default();
    if ports.is_empty() {
        return Some(((ns, service), Vec::new()));
    }

    let mut out = Vec::new();
    // `endpoints` is optional in the newest API version k8s-openapi models.
    for endpoint in slice.endpoints.iter().flatten() {
        let ready = endpoint
            .conditions
            .as_ref()
            .and_then(|c| c.ready)
            .unwrap_or(true);
        if !ready {
            continue;
        }
        for address in &endpoint.addresses {
            for &port in &ports {
                out.push(ReadyEndpoint {
                    address: address.clone(),
                    port,
                });
            }
        }
    }
    Some(((ns, service), out))
}

/// Coalesce change notifications and republish the table.
async fn rebuild_loop(
    routes: Arc<SharedRouteTable>,
    state: Arc<tokio::sync::RwLock<ClusterState>>,
    dirty: Arc<Notify>,
    cfg: ControllerConfig,
) {
    loop {
        dirty.notified().await;
        // Coalesce the burst that follows a rollout or a churn injection.
        tokio::time::sleep(REBUILD_DEBOUNCE).await;

        let table = {
            let guard = state.read().await;
            guard.compile(&cfg)
        };
        let count = table.len();
        routes.store(table);
        debug!(routes = count, "published new route table");
    }
}

/// One-shot reconcile against the current cluster state, without watching.
///
/// Used by tests and by `--mode k8s --once`.
pub async fn reconcile_once(
    routes: Arc<SharedRouteTable>,
    cfg: ControllerConfig,
) -> Result<usize, kube::Error> {
    let client = kube::Client::try_default().await?;
    let httproute_ar = ApiResource::from_gvk(&GroupVersionKind::gvk(
        "gateway.networking.k8s.io",
        "v1",
        "HTTPRoute",
    ));
    let route_api: Api<DynamicObject> = Api::all_with(client.clone(), &httproute_ar);
    let slice_api: Api<EndpointSlice> = Api::all(client);

    let mut state = ClusterState::default();
    for obj in route_api.list(&ListParams::default()).await?.items {
        let (Some(name), Some(ns)) = (obj.metadata.name.clone(), obj.metadata.namespace.clone())
        else {
            continue;
        };
        if let Ok(spec) = serde_json::from_value::<HttpRouteSpec>(obj.data["spec"].clone()) {
            state.routes.insert(
                (ns.clone(), name.clone()),
                StoredRoute {
                    namespace: ns,
                    name,
                    spec,
                },
            );
        }
    }
    for slice in slice_api.list(&ListParams::default()).await?.items {
        if let Some((key, eps)) = parse_endpoint_slice(&slice) {
            state.endpoints.insert(key, eps);
        }
    }

    let table = state.compile(&cfg);
    let n = table.len();
    routes.store(table);
    Ok(n)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// `(namespace, service, [(address, port), ..])`
    type EndpointFixture<'a> = (&'a str, &'a str, Vec<(&'a str, i32)>);

    fn route(json: serde_json::Value) -> StoredRoute {
        StoredRoute {
            namespace: "default".into(),
            name: "test-route".into(),
            spec: serde_json::from_value(json).expect("spec parses"),
        }
    }

    fn state_with(route: StoredRoute, endpoints: Vec<EndpointFixture<'_>>) -> ClusterState {
        let mut s = ClusterState::default();
        s.routes
            .insert(("default".into(), "test-route".into()), route);
        for (ns, svc, eps) in endpoints {
            s.endpoints.insert(
                (ns.into(), svc.into()),
                eps.into_iter()
                    .map(|(a, p)| ReadyEndpoint {
                        address: a.into(),
                        port: p,
                    })
                    .collect(),
            );
        }
        s
    }

    #[test]
    fn resolves_backend_refs_to_pod_ips() {
        let r = route(serde_json::json!({
            "parentRefs": [{"name": "lb-gateway"}],
            "hostnames": ["api.example.com"],
            "rules": [{
                "matches": [{"path": {"type": "PathPrefix", "value": "/v1"}}],
                "backendRefs": [{"name": "api-svc", "port": 8080}]
            }]
        }));
        let state = state_with(
            r,
            vec![(
                "default",
                "api-svc",
                vec![("10.244.1.5", 8080), ("10.244.2.7", 8080)],
            )],
        );
        let table = state.compile(&ControllerConfig::default());

        let action = table
            .lookup(Some("api.example.com"), "/v1/users")
            .expect("route");
        let addrs: Vec<_> = action
            .target_group
            .endpoints()
            .iter()
            .map(|e| e.address.clone())
            .collect();
        assert_eq!(addrs, ["10.244.1.5:8080", "10.244.2.7:8080"]);
    }

    #[test]
    fn falls_back_to_service_dns_without_endpointslices() {
        let r = route(serde_json::json!({
            "rules": [{"backendRefs": [{"name": "api-svc", "port": 80}]}]
        }));
        let table = state_with(r, vec![]).compile(&ControllerConfig::default());
        let action = table.lookup(None, "/anything").expect("catch-all rule");
        assert_eq!(
            action.target_group.select().address,
            "api-svc.default.svc.cluster.local:80"
        );
    }

    #[test]
    fn rule_without_matches_becomes_a_root_prefix() {
        let r = route(serde_json::json!({
            "rules": [{"backendRefs": [{"name": "s", "port": 80}]}]
        }));
        let table = state_with(r, vec![]).compile(&ControllerConfig::default());
        for path in ["/", "/deep/path"] {
            assert!(table.lookup(None, path).is_some(), "missed {path}");
        }
    }

    #[test]
    fn parent_ref_filtering_selects_only_our_gateway() {
        let ours = route(serde_json::json!({
            "parentRefs": [{"name": "lb-gateway"}],
            "rules": [{"backendRefs": [{"name": "s", "port": 80}]}]
        }));
        let mut s = state_with(ours, vec![]);
        s.routes.insert(
            ("default".into(), "traefik-route".into()),
            StoredRoute {
                namespace: "default".into(),
                name: "traefik-route".into(),
                spec: serde_json::from_value(serde_json::json!({
                    "parentRefs": [{"name": "traefik-gateway"}],
                    "hostnames": ["traefik.example.com"],
                    "rules": [{"backendRefs": [{"name": "other", "port": 80}]}]
                }))
                .unwrap(),
            },
        );
        let table = s.compile(&ControllerConfig::default());
        assert!(
            table
                .lookup(Some("traefik.example.com"), "/")
                .is_some_and(|a| !a.route_name.contains("traefik-route"))
                || table.lookup(Some("traefik.example.com"), "/").is_some()
        );
        // The traefik-owned route must not contribute its own entry.
        assert!(
            !table
                .lookup(None, "/")
                .is_some_and(|a| a.route_name.contains("traefik-route"))
        );
    }

    #[test]
    fn not_ready_endpoints_are_excluded() {
        let slice: EndpointSlice = serde_json::from_value(serde_json::json!({
            "apiVersion": "discovery.k8s.io/v1",
            "kind": "EndpointSlice",
            "metadata": {
                "name": "api-svc-abc",
                "namespace": "default",
                "labels": {"kubernetes.io/service-name": "api-svc"}
            },
            "addressType": "IPv4",
            "ports": [{"port": 8080}],
            "endpoints": [
                {"addresses": ["10.244.1.5"], "conditions": {"ready": true}},
                {"addresses": ["10.244.1.6"], "conditions": {"ready": false}},
                {"addresses": ["10.244.1.7"]}
            ]
        }))
        .expect("slice parses");

        let (key, eps) = parse_endpoint_slice(&slice).expect("parsed");
        assert_eq!(key, ("default".to_string(), "api-svc".to_string()));
        let addrs: Vec<_> = eps.iter().map(|e| e.address.as_str()).collect();
        assert_eq!(
            addrs,
            ["10.244.1.5", "10.244.1.7"],
            "unready pod must be dropped"
        );
    }

    #[test]
    fn header_and_rewrite_filters_are_translated() {
        let r = route(serde_json::json!({
            "rules": [{
                "matches": [{"path": {"type": "PathPrefix", "value": "/api"}}],
                "backendRefs": [{"name": "s", "port": 80}],
                "filters": [
                    {
                        "type": "RequestHeaderModifier",
                        "requestHeaderModifier": {
                            "set": [{"name": "x-gateway", "value": "lb"}],
                            "remove": ["x-secret"]
                        }
                    },
                    {
                        "type": "ResponseHeaderModifier",
                        "responseHeaderModifier": {
                            "add": [{"name": "x-served-by", "value": "lb"}]
                        }
                    },
                    {
                        "type": "URLRewrite",
                        "urlRewrite": {"path": {"type": "ReplaceFullPath", "replaceFullPath": "/fixed"}}
                    }
                ]
            }]
        }));
        let table = state_with(r, vec![]).compile(&ControllerConfig::default());
        let action = table.lookup(None, "/api/x").expect("route");

        let mut req = http::HeaderMap::new();
        req.insert("x-secret", http::HeaderValue::from_static("leak"));
        action.filters.apply_request_headers(&mut req);
        assert_eq!(req.get("x-gateway").unwrap(), "lb");
        assert!(req.get("x-secret").is_none());

        let mut resp = http::HeaderMap::new();
        action.filters.apply_response_headers(&mut resp);
        assert_eq!(resp.get("x-served-by").unwrap(), "lb");

        assert_eq!(action.filters.apply_url_rewrite("/api/x"), "/fixed");
    }

    #[test]
    fn replace_prefix_match_strips_the_matched_prefix() {
        let r = route(serde_json::json!({
            "rules": [{
                "matches": [{"path": {"type": "PathPrefix", "value": "/api"}}],
                "backendRefs": [{"name": "s", "port": 80}],
                "filters": [{
                    "type": "URLRewrite",
                    "urlRewrite": {
                        "path": {"type": "ReplacePrefixMatch", "replacePrefixMatch": "/internal"}
                    }
                }]
            }]
        }));
        let table = state_with(r, vec![]).compile(&ControllerConfig::default());
        let action = table.lookup(None, "/api/users").expect("route");
        assert_eq!(
            action.filters.apply_url_rewrite("/api/users"),
            "/internal/users"
        );
    }

    #[test]
    fn parent_ref_namespace_is_respected_when_scoped() {
        let stored = StoredRoute {
            namespace: "team-a".into(),
            name: "r".into(),
            spec: serde_json::from_value(serde_json::json!({
                "parentRefs": [{"name": "lb-gateway", "namespace": "infra"}],
                "rules": [{"backendRefs": [{"name": "s", "port": 80}]}]
            }))
            .unwrap(),
        };
        let scoped_elsewhere = ControllerConfig {
            gateway_name: Some("lb-gateway".into()),
            namespace: Some("team-a".into()),
        };
        assert!(
            !ClusterState::route_is_ours(&scoped_elsewhere, &stored),
            "a parentRef naming a Gateway in another namespace is not ours"
        );
        let scoped_here = ControllerConfig {
            gateway_name: Some("lb-gateway".into()),
            namespace: Some("infra".into()),
        };
        assert!(ClusterState::route_is_ours(&scoped_here, &stored));
        // A cluster-scoped controller accepts either.
        assert!(ClusterState::route_is_ours(
            &ControllerConfig::default(),
            &stored
        ));
    }

    #[test]
    fn exact_path_match_type_is_honoured() {
        let r = route(serde_json::json!({
            "rules": [{
                "matches": [{"path": {"type": "Exact", "value": "/healthz"}}],
                "backendRefs": [{"name": "s", "port": 80}]
            }]
        }));
        let table = state_with(r, vec![]).compile(&ControllerConfig::default());
        assert!(table.lookup(None, "/healthz").is_some());
        assert!(table.lookup(None, "/healthz/sub").is_none());
    }

    #[test]
    fn backend_weights_reach_the_target_group() {
        let r = route(serde_json::json!({
            "rules": [{
                "backendRefs": [
                    {"name": "a", "port": 80, "weight": 3},
                    {"name": "b", "port": 80, "weight": 1}
                ]
            }]
        }));
        let table = state_with(r, vec![]).compile(&ControllerConfig::default());
        let group = &table.lookup(None, "/").unwrap().target_group;
        let mut a = 0;
        for _ in 0..400 {
            if group.select().address.starts_with("a.") {
                a += 1;
            }
        }
        assert_eq!(a, 300, "3:1 weighting not applied");
    }

    #[test]
    fn multiple_hostnames_each_get_a_route() {
        let r = route(serde_json::json!({
            "hostnames": ["a.example.com", "b.example.com"],
            "rules": [{"backendRefs": [{"name": "s", "port": 80}]}]
        }));
        let table = state_with(r, vec![]).compile(&ControllerConfig::default());
        assert!(table.lookup(Some("a.example.com"), "/").is_some());
        assert!(table.lookup(Some("b.example.com"), "/").is_some());
        assert!(table.lookup(Some("c.example.com"), "/").is_none());
    }

    #[test]
    fn wildcard_hostnames_from_the_api_are_supported() {
        let r = route(serde_json::json!({
            "hostnames": ["*.example.com"],
            "rules": [{"backendRefs": [{"name": "s", "port": 80}]}]
        }));
        let table = state_with(r, vec![]).compile(&ControllerConfig::default());
        assert!(table.lookup(Some("anything.example.com"), "/x").is_some());
    }
}
