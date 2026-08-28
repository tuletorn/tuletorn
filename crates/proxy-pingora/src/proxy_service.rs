//! Pingora `ProxyHttp` implementation with Gateway API routing.

use async_trait::async_trait;
use lb_core::{SharedRouteTable, proxy};
use pingora_core::Result;
use pingora_core::apps::HttpServerOptions;
use pingora_core::listeners::TcpSocketOptions;
use pingora_core::server::Server;
use pingora_core::server::configuration::ServerConf;
use pingora_core::upstreams::peer::HttpPeer;
use pingora_http::{RequestHeader, ResponseHeader};
use pingora_proxy::{ProxyHttp, ProxyServiceBuilder, Session};
use std::net::SocketAddr;
use std::sync::Arc;
use tracing::{error, info};

/// Server tuning.
#[derive(Debug, Clone)]
pub struct ServiceConfig {
    pub listen_addr: SocketAddr,
    /// Worker threads for the proxy service.
    ///
    /// Pingora's `ServerConf` default is **1**. Left at the default, this
    /// candidate would be benchmarked single-threaded against a Hyper candidate
    /// using every core, which is the single biggest fairness bug available in
    /// this comparison. Defaults to the logical CPU count here.
    pub threads: usize,
    /// Idle upstream connections kept per backend.
    pub upstream_keepalive_pool_size: usize,
    /// Send `X-Forwarded-For` / `X-Forwarded-Proto`.
    pub forwarded_headers: bool,
    /// Accept HTTP/2 over cleartext.
    ///
    /// Off by default in Pingora, which would make this candidate return a
    /// connection error for every request in the HTTP/2 half of the plan §8
    /// sweep while the Hyper candidate served them.
    pub h2c: bool,
    /// Bind the listener with `SO_REUSEPORT`, matching the other candidates.
    pub so_reuseport: bool,
}

impl ServiceConfig {
    #[must_use]
    pub fn new(listen_addr: SocketAddr) -> Self {
        Self {
            listen_addr,
            threads: num_cpus::get(),
            upstream_keepalive_pool_size: 8_192,
            forwarded_headers: true,
            h2c: true,
            so_reuseport: true,
        }
    }
}

/// Per-request context.
///
/// Deliberately holds no `String`s: the previous version cloned the route name
/// and the upstream address into the context on every request and then never
/// read either, which is two heap allocations per request on the hot path.
#[derive(Default)]
pub struct ProxyCtx {
    /// Whether the request matched a route, for `logging`.
    pub matched: bool,
}

/// Pingora-based HTTP proxy service.
pub struct ProxyPingora {
    pub routes: Arc<SharedRouteTable>,
    forwarded_headers: bool,
}

impl ProxyPingora {
    #[must_use]
    pub fn new(routes: Arc<SharedRouteTable>) -> Self {
        Self {
            routes,
            forwarded_headers: true,
        }
    }

    #[must_use]
    pub fn with_forwarded_headers(mut self, enabled: bool) -> Self {
        self.forwarded_headers = enabled;
        self
    }

    /// Build and run the Pingora server. Blocks until the process is signalled.
    pub fn run_server(self, config: &ServiceConfig) -> Result<()> {
        let conf = ServerConf {
            // Pingora's own default is 1; see `ServiceConfig::threads`.
            threads: config.threads.max(1),
            upstream_keepalive_pool_size: config.upstream_keepalive_pool_size,
            work_stealing: true,
            ..ServerConf::default()
        };

        let mut server = Server::new_with_opt_and_conf(None, conf);
        server.bootstrap();

        let mut service = ProxyServiceBuilder::new(&server.configuration, self)
            .name("lb-proxy-pingora")
            .server_options({
                // `HttpServerOptions` is #[non_exhaustive], so it must be
                // built by mutation rather than with struct-update syntax.
                let mut opts = HttpServerOptions::default();
                opts.h2c = config.h2c;
                opts
            })
            .build();

        // SO_REUSEPORT so the kernel shards the accept queue, the same as the
        // Hyper and Monoio candidates.
        service.add_tcp_with_settings(&config.listen_addr.to_string(), {
            let mut opts = TcpSocketOptions::default();
            opts.so_reuseport = Some(config.so_reuseport);
            opts
        });

        info!(
            addr = %config.listen_addr,
            threads = config.threads,
            h2c = config.h2c,
            "ProxyPingora starting"
        );
        server.add_service(service);
        server.run_forever();
    }
}

#[async_trait]
impl ProxyHttp for ProxyPingora {
    type CTX = ProxyCtx;

    fn new_ctx(&self) -> Self::CTX {
        ProxyCtx::default()
    }

    async fn upstream_peer(
        &self,
        session: &mut Session,
        ctx: &mut Self::CTX,
    ) -> Result<Box<HttpPeer>> {
        let req = session.req_header();
        // HTTP/2 puts the authority in the URI; HTTP/1.1 in the Host header.
        let host = req
            .uri
            .host()
            .or_else(|| req.headers.get("host").and_then(|h| h.to_str().ok()));
        let path = req.uri.path();

        let table = self.routes.load();
        let Some(action) = table.lookup(host, path) else {
            ctx.matched = false;
            // Pingora has no "no upstream" outcome here; `request_filter` has
            // already short-circuited unmatched requests with a 404, so this
            // branch is only reachable if the table changed mid-request.
            return Err(pingora_core::Error::explain(
                pingora_core::ErrorType::HTTPStatus(404),
                "no route matched",
            ));
        };
        ctx.matched = true;

        let target = action.target_group.select();
        Ok(Box::new(HttpPeer::new(
            target.address.as_str(),
            false,
            String::new(),
        )))
    }

    /// Short-circuit unmatched requests before any upstream work happens.
    async fn request_filter(&self, session: &mut Session, _ctx: &mut Self::CTX) -> Result<bool> {
        let req = session.req_header();
        let host = req
            .uri
            .host()
            .or_else(|| req.headers.get("host").and_then(|h| h.to_str().ok()));
        let path = req.uri.path();

        if self.routes.load().lookup(host, path).is_some() {
            return Ok(false);
        }
        let mut resp = ResponseHeader::build(404, Some(1))?;
        resp.insert_header("content-length", "0")?;
        session.write_response_header(Box::new(resp), true).await?;
        // `true` tells Pingora the response is complete: stop here.
        Ok(true)
    }

    /// Apply routing filters and proxy hygiene to the upstream request.
    async fn upstream_request_filter(
        &self,
        session: &mut Session,
        upstream_request: &mut RequestHeader,
        _ctx: &mut Self::CTX,
    ) -> Result<()> {
        let (rewritten_path, filters) = {
            let table = self.routes.load();
            let host = upstream_request.uri.host().or_else(|| {
                upstream_request
                    .headers
                    .get("host")
                    .and_then(|h| h.to_str().ok())
            });
            let path = upstream_request.uri.path();
            match table.lookup(host, path) {
                Some(action) => {
                    let rewritten = if action.filters.rewrites_path() {
                        Some(action.filters.apply_url_rewrite(path).into_owned())
                    } else {
                        None
                    };
                    (rewritten, action.filters.clone())
                }
                None => (None, lb_core::RouteFilters::default()),
            }
        };

        if let Some(path) = rewritten_path {
            let query = upstream_request.uri.query();
            let full = match query {
                Some(q) => format!("{path}?{q}"),
                None => path,
            };
            match full.parse::<http::Uri>() {
                Ok(uri) => upstream_request.set_uri(uri),
                Err(err) => error!(%err, "rewritten path is not a valid URI"),
            }
        }

        // Same hop-by-hop hygiene as the Hyper candidate, so the two do equal
        // work per request (RFC 9110 §7.6.1).
        strip_hop_by_hop_request(upstream_request);

        if self.forwarded_headers {
            let chain = session
                .client_addr()
                .and_then(|addr| addr.as_inet())
                .map(
                    |peer| match upstream_request.headers.get(&proxy::FORWARDED_FOR) {
                        Some(existing) => match existing.to_str() {
                            Ok(prev) => format!("{prev}, {}", peer.ip()),
                            Err(_) => peer.ip().to_string(),
                        },
                        None => peer.ip().to_string(),
                    },
                );
            if let Some(chain) = chain {
                let _ = upstream_request.insert_header("x-forwarded-for", chain);
            }
            let _ = upstream_request.insert_header("x-forwarded-proto", "http");
        }
        apply_request_filters(upstream_request, &filters);
        Ok(())
    }

    async fn response_filter(
        &self,
        session: &mut Session,
        upstream_response: &mut ResponseHeader,
        _ctx: &mut Self::CTX,
    ) -> Result<()> {
        let filters = {
            let table = self.routes.load();
            let req = session.req_header();
            let host = req
                .uri
                .host()
                .or_else(|| req.headers.get("host").and_then(|h| h.to_str().ok()));
            table
                .lookup(host, req.uri.path())
                .map(|a| a.filters.clone())
                .unwrap_or_default()
        };
        strip_hop_by_hop_response(upstream_response);
        apply_response_filters(upstream_response, &filters);
        Ok(())
    }
}

/// Remove hop-by-hop headers from a Pingora request header.
///
/// Pingora's `RequestHeader` keeps a case-preserving name map beside the
/// `HeaderMap`. Mutating the map directly desynchronises the two and trips an
/// assertion inside `pingora-http` on the next header iteration, which drops
/// the connection with no response at all. Every mutation must therefore go
/// through Pingora's own `insert_header` / `remove_header`.
fn strip_hop_by_hop_request(req: &mut RequestHeader) {
    let nominated = proxy::connection_nominated(&req.headers);
    let _ = req.remove_header(&http::header::CONNECTION);
    for name in proxy::HOP_BY_HOP {
        let _ = req.remove_header(&name);
    }
    for name in nominated {
        let _ = req.remove_header(&name);
    }
}

/// The response-side equivalent of [`strip_hop_by_hop_request`].
fn strip_hop_by_hop_response(resp: &mut ResponseHeader) {
    let nominated = proxy::connection_nominated(&resp.headers);
    let _ = resp.remove_header(&http::header::CONNECTION);
    for name in proxy::HOP_BY_HOP {
        let _ = resp.remove_header(&name);
    }
    for name in nominated {
        let _ = resp.remove_header(&name);
    }
}

/// Apply route filters through Pingora's header API.
fn apply_request_filters(req: &mut RequestHeader, filters: &lb_core::RouteFilters) {
    if filters.is_noop() {
        return;
    }
    // Build the target set in a plain HeaderMap, then replay the difference
    // through Pingora's API so its name map stays in step.
    let mut staged = req.headers.clone();
    filters.apply_request_headers(&mut staged);
    replay_request(req, &staged);
}

/// Apply route filters to a response through Pingora's header API.
fn apply_response_filters(resp: &mut ResponseHeader, filters: &lb_core::RouteFilters) {
    if filters.is_noop() {
        return;
    }
    let mut staged = resp.headers.clone();
    filters.apply_response_headers(&mut staged);
    let removed: Vec<_> = resp
        .headers
        .keys()
        .filter(|k| !staged.contains_key(*k))
        .cloned()
        .collect();
    for name in removed {
        let _ = resp.remove_header(&name);
    }
    for name in staged.keys() {
        let mut values = staged.get_all(name).iter();
        if let Some(first) = values.next() {
            let _ = resp.insert_header(name.clone(), first.clone());
        }
        for extra in values {
            let _ = resp.append_header(name.clone(), extra.clone());
        }
    }
}

fn replay_request(req: &mut RequestHeader, staged: &http::HeaderMap) {
    let removed: Vec<_> = req
        .headers
        .keys()
        .filter(|k| !staged.contains_key(*k))
        .cloned()
        .collect();
    for name in removed {
        let _ = req.remove_header(&name);
    }
    for name in staged.keys() {
        let mut values = staged.get_all(name).iter();
        if let Some(first) = values.next() {
            let _ = req.insert_header(name.clone(), first.clone());
        }
        for extra in values {
            let _ = req.append_header(name.clone(), extra.clone());
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_thread_count_uses_every_core() {
        let cfg = ServiceConfig::new("127.0.0.1:0".parse().unwrap());
        assert_eq!(
            cfg.threads,
            num_cpus::get(),
            "Pingora's own ServerConf default is 1 thread; leaving it there would \
             benchmark this candidate single-threaded"
        );
    }

    fn request_with(headers: &[(&str, &str)]) -> RequestHeader {
        let mut req = RequestHeader::build("GET", b"/", None).expect("request builds");
        for (name, value) in headers {
            req.append_header(name.to_string(), *value)
                .expect("header appends");
        }
        req
    }

    /// Regression: mutating `req.headers` directly desynchronises Pingora's
    /// case-preserving name map and panics on the next iteration, which the
    /// client observes as a closed connection with no response.
    #[test]
    fn stripping_leaves_the_header_map_iterable() {
        let mut req = request_with(&[
            ("connection", "keep-alive, x-custom-hop"),
            ("keep-alive", "timeout=5"),
            ("proxy-connection", "keep-alive"),
            ("te", "trailers"),
            ("upgrade", "h2c"),
            ("x-custom-hop", "secret"),
            ("x-keep", "yes"),
        ]);
        strip_hop_by_hop_request(&mut req);

        // Iterating is what trips the assertion when the maps disagree.
        let names: Vec<String> = req.headers.keys().map(|k| k.to_string()).collect();
        assert_eq!(names, ["x-keep"], "surviving headers: {names:?}");
        assert!(req.headers.get("x-custom-hop").is_none());
    }

    #[test]
    fn filters_are_applied_through_the_pingora_api() {
        let mut req = request_with(&[("x-drop", "1")]);
        let filters = lb_core::RouteFilters::new(
            &[
                lb_core::HeaderModifier::Set {
                    name: "x-gateway".into(),
                    value: "lb".into(),
                },
                lb_core::HeaderModifier::Remove {
                    name: "x-drop".into(),
                },
            ],
            &[],
            None,
        );
        apply_request_filters(&mut req, &filters);
        assert_eq!(req.headers.get("x-gateway").unwrap(), "lb");
        assert!(req.headers.get("x-drop").is_none());
        // Must still iterate cleanly.
        assert!(req.headers.keys().count() >= 1);
    }

    #[test]
    fn noop_filters_do_not_touch_the_header_map() {
        let mut req = request_with(&[("x-keep", "1")]);
        apply_request_filters(&mut req, &lb_core::RouteFilters::default());
        assert_eq!(req.headers.get("x-keep").unwrap(), "1");
    }

    #[test]
    fn h2c_and_reuseport_are_on_by_default() {
        let cfg = ServiceConfig::new("127.0.0.1:0".parse().unwrap());
        assert!(cfg.h2c, "plan §8 sweeps HTTP/2; Pingora defaults h2c off");
        assert!(
            cfg.so_reuseport,
            "must shard accepts like the other candidates"
        );
    }

    #[test]
    fn context_is_allocation_free() {
        assert_eq!(std::mem::size_of::<ProxyCtx>(), 1);
    }
}
