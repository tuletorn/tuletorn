//! Hyper 1.x data-plane server.
//!
//! # Hot path
//!
//! One request costs: a wait-free `ArcSwap` load, a radix-trie walk, a relaxed
//! `fetch_add` to pick a backend, and a `Uri` reassembly that reuses the
//! incoming `PathAndQuery` and the endpoint's cached `Authority`. Neither of
//! the last two allocates unless the route rewrites the path, so a plain
//! forward does no heap work at all beyond what Hyper's own buffers require.
//!
//! # Accept path
//!
//! `SO_REUSEPORT` gives every worker its own listening socket on the same port,
//! so the kernel load-balances incoming connections across accept queues. A
//! single shared listener serialises every accept through one queue and shows
//! up as p99 jitter at the connection counts in plan §8.

use crate::client::{BoxedBody, UpstreamClient, empty_body};
use http::uri::{Authority, PathAndQuery, Scheme};
use http::{Request, Response, StatusCode, Uri, Version, header};
use http_body_util::BodyExt;
use hyper::service::service_fn;
use hyper_util::rt::{TokioExecutor, TokioIo};
use hyper_util::server::conn::auto::Builder as AutoConnBuilder;
use lb_core::{SharedRouteTable, proxy};
use socket2::{Domain, Protocol, SockRef, Socket, Type};
use std::net::{IpAddr, SocketAddr};
use std::sync::Arc;
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::watch;
use tracing::{debug, error, info, trace};

/// Listen backlog. Deep enough that a 25k-connection ramp does not overflow the
/// SYN queue and get counted as proxy latency.
const LISTEN_BACKLOG: i32 = 16_384;

/// Server tuning.
#[derive(Debug, Clone)]
pub struct ServerConfig {
    pub listen_addr: SocketAddr,
    /// Accept loops, each with its own `SO_REUSEPORT` socket.
    /// Defaults to the logical CPU count so every candidate gets the whole box.
    pub workers: usize,
    /// Send `X-Forwarded-For` / `X-Forwarded-Proto`.
    pub forwarded_headers: bool,
    /// HTTP/1 read buffer per connection.
    pub h1_max_buf_size: usize,
    /// HTTP/2 initial stream window.
    pub h2_stream_window: u32,
    /// HTTP/2 initial connection window.
    pub h2_connection_window: u32,
}

impl ServerConfig {
    #[must_use]
    pub fn new(listen_addr: SocketAddr) -> Self {
        Self {
            listen_addr,
            workers: num_cpus::get(),
            forwarded_headers: true,
            h1_max_buf_size: 64 * 1024,
            // 1 MiB per stream / 4 MiB per connection: the 1 MB payload in plan
            // §8 must not stall on flow control mid-body.
            h2_stream_window: 1024 * 1024,
            h2_connection_window: 4 * 1024 * 1024,
        }
    }
}

/// Hyper reverse-proxy engine.
pub struct ProxyHyper {
    routes: Arc<SharedRouteTable>,
    upstream: UpstreamClient,
    config: ServerConfig,
}

impl ProxyHyper {
    #[must_use]
    pub fn new(routes: Arc<SharedRouteTable>, listen_addr: SocketAddr) -> Self {
        Self::with_config(
            routes,
            UpstreamClient::default(),
            ServerConfig::new(listen_addr),
        )
    }

    #[must_use]
    pub fn with_config(
        routes: Arc<SharedRouteTable>,
        upstream: UpstreamClient,
        config: ServerConfig,
    ) -> Self {
        Self {
            routes,
            upstream,
            config,
        }
    }

    /// Bind a listener with `SO_REUSEPORT`, so N workers can share one port.
    fn bind_reuseport(addr: SocketAddr) -> std::io::Result<TcpListener> {
        let domain = if addr.is_ipv4() {
            Domain::IPV4
        } else {
            Domain::IPV6
        };
        let socket = Socket::new(domain, Type::STREAM, Some(Protocol::TCP))?;
        socket.set_reuse_address(true)?;
        // The whole point: each worker gets its own accept queue for this port.
        socket.set_reuse_port(true)?;
        socket.set_tcp_nodelay(true)?;
        socket.set_nonblocking(true)?;
        socket.bind(&addr.into())?;
        socket.listen(LISTEN_BACKLOG)?;
        TcpListener::from_std(std::net::TcpListener::from(socket))
    }

    /// Run until `shutdown_rx` goes true.
    pub async fn run(self, shutdown_rx: watch::Receiver<bool>) -> Result<(), anyhow::Error> {
        let workers = self.config.workers.max(1);
        let routes = self.routes;
        let upstream = self.upstream;
        let config = Arc::new(self.config);

        let mut builder = AutoConnBuilder::new(TokioExecutor::new());
        builder
            .http1()
            .max_buf_size(config.h1_max_buf_size)
            .pipeline_flush(true);
        builder
            .http2()
            .initial_stream_window_size(config.h2_stream_window)
            .initial_connection_window_size(config.h2_connection_window)
            .adaptive_window(false)
            .max_concurrent_streams(1024);
        let builder = Arc::new(builder);

        info!(
            addr = %config.listen_addr,
            workers,
            "ProxyHyper listening (SO_REUSEPORT)"
        );

        let mut handles = Vec::with_capacity(workers);
        for worker_id in 0..workers {
            let listener = Self::bind_reuseport(config.listen_addr)?;
            handles.push(tokio::spawn(accept_loop(
                worker_id,
                listener,
                routes.clone(),
                upstream.clone(),
                builder.clone(),
                config.clone(),
                shutdown_rx.clone(),
            )));
        }

        for h in handles {
            let _ = h.await;
        }
        info!("ProxyHyper stopped");
        Ok(())
    }
}

#[allow(clippy::too_many_arguments)]
async fn accept_loop(
    worker_id: usize,
    listener: TcpListener,
    routes: Arc<SharedRouteTable>,
    upstream: UpstreamClient,
    builder: Arc<AutoConnBuilder<TokioExecutor>>,
    config: Arc<ServerConfig>,
    mut shutdown_rx: watch::Receiver<bool>,
) {
    debug!(worker = worker_id, "accept loop started");
    loop {
        tokio::select! {
            accepted = listener.accept() => {
                let (stream, peer) = match accepted {
                    Ok(v) => v,
                    Err(err) => {
                        // EMFILE under a connection-density sweep is expected;
                        // yield rather than spinning the CPU on a hot error.
                        error!(worker = worker_id, %err, "accept failed");
                        tokio::task::yield_now().await;
                        continue;
                    }
                };
                tune_stream(&stream);

                let routes = routes.clone();
                let upstream = upstream.clone();
                let builder = builder.clone();
                let config = config.clone();

                tokio::spawn(async move {
                    let service = service_fn(move |req: Request<hyper::body::Incoming>| {
                        let routes = routes.clone();
                        let upstream = upstream.clone();
                        let config = config.clone();
                        async move { handle_request(req, peer.ip(), &routes, &upstream, &config).await }
                    });

                    if let Err(err) = builder
                        .serve_connection_with_upgrades(TokioIo::new(stream), service)
                        .await
                    {
                        trace!(%peer, error = %err, "connection closed");
                    }
                });
            }
            _ = shutdown_rx.changed() => {
                if *shutdown_rx.borrow() {
                    debug!(worker = worker_id, "accept loop shutting down");
                    return;
                }
            }
        }
    }
}

fn tune_stream(stream: &TcpStream) {
    let _ = stream.set_nodelay(true);
    // Keepalive so half-open connections from a killed load generator are
    // reaped instead of counting against the connection-density measurement.
    let sock = SockRef::from(stream);
    let _ = sock.set_keepalive(true);
}

/// Handle one request end to end.
async fn handle_request(
    req: Request<hyper::body::Incoming>,
    peer_ip: IpAddr,
    routes: &SharedRouteTable,
    upstream: &UpstreamClient,
    config: &ServerConfig,
) -> Result<Response<BoxedBody>, hyper::Error> {
    // HTTP/2 carries the authority in :authority, HTTP/1.1 in Host.
    let host = req.uri().host().or_else(|| {
        req.headers()
            .get(header::HOST)
            .and_then(|h| h.to_str().ok())
    });
    let path = req.uri().path();

    // 1. Wait-free route lookup.
    let table = routes.load();
    let Some(action) = table.lookup(host, path) else {
        return Ok(not_found());
    };

    // 2. Pick a backend.
    let target = action.target_group.select();
    let Some(authority) = target.authority() else {
        error!(address = %target.address, "backend address is not a valid URI authority");
        return Ok(status_only(StatusCode::BAD_GATEWAY));
    };

    // 3. Build the upstream URI.
    //
    // No rewrite is the common case, and there the incoming `PathAndQuery` is
    // reused as-is: `Uri` is `Bytes`-backed, so this clone is a refcount bump
    // rather than a string format + reparse.
    let (mut parts, body) = req.into_parts();
    let path_and_query = if action.filters.rewrites_path() {
        let rewritten = action.filters.apply_url_rewrite(parts.uri.path());
        let with_query = match parts.uri.query() {
            Some(q) => {
                let mut s = String::with_capacity(rewritten.len() + q.len() + 1);
                s.push_str(&rewritten);
                s.push('?');
                s.push_str(q);
                s
            }
            None => rewritten.into_owned(),
        };
        match PathAndQuery::try_from(with_query) {
            Ok(pq) => pq,
            Err(err) => {
                error!(%err, "rewritten path is not a valid URI path");
                return Ok(status_only(StatusCode::INTERNAL_SERVER_ERROR));
            }
        }
    } else {
        parts
            .uri
            .path_and_query()
            .cloned()
            .unwrap_or_else(|| PathAndQuery::from_static("/"))
    };

    let mut uri_parts = http::uri::Parts::default();
    uri_parts.scheme = Some(Scheme::HTTP);
    uri_parts.authority = Some(authority.clone());
    uri_parts.path_and_query = Some(path_and_query);
    let upstream_uri = match Uri::from_parts(uri_parts) {
        Ok(uri) => uri,
        Err(err) => {
            error!(%err, "failed to assemble upstream URI");
            return Ok(status_only(StatusCode::INTERNAL_SERVER_ERROR));
        }
    };

    // 4. Header hygiene. A proxy must not forward hop-by-hop headers
    //    (RFC 9110 §7.6.1); Traefik strips them, so we must too or the
    //    comparison is not like-for-like.
    proxy::strip_hop_by_hop(&mut parts.headers);
    if config.forwarded_headers {
        proxy::append_forwarded_for(&mut parts.headers, peer_ip);
        proxy::set_forwarded_proto(&mut parts.headers, "http");
    }
    action.filters.apply_request_headers(&mut parts.headers);

    // The downstream protocol version must not leak upstream: an h2c client
    // would otherwise force an HTTP/2 request onto an HTTP/1.1 upstream pool
    // and get `UserUnsupportedVersion` back as a 502.
    parts.version = if upstream.is_http2_upstream() {
        Version::HTTP_2
    } else {
        Version::HTTP_11
    };
    // HTTP/2 has no Host header; HTTP/1.1 requires one matching the authority.
    if parts.version == Version::HTTP_11 {
        if let Ok(value) = header::HeaderValue::from_str(authority.as_str()) {
            parts.headers.insert(header::HOST, value);
        }
    } else {
        parts.headers.remove(header::HOST);
    }
    parts.uri = upstream_uri;

    // Release the route table guard before awaiting: holding it across the
    // upstream round trip would pin an old table for the whole request and
    // delay reclamation during a churn burst.
    let filters = action.filters.clone();
    let target_addr = target.address.clone();
    drop(table);

    // 5. Forward. Bodies stream through untouched.
    let out_req = Request::from_parts(parts, body.boxed());
    match upstream.forward(out_req).await {
        Ok(mut resp) => {
            proxy::strip_hop_by_hop(resp.headers_mut());
            filters.apply_response_headers(resp.headers_mut());
            Ok(resp)
        }
        Err(err) => {
            error!(error = %err, target = %target_addr, "upstream request failed");
            Ok(status_only(StatusCode::BAD_GATEWAY))
        }
    }
}

fn not_found() -> Response<BoxedBody> {
    Response::builder()
        .status(StatusCode::NOT_FOUND)
        .header(header::CONTENT_TYPE, "text/plain")
        .header(header::CONTENT_LENGTH, "0")
        .body(empty_body())
        .expect("static response is valid")
}

fn status_only(status: StatusCode) -> Response<BoxedBody> {
    Response::builder()
        .status(status)
        .header(header::CONTENT_LENGTH, "0")
        .body(empty_body())
        .expect("static response is valid")
}

/// Exposed for tests: the authority a backend address resolves to.
#[must_use]
pub fn authority_of(address: &str) -> Option<Authority> {
    Authority::try_from(address).ok()
}

#[cfg(test)]
mod tests {
    use super::*;

    // `TcpListener::from_std` registers with the Tokio reactor, so this needs
    // a runtime even though nothing here is awaited.
    #[tokio::test]
    async fn reuseport_allows_several_listeners_on_one_port() {
        let addr: SocketAddr = "127.0.0.1:0".parse().unwrap();
        let first = ProxyHyper::bind_reuseport(addr).expect("first bind");
        let bound = first.local_addr().unwrap();
        // The whole accept-sharding design depends on this succeeding.
        let second = ProxyHyper::bind_reuseport(bound).expect("second bind on the same port");
        assert_eq!(second.local_addr().unwrap().port(), bound.port());
    }

    #[test]
    fn default_worker_count_uses_every_core() {
        let cfg = ServerConfig::new("127.0.0.1:0".parse().unwrap());
        assert_eq!(cfg.workers, num_cpus::get());
    }

    #[test]
    fn authority_parsing_accepts_hosts_and_ips() {
        assert!(authority_of("10.0.0.1:8080").is_some());
        assert!(authority_of("svc.default.svc.cluster.local:80").is_some());
        assert!(authority_of("not a host").is_none());
    }
}
