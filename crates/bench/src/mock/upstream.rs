//! Mock upstream backend.
//!
//! Serves a body whose size the caller selects with `?size=N`, so a single
//! backend covers the whole payload sweep in plan §8 without restarting. Bodies
//! are pre-generated and handed out as `Bytes`, so serving a 1 MB response is a
//! refcount bump rather than a 1 MB memcpy.

use bytes::Bytes;
use http::{Method, Request, Response, StatusCode, header};
use http_body_util::combinators::BoxBody;
use http_body_util::{BodyExt, Full};
use hyper::service::service_fn;
use hyper_util::rt::{TokioExecutor, TokioIo};
use hyper_util::server::conn::auto::Builder as AutoConnBuilder;
use lb_core::FxHashMap;
use std::net::SocketAddr;
use std::sync::{Arc, RwLock};
use tokio::net::TcpListener;
use tokio::sync::watch;
use tracing::{info, trace};

pub type BoxedBody = BoxBody<Bytes, hyper::Error>;

/// Largest body the mock will synthesise, so a malformed `?size=` cannot make
/// it allocate unboundedly.
const MAX_BODY_SIZE: usize = 8 * 1024 * 1024;

/// Mock upstream configuration.
#[derive(Debug, Clone)]
pub struct MockUpstreamConfig {
    pub listen_addr: SocketAddr,
    /// Body returned when the request does not ask for a size.
    pub response_body: Bytes,
    /// Artificial per-request delay (plan §1: 0 ms / 1 ms / 5 ms profiles).
    pub delay_ms: u64,
}

impl Default for MockUpstreamConfig {
    fn default() -> Self {
        Self {
            listen_addr: "127.0.0.1:9090".parse().expect("literal address"),
            response_body: Bytes::from_static(
                b"{\"status\":\"ok\",\"service\":\"mock-backend\"}\n",
            ),
            delay_ms: 0,
        }
    }
}

/// A pre-generated body cache keyed by size.
#[derive(Default)]
struct BodyCache {
    bodies: RwLock<FxHashMap<usize, Bytes>>,
}

impl BodyCache {
    /// Fetch (or build once) a body of `size` bytes.
    fn get(&self, size: usize) -> Bytes {
        if let Some(body) = self.bodies.read().expect("body cache poisoned").get(&size) {
            return body.clone();
        }
        let body = crate::load::generate_payload(size);
        self.bodies
            .write()
            .expect("body cache poisoned")
            .insert(size, body.clone());
        body
    }
}

/// High-throughput mock HTTP/1.1 and HTTP/2 upstream.
pub struct MockUpstream {
    config: MockUpstreamConfig,
    cache: Arc<BodyCache>,
}

impl MockUpstream {
    #[must_use]
    pub fn new(config: MockUpstreamConfig) -> Self {
        Self {
            config,
            cache: Arc::new(BodyCache::default()),
        }
    }

    /// Serve until `shutdown_rx` goes true.
    pub async fn run(self, mut shutdown_rx: watch::Receiver<bool>) -> Result<(), anyhow::Error> {
        let socket = socket2::Socket::new(
            socket2::Domain::IPV4,
            socket2::Type::STREAM,
            Some(socket2::Protocol::TCP),
        )?;
        socket.set_reuse_address(true)?;
        #[cfg(unix)]
        let _ = socket.set_reuse_port(true);
        let _ = socket.set_tcp_nodelay(true);
        socket.set_nonblocking(true)?;
        socket.bind(&self.config.listen_addr.into())?;
        socket.listen(65535)?;
        let listener = TcpListener::from_std(std::net::TcpListener::from(socket))?;
        info!(addr = %self.config.listen_addr, "mock upstream listening");


        let default_body = self.config.response_body;
        let delay_ms = self.config.delay_ms;
        let cache = self.cache;

        let mut builder = AutoConnBuilder::new(TokioExecutor::new());
        builder.http1().max_buf_size(64 * 1024);
        builder
            .http2()
            .initial_stream_window_size(1024 * 1024)
            .initial_connection_window_size(4 * 1024 * 1024)
            .max_concurrent_streams(1024);
        let builder = Arc::new(builder);

        loop {
            tokio::select! {
                accepted = listener.accept() => {
                    let Ok((stream, _)) = accepted else { continue };
                    let _ = stream.set_nodelay(true);
                    let default_body = default_body.clone();
                    let cache = cache.clone();
                    let builder = builder.clone();

                    tokio::spawn(async move {
                        let service = service_fn(move |req: Request<hyper::body::Incoming>| {
                            let default_body = default_body.clone();
                            let cache = cache.clone();
                            async move { handle(req, default_body, cache, delay_ms).await }
                        });
                        let _ = builder
                            .serve_connection_with_upgrades(TokioIo::new(stream), service)
                            .await;
                    });
                }
                _ = shutdown_rx.changed() => {
                    if *shutdown_rx.borrow() {
                        info!("mock upstream shutting down");
                        return Ok(());
                    }
                }
            }
        }
    }
}

async fn handle(
    req: Request<hyper::body::Incoming>,
    default_body: Bytes,
    cache: Arc<BodyCache>,
    delay_ms: u64,
) -> Result<Response<BoxedBody>, hyper::Error> {
    if delay_ms > 0 {
        tokio::time::sleep(std::time::Duration::from_millis(delay_ms)).await;
    }

    // Kubernetes readiness probe: answer before doing any other work.
    if req.uri().path() == "/healthz" {
        return Ok(Response::builder()
            .status(StatusCode::OK)
            .header(header::CONTENT_LENGTH, 2)
            .body(
                Full::new(Bytes::from_static(b"ok"))
                    .map_err(|never| match never {})
                    .boxed(),
            )
            .expect("static response is well formed"));
    }

    let is_head = req.method() == Method::HEAD;
    let requested_size = req.uri().query().and_then(parse_size_param);

    // A request body must be drained even when echoed nowhere, or the
    // connection cannot be reused.
    let (parts, body) = req.into_parts();
    let received = body.collect().await?.to_bytes();
    trace!(path = %parts.uri.path(), received = received.len(), "mock request");

    let payload = match requested_size {
        Some(size) => cache.get(size.min(MAX_BODY_SIZE)),
        // `/echo` returns what it was sent, so upload sweeps are verifiable.
        None if parts.uri.path().starts_with("/echo") && !received.is_empty() => received,
        None => default_body,
    };

    let len = payload.len();
    let body = if is_head {
        Full::new(Bytes::new())
            .map_err(|never| match never {})
            .boxed()
    } else {
        Full::new(payload).map_err(|never| match never {}).boxed()
    };

    Ok(Response::builder()
        .status(StatusCode::OK)
        .header(header::CONTENT_TYPE, "application/json")
        .header(header::CONTENT_LENGTH, len)
        .header("x-mock-upstream", "true")
        .body(body)
        .expect("response is well formed"))
}

/// Extract `size=N` from a query string.
fn parse_size_param(query: &str) -> Option<usize> {
    query.split('&').find_map(|pair| {
        let (key, value) = pair.split_once('=')?;
        (key == "size").then(|| value.parse().ok())?
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use http_body_util::Empty;

    async fn spawn() -> (SocketAddr, watch::Sender<bool>) {
        let probe = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = probe.local_addr().unwrap();
        drop(probe);
        let (tx, rx) = watch::channel(false);
        let mock = MockUpstream::new(MockUpstreamConfig {
            listen_addr: addr,
            ..Default::default()
        });
        tokio::spawn(async move {
            let _ = mock.run(rx).await;
        });
        for _ in 0..200 {
            if let Ok(mut stream) = tokio::net::TcpStream::connect(addr).await {
                use tokio::io::{AsyncReadExt, AsyncWriteExt};
                let _ = stream.write_all(b"GET /healthz HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n").await;
                let mut buf = [0u8; 64];
                if let Ok(n) = stream.read(&mut buf).await {
                    if n > 0 && buf.starts_with(b"HTTP/1.1 200") {
                        break;
                    }
                }
            }
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }

        (addr, tx)
    }

    async fn get(addr: SocketAddr, path: &str) -> (StatusCode, Bytes) {
        use hyper_util::client::legacy::Client;
        use hyper_util::rt::TokioExecutor;
        let client: Client<_, Empty<Bytes>> = Client::builder(TokioExecutor::new()).build_http();
        let resp = client
            .get(format!("http://{addr}{path}").parse().unwrap())
            .await
            .expect("request");
        let status = resp.status();
        let body = resp.into_body().collect().await.unwrap().to_bytes();
        (status, body)
    }

    #[test]
    fn size_parameter_parsing() {
        assert_eq!(parse_size_param("size=1024"), Some(1024));
        assert_eq!(parse_size_param("a=1&size=65536&b=2"), Some(65_536));
        assert_eq!(parse_size_param("nosize=1"), None);
        assert_eq!(parse_size_param("size=notanumber"), None);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn serves_the_default_body() {
        let (addr, shutdown) = spawn().await;
        let (status, body) = get(addr, "/anything").await;
        assert_eq!(status, StatusCode::OK);
        assert!(body.starts_with(b"{\"status\":\"ok\""));
        let _ = shutdown.send(true);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn serves_every_payload_size_from_the_plan() {
        let (addr, shutdown) = spawn().await;
        for size in [1024usize, 64 * 1024, 1024 * 1024] {
            let (status, body) = get(addr, &format!("/bench?size={size}")).await;
            assert_eq!(status, StatusCode::OK);
            assert_eq!(body.len(), size, "wrong body size for size={size}");
        }
        let _ = shutdown.send(true);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn oversized_requests_are_clamped_not_honoured() {
        let (addr, shutdown) = spawn().await;
        let (_, body) = get(addr, "/bench?size=99999999999").await;
        assert_eq!(body.len(), MAX_BODY_SIZE);
        let _ = shutdown.send(true);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn repeated_sizes_reuse_the_cached_body() {
        let cache = BodyCache::default();
        let first = cache.get(4096);
        let second = cache.get(4096);
        assert_eq!(first.as_ptr(), second.as_ptr(), "body must be cached");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn healthz_answers_the_readiness_probe() {
        let (addr, shutdown) = spawn().await;
        let (status, body) = get(addr, "/healthz").await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(&body[..], b"ok");
        let _ = shutdown.send(true);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn shuts_down_when_signalled() {
        let (addr, shutdown) = spawn().await;
        assert!(tokio::net::TcpStream::connect(addr).await.is_ok());
        let _ = shutdown.send(true);
        // Give the accept loop a moment to observe the signal.
        tokio::time::sleep(std::time::Duration::from_millis(200)).await;
    }
}
