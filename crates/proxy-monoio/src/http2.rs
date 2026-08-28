//! HTTP/2 (h2c) support for the Monoio data plane.
//!
//! Monoio's I/O is completion-based and its tasks are `!Send`, so neither
//! Hyper's server nor the `h2` crate can be used against a raw `monoio::TcpStream`.
//! Monoio's `poll-io` feature bridges that: [`IntoPollIo`] converts a
//! completion-based stream into one implementing Tokio's `AsyncRead`/`AsyncWrite`,
//! which Hyper accepts once paired with an executor that spawns onto the local
//! runtime instead of a work-stealing one.
//!
//! HTTP/1.1 keeps the hand-written zero-copy path in [`crate::server`]; only
//! HTTP/2 connections take this route. That is the right split: h2 needs HPACK,
//! flow control and stream multiplexing, none of which benefit from owned-buffer
//! I/O the way a byte-relaying h1 proxy does.

use bytes::Bytes;
use http::uri::{PathAndQuery, Scheme};
use http::{Request, Response, StatusCode, Uri, Version, header};
use http_body_util::combinators::BoxBody;
use http_body_util::{BodyExt, Empty};
use hyper::body::Incoming;
use hyper::client::conn::http1 as client_http1;
use hyper::server::conn::http2 as server_http2;
use hyper::service::service_fn;
use hyper_util::rt::TokioIo;
use lb_core::{SharedRouteTable, proxy};
use monoio::io::IntoPollIo;
use monoio::net::TcpStream;
use std::future::Future;
use std::net::IpAddr;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use tracing::{error, trace};

/// The HTTP/2 connection preface. A client that opens with these 24 bytes is
/// speaking h2c with prior knowledge.
pub const H2_PREFACE: &[u8] = b"PRI * HTTP/2.0\r\n\r\nSM\r\n\r\n";

pub type BoxedBody = BoxBody<Bytes, hyper::Error>;

/// Whether `buf` is (or could still become) the HTTP/2 preface.
///
/// Returns `None` when there are not yet enough bytes to decide, so the caller
/// keeps reading rather than mis-parsing a partial preface as HTTP/1.
#[must_use]
pub fn detect_h2(buf: &[u8]) -> Option<bool> {
    let n = buf.len().min(H2_PREFACE.len());
    if buf[..n] != H2_PREFACE[..n] {
        return Some(false);
    }
    if buf.len() >= H2_PREFACE.len() {
        Some(true)
    } else {
        None
    }
}

/// An executor that spawns onto the current Monoio runtime.
///
/// Hyper's HTTP/2 server needs to spawn per-stream tasks. The default Tokio
/// executor would require `Send` futures and a Tokio runtime, neither of which
/// exists on a Monoio worker thread.
#[derive(Clone, Copy, Default)]
pub struct MonoioExecutor;

impl<F> hyper::rt::Executor<F> for MonoioExecutor
where
    F: Future + 'static,
    F::Output: 'static,
{
    fn execute(&self, fut: F) {
        monoio::spawn(async move {
            fut.await;
        });
    }
}

/// A reader that replays already-buffered bytes before delegating to the stream.
///
/// The preface is consumed while sniffing the protocol, but Hyper's h2 server
/// expects to read it itself, so those bytes have to be handed back.
pub struct PrefixedIo<T> {
    prefix: Vec<u8>,
    position: usize,
    inner: T,
}

impl<T> PrefixedIo<T> {
    #[must_use]
    pub fn new(prefix: Vec<u8>, inner: T) -> Self {
        Self {
            prefix,
            position: 0,
            inner,
        }
    }

    /// Bytes of the prefix still to be replayed.
    #[must_use]
    pub fn remaining_prefix(&self) -> usize {
        self.prefix.len().saturating_sub(self.position)
    }
}

impl<T: tokio::io::AsyncRead + Unpin> tokio::io::AsyncRead for PrefixedIo<T> {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut tokio::io::ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        if self.position < self.prefix.len() {
            let available = &self.prefix[self.position..];
            let n = available.len().min(buf.remaining());
            buf.put_slice(&available[..n]);
            self.position += n;
            return Poll::Ready(Ok(()));
        }
        Pin::new(&mut self.inner).poll_read(cx, buf)
    }
}

impl<T: tokio::io::AsyncWrite + Unpin> tokio::io::AsyncWrite for PrefixedIo<T> {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<std::io::Result<usize>> {
        Pin::new(&mut self.inner).poll_write(cx, buf)
    }
    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        Pin::new(&mut self.inner).poll_flush(cx)
    }
    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        Pin::new(&mut self.inner).poll_shutdown(cx)
    }
}

/// Serve one HTTP/2 connection.
///
/// `buffered` holds the bytes already read from the socket while sniffing the
/// protocol, including the preface.
pub async fn serve_connection(
    stream: TcpStream,
    buffered: Vec<u8>,
    peer: IpAddr,
    routes: Arc<SharedRouteTable>,
    forwarded_headers: bool,
) -> Result<(), anyhow::Error> {
    let poll_stream = stream
        .into_poll_io()
        .map_err(|err| anyhow::anyhow!("cannot switch the stream to poll IO: {err}"))?;
    let io = TokioIo::new(PrefixedIo::new(buffered, poll_stream));

    let mut builder = server_http2::Builder::new(MonoioExecutor);
    builder
        // Matched to the Hyper candidate so the two are compared on their
        // runtimes, not on their flow-control windows.
        .initial_stream_window_size(1024 * 1024)
        .initial_connection_window_size(4 * 1024 * 1024)
        .adaptive_window(false)
        .max_concurrent_streams(1024);

    let service = service_fn(move |req: Request<Incoming>| {
        let routes = routes.clone();
        async move { handle(req, peer, routes, forwarded_headers).await }
    });

    builder
        .serve_connection(io, service)
        .await
        .map_err(|err| anyhow::anyhow!("h2 connection error: {err}"))
}

/// Route and forward one HTTP/2 request, speaking HTTP/1.1 upstream.
async fn handle(
    req: Request<Incoming>,
    peer: IpAddr,
    routes: Arc<SharedRouteTable>,
    forwarded_headers: bool,
) -> Result<Response<BoxedBody>, hyper::Error> {
    // HTTP/2 carries the authority in :authority rather than a Host header.
    let host = req.uri().host().map(str::to_owned).or_else(|| {
        req.headers()
            .get(header::HOST)
            .and_then(|h| h.to_str().ok())
            .map(str::to_owned)
    });
    let path = req.uri().path().to_owned();

    let table = routes.load_full();
    let Some(action) = table.lookup(host.as_deref(), &path) else {
        return Ok(status_only(StatusCode::NOT_FOUND));
    };

    let target = action.target_group.select();
    let upstream_addr = target.address.clone();
    let Some(authority) = target.authority().cloned() else {
        error!(address = %upstream_addr, "backend address is not a valid URI authority");
        return Ok(status_only(StatusCode::BAD_GATEWAY));
    };
    let filters = action.filters.clone();
    let rewrites = filters.rewrites_path();
    let rewritten = rewrites.then(|| filters.apply_url_rewrite(&path).into_owned());
    drop(table);

    let (mut parts, body) = req.into_parts();

    let path_and_query = match rewritten {
        Some(new_path) => {
            let full = match parts.uri.query() {
                Some(q) => format!("{new_path}?{q}"),
                None => new_path,
            };
            PathAndQuery::try_from(full).unwrap_or_else(|_| PathAndQuery::from_static("/"))
        }
        None => parts
            .uri
            .path_and_query()
            .cloned()
            .unwrap_or_else(|| PathAndQuery::from_static("/")),
    };

    let mut uri_parts = http::uri::Parts::default();
    uri_parts.scheme = Some(Scheme::HTTP);
    uri_parts.authority = Some(authority.clone());
    uri_parts.path_and_query = Some(path_and_query.clone());
    let Ok(upstream_uri) = Uri::from_parts(uri_parts) else {
        return Ok(status_only(StatusCode::INTERNAL_SERVER_ERROR));
    };

    proxy::strip_hop_by_hop(&mut parts.headers);
    if forwarded_headers {
        proxy::append_forwarded_for(&mut parts.headers, peer);
        proxy::set_forwarded_proto(&mut parts.headers, "http");
    }
    filters.apply_request_headers(&mut parts.headers);

    // Downstream is HTTP/2, upstream is HTTP/1.1: the version must be rewritten
    // or the client connection would reject the request outright.
    parts.version = Version::HTTP_11;
    if let Ok(value) = header::HeaderValue::from_str(authority.as_str()) {
        parts.headers.insert(header::HOST, value);
    }
    parts.uri = upstream_uri;

    let out_req = Request::from_parts(parts, body.boxed());
    match forward_h1(&upstream_addr, out_req).await {
        Ok(mut resp) => {
            proxy::strip_hop_by_hop(resp.headers_mut());
            filters.apply_response_headers(resp.headers_mut());
            Ok(resp)
        }
        Err(err) => {
            error!(target = %upstream_addr, %err, "upstream request failed");
            Ok(status_only(StatusCode::BAD_GATEWAY))
        }
    }
}

/// Open an HTTP/1.1 connection to `address` and send one request.
///
/// A fresh connection per request, unlike the pooled HTTP/1.1 path: pooling
/// Hyper `SendRequest` handles across `!Send` Monoio tasks needs a
/// thread-local pool keyed by both address and protocol, which is only worth
/// building once HTTP/2 stops being the minority of this candidate's traffic.
/// The cost is stated here rather than hidden, because it shows up in the
/// HTTP/2 rows of the report.
async fn forward_h1(
    address: &str,
    req: Request<BoxedBody>,
) -> Result<Response<BoxedBody>, anyhow::Error> {
    let stream = TcpStream::connect(address).await?;
    let _ = stream.set_nodelay(true);
    let poll_stream = stream
        .into_poll_io()
        .map_err(|err| anyhow::anyhow!("cannot switch the upstream stream to poll IO: {err}"))?;

    let (mut sender, connection) = client_http1::handshake(TokioIo::new(poll_stream)).await?;

    monoio::spawn(async move {
        if let Err(err) = connection.await {
            trace!(%err, "upstream connection closed");
        }
    });

    let response = sender.send_request(req).await?;
    let (parts, body) = response.into_parts();
    Ok(Response::from_parts(parts, body.boxed()))
}

fn status_only(status: StatusCode) -> Response<BoxedBody> {
    Response::builder()
        .status(status)
        .header(header::CONTENT_LENGTH, "0")
        .body(
            Empty::<Bytes>::new()
                .map_err(|never| match never {})
                .boxed(),
        )
        .expect("static response is well formed")
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::io::AsyncReadExt;

    #[test]
    fn preface_detection_is_decisive_only_once_it_can_be() {
        assert_eq!(detect_h2(H2_PREFACE), Some(true));
        assert_eq!(detect_h2(b"GET / HTTP/1.1\r\n"), Some(false));
        // A partial preface is undecidable, and must not be parsed as HTTP/1.
        assert_eq!(detect_h2(b"PRI * HT"), None);
        assert_eq!(detect_h2(b""), None);
        // A prefix that diverges is decisively not h2.
        assert_eq!(detect_h2(b"PRI * HTTX"), Some(false));
    }

    #[test]
    fn preface_detection_handles_trailing_data() {
        let mut buf = H2_PREFACE.to_vec();
        buf.extend_from_slice(b"\x00\x00\x00\x04\x00\x00\x00\x00\x00");
        assert_eq!(detect_h2(&buf), Some(true));
    }

    #[tokio::test]
    async fn prefixed_io_replays_the_buffer_then_the_stream() {
        let inner = std::io::Cursor::new(b"world".to_vec());
        let mut io = PrefixedIo::new(b"hello ".to_vec(), inner);
        assert_eq!(io.remaining_prefix(), 6);

        let mut out = Vec::new();
        io.read_to_end(&mut out).await.expect("read");
        assert_eq!(out, b"hello world");
        assert_eq!(io.remaining_prefix(), 0);
    }

    #[tokio::test]
    async fn prefixed_io_with_an_empty_prefix_is_transparent() {
        let inner = std::io::Cursor::new(b"payload".to_vec());
        let mut io = PrefixedIo::new(Vec::new(), inner);
        let mut out = Vec::new();
        io.read_to_end(&mut out).await.expect("read");
        assert_eq!(out, b"payload");
    }

    #[tokio::test]
    async fn prefixed_io_handles_a_short_destination_buffer() {
        let inner = std::io::Cursor::new(b"CD".to_vec());
        let mut io = PrefixedIo::new(b"AB".to_vec(), inner);
        let mut one = [0u8; 1];
        for expected in *b"ABCD" {
            io.read_exact(&mut one).await.expect("read");
            assert_eq!(one[0], expected);
        }
    }
}
