//! Connection serving, shared by both topologies.
//!
//! Nothing here knows which scheduler it is running under. That is deliberate:
//! the work done per request has to be byte-for-byte identical between the
//! work-stealing and thread-per-core candidates, or the difference between them
//! measures this file instead of the scheduler.

use bytes::Bytes;
use http::{Request, Response, StatusCode, header};
use http_body_util::combinators::BoxBody;
use http_body_util::{BodyExt, Empty};
use hyper::service::service_fn;
use hyper_util::client::legacy::Client;
use hyper_util::rt::{TokioExecutor, TokioIo};
use lb_core::SharedRouteTable;
use lb_core::forward::{self, UpstreamProtocol};
use lb_uring::{UringConnector, UringStream};
use std::net::{IpAddr, SocketAddr};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use tracing::{error, trace};

pub type BoxedBody = BoxBody<Bytes, hyper::Error>;
pub type UringClient = Client<UringConnector, BoxedBody>;

/// Everything a connection needs that does not depend on the topology.
pub struct Shared {
    pub routes: Arc<SharedRouteTable>,
    pub forwarded_headers: bool,
    pub h1_max_buf_size: usize,
}

/// Per-core queue depth, used by the thread-per-core dispatcher to place a new
/// connection and by both topologies for reporting.
#[derive(Default)]
pub struct CoreLoad {
    pub connections: AtomicUsize,
    pub requests: AtomicUsize,
}

/// Build the pooled upstream client. Every upstream hop goes over the ring, so
/// the candidate is not secretly half epoll.
#[must_use]
pub fn build_client(connector: UringConnector, pool_idle_per_host: usize) -> UringClient {
    Client::builder(TokioExecutor::new())
        .pool_idle_timeout(std::time::Duration::from_secs(90))
        .pool_max_idle_per_host(pool_idle_per_host)
        .pool_timer(hyper_util::rt::TokioTimer::new())
        .http1_max_buf_size(64 * 1024)
        .retry_canceled_requests(true)
        .build(connector)
}

/// Decrements the core's connection count however the connection ends.
struct ConnGuard(Arc<CoreLoad>);

impl Drop for ConnGuard {
    fn drop(&mut self) {
        self.0.connections.fetch_sub(1, Ordering::Relaxed);
    }
}

/// Serve one accepted connection to completion.
pub async fn serve_connection(
    stream: UringStream,
    peer: SocketAddr,
    shared: Arc<Shared>,
    client: UringClient,
    load: Arc<CoreLoad>,
) {
    load.connections.fetch_add(1, Ordering::Relaxed);
    let _guard = ConnGuard(load.clone());
    let h1_max_buf_size = shared.h1_max_buf_size;

    let service = service_fn(move |req: Request<hyper::body::Incoming>| {
        let shared = shared.clone();
        let client = client.clone();
        let load = load.clone();
        async move {
            // In-flight request count is the dispatcher's load signal: it
            // tracks actual work, where a connection count only tracks how
            // many sockets happen to be parked on this core.
            load.requests.fetch_add(1, Ordering::Relaxed);
            let out = handle_request(req, peer.ip(), &shared, &client).await;
            load.requests.fetch_sub(1, Ordering::Relaxed);
            out
        }
    });

    // HTTP/1.1 only. The comparison runs h1 across every candidate, and an h2
    // path here would add a code path the other candidates do not share.
    let mut builder = hyper::server::conn::http1::Builder::new();
    builder.max_buf_size(h1_max_buf_size).pipeline_flush(true);

    if let Err(err) = builder.serve_connection(TokioIo::new(stream), service).await {
        trace!(%peer, error = %err, "connection closed");
    }
}

async fn handle_request(
    req: Request<hyper::body::Incoming>,
    peer_ip: IpAddr,
    shared: &Shared,
    client: &UringClient,
) -> Result<Response<BoxedBody>, hyper::Error> {
    let (mut parts, body) = req.into_parts();

    let (filters, target_addr) = match forward::prepare(
        &mut parts,
        peer_ip,
        &shared.routes,
        shared.forwarded_headers,
        UpstreamProtocol::Http1,
    ) {
        forward::Prepared::Forward { filters, address } => (filters, address),
        forward::Prepared::Reject(status) => return Ok(status_only(status)),
    };

    let out_req = Request::from_parts(parts, body.boxed());
    match client.request(out_req).await {
        Ok(resp) => {
            let (mut resp_parts, resp_body) = resp.into_parts();
            forward::finish_response(&mut resp_parts.headers, &filters);
            Ok(Response::from_parts(resp_parts, resp_body.boxed()))
        }
        Err(err) => {
            error!(error = %err, target = %target_addr, "upstream request failed");
            Ok(status_only(StatusCode::BAD_GATEWAY))
        }
    }
}

fn status_only(status: StatusCode) -> Response<BoxedBody> {
    Response::builder()
        .status(status)
        .header(header::CONTENT_LENGTH, "0")
        .body(Empty::<Bytes>::new().map_err(|never| match never {}).boxed())
        .expect("static response is valid")
}

/// Pin the calling thread to one CPU.
///
/// Thread-per-core is only thread-per-core if the threads actually stay on
/// their cores; without this the scheduler migrates them and the per-core ring
/// loses the cache locality that is the entire argument for the design.
pub fn pin_to_cpu(cpu: usize) -> std::io::Result<()> {
    // SAFETY: `set` is zeroed then written through the documented macro-
    // equivalents before being passed with its own size.
    unsafe {
        let mut set: libc::cpu_set_t = std::mem::zeroed();
        libc::CPU_ZERO(&mut set);
        libc::CPU_SET(cpu, &mut set);
        if libc::sched_setaffinity(0, std::mem::size_of::<libc::cpu_set_t>(), &raw const set) != 0 {
            return Err(std::io::Error::last_os_error());
        }
    }
    Ok(())
}
