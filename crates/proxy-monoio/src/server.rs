//! Monoio thread-per-core data plane.
//!
//! # What changed from a naive completion-IO proxy
//!
//! Three things dominated the original implementation's numbers, and all three
//! were bugs rather than properties of the runtime:
//!
//! * It served **one request per connection** and then closed. Every benchmark
//!   request paid a fresh downstream accept *and* a fresh upstream connect.
//! * It relayed **one 8 KiB read** of the response and stopped, silently
//!   truncating anything larger while still forwarding the upstream's
//!   `Content-Length`. The 64 KB and 1 MB payloads in plan §8 were corrupt.
//! * It called `.to_vec()` on every buffer in both directions, which is exactly
//!   the copy that Monoio's owned-buffer model exists to avoid.
//!
//! This implementation keeps connections alive in both directions, relays
//! bodies of any length with correct framing, and moves buffers by value
//! through `IoBuf`/`IoBufMut` so the data path performs no intermediate copies.

use crate::http1::{self, BodyLength, Parsed};
use crate::http2;
use crate::pool::ConnectionPool;
use bytes::BytesMut;
use lb_core::SharedRouteTable;
use monoio::buf::{IoBuf, IoBufMut};
use monoio::io::{AsyncReadRent, AsyncWriteRent, AsyncWriteRentExt};
use monoio::net::{ListenerOpts, TcpListener, TcpStream};
use std::net::{IpAddr, SocketAddr};
use std::rc::Rc;
use std::sync::Arc;
use tracing::{debug, error, info, trace};

/// Ceiling for a connection's read buffer. Large enough that a 64 KB payload
/// arrives in one or two completions.
const IO_BUF_SIZE: usize = 32 * 1024;
/// Starting read-buffer size.
///
/// Buffers grow on demand rather than starting at [`IO_BUF_SIZE`]. In the
/// connection-density scenario (plan §8.2) 95 % of connections are idle and
/// never carry more than a request head; preallocating 32 KiB for each of 50 000
/// of them costs 1.6 GB of RSS that the measurement would then attribute to the
/// runtime rather than to the buffer policy.
const IO_BUF_INITIAL: usize = 4 * 1024;
/// Listen backlog per worker socket.
const LISTEN_BACKLOG: i32 = 65_535;

/// Requests served on one downstream connection before closing it, so a
/// long-lived benchmark connection cannot pin resources forever.
const MAX_REQUESTS_PER_CONNECTION: u32 = 100_000;

/// Per-worker configuration.
#[derive(Debug, Clone)]
pub struct WorkerConfig {
    pub listen_addr: SocketAddr,
    /// Number of thread-per-core workers. Defaults to the logical CPU count.
    pub workers: usize,
    /// Send `X-Forwarded-For` / `X-Forwarded-Proto`.
    pub forwarded_headers: bool,
}

impl WorkerConfig {
    #[must_use]
    pub fn new(listen_addr: SocketAddr) -> Self {
        Self {
            listen_addr,
            workers: num_cpus::get(),
            forwarded_headers: true,
        }
    }
}

/// Thread-per-core Monoio proxy worker.
pub struct ProxyMonoio {
    pub routes: Arc<SharedRouteTable>,
    pub config: WorkerConfig,
}

impl ProxyMonoio {
    #[must_use]
    pub fn new(routes: Arc<SharedRouteTable>, listen_addr: SocketAddr) -> Self {
        Self {
            routes,
            config: WorkerConfig::new(listen_addr),
        }
    }

    #[must_use]
    pub fn with_config(routes: Arc<SharedRouteTable>, config: WorkerConfig) -> Self {
        Self { routes, config }
    }

    /// Run one worker's accept loop on the current thread.
    ///
    /// Every worker binds the same address with `SO_REUSEPORT` (Monoio's
    /// `ListenerOpts` default), so the kernel shards accepts across cores.
    pub async fn run_worker(&self, worker_id: usize) -> Result<(), anyhow::Error> {
        let opts = ListenerOpts::default()
            .reuse_port(true)
            .reuse_addr(true)
            .backlog(LISTEN_BACKLOG);
        let listener = TcpListener::bind_with_config(self.config.listen_addr, &opts)?;
        info!(
            worker = worker_id,
            addr = %self.config.listen_addr,
            "ProxyMonoio worker listening (SO_REUSEPORT)"
        );

        // One pool per worker: Monoio's TcpStream is !Send, and a per-core pool
        // needs no synchronisation at all.
        let pool = ConnectionPool::new();
        let forwarded = self.config.forwarded_headers;

        loop {
            match listener.accept().await {
                Ok((stream, peer)) => {
                    let _ = stream.set_nodelay(true);
                    let routes = self.routes.clone();
                    let pool = pool.clone();
                    monoio::spawn(async move {
                        if let Err(err) =
                            serve_connection(stream, peer.ip(), routes, pool, forwarded).await
                        {
                            trace!(%peer, %err, "connection ended");
                        }
                    });
                }
                Err(err) => {
                    error!(worker = worker_id, %err, "accept failed");
                    monoio::time::sleep(std::time::Duration::from_millis(5)).await;
                }
            }
        }
    }
}

/// Serve one downstream connection, keeping it alive across requests.
async fn serve_connection(
    mut downstream: TcpStream,
    peer: IpAddr,
    routes: Arc<SharedRouteTable>,
    pool: ConnectionPool,
    forwarded_headers: bool,
) -> Result<(), anyhow::Error> {
    // Buffers live for the whole connection and are moved in and out of every
    // I/O call, so a busy connection settles into steady state with zero
    // further allocation.
    let mut read_buf = Some(Vec::with_capacity(IO_BUF_INITIAL));
    let mut head_buf = BytesMut::with_capacity(8 * 1024);
    let mut filled = 0usize;
    let mut served = 0u32;

    loop {
        // --- read a complete request head -------------------------------
        let head = loop {
            if filled > 0 {
                let buf = read_buf.as_ref().expect("buffer is always returned");

                // Protocol sniffing must happen before HTTP/1 parsing: the h2
                // preface ("PRI * HTTP/2.0...") parses as a bogus HTTP/1
                // request line otherwise, and the connection would be answered
                // with a 400 instead of being upgraded.
                if served == 0 {
                    match http2::detect_h2(&buf[..filled]) {
                        Some(true) => {
                            let buffered = buf[..filled].to_vec();
                            return http2::serve_connection(
                                downstream,
                                buffered,
                                peer,
                                routes,
                                forwarded_headers,
                            )
                            .await;
                        }
                        // Undecidable: keep reading rather than guessing.
                        None => {
                            let mut buf = read_buf.take().expect("buffer is always returned");
                            grow_for_read(&mut buf, filled);
                            let (res, slice) = downstream.read(buf.slice_mut(filled..)).await;
                            buf = slice.into_inner();
                            let n = res?;
                            read_buf = Some(buf);
                            if n == 0 {
                                return Ok(());
                            }
                            filled += n;
                            continue;
                        }
                        Some(false) => {}
                    }
                }

                let buf = read_buf.as_ref().expect("buffer is always returned");
                match http1::parse_request(&buf[..filled]) {
                    Parsed::Complete(head) => break head,
                    Parsed::Invalid(reason) => {
                        debug!(reason, "malformed request");
                        let _ = downstream
                            .write_all(http1::error_response(400, "Bad Request"))
                            .await
                            .0;
                        return Ok(());
                    }
                    Parsed::Partial => {}
                }
                if filled >= http1::MAX_HEAD_SIZE {
                    let _ = downstream
                        .write_all(http1::error_response(
                            431,
                            "Request Header Fields Too Large",
                        ))
                        .await
                        .0;
                    return Ok(());
                }
            }

            let mut buf = read_buf.take().expect("buffer is always returned");
            // Read into the tail of the buffer without disturbing what is
            // already there. `slice_mut` hands ownership to the kernel and
            // gives it back, which is the whole point of the model.
            grow_for_read(&mut buf, filled);
            let (res, slice) = downstream.read(buf.slice_mut(filled..)).await;
            buf = slice.into_inner();
            let n = res?;
            read_buf = Some(buf);
            if n == 0 {
                // Clean close between requests is normal; mid-head is not.
                return Ok(());
            }
            filled += n;
        };

        served += 1;

        // --- route ------------------------------------------------------
        //
        // `load_full` rather than `load`: the guard cannot be held across the
        // awaits below, and cloning the Arc is a refcount bump.
        let table = routes.load_full();
        let Some(action) = table.lookup(head.host.as_deref(), path_of(&head.target)) else {
            let _ = downstream
                .write_all(http1::error_response(404, "Not Found"))
                .await
                .0;
            return Ok(());
        };

        let target = action.target_group.select();
        let upstream_addr = target.address.clone();
        let forward_target = if action.filters.rewrites_path() {
            let (path, query) = split_target(&head.target);
            let rewritten = action.filters.apply_url_rewrite(path);
            match query {
                Some(q) => format!("{rewritten}?{q}"),
                None => rewritten.into_owned(),
            }
        } else {
            head.target.clone()
        };
        drop(table);

        // --- forward the head -------------------------------------------
        http1::write_forward_head(
            &mut head_buf,
            read_buf.as_ref().expect("buffer present")[..filled].as_ref(),
            &head,
            &forward_target,
            &upstream_addr,
            forwarded_headers.then_some(peer),
        );

        let mut upstream = match acquire_upstream(&pool, &upstream_addr).await {
            Ok(s) => s,
            Err(err) => {
                error!(target = %upstream_addr, %err, "upstream connect failed");
                let _ = downstream
                    .write_all(http1::error_response(502, "Bad Gateway"))
                    .await
                    .0;
                return Ok(());
            }
        };

        let (res, _) = upstream.write_all(head_buf.split().freeze()).await;
        res?;

        // --- relay the request body -------------------------------------
        let mut buf = read_buf.take().expect("buffer present");
        let consumed = head.head_len;
        let mut leftover = filled - consumed;

        match head.body {
            BodyLength::Empty => {}
            BodyLength::Fixed(len) => {
                let already = leftover.min(len as usize);
                if already > 0 {
                    let (res, slice) = upstream
                        .write_all(buf.slice(consumed..consumed + already))
                        .await;
                    buf = slice.into_inner();
                    res?;
                    leftover -= already;
                }
                let remaining = len - already as u64;
                if remaining > 0 {
                    buf = relay_exact(&mut downstream, &mut upstream, buf, remaining).await?;
                    leftover = 0;
                }
            }
            BodyLength::Chunked => {
                if leftover > 0 {
                    let (res, slice) = upstream
                        .write_all(buf.slice(consumed..consumed + leftover))
                        .await;
                    buf = slice.into_inner();
                    res?;
                    leftover = 0;
                }
                buf = relay_chunked(&mut downstream, &mut upstream, buf).await?;
            }
            BodyLength::UntilClose => {
                // Not legal for a request; treat as no body.
            }
        }

        // Anything still buffered belongs to the *next* pipelined request.
        if leftover > 0 {
            buf.copy_within(consumed + (filled - consumed - leftover)..filled, 0);
            filled = leftover;
        } else {
            let pipelined_start = consumed + body_prefix_len(&head, filled - consumed);
            if pipelined_start < filled {
                buf.copy_within(pipelined_start..filled, 0);
                filled -= pipelined_start;
            } else {
                filled = 0;
            }
        }
        read_buf = Some(buf);

        // --- relay the response -----------------------------------------
        let keep_alive = !head.close_requested && served < MAX_REQUESTS_PER_CONNECTION;
        let outcome = relay_response(
            &mut upstream,
            &mut downstream,
            &mut head_buf,
            &head.method,
            keep_alive,
        )
        .await?;

        if outcome.upstream_reusable {
            pool.put(&upstream_addr, upstream);
        }
        if !keep_alive || !outcome.downstream_reusable {
            return Ok(());
        }
    }
}

/// Take a pooled upstream connection, or dial a new one.
async fn acquire_upstream(
    pool: &ConnectionPool,
    address: &str,
) -> Result<TcpStream, anyhow::Error> {
    if let Some(stream) = pool.take(address) {
        return Ok(stream);
    }
    // `to_socket_addrs` handles both `ip:port` and Kubernetes DNS names. The
    // original implementation only parsed `SocketAddr` and silently fell back
    // to 127.0.0.1:8080 for every in-cluster Service name.
    let stream = TcpStream::connect(address).await?;
    let _ = stream.set_nodelay(true);
    Ok(stream)
}

struct RelayOutcome {
    upstream_reusable: bool,
    downstream_reusable: bool,
}

/// Read the response head from upstream and relay head + body downstream.
async fn relay_response(
    upstream: &mut TcpStream,
    downstream: &mut TcpStream,
    head_buf: &mut BytesMut,
    request_method: &str,
    keep_alive: bool,
) -> Result<RelayOutcome, anyhow::Error> {
    let mut buf = Vec::with_capacity(IO_BUF_INITIAL);
    let mut filled = 0usize;

    let head = loop {
        if filled > 0 {
            match http1::parse_response(&buf[..filled], request_method) {
                Parsed::Complete(head) => break head,
                Parsed::Invalid(reason) => {
                    error!(reason, "malformed upstream response");
                    let _ = downstream
                        .write_all(http1::error_response(502, "Bad Gateway"))
                        .await
                        .0;
                    return Ok(RelayOutcome {
                        upstream_reusable: false,
                        downstream_reusable: false,
                    });
                }
                Parsed::Partial => {}
            }
        }
        grow_for_read(&mut buf, filled);
        let (res, slice) = upstream.read(buf.slice_mut(filled..)).await;
        buf = slice.into_inner();
        let n = res?;
        if n == 0 {
            return Ok(RelayOutcome {
                upstream_reusable: false,
                downstream_reusable: false,
            });
        }
        filled += n;
    };

    // A connection kept alive downstream can still be closed upstream, and vice
    // versa; the two dispositions are independent.
    let upstream_reusable = !head.close_requested && head.body != BodyLength::UntilClose;
    let downstream_keep = keep_alive && head.body != BodyLength::UntilClose;

    http1::write_return_head(head_buf, &buf[..filled], &head, downstream_keep);
    let (res, _) = downstream.write_all(head_buf.split().freeze()).await;
    res?;

    let body_start = head.head_len;
    let buffered_body = filled - body_start;

    match head.body {
        BodyLength::Empty => {}
        BodyLength::Fixed(len) => {
            let already = buffered_body.min(len as usize);
            if already > 0 {
                let (res, slice) = downstream
                    .write_all(buf.slice(body_start..body_start + already))
                    .await;
                buf = slice.into_inner();
                res?;
            }
            let remaining = len - already as u64;
            if remaining > 0 {
                relay_exact(upstream, downstream, buf, remaining).await?;
            }
        }
        BodyLength::Chunked => {
            if buffered_body > 0 {
                let (res, slice) = downstream.write_all(buf.slice(body_start..filled)).await;
                buf = slice.into_inner();
                res?;
            }
            relay_chunked(upstream, downstream, buf).await?;
        }
        BodyLength::UntilClose => {
            if buffered_body > 0 {
                let (res, slice) = downstream.write_all(buf.slice(body_start..filled)).await;
                buf = slice.into_inner();
                res?;
            }
            relay_until_close(upstream, downstream, buf).await?;
        }
    }

    Ok(RelayOutcome {
        upstream_reusable,
        downstream_reusable: downstream_keep,
    })
}

/// Relay exactly `remaining` bytes, moving one buffer back and forth.
async fn relay_exact<R, W>(
    src: &mut R,
    dst: &mut W,
    mut buf: Vec<u8>,
    mut remaining: u64,
) -> Result<Vec<u8>, anyhow::Error>
where
    R: AsyncReadRent,
    W: AsyncWriteRent,
{
    if buf.len() < IO_BUF_SIZE {
        buf.resize(IO_BUF_SIZE, 0);
    }
    while remaining > 0 {
        let want = remaining.min(buf.len() as u64) as usize;
        let (res, slice) = src.read(buf.slice_mut(..want)).await;
        buf = slice.into_inner();
        let n = res?;
        if n == 0 {
            return Err(anyhow::anyhow!(
                "upstream closed with {remaining} body bytes outstanding"
            ));
        }
        let (res, slice) = dst.write_all(buf.slice(..n)).await;
        buf = slice.into_inner();
        res?;
        remaining -= n as u64;
    }
    Ok(buf)
}

/// Relay a chunked body until the terminating zero-length chunk.
///
/// Chunk framing is relayed verbatim; only the terminator is detected, so no
/// re-chunking or buffering of whole chunks is needed.
async fn relay_chunked<R, W>(
    src: &mut R,
    dst: &mut W,
    mut buf: Vec<u8>,
) -> Result<Vec<u8>, anyhow::Error>
where
    R: AsyncReadRent,
    W: AsyncWriteRent,
{
    if buf.len() < IO_BUF_SIZE {
        buf.resize(IO_BUF_SIZE, 0);
    }
    loop {
        let (res, slice) = src.read(buf.slice_mut(..)).await;
        buf = slice.into_inner();
        let n = res?;
        if n == 0 {
            return Ok(buf);
        }
        let saw_terminator = memchr::memmem::find(&buf[..n], b"0\r\n\r\n").is_some();
        let (res, slice) = dst.write_all(buf.slice(..n)).await;
        buf = slice.into_inner();
        res?;
        if saw_terminator {
            return Ok(buf);
        }
    }
}

/// Relay until the source closes (a response with no framing headers).
async fn relay_until_close<R, W>(
    src: &mut R,
    dst: &mut W,
    mut buf: Vec<u8>,
) -> Result<Vec<u8>, anyhow::Error>
where
    R: AsyncReadRent,
    W: AsyncWriteRent,
{
    if buf.len() < IO_BUF_SIZE {
        buf.resize(IO_BUF_SIZE, 0);
    }
    loop {
        let (res, slice) = src.read(buf.slice_mut(..)).await;
        buf = slice.into_inner();
        let n = res?;
        if n == 0 {
            return Ok(buf);
        }
        let (res, slice) = dst.write_all(buf.slice(..n)).await;
        buf = slice.into_inner();
        res?;
    }
}

/// Ensure `buf` has room for another read past `filled`, growing geometrically
/// toward [`IO_BUF_SIZE`].
///
/// Doubling rather than adding a fixed block keeps an idle keep-alive
/// connection at [`IO_BUF_INITIAL`] while still reaching the ceiling in a
/// handful of reads once a connection is actually streaming.
#[inline]
fn grow_for_read(buf: &mut Vec<u8>, filled: usize) {
    let headroom = buf.len().saturating_sub(filled);
    if headroom >= IO_BUF_INITIAL {
        return;
    }
    if buf.len() < IO_BUF_INITIAL {
        buf.resize(IO_BUF_INITIAL, 0);
        return;
    }
    let target = (buf.len() * 2).min(filled + IO_BUF_SIZE);
    buf.resize(target.max(filled + IO_BUF_INITIAL), 0);
}

/// How many of the bytes after the head belonged to this request's body.
fn body_prefix_len(head: &http1::RequestHead, available: usize) -> usize {
    match head.body {
        BodyLength::Empty => 0,
        BodyLength::Fixed(n) => (n as usize).min(available),
        // Chunked and UntilClose consume everything buffered.
        _ => available,
    }
}

/// The path portion of a request target, without the query string.
#[inline]
fn path_of(target: &str) -> &str {
    match memchr::memchr(b'?', target.as_bytes()) {
        Some(pos) => &target[..pos],
        None => target,
    }
}

/// Split a request target into path and optional query.
#[inline]
fn split_target(target: &str) -> (&str, Option<&str>) {
    match memchr::memchr(b'?', target.as_bytes()) {
        Some(pos) => (&target[..pos], Some(&target[pos + 1..])),
        None => (target, None),
    }
}

/// Kept so `Rc`-based pools are visibly per-worker in the public API.
#[allow(dead_code)]
type NotSendMarker = Rc<()>;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn path_extraction_drops_the_query() {
        assert_eq!(path_of("/api/v1"), "/api/v1");
        assert_eq!(path_of("/api/v1?x=1&y=2"), "/api/v1");
        assert_eq!(path_of("/?"), "/");
    }

    #[test]
    fn target_splitting_round_trips() {
        assert_eq!(split_target("/a/b"), ("/a/b", None));
        assert_eq!(split_target("/a/b?c=d"), ("/a/b", Some("c=d")));
        assert_eq!(split_target("/a?"), ("/a", Some("")));
    }

    #[test]
    fn read_buffers_start_small_and_grow_geometrically() {
        let mut buf = Vec::with_capacity(IO_BUF_INITIAL);
        grow_for_read(&mut buf, 0);
        assert_eq!(buf.len(), IO_BUF_INITIAL, "an idle connection must not preallocate 32 KiB");

        // Streaming: it should climb, but never past the ceiling above `filled`.
        let mut filled = 0usize;
        for _ in 0..12 {
            grow_for_read(&mut buf, filled);
            assert!(
                buf.len() <= filled + IO_BUF_SIZE,
                "buffer overshot the ceiling: {} > {}",
                buf.len(),
                filled + IO_BUF_SIZE
            );
            filled = buf.len();
        }
        assert!(buf.len() >= IO_BUF_SIZE, "buffer never reached the ceiling");
    }

    #[test]
    fn grow_is_a_no_op_when_there_is_already_headroom() {
        let mut buf = vec![0u8; 64 * 1024];
        let before = buf.len();
        grow_for_read(&mut buf, 0);
        assert_eq!(buf.len(), before);
    }

    #[test]
    fn worker_default_uses_every_core() {
        let cfg = WorkerConfig::new("127.0.0.1:0".parse().unwrap());
        assert_eq!(cfg.workers, num_cpus::get());
    }

    #[test]
    fn body_prefix_length_matches_framing() {
        let head = |body| http1::RequestHead {
            head_len: 0,
            method: "GET".into(),
            target: "/".into(),
            host: None,
            body,
            close_requested: false,
            target_span: (0, 0),
        };
        assert_eq!(body_prefix_len(&head(BodyLength::Empty), 100), 0);
        assert_eq!(body_prefix_len(&head(BodyLength::Fixed(10)), 100), 10);
        assert_eq!(body_prefix_len(&head(BodyLength::Fixed(500)), 100), 100);
        assert_eq!(body_prefix_len(&head(BodyLength::Chunked), 100), 100);
    }
}
