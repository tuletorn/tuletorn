//! One pinned thread, one ring, one event loop.
//!
//! # Shape
//!
//! ```text
//! loop {
//!     submit_and_wait(1)      // one syscall, carries the whole queued batch
//!     for cqe in completions  // advance each connection's state machine
//! }
//! ```
//!
//! There is no executor, no future, and no waker. A completion identifies its
//! connection by slot index and advances it directly, which is the difference
//! between this and running Hyper over an io_uring reactor: no task wakeup per
//! read, and no copy between a kernel-owned buffer and a caller-owned one.

use crate::http::{self, Body};
use crate::ring::{BufferPool, Op, Ring, pack, unpack};
use io_uring::{opcode, types};
use lb_core::{RouteFilters, SharedRouteTable};
use std::io;
use std::net::SocketAddr;
use std::os::unix::io::RawFd;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

const LISTEN_BACKLOG: libc::c_int = 65_535;

/// Bodies at or below this size are copied in behind the head so the response
/// costs one write. Above it, the body is written straight from the registered
/// buffer and the copy is skipped entirely.
const COALESCE_LIMIT: usize = 2048;

/// Per-worker counters, aggregated for logging only.
#[derive(Default)]
pub struct WorkerStats {
    pub requests: AtomicU64,
    pub sqes: AtomicU64,
    pub enters: AtomicU64,
    pub cqes: AtomicU64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum State {
    /// Reading a request head from the client.
    ReadHead,
    /// Waiting for an upstream connection to complete.
    Connecting,
    /// Writing the rewritten request to the upstream.
    SendUpstream,
    /// Reading the response head from the upstream.
    RecvHead,
    /// Writing the rewritten head (and any coalesced body) from `out`.
    SendClient,
    /// Writing body bytes straight out of the registered upstream buffer.
    SendBody,
    /// Waiting on more body bytes from the upstream.
    StreamBody,
    Closed,
}

struct Conn {
    generation: u32,
    client_fd: RawFd,
    upstream_fd: RawFd,
    peer: String,
    state: State,

    /// Registered buffer holding client-side bytes.
    cbuf: u16,
    cfilled: usize,
    /// Registered buffer holding upstream-side bytes.
    ubuf: u16,
    ufilled: usize,
    /// Region of `ubuf` still owed to the client. Writing from here rather
    /// than copying into `out` is what keeps a large body zero-copy: the
    /// buffer the response was read into is the buffer it is written out of.
    ubuf_off: usize,
    ubuf_len: usize,

    /// Rewritten head plus any coalesced body, written from here.
    out: Vec<u8>,
    out_sent: usize,

    keep_alive: bool,
    request_was_head: bool,
    /// Response body bytes still to forward, when length-delimited.
    body_remaining: usize,
    body_until_close: bool,
    /// Chunked-transfer scanner, when the response is chunked.
    chunked: Option<crate::http::ChunkedScan>,
    filters: RouteFilters,
    upstream_addr: Option<SocketAddr>,
    /// Kernel-owned storage for an in-flight `connect`.
    connect_addr: Box<libc::sockaddr_storage>,
    connect_len: libc::socklen_t,
    in_use: bool,
}

impl Conn {
    fn reset_for_next_request(&mut self) {
        self.state = State::ReadHead;
        self.cfilled = 0;
        self.ufilled = 0;
        self.ubuf_off = 0;
        self.ubuf_len = 0;
        self.chunked = None;
        self.out.clear();
        self.out_sent = 0;
        self.body_remaining = 0;
        self.body_until_close = false;
        self.request_was_head = false;
    }
}

pub struct WorkerConfig {
    pub core: usize,
    pub listen: SocketAddr,
    pub ring_entries: u32,
    pub buf_count: usize,
    pub buf_size: usize,
    pub max_conns: usize,
    pub pin: bool,
}

pub struct Worker {
    ring: Ring,
    pool: BufferPool,
    conns: Vec<Conn>,
    free_conns: Vec<u32>,
    routes: Arc<SharedRouteTable>,
    listen_fd: RawFd,
    /// Idle upstream sockets, reused across requests on this core alone.
    idle_upstream: Vec<RawFd>,
    stats: Arc<WorkerStats>,
    accept_armed: bool,
}

impl Worker {
    pub fn new(
        cfg: &WorkerConfig,
        routes: Arc<SharedRouteTable>,
        stats: Arc<WorkerStats>,
    ) -> io::Result<Self> {
        if cfg.pin {
            pin_to_cpu(cfg.core)?;
        }
        let ring = Ring::new(cfg.ring_entries, true)?;
        let mut pool = BufferPool::new(cfg.buf_count, cfg.buf_size)?;

        let mut iovecs = pool.iovecs();
        // SAFETY: the iovecs address `pool`'s allocation, which lives as long
        // as this worker and is never reallocated.
        unsafe { ring.io.submitter().register_buffers(&iovecs) }?;
        iovecs.clear();

        let listen_fd = bind_reuseport(cfg.listen)?;

        let conns = (0..cfg.max_conns)
            .map(|_| Conn {
                generation: 0,
                client_fd: -1,
                upstream_fd: -1,
                peer: String::new(),
                state: State::Closed,
                cbuf: u16::MAX,
                cfilled: 0,
                ubuf: u16::MAX,
                ufilled: 0,
                ubuf_off: 0,
                ubuf_len: 0,
                chunked: None,
                out: Vec::with_capacity(1024),
                out_sent: 0,
                keep_alive: true,
                request_was_head: false,
                body_remaining: 0,
                body_until_close: false,
                filters: RouteFilters::default(),
                upstream_addr: None,
                // SAFETY: `sockaddr_storage` is POD; all-zero is valid.
                connect_addr: Box::new(unsafe { std::mem::zeroed() }),
                connect_len: 0,
                in_use: false,
            })
            .collect();

        Ok(Self {
            ring,
            pool,
            conns,
            free_conns: (0..cfg.max_conns as u32).rev().collect(),
            routes,
            listen_fd,
            idle_upstream: Vec::with_capacity(1024),
            stats,
            accept_armed: false,
        })
    }

    /// Run until the process is killed.
    pub fn run(&mut self) -> io::Result<()> {
        self.arm_accept()?;
        loop {
            let submitted = self.ring.submit_and_wait(1)?;
            self.stats.enters.fetch_add(1, Ordering::Relaxed);
            self.stats
                .sqes
                .fetch_add(u64::from(submitted), Ordering::Relaxed);

            let mut completions = Vec::with_capacity(64);
            {
                let mut cq = self.ring.io.completion();
                cq.sync();
                for cqe in &mut cq {
                    completions.push((cqe.user_data(), cqe.result(), cqe.flags()));
                }
            }
            self.stats
                .cqes
                .fetch_add(completions.len() as u64, Ordering::Relaxed);

            for (user_data, res, flags) in completions {
                self.dispatch(user_data, res, flags)?;
            }
            if !self.accept_armed {
                self.arm_accept()?;
            }
        }
    }

    /// Multishot accept: one SQE yields a CQE per connection until the kernel
    /// says it is done, so the accept path costs no submission per connection.
    fn arm_accept(&mut self) -> io::Result<()> {
        let entry = opcode::AcceptMulti::new(types::Fd(self.listen_fd))
            .build()
            .user_data(pack(Op::Accept, 0, 0));
        self.ring.push(entry)?;
        self.accept_armed = true;
        Ok(())
    }

    fn dispatch(&mut self, user_data: u64, res: i32, flags: u32) -> io::Result<()> {
        let Some((op, generation, index)) = unpack(user_data) else {
            return Ok(());
        };
        if op == Op::Accept {
            if !io_uring::cqueue::more(flags) {
                self.accept_armed = false;
            }
            if res >= 0 {
                self.on_accept(res as RawFd)?;
            }
            return Ok(());
        }
        if op == Op::Close {
            return Ok(());
        }

        let idx = index as usize;
        // A completion for a slot that has since been recycled belongs to the
        // previous occupant; applying it would corrupt the current one.
        if idx >= self.conns.len()
            || !self.conns[idx].in_use
            || self.conns[idx].generation != generation
        {
            return Ok(());
        }

        match op {
            Op::ClientRead => self.on_client_read(idx, res),
            Op::UpstreamConnect => self.on_upstream_connect(idx, res),
            Op::UpstreamWrite => self.on_upstream_write(idx, res),
            Op::UpstreamRead => self.on_upstream_read(idx, res),
            Op::ClientWrite => self.on_client_write(idx, res),
            Op::Accept | Op::Close => Ok(()),
        }
    }
    fn on_accept(&mut self, fd: RawFd) -> io::Result<()> {
        let (Some(index), Some(cbuf)) = (self.free_conns.pop(), self.pool.alloc()) else {
            // Out of slots or buffers: refuse rather than queue unboundedly.
            // SAFETY: `fd` was just accepted and is owned here.
            unsafe { libc::close(fd) };
            return Ok(());
        };
        set_nodelay(fd);

        let idx = index as usize;
        let generation = self.conns[idx].generation.wrapping_add(1) & 0xff_ffff;
        let peer = peer_addr(fd).map_or_else(|| "0.0.0.0".to_string(), |a| a.ip().to_string());

        let conn = &mut self.conns[idx];
        conn.generation = generation;
        conn.client_fd = fd;
        conn.upstream_fd = -1;
        conn.peer = peer;
        conn.cbuf = cbuf;
        conn.ubuf = u16::MAX;
        conn.keep_alive = true;
        conn.in_use = true;
        conn.upstream_addr = None;
        conn.reset_for_next_request();

        self.submit_client_read(idx)
    }

    fn submit_client_read(&mut self, idx: usize) -> io::Result<()> {
        let conn = &self.conns[idx];
        let (fd, cbuf, filled, generation) =
            (conn.client_fd, conn.cbuf, conn.cfilled, conn.generation);
        let cap = self.pool.buf_size() - filled;
        if cap == 0 {
            // The head does not fit in one buffer; refusing is the honest
            // answer at this size rather than growing without bound.
            return self.fail(idx, 431, "Request Header Fields Too Large");
        }
        // SAFETY: the slot is registered and stays alive until the CQE lands.
        let ptr = unsafe { self.pool.ptr(cbuf).add(filled) };
        let entry = opcode::ReadFixed::new(types::Fd(fd), ptr, cap as u32, cbuf)
            .build()
            .user_data(pack(Op::ClientRead, generation, idx as u32));
        self.ring.push(entry)
    }

    fn on_client_read(&mut self, idx: usize, res: i32) -> io::Result<()> {
        if res <= 0 {
            return self.close(idx);
        }
        self.conns[idx].cfilled += res as usize;

        let filled = self.conns[idx].cfilled;
        let cbuf = self.conns[idx].cbuf;
        let head = {
            let bytes = self.pool.slice(cbuf, 0, filled);
            match http::parse_request(bytes) {
                Ok(Some(h)) => h,
                // Head is incomplete: read more into the same buffer.
                Ok(None) => return self.submit_client_read(idx),
                Err(()) => return self.fail(idx, 400, "Bad Request"),
            }
        };

        self.stats.requests.fetch_add(1, Ordering::Relaxed);
        self.conns[idx].keep_alive = head.keep_alive;
        self.conns[idx].request_was_head = head.method_is_head;

        // Route with the same shared table and the same lookup every other
        // candidate uses, so the comparison stays like-for-like.
        let (authority, filters) = {
            let bytes = self.pool.slice(cbuf, 0, filled);
            let path =
                std::str::from_utf8(&bytes[head.path.0..head.path.0 + head.path.1]).unwrap_or("/");
            let host = head
                .host
                .and_then(|(s, l)| std::str::from_utf8(&bytes[s..s + l]).ok())
                .map(str::trim);
            let path_only = path.split('?').next().unwrap_or("/");

            let table = self.routes.load();
            let Some(action) = table.lookup(host, path_only) else {
                drop(table);
                return self.fail(idx, 404, "Not Found");
            };
            let target = action.target_group.select();
            (target.address.clone(), action.filters.clone())
        };

        let Ok(addr) = authority.parse::<SocketAddr>() else {
            return self.fail(idx, 502, "Bad Gateway");
        };

        // Build the upstream request. Body bytes already in the buffer are
        // appended here so head and body cross in a single write.
        let rewritten = {
            let bytes = self.pool.slice(cbuf, 0, filled);
            let mut out = std::mem::take(&mut self.conns[idx].out);
            let peer = self.conns[idx].peer.clone();
            let built =
                http::write_request_head(&mut out, bytes, &head, &authority, &peer, &filters, None);
            if built.is_ok() {
                let body = &bytes[head.head_len..];
                let take = match head.body {
                    Body::None => 0,
                    Body::Length(n) => n.min(body.len()),
                    Body::Chunked | Body::UntilClose => body.len(),
                };
                out.extend_from_slice(&body[..take]);
            }
            self.conns[idx].out = out;
            built
        };
        if rewritten.is_err() {
            return self.fail(idx, 400, "Bad Request");
        }

        self.conns[idx].filters = filters;
        self.conns[idx].upstream_addr = Some(addr);
        self.conns[idx].out_sent = 0;
        self.conns[idx].cfilled = 0;

        // Reuse a pooled upstream socket if this core has one parked.
        if let Some(fd) = self.idle_upstream.pop() {
            self.conns[idx].upstream_fd = fd;
            self.conns[idx].state = State::SendUpstream;
            return self.submit_upstream_write(idx);
        }
        self.start_connect(idx, addr)
    }

    fn start_connect(&mut self, idx: usize, addr: SocketAddr) -> io::Result<()> {
        let fd = match new_socket(addr) {
            Ok(fd) => fd,
            Err(_) => return self.fail(idx, 502, "Bad Gateway"),
        };
        set_nodelay(fd);
        let conn = &mut self.conns[idx];
        conn.upstream_fd = fd;
        conn.state = State::Connecting;
        conn.connect_len = write_sockaddr(&mut conn.connect_addr, addr);

        let generation = conn.generation;
        let len = conn.connect_len;
        let ptr = std::ptr::addr_of!(*conn.connect_addr).cast::<libc::sockaddr>();
        let entry = opcode::Connect::new(types::Fd(fd), ptr, len)
            .build()
            .user_data(pack(Op::UpstreamConnect, generation, idx as u32));
        self.ring.push(entry)
    }

    fn on_upstream_connect(&mut self, idx: usize, res: i32) -> io::Result<()> {
        if res < 0 {
            return self.fail(idx, 502, "Bad Gateway");
        }
        self.conns[idx].state = State::SendUpstream;
        self.submit_upstream_write(idx)
    }

    fn submit_upstream_write(&mut self, idx: usize) -> io::Result<()> {
        let conn = &self.conns[idx];
        let (fd, sent, generation) = (conn.upstream_fd, conn.out_sent, conn.generation);
        let remaining = conn.out.len() - sent;
        // SAFETY: `out` lives in the connection slot and is not touched again
        // until this write completes.
        let ptr = unsafe { conn.out.as_ptr().add(sent) };
        let entry = opcode::Send::new(types::Fd(fd), ptr, remaining as u32)
            .build()
            .user_data(pack(Op::UpstreamWrite, generation, idx as u32));
        self.ring.push(entry)
    }

    fn on_upstream_write(&mut self, idx: usize, res: i32) -> io::Result<()> {
        if res <= 0 {
            return self.fail(idx, 502, "Bad Gateway");
        }
        self.conns[idx].out_sent += res as usize;
        if self.conns[idx].out_sent < self.conns[idx].out.len() {
            return self.submit_upstream_write(idx);
        }
        // Request is away; start reading the response.
        if self.conns[idx].ubuf == u16::MAX {
            let Some(ubuf) = self.pool.alloc() else {
                return self.fail(idx, 503, "Service Unavailable");
            };
            self.conns[idx].ubuf = ubuf;
        }
        self.conns[idx].ufilled = 0;
        self.conns[idx].state = State::RecvHead;
        self.submit_upstream_read(idx)
    }

    fn submit_upstream_read(&mut self, idx: usize) -> io::Result<()> {
        let conn = &self.conns[idx];
        let (fd, ubuf, filled, generation) =
            (conn.upstream_fd, conn.ubuf, conn.ufilled, conn.generation);
        let cap = self.pool.buf_size() - filled;
        if cap == 0 {
            return self.fail(idx, 502, "Bad Gateway");
        }
        // SAFETY: registered slot, alive until the CQE lands.
        let ptr = unsafe { self.pool.ptr(ubuf).add(filled) };
        let entry = opcode::ReadFixed::new(types::Fd(fd), ptr, cap as u32, ubuf)
            .build()
            .user_data(pack(Op::UpstreamRead, generation, idx as u32));
        self.ring.push(entry)
    }
    fn on_upstream_read(&mut self, idx: usize, res: i32) -> io::Result<()> {
        if res < 0 {
            return self.fail(idx, 502, "Bad Gateway");
        }
        if res == 0 {
            // Upstream closed. For an until-close body that is the terminator.
            if self.conns[idx].state == State::StreamBody && self.conns[idx].body_until_close {
                self.conns[idx].keep_alive = false;
                return self.finish_response(idx);
            }
            return self.fail(idx, 502, "Bad Gateway");
        }
        self.conns[idx].ufilled += res as usize;

        if self.conns[idx].state == State::StreamBody {
            return self.forward_body_chunk(idx);
        }

        let filled = self.conns[idx].ufilled;
        let ubuf = self.conns[idx].ubuf;
        let was_head = self.conns[idx].request_was_head;
        let head = {
            let bytes = self.pool.slice(ubuf, 0, filled);
            match http::parse_response(bytes, was_head) {
                Ok(Some(h)) => h,
                Ok(None) => return self.submit_upstream_read(idx),
                Err(()) => return self.fail(idx, 502, "Bad Gateway"),
            }
        };

        let keep_alive = self.conns[idx].keep_alive && head.keep_alive;
        let filters = self.conns[idx].filters.clone();
        let built = {
            let bytes = self.pool.slice(ubuf, 0, filled);
            let mut out = std::mem::take(&mut self.conns[idx].out);
            let ok = http::write_response_head(&mut out, bytes, keep_alive, &filters);
            if ok.is_ok() && filled - head.head_len <= COALESCE_LIMIT {
                // A small body rides out with the head, so the common case is
                // exactly one send. A large one is written from the registered
                // buffer instead, with no copy at all.
                out.extend_from_slice(&bytes[head.head_len..]);
            }
            self.conns[idx].out = out;
            ok
        };
        if built.is_err() {
            return self.fail(idx, 502, "Bad Gateway");
        }

        let body_present = filled - head.head_len;
        let coalesced = body_present <= COALESCE_LIMIT;

        let mut chunked = None;
        let (remaining, until_close) = match head.body {
            Body::None => (0, false),
            Body::Length(n) => (n.saturating_sub(body_present), false),
            Body::Chunked => {
                // Chunked bodies are forwarded verbatim but must still be
                // framed, or the proxy cannot tell where the response ends and
                // keep-alive would desynchronise the connection.
                let mut scan = http::ChunkedScan::new();
                scan.consume(self.pool.slice(ubuf, head.head_len, body_present));
                let done = scan.is_complete();
                chunked = Some(scan);
                (if done { 0 } else { usize::MAX }, false)
            }
            Body::UntilClose => (usize::MAX, true),
        };

        let conn = &mut self.conns[idx];
        conn.keep_alive = keep_alive;
        conn.body_remaining = remaining;
        conn.body_until_close = until_close;
        conn.chunked = chunked;
        conn.ufilled = 0;
        conn.ubuf_off = if coalesced { 0 } else { head.head_len };
        conn.ubuf_len = if coalesced { 0 } else { filled };
        conn.out_sent = 0;
        conn.state = State::SendClient;
        self.submit_client_write(idx)
    }

    /// Forward already-buffered upstream bytes without reparsing and without
    /// copying: the client write reads from the same registered buffer the
    /// upstream read filled.
    fn forward_body_chunk(&mut self, idx: usize) -> io::Result<()> {
        let filled = self.conns[idx].ufilled;
        let ubuf = self.conns[idx].ubuf;
        // Destructure so the borrow checker can see that the buffer pool and
        // the connection slab are disjoint; no raw pointer needed.
        let Self { pool, conns, .. } = self;
        let mut chunked_done = false;
        if let Some(scan) = conns[idx].chunked.as_mut() {
            scan.consume(pool.slice(ubuf, 0, filled));
            chunked_done = scan.is_complete();
        }
        if chunked_done {
            self.conns[idx].body_remaining = 0;
        }
        let conn = &mut self.conns[idx];
        if conn.body_remaining != usize::MAX {
            conn.body_remaining = conn.body_remaining.saturating_sub(filled);
        }
        conn.ubuf_off = 0;
        conn.ubuf_len = filled;
        conn.ufilled = 0;
        conn.state = State::SendBody;
        self.submit_body_write(idx)
    }

    /// Write the pending region of the registered upstream buffer.
    fn submit_body_write(&mut self, idx: usize) -> io::Result<()> {
        let conn = &self.conns[idx];
        let (fd, ubuf, off, len, generation) = (
            conn.client_fd,
            conn.ubuf,
            conn.ubuf_off,
            conn.ubuf_len,
            conn.generation,
        );
        // SAFETY: registered slot, owned by this connection until its CQE.
        let ptr = unsafe { self.pool.ptr(ubuf).add(off) };
        let entry = opcode::WriteFixed::new(types::Fd(fd), ptr, (len - off) as u32, ubuf)
            .build()
            .user_data(pack(Op::ClientWrite, generation, idx as u32));
        self.ring.push(entry)
    }

    fn submit_client_write(&mut self, idx: usize) -> io::Result<()> {
        let conn = &self.conns[idx];
        let (fd, sent, generation) = (conn.client_fd, conn.out_sent, conn.generation);
        let remaining = conn.out.len() - sent;
        // SAFETY: `out` is owned by the slot and untouched until completion.
        let ptr = unsafe { conn.out.as_ptr().add(sent) };
        let entry = opcode::Send::new(types::Fd(fd), ptr, remaining as u32)
            .build()
            .user_data(pack(Op::ClientWrite, generation, idx as u32));
        self.ring.push(entry)
    }

    fn on_client_write(&mut self, idx: usize, res: i32) -> io::Result<()> {
        if res <= 0 {
            return self.close(idx);
        }
        let written = res as usize;

        if self.conns[idx].state == State::SendBody {
            self.conns[idx].ubuf_off += written;
            if self.conns[idx].ubuf_off < self.conns[idx].ubuf_len {
                return self.submit_body_write(idx);
            }
            self.conns[idx].ubuf_off = 0;
            self.conns[idx].ubuf_len = 0;
            return self.after_client_write(idx);
        }

        self.conns[idx].out_sent += written;
        if self.conns[idx].out_sent < self.conns[idx].out.len() {
            return self.submit_client_write(idx);
        }
        // The head is away; any body left in the registered buffer follows.
        if self.conns[idx].ubuf_len > self.conns[idx].ubuf_off {
            self.conns[idx].state = State::SendBody;
            return self.submit_body_write(idx);
        }
        self.after_client_write(idx)
    }

    /// Decide whether more body is owed once a client write has drained.
    fn after_client_write(&mut self, idx: usize) -> io::Result<()> {
        let conn = &self.conns[idx];
        let more_body = conn.body_until_close || conn.body_remaining > 0;
        if more_body && conn.upstream_fd >= 0 {
            self.conns[idx].state = State::StreamBody;
            return self.submit_upstream_read(idx);
        }
        self.finish_response(idx)
    }

    fn finish_response(&mut self, idx: usize) -> io::Result<()> {
        let keep_alive = self.conns[idx].keep_alive;
        // Park the upstream socket for the next request on this core.
        let up = std::mem::replace(&mut self.conns[idx].upstream_fd, -1);
        if up >= 0 {
            if keep_alive && self.idle_upstream.len() < self.idle_upstream.capacity() {
                self.idle_upstream.push(up);
            } else {
                // SAFETY: this worker owns the socket.
                unsafe { libc::close(up) };
            }
        }
        if let Some(ubuf) = take_buf(&mut self.conns[idx].ubuf) {
            self.pool.release(ubuf);
        }
        if !keep_alive {
            return self.close(idx);
        }
        self.conns[idx].reset_for_next_request();
        self.submit_client_read(idx)
    }

    /// Reply with a fixed status and then continue or close.
    fn fail(&mut self, idx: usize, status: u16, reason: &str) -> io::Result<()> {
        let up = std::mem::replace(&mut self.conns[idx].upstream_fd, -1);
        if up >= 0 {
            // SAFETY: owned by this worker; a failed exchange must not return
            // a possibly-desynchronised socket to the idle pool.
            unsafe { libc::close(up) };
        }
        if let Some(ubuf) = take_buf(&mut self.conns[idx].ubuf) {
            self.pool.release(ubuf);
        }
        let conn = &mut self.conns[idx];
        conn.out = http::status_response(status, reason);
        conn.out_sent = 0;
        conn.cfilled = 0;
        conn.ufilled = 0;
        conn.ubuf_off = 0;
        conn.ubuf_len = 0;
        conn.chunked = None;
        conn.body_remaining = 0;
        conn.body_until_close = false;
        conn.state = State::SendClient;
        self.submit_client_write(idx)
    }

    fn close(&mut self, idx: usize) -> io::Result<()> {
        let conn = &mut self.conns[idx];
        if !conn.in_use {
            return Ok(());
        }
        conn.in_use = false;
        conn.state = State::Closed;
        let client = std::mem::replace(&mut conn.client_fd, -1);
        let upstream = std::mem::replace(&mut conn.upstream_fd, -1);
        let cbuf = take_buf(&mut conn.cbuf);
        let ubuf = take_buf(&mut conn.ubuf);

        if let Some(b) = cbuf {
            self.pool.release(b);
        }
        if let Some(b) = ubuf {
            self.pool.release(b);
        }
        // SAFETY: this worker owns both descriptors. io_uring resolved each fd
        // to a `struct file` at submission and holds its own reference, so any
        // op still in flight stays valid.
        if client >= 0 {
            unsafe { libc::close(client) };
        }
        if upstream >= 0 {
            unsafe { libc::close(upstream) };
        }
        self.free_conns.push(idx as u32);
        Ok(())
    }
}

fn take_buf(slot: &mut u16) -> Option<u16> {
    if *slot == u16::MAX {
        None
    } else {
        Some(std::mem::replace(slot, u16::MAX))
    }
}

fn set_nodelay(fd: RawFd) {
    let v: libc::c_int = 1;
    // SAFETY: `v` outlives the call and its size is passed correctly.
    unsafe {
        libc::setsockopt(
            fd,
            libc::IPPROTO_TCP,
            libc::TCP_NODELAY,
            std::ptr::addr_of!(v).cast(),
            std::mem::size_of::<libc::c_int>() as libc::socklen_t,
        );
    }
}

fn peer_addr(fd: RawFd) -> Option<SocketAddr> {
    // SAFETY: `storage` is sized correctly and only read on success.
    unsafe {
        let mut storage: libc::sockaddr_storage = std::mem::zeroed();
        let mut len = std::mem::size_of::<libc::sockaddr_storage>() as libc::socklen_t;
        if libc::getpeername(fd, std::ptr::addr_of_mut!(storage).cast(), &mut len) != 0 {
            return None;
        }
        if i32::from(storage.ss_family) == libc::AF_INET {
            let sin = &*std::ptr::addr_of!(storage).cast::<libc::sockaddr_in>();
            Some(SocketAddr::from((
                std::net::Ipv4Addr::from(u32::from_be(sin.sin_addr.s_addr)),
                u16::from_be(sin.sin_port),
            )))
        } else {
            None
        }
    }
}

fn write_sockaddr(storage: &mut libc::sockaddr_storage, addr: SocketAddr) -> libc::socklen_t {
    match addr {
        SocketAddr::V4(v4) => {
            let sin = libc::sockaddr_in {
                sin_family: libc::AF_INET as libc::sa_family_t,
                sin_port: v4.port().to_be(),
                sin_addr: libc::in_addr {
                    s_addr: u32::from(*v4.ip()).to_be(),
                },
                sin_zero: [0; 8],
            };
            // SAFETY: `sockaddr_in` is a prefix-compatible union member.
            unsafe {
                std::ptr::write(
                    std::ptr::addr_of_mut!(*storage).cast::<libc::sockaddr_in>(),
                    sin,
                )
            };
            std::mem::size_of::<libc::sockaddr_in>() as libc::socklen_t
        }
        SocketAddr::V6(_) => 0,
    }
}

fn new_socket(addr: SocketAddr) -> io::Result<RawFd> {
    let domain = if addr.is_ipv4() {
        libc::AF_INET
    } else {
        libc::AF_INET6
    };
    // SAFETY: constant arguments.
    let fd = unsafe {
        libc::socket(
            domain,
            libc::SOCK_STREAM | libc::SOCK_CLOEXEC | libc::SOCK_NONBLOCK,
            libc::IPPROTO_TCP,
        )
    };
    if fd < 0 {
        return Err(io::Error::last_os_error());
    }
    Ok(fd)
}

fn bind_reuseport(addr: SocketAddr) -> io::Result<RawFd> {
    let fd = new_socket(addr)?;
    for (level, name) in [
        (libc::SOL_SOCKET, libc::SO_REUSEADDR),
        (libc::SOL_SOCKET, libc::SO_REUSEPORT),
    ] {
        let v: libc::c_int = 1;
        // SAFETY: `v` outlives the call.
        let rc = unsafe {
            libc::setsockopt(
                fd,
                level,
                name,
                std::ptr::addr_of!(v).cast(),
                std::mem::size_of::<libc::c_int>() as libc::socklen_t,
            )
        };
        if rc != 0 {
            return Err(io::Error::last_os_error());
        }
    }
    // SAFETY: `storage` outlives the call and carries its own length.
    unsafe {
        let mut storage: libc::sockaddr_storage = std::mem::zeroed();
        let len = write_sockaddr(&mut storage, addr);
        if libc::bind(fd, std::ptr::addr_of!(storage).cast(), len) != 0 {
            return Err(io::Error::last_os_error());
        }
        if libc::listen(fd, LISTEN_BACKLOG) != 0 {
            return Err(io::Error::last_os_error());
        }
    }
    Ok(fd)
}

/// Pin this thread to one CPU. Thread-per-core is only thread-per-core if the
/// threads stay put; otherwise the per-core ring loses its cache locality.
pub fn pin_to_cpu(cpu: usize) -> io::Result<()> {
    // SAFETY: `set` is zeroed then written through the documented macros
    // before being passed with its own size.
    unsafe {
        let mut set: libc::cpu_set_t = std::mem::zeroed();
        libc::CPU_ZERO(&mut set);
        libc::CPU_SET(cpu, &mut set);
        if libc::sched_setaffinity(0, std::mem::size_of::<libc::cpu_set_t>(), &raw const set) != 0 {
            return Err(io::Error::last_os_error());
        }
    }
    Ok(())
}
