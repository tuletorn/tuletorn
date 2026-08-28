//! An `io_uring` reactor that completion-based ops can be driven through from a
//! poll-based executor.
//!
//! # Why this shape
//!
//! `io_uring` is a *completion* interface: the kernel owns a submitted buffer
//! until its CQE arrives. Rust's `AsyncRead`/`AsyncWrite` are *readiness*
//! interfaces: the caller owns the buffer and expects the I/O to happen inside
//! `poll_read`. Bridging the two requires the reactor, not the stream, to own
//! every in-flight buffer — otherwise dropping a future while the kernel still
//! holds a pointer into it is a use-after-free.
//!
//! So each op parks its buffer in [`Reactor`]'s slab. A stream that is dropped
//! mid-flight marks its op orphaned; the reactor frees the buffer when the CQE
//! finally lands. That is the whole safety argument for this module.
//!
//! # Wakeups
//!
//! The ring is registered with an eventfd, so completions can be waited on with
//! whatever the host executor already parks in. That keeps one reactor
//! implementation usable both from a work-stealing multi-thread runtime and
//! from a set of pinned single-thread runtimes.

use io_uring::{IoUring, cqueue, opcode, squeue, types};
use parking_lot::Mutex;
use slab::Slab;
use std::io;
use std::os::unix::io::RawFd;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::task::{Context, Poll, Waker};

/// `user_data` for fire-and-forget SQEs whose completion carries no state.
const IGNORED: u64 = u64::MAX;

/// Identifies an in-flight operation.
pub type OpId = usize;

/// Buffer or address the kernel borrows for the lifetime of an op.
pub enum Payload {
    None,
    Buf(Vec<u8>),
    Addr(Box<AddrSlot>),
}

/// `accept`/`connect` address storage, boxed so its address is stable.
pub struct AddrSlot {
    pub storage: libc::sockaddr_storage,
    pub len: libc::socklen_t,
}

impl AddrSlot {
    #[must_use]
    pub fn empty() -> Box<Self> {
        Box::new(Self {
            // SAFETY: `sockaddr_storage` is a POD union; all-zeroes is a valid
            // bit pattern and the kernel overwrites it before we read it.
            storage: unsafe { std::mem::zeroed() },
            len: std::mem::size_of::<libc::sockaddr_storage>() as libc::socklen_t,
        })
    }
}

enum OpState {
    /// Submitted; nobody has polled it yet.
    InFlight,
    /// Submitted; `Waker` belongs to the task blocked on it.
    Waiting(Waker),
    /// CQE arrived, result not yet collected.
    Done(i32),
    /// The owning future was dropped. The reactor must free the payload when
    /// the CQE lands, and must not touch it before then.
    Orphaned,
}

struct Op {
    state: OpState,
    payload: Payload,
}

struct Inner {
    ring: IoUring,
    ops: Slab<Op>,
    /// SQEs queued but not yet handed to the kernel.
    pending: u32,
}

/// Counters proving how much syscall batching actually happened.
#[derive(Default)]
pub struct Stats {
    /// SQEs pushed.
    pub submitted: AtomicU64,
    /// `io_uring_enter` calls made to submit them. With SQPOLL this stays near
    /// zero; without it, the ratio against `submitted` is the batching factor.
    pub enters: AtomicU64,
    /// CQEs reaped.
    pub completed: AtomicU64,
    /// Reap passes, i.e. eventfd wakeups.
    pub reaps: AtomicU64,
}

impl Stats {
    fn snapshot(&self) -> (u64, u64, u64, u64) {
        (
            self.submitted.load(Ordering::Relaxed),
            self.enters.load(Ordering::Relaxed),
            self.completed.load(Ordering::Relaxed),
            self.reaps.load(Ordering::Relaxed),
        )
    }
}

/// Reactor configuration.
#[derive(Debug, Clone)]
pub struct ReactorConfig {
    /// Ring size. Must be a power of two.
    pub entries: u32,
    /// Let a kernel thread consume the submission queue, so submitting an op
    /// costs no syscall at all. This is the configuration io_uring is fastest
    /// in, and the one epoll has no answer to.
    pub sqpoll: bool,
    /// SQPOLL kernel-thread idle timeout before it needs waking again.
    pub sqpoll_idle_ms: u32,
    /// Hint that only one task issues submissions. Invalid for a shared ring.
    pub single_issuer: bool,
    /// Pin the SQPOLL kernel thread to this CPU, so it lives inside the same
    /// core budget as the workers instead of spilling onto whatever is free.
    pub sqpoll_cpu: Option<u32>,
    /// Queue SQEs without entering the kernel, and let [`crate::driver::flush_loop`]
    /// issue one `io_uring_enter` for the whole batch.
    ///
    /// This is the single biggest lever in this crate. Submitting per operation
    /// costs one syscall per read and per write, which is what epoll costs too —
    /// so io_uring's actual advantage never appears. Batching is what makes the
    /// syscall count sublinear in the operation count.
    pub defer_submit: bool,
    /// Submit inline anyway once this many SQEs are queued, so a burst cannot
    /// outrun the flusher and overflow the ring.
    pub submit_threshold: u32,
}

impl Default for ReactorConfig {
    fn default() -> Self {
        Self {
            entries: 4096,
            sqpoll: false,
            sqpoll_idle_ms: 2_000,
            single_issuer: false,
            sqpoll_cpu: None,
            defer_submit: true,
            submit_threshold: 64,
        }
    }
}

/// An `io_uring` instance plus the state of every op in flight on it.
pub struct Reactor {
    inner: Mutex<Inner>,
    eventfd: RawFd,
    sqpoll: bool,
    defer_submit: bool,
    submit_threshold: u32,
    /// Raised whenever an SQE is queued, so the flusher wakes exactly once per
    /// batch rather than being polled.
    flush_signal: tokio::sync::Notify,
    pub stats: Stats,
}

// SAFETY: every field is either behind the mutex or an immutable scalar.
unsafe impl Send for Reactor {}
unsafe impl Sync for Reactor {}

thread_local! {
    static CURRENT: std::cell::RefCell<Option<Arc<Reactor>>> =
        const { std::cell::RefCell::new(None) };
}

impl Reactor {
    /// Build a reactor and its wakeup eventfd.
    pub fn new(cfg: &ReactorConfig) -> io::Result<Arc<Self>> {
        let mut builder = IoUring::builder();
        if cfg.sqpoll {
            builder.setup_sqpoll(cfg.sqpoll_idle_ms);
        }
        if cfg.single_issuer {
            builder.setup_single_issuer();
        }
        if let Some(cpu) = cfg.sqpoll_cpu
            && cfg.sqpoll
        {
            // IORING_SETUP_SQ_AFF. Without this the poller thread floats onto
            // whatever core is idle, which silently hands the process more CPU
            // than its `taskset` budget allows.
            builder.setup_sqpoll_cpu(cpu);
        }
        let ring = builder.build(cfg.entries)?;

        // SAFETY: `eventfd` with no flags beyond the two below returns a new fd
        // or -1; we check for -1.
        let eventfd = unsafe { libc::eventfd(0, libc::EFD_CLOEXEC | libc::EFD_NONBLOCK) };
        if eventfd < 0 {
            return Err(io::Error::last_os_error());
        }
        ring.submitter().register_eventfd(eventfd)?;

        Ok(Arc::new(Self {
            inner: Mutex::new(Inner {
                ring,
                ops: Slab::with_capacity(1024),
                pending: 0,
            }),
            eventfd,
            sqpoll: cfg.sqpoll,
            defer_submit: cfg.defer_submit,
            submit_threshold: cfg.submit_threshold.max(1),
            flush_signal: tokio::sync::Notify::new(),
            stats: Stats::default(),
        }))
    }

    /// The eventfd the kernel pokes when a CQE is posted.
    #[must_use]
    pub fn eventfd(&self) -> RawFd {
        self.eventfd
    }

    /// Install `reactor` as this thread's reactor.
    pub fn set_current(reactor: Arc<Reactor>) {
        CURRENT.with(|c| *c.borrow_mut() = Some(reactor));
    }

    /// This thread's reactor.
    ///
    /// # Panics
    /// If called from a thread where [`Reactor::set_current`] never ran, which
    /// is a wiring bug rather than a runtime condition.
    #[must_use]
    pub fn current() -> Arc<Reactor> {
        CURRENT.with(|c| {
            c.borrow()
                .clone()
                .expect("no io_uring reactor on this thread")
        })
    }

    /// Whether a reactor is installed on this thread.
    #[must_use]
    pub fn has_current() -> bool {
        CURRENT.with(|c| c.borrow().is_some())
    }

    /// Push one SQE, parking `payload` where the kernel can safely borrow it.
    ///
    /// `build` receives the parked payload — so it can take a pointer that
    /// stays valid for the whole op — and the `user_data` to stamp on the SQE.
    fn submit<F>(&self, payload: Payload, build: F) -> io::Result<OpId>
    where
        F: FnOnce(&mut Payload, u64) -> squeue::Entry,
    {
        let mut guard = self.inner.lock();
        let inner = &mut *guard;

        let key = inner.ops.insert(Op {
            state: OpState::InFlight,
            payload,
        });
        let entry = build(&mut inner.ops[key].payload, key as u64);

        // SAFETY: the payload the SQE points into is owned by the slab and is
        // only released once this op's CQE has been observed.
        let push = unsafe { inner.ring.submission().push(&entry) };
        if push.is_err() {
            // Ring full: flush what is queued and retry once.
            inner.ring.submit()?;
            inner.pending = 0;
            self.stats.enters.fetch_add(1, Ordering::Relaxed);
            // SAFETY: as above.
            if unsafe { inner.ring.submission().push(&entry) }.is_err() {
                inner.ops.remove(key);
                return Err(io::Error::other("io_uring submission queue full"));
            }
        }
        inner.ring.submission().sync();
        inner.pending += 1;
        self.stats.submitted.fetch_add(1, Ordering::Relaxed);

        // Decide whether this SQE goes to the kernel now or rides along with a
        // batch. The whole point of deferring is that `io_uring_enter` is the
        // dominant per-operation cost; see `ReactorConfig::defer_submit`.
        let must_submit = !self.defer_submit || inner.pending >= self.submit_threshold;
        if must_submit {
            self.enter(inner)?;
        }
        drop(guard);

        if !must_submit {
            // Wake the flusher. `Notify` keeps one permit if it is not yet
            // waiting, so this cannot be lost and the ring cannot stall.
            self.flush_signal.notify_one();
        }
        Ok(key)
    }

    /// Hand every queued SQE to the kernel. Caller holds the lock.
    fn enter(&self, inner: &mut Inner) -> io::Result<()> {
        if inner.pending == 0 {
            return Ok(());
        }
        if self.sqpoll {
            // The kernel poller normally picks SQEs up with no syscall at all;
            // it only needs a nudge if it timed out and parked.
            if inner.ring.submission().need_wakeup() {
                inner.ring.submit()?;
                self.stats.enters.fetch_add(1, Ordering::Relaxed);
            }
        } else {
            inner.ring.submit()?;
            self.stats.enters.fetch_add(1, Ordering::Relaxed);
        }
        inner.pending = 0;
        Ok(())
    }

    /// Submit everything queued since the last flush.
    ///
    /// Called from [`crate::driver::flush_loop`] at the end of a scheduler
    /// batch, so one syscall covers every operation the batch produced.
    pub fn flush(&self) -> io::Result<()> {
        let mut guard = self.inner.lock();
        self.enter(&mut guard)
    }

    /// Block until at least one SQE is waiting to be flushed.
    pub async fn wait_for_pending(&self) {
        self.flush_signal.notified().await;
    }

    /// `recv(2)` into a reactor-owned buffer.
    pub fn submit_recv(&self, fd: RawFd, buf: Vec<u8>) -> io::Result<OpId> {
        self.submit(Payload::Buf(buf), |payload, ud| {
            let Payload::Buf(b) = payload else {
                unreachable!("recv payload is always a buffer")
            };
            let cap = b.len() as u32;
            opcode::Recv::new(types::Fd(fd), b.as_mut_ptr(), cap)
                .build()
                .user_data(ud)
        })
    }

    /// `send(2)` of the first `len` bytes of a reactor-owned buffer.
    pub fn submit_send(&self, fd: RawFd, buf: Vec<u8>, len: usize) -> io::Result<OpId> {
        self.submit(Payload::Buf(buf), move |payload, ud| {
            let Payload::Buf(b) = payload else {
                unreachable!("send payload is always a buffer")
            };
            opcode::Send::new(types::Fd(fd), b.as_ptr(), len as u32)
                .build()
                .user_data(ud)
        })
    }

    /// `accept4(2)`, filling a reactor-owned address slot.
    pub fn submit_accept(&self, fd: RawFd) -> io::Result<OpId> {
        self.submit(Payload::Addr(AddrSlot::empty()), |payload, ud| {
            let Payload::Addr(slot) = payload else {
                unreachable!("accept payload is always an address")
            };
            opcode::Accept::new(
                types::Fd(fd),
                std::ptr::addr_of_mut!(slot.storage).cast::<libc::sockaddr>(),
                std::ptr::addr_of_mut!(slot.len),
            )
            // Non-blocking, so the kernel arms an internal poll for later
            // recv/send instead of shunting them onto an io-wq worker thread.
            .flags(libc::SOCK_CLOEXEC | libc::SOCK_NONBLOCK)
            .build()
            .user_data(ud)
        })
    }

    /// `connect(2)` to a reactor-owned address.
    pub fn submit_connect(&self, fd: RawFd, addr: Box<AddrSlot>) -> io::Result<OpId> {
        self.submit(Payload::Addr(addr), |payload, ud| {
            let Payload::Addr(slot) = payload else {
                unreachable!("connect payload is always an address")
            };
            let len = slot.len;
            opcode::Connect::new(
                types::Fd(fd),
                std::ptr::addr_of!(slot.storage).cast::<libc::sockaddr>(),
                len,
            )
            .build()
            .user_data(ud)
        })
    }

    /// Poll an op, registering `cx`'s waker if it is still in flight.
    ///
    /// Before parking, this drains the completion ring itself. On loopback a
    /// `send` often completes between submission and the next poll, and
    /// picking that up here turns a round trip through the driver task — park,
    /// eventfd wake, reschedule — into an inline return. Without it, adapting
    /// completion I/O to a poll-based executor would cost a task wakeup per
    /// read *and* per write, which is a tax epoll never pays.
    ///
    /// Returns the raw CQE result and hands the payload back to the caller.
    pub fn poll_op(&self, id: OpId, cx: &mut Context<'_>) -> Poll<(i32, Payload)> {
        let mut wakers: Vec<Waker> = Vec::new();
        let out = {
            let mut guard = self.inner.lock();

            let ready = match guard.ops.get(id) {
                Some(op) => matches!(op.state, OpState::Done(_) | OpState::Orphaned),
                None => true,
            };
            if !ready {
                let drained = Self::drain_cq(&mut guard, &mut wakers);
                if drained > 0 {
                    self.stats
                        .completed
                        .fetch_add(drained as u64, Ordering::Relaxed);
                }
            }

            match guard.ops.get_mut(id) {
                // Only reachable if a caller polls an op it already collected.
                None => Poll::Ready((-libc::EINVAL, Payload::None)),
                Some(op) => match &op.state {
                    OpState::Done(res) => {
                        let res = *res;
                        let op = guard.ops.remove(id);
                        Poll::Ready((res, op.payload))
                    }
                    OpState::Orphaned => Poll::Ready((-libc::ECANCELED, Payload::None)),
                    _ => {
                        op.state = OpState::Waiting(cx.waker().clone());
                        Poll::Pending
                    }
                },
            }
        };
        // Waking the current task here is possible and harmless: it costs one
        // redundant reschedule, never a missed completion.
        for w in wakers {
            w.wake();
        }
        out
    }

    /// Abandon an op. The payload stays alive until its CQE arrives.
    pub fn forget_op(&self, id: OpId) {
        let mut guard = self.inner.lock();
        let inner = &mut *guard;
        let Some(op) = inner.ops.get_mut(id) else {
            return;
        };
        if matches!(op.state, OpState::Done(_)) {
            inner.ops.remove(id);
            return;
        }
        op.state = OpState::Orphaned;

        // Ask the kernel to finish it now, so a long-parked recv does not hold
        // its buffer for the lifetime of the process.
        let entry = opcode::AsyncCancel::new(id as u64)
            .build()
            .user_data(IGNORED);
        // SAFETY: `AsyncCancel` borrows nothing from userspace.
        if unsafe { inner.ring.submission().push(&entry) }.is_ok() {
            inner.ring.submission().sync();
            inner.pending += 1;
            // Cancels are rare and want to land promptly, so they are not
            // deferred; this also flushes anything queued behind them.
            let _ = self.enter(inner);
        }
    }

    /// Drain the completion queue into `wakers`, under an already-held lock.
    ///
    /// Reading the completion ring is a shared-memory operation, not a
    /// syscall, so doing it speculatively is nearly free.
    fn drain_cq(inner: &mut Inner, wakers: &mut Vec<Waker>) -> usize {
        let mut count = 0usize;
        // SAFETY: the completion queue is only ever consumed under the reactor
        // mutex, so there is no concurrent consumer.
        let mut cq = unsafe { inner.ring.completion_shared() };
        cq.sync();
        for cqe in &mut cq {
            count += 1;
            let ud = cqe.user_data();
            if ud == IGNORED {
                continue;
            }
            let key = ud as usize;
            let Some(op) = inner.ops.get_mut(key) else {
                continue;
            };
            let previous = std::mem::replace(&mut op.state, OpState::Done(cqe.result()));
            match previous {
                OpState::Waiting(w) => wakers.push(w),
                OpState::Orphaned => {
                    // Nobody will collect this; the payload dies here, which is
                    // the point at which it stops being aliased by the kernel.
                    inner.ops.remove(key);
                }
                OpState::InFlight | OpState::Done(_) => {}
            }
        }
        count
    }

    /// Drain the completion queue and wake whoever was waiting. Returns the
    /// number of CQEs consumed.
    pub fn reap(&self) -> usize {
        let mut wakers: Vec<Waker> = Vec::new();
        let count = {
            let mut guard = self.inner.lock();
            Self::drain_cq(&mut guard, &mut wakers)
        };
        self.stats
            .completed
            .fetch_add(count as u64, Ordering::Relaxed);
        self.stats.reaps.fetch_add(1, Ordering::Relaxed);
        for w in wakers {
            w.wake();
        }
        count
    }

    /// In-flight plus uncollected ops. Used by the per-core dispatcher as a
    /// cheap load signal.
    #[must_use]
    pub fn inflight(&self) -> usize {
        self.inner.lock().ops.len()
    }

    /// `(submitted, enters, completed, reaps)`.
    #[must_use]
    pub fn stats_snapshot(&self) -> (u64, u64, u64, u64) {
        self.stats.snapshot()
    }
}

impl Drop for Reactor {
    fn drop(&mut self) {
        // SAFETY: the reactor owns this fd and is being torn down.
        unsafe { libc::close(self.eventfd) };
    }
}

/// Turn a CQE result into an `io::Result`.
pub fn cqe_result(res: i32) -> io::Result<u32> {
    if res < 0 {
        Err(io::Error::from_raw_os_error(-res))
    } else {
        Ok(res as u32)
    }
}

/// Statically assert the CQE type is the 16-byte one we assume.
const _: () = assert!(std::mem::size_of::<cqueue::Entry>() == 16);
