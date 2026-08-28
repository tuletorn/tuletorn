//! Pumping the reactor from a Tokio runtime.
//!
//! The ring is registered with an eventfd, and the eventfd is registered with
//! Tokio's own poller. So the executor parks exactly once no matter how many
//! completions are outstanding, and one wakeup drains the whole completion
//! queue. That is where the syscall saving against epoll comes from: epoll
//! wakes once per readiness change and then pays a `read`/`write` per socket,
//! while this pays one `read` of the eventfd for a batch of finished I/O.

use crate::reactor::Reactor;
use std::os::unix::io::{AsRawFd, RawFd};
use std::sync::Arc;
use tokio::io::unix::AsyncFd;

/// Borrowed eventfd, so `AsyncFd` can register it without owning it.
struct EventFd(RawFd);

impl AsRawFd for EventFd {
    fn as_raw_fd(&self) -> RawFd {
        self.0
    }
}

/// Drain completions forever. Spawn one of these per reactor.
pub async fn drive(reactor: Arc<Reactor>) {
    let async_fd =
        match AsyncFd::with_interest(EventFd(reactor.eventfd()), tokio::io::Interest::READABLE) {
            Ok(fd) => fd,
            Err(err) => {
                tracing::error!(%err, "cannot register io_uring eventfd with the reactor");
                return;
            }
        };

    loop {
        let mut guard = match async_fd.readable().await {
            Ok(g) => g,
            Err(err) => {
                tracing::error!(%err, "io_uring eventfd wait failed");
                return;
            }
        };

        // `try_io` is the only race-free way to consume readiness here: it
        // clears Tokio's cached readiness *only* when the read reports
        // `WouldBlock`. Clearing it unconditionally would discard an edge the
        // kernel raised between the read and the clear, and the reactor would
        // then sleep forever on completions that had already landed.
        let outcome = guard.try_io(|_| {
            let mut counter = [0u8; 8];
            // SAFETY: an eventfd read returns exactly 8 bytes into `counter`.
            let n = unsafe {
                libc::read(
                    reactor.eventfd(),
                    counter.as_mut_ptr().cast(),
                    counter.len(),
                )
            };
            if n < 0 {
                Err(std::io::Error::last_os_error())
            } else {
                Ok(())
            }
        });

        match outcome {
            // Read succeeded, so at least one CQE is waiting.
            Ok(Ok(())) => {
                reactor.reap();
            }
            Ok(Err(err)) => {
                tracing::error!(%err, "io_uring eventfd read failed");
                return;
            }
            // Spurious wakeup; readiness has been cleared for us. Reap anyway,
            // since draining an empty completion queue costs nothing.
            Err(_would_block) => {
                reactor.reap();
            }
        }
    }
}

/// Issue one `io_uring_enter` per scheduler batch instead of one per operation.
///
/// The trick is the `yield_now` after the wakeup. Tokio runs every ready task
/// before it parks, so a task that yields is re-queued behind the rest of the
/// current batch — which makes "after the yield" the closest thing the runtime
/// offers to a pre-park hook. Every task that queued an SQE during that batch
/// gets its submission carried by this single syscall.
///
/// This is what closes the gap against epoll. Submitting per operation costs a
/// syscall per read and per write, which is exactly what epoll pays; only once
/// submissions are batched does the syscall count stop scaling with the
/// operation count.
pub async fn flush_loop(reactor: Arc<Reactor>) {
    loop {
        // Sleeps until something is actually queued, so an idle proxy costs
        // nothing. `Notify` holds a permit if the submitter got here first,
        // so a submission can never be left un-flushed.
        reactor.wait_for_pending().await;
        tokio::task::yield_now().await;
        if let Err(err) = reactor.flush() {
            tracing::error!(%err, "io_uring batch submit failed");
            return;
        }
    }
}
