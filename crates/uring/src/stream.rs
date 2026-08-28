//! A TCP stream whose reads and writes are `io_uring` ops, presented as
//! `AsyncRead`/`AsyncWrite` so Hyper can drive it unchanged.
//!
//! # The cost being measured
//!
//! Completion I/O needs a buffer the kernel owns; `poll_read` hands us one the
//! *caller* owns. So every byte crosses one extra `memcpy` on each side that
//! an epoll stream does not pay. That copy is the price of admission, and the
//! syscalls io_uring saves have to beat it. Making that trade visible is the
//! whole point of this candidate.

use crate::reactor::{OpId, Payload, Reactor, cqe_result};
use std::io;
use std::os::unix::io::{AsRawFd, RawFd};
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll, ready};
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};

/// Per-direction buffer size. Sits *under* Hyper's own buffering, so it only
/// needs to be large enough that a typical request or response crosses in one
/// op, not large enough to hold a whole body.
pub const DEFAULT_BUF: usize = 16 * 1024;

enum ReadState {
    /// No op in flight; the buffer is ours.
    Idle(Vec<u8>),
    /// A `recv` is in flight and the reactor owns the buffer.
    Pending(OpId),
    /// Bytes are buffered, `pos..len` still unread.
    Filled {
        buf: Vec<u8>,
        pos: usize,
        len: usize,
    },
    Eof,
    Failed,
}

enum WriteState {
    Idle(Vec<u8>),
    /// A `send` of `len` bytes is in flight.
    Pending {
        op: OpId,
        len: usize,
    },
    Failed,
}

/// TCP stream backed by `io_uring` `recv`/`send`.
pub struct UringStream {
    reactor: Arc<Reactor>,
    fd: RawFd,
    read: ReadState,
    write: WriteState,
}

impl UringStream {
    /// Adopt an already-connected socket.
    #[must_use]
    pub fn from_fd(reactor: Arc<Reactor>, fd: RawFd, buf_size: usize) -> Self {
        Self {
            reactor,
            fd,
            read: ReadState::Idle(vec![0; buf_size]),
            write: WriteState::Idle(vec![0; buf_size]),
        }
    }

    /// Disable Nagle, matching what every other candidate does on accept.
    pub fn set_nodelay(&self, on: bool) -> io::Result<()> {
        let v: libc::c_int = i32::from(on);
        // SAFETY: `fd` is owned by this stream and `v` outlives the call.
        let rc = unsafe {
            libc::setsockopt(
                self.fd,
                libc::IPPROTO_TCP,
                libc::TCP_NODELAY,
                std::ptr::addr_of!(v).cast(),
                std::mem::size_of::<libc::c_int>() as libc::socklen_t,
            )
        };
        if rc < 0 {
            return Err(io::Error::last_os_error());
        }
        Ok(())
    }
}

impl AsRawFd for UringStream {
    fn as_raw_fd(&self) -> RawFd {
        self.fd
    }
}

impl AsyncRead for UringStream {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        out: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        let this = self.get_mut();
        loop {
            match std::mem::replace(&mut this.read, ReadState::Failed) {
                ReadState::Filled { buf, pos, len } => {
                    let n = (len - pos).min(out.remaining());
                    out.put_slice(&buf[pos..pos + n]);
                    this.read = if pos + n == len {
                        ReadState::Idle(buf)
                    } else {
                        ReadState::Filled {
                            buf,
                            pos: pos + n,
                            len,
                        }
                    };
                    return Poll::Ready(Ok(()));
                }
                ReadState::Eof => {
                    this.read = ReadState::Eof;
                    return Poll::Ready(Ok(()));
                }
                ReadState::Failed => {
                    return Poll::Ready(Err(io::Error::other("uring stream read failed")));
                }
                ReadState::Idle(buf) => match this.reactor.submit_recv(this.fd, buf) {
                    Ok(op) => this.read = ReadState::Pending(op),
                    Err(err) => return Poll::Ready(Err(err)),
                },
                ReadState::Pending(op) => {
                    let (res, payload) = match this.reactor.poll_op(op, cx) {
                        Poll::Pending => {
                            this.read = ReadState::Pending(op);
                            return Poll::Pending;
                        }
                        Poll::Ready(v) => v,
                    };
                    let buf = match payload {
                        Payload::Buf(b) => b,
                        _ => vec![0; DEFAULT_BUF],
                    };
                    match cqe_result(res) {
                        Ok(0) => this.read = ReadState::Eof,
                        Ok(n) => {
                            this.read = ReadState::Filled {
                                buf,
                                pos: 0,
                                len: n as usize,
                            }
                        }
                        Err(err) => return Poll::Ready(Err(err)),
                    }
                }
            }
        }
    }
}

impl AsyncWrite for UringStream {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        data: &[u8],
    ) -> Poll<io::Result<usize>> {
        let this = self.get_mut();
        this.poll_write_impl(cx, &mut |dst| {
            let n = data.len().min(dst.len());
            dst[..n].copy_from_slice(&data[..n]);
            n
        })
    }

    fn poll_write_vectored(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        bufs: &[io::IoSlice<'_>],
    ) -> Poll<io::Result<usize>> {
        let this = self.get_mut();
        this.poll_write_impl(cx, &mut |dst| {
            // Coalescing here is what keeps Hyper's header+body write a single
            // `send` rather than two.
            let mut n = 0;
            for b in bufs {
                if n == dst.len() {
                    break;
                }
                let take = b.len().min(dst.len() - n);
                dst[n..n + take].copy_from_slice(&b[..take]);
                n += take;
            }
            n
        })
    }

    fn is_write_vectored(&self) -> bool {
        true
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        let this = self.get_mut();
        match &this.write {
            WriteState::Idle(_) => Poll::Ready(Ok(())),
            WriteState::Failed => Poll::Ready(Err(io::Error::other("uring stream write failed"))),
            WriteState::Pending { .. } => {
                // Drain the outstanding send; the byte count is discarded
                // because the caller will re-present anything unwritten.
                let _ = ready!(this.poll_write_impl(cx, &mut |_| 0))?;
                Poll::Ready(Ok(()))
            }
        }
    }

    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        let this = self.get_mut();
        // Half-closing on top of an outstanding `send` would truncate it, and
        // the peer would see a short response rather than an error.
        if matches!(this.write, WriteState::Pending { .. }) {
            let _ = ready!(this.poll_write_impl(cx, &mut |_| 0))?;
        }
        // SAFETY: `fd` is owned by this stream.
        unsafe { libc::shutdown(this.fd, libc::SHUT_WR) };
        Poll::Ready(Ok(()))
    }
}

impl UringStream {
    /// Shared write path.
    ///
    /// `fill` copies caller bytes into the reactor-owned buffer and returns how
    /// many it wrote. Returning `Pending` after a successful copy is safe
    /// because `AsyncWrite` requires the caller to re-present the same bytes on
    /// the retry, so the bytes the kernel sent stay a prefix of what the caller
    /// believes it wrote.
    fn poll_write_impl(
        &mut self,
        cx: &mut Context<'_>,
        fill: &mut dyn FnMut(&mut [u8]) -> usize,
    ) -> Poll<io::Result<usize>> {
        loop {
            match std::mem::replace(&mut self.write, WriteState::Failed) {
                WriteState::Failed => {
                    return Poll::Ready(Err(io::Error::other("uring stream write failed")));
                }
                WriteState::Idle(mut buf) => {
                    let n = fill(&mut buf);
                    if n == 0 {
                        self.write = WriteState::Idle(buf);
                        return Poll::Ready(Ok(0));
                    }
                    match self.reactor.submit_send(self.fd, buf, n) {
                        Ok(op) => self.write = WriteState::Pending { op, len: n },
                        Err(err) => return Poll::Ready(Err(err)),
                    }
                }
                WriteState::Pending { op, len } => {
                    let (res, payload) = match self.reactor.poll_op(op, cx) {
                        Poll::Pending => {
                            self.write = WriteState::Pending { op, len };
                            return Poll::Pending;
                        }
                        Poll::Ready(v) => v,
                    };
                    let buf = match payload {
                        Payload::Buf(b) => b,
                        _ => vec![0; DEFAULT_BUF],
                    };
                    self.write = WriteState::Idle(buf);
                    return match cqe_result(res) {
                        Ok(n) => Poll::Ready(Ok((n as usize).min(len))),
                        Err(err) => Poll::Ready(Err(err)),
                    };
                }
            }
        }
    }
}

impl Drop for UringStream {
    fn drop(&mut self) {
        // Ops still in flight keep their buffers alive inside the reactor until
        // their CQE lands; that is what makes dropping mid-read sound.
        if let ReadState::Pending(op) = self.read {
            self.reactor.forget_op(op);
        }
        if let WriteState::Pending { op, .. } = self.write {
            self.reactor.forget_op(op);
        }
        // SAFETY: this stream owns `fd`. Closing it while ops are queued is
        // safe: io_uring resolved the fd to a `struct file` at submission and
        // holds its own reference for the duration of the op.
        unsafe { libc::close(self.fd) };
    }
}
