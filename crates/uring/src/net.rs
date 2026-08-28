//! Listener, connector and address plumbing on top of the reactor.

use crate::reactor::{AddrSlot, Payload, Reactor, cqe_result};
use crate::stream::{DEFAULT_BUF, UringStream};
use std::future::poll_fn;
use std::io;
use std::net::{Ipv4Addr, Ipv6Addr, SocketAddr, SocketAddrV4, SocketAddrV6};
use std::os::unix::io::RawFd;
use std::sync::Arc;

/// Deep enough that a connection ramp does not overflow the SYN queue and get
/// charged to the proxy as latency. Matches the Hyper candidate.
const LISTEN_BACKLOG: libc::c_int = 65_535;

/// Decode a kernel-filled `sockaddr_storage`.
#[must_use]
pub fn to_socket_addr(slot: &AddrSlot) -> Option<SocketAddr> {
    match i32::from(slot.storage.ss_family) {
        libc::AF_INET => {
            // SAFETY: the family field says this is a `sockaddr_in`.
            let sin = unsafe { &*std::ptr::addr_of!(slot.storage).cast::<libc::sockaddr_in>() };
            Some(SocketAddr::V4(SocketAddrV4::new(
                Ipv4Addr::from(u32::from_be(sin.sin_addr.s_addr)),
                u16::from_be(sin.sin_port),
            )))
        }
        libc::AF_INET6 => {
            // SAFETY: the family field says this is a `sockaddr_in6`.
            let sin6 = unsafe { &*std::ptr::addr_of!(slot.storage).cast::<libc::sockaddr_in6>() };
            Some(SocketAddr::V6(SocketAddrV6::new(
                Ipv6Addr::from(sin6.sin6_addr.s6_addr),
                u16::from_be(sin6.sin6_port),
                sin6.sin6_flowinfo,
                sin6.sin6_scope_id,
            )))
        }
        _ => None,
    }
}

/// Encode a `SocketAddr` into a boxed slot the kernel can borrow.
#[must_use]
pub fn from_socket_addr(addr: SocketAddr) -> Box<AddrSlot> {
    let mut slot = AddrSlot::empty();
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
            // SAFETY: `sockaddr_in` is a prefix-compatible member of the
            // `sockaddr_storage` union.
            unsafe {
                std::ptr::write(
                    std::ptr::addr_of_mut!(slot.storage).cast::<libc::sockaddr_in>(),
                    sin,
                )
            };
            slot.len = std::mem::size_of::<libc::sockaddr_in>() as libc::socklen_t;
        }
        SocketAddr::V6(v6) => {
            let sin6 = libc::sockaddr_in6 {
                sin6_family: libc::AF_INET6 as libc::sa_family_t,
                sin6_port: v6.port().to_be(),
                sin6_flowinfo: v6.flowinfo(),
                sin6_addr: libc::in6_addr {
                    s6_addr: v6.ip().octets(),
                },
                sin6_scope_id: v6.scope_id(),
            };
            // SAFETY: as above, for the v6 member.
            unsafe {
                std::ptr::write(
                    std::ptr::addr_of_mut!(slot.storage).cast::<libc::sockaddr_in6>(),
                    sin6,
                )
            };
            slot.len = std::mem::size_of::<libc::sockaddr_in6>() as libc::socklen_t;
        }
    }
    slot
}

fn last_error<T>(rc: T) -> io::Result<T>
where
    T: PartialOrd<T> + Default,
{
    if rc < T::default() {
        Err(io::Error::last_os_error())
    } else {
        Ok(rc)
    }
}

/// Create a non-blocking TCP socket for `addr`'s family.
pub fn new_socket(addr: SocketAddr) -> io::Result<RawFd> {
    let domain = if addr.is_ipv4() {
        libc::AF_INET
    } else {
        libc::AF_INET6
    };
    // SAFETY: plain syscall with constant arguments.
    let fd = unsafe {
        libc::socket(
            domain,
            libc::SOCK_STREAM | libc::SOCK_CLOEXEC | libc::SOCK_NONBLOCK,
            libc::IPPROTO_TCP,
        )
    };
    last_error(fd)
}

fn set_opt(fd: RawFd, level: libc::c_int, name: libc::c_int, on: bool) -> io::Result<()> {
    let v: libc::c_int = i32::from(on);
    // SAFETY: `v` outlives the call and its size is passed correctly.
    let rc = unsafe {
        libc::setsockopt(
            fd,
            level,
            name,
            std::ptr::addr_of!(v).cast(),
            std::mem::size_of::<libc::c_int>() as libc::socklen_t,
        )
    };
    last_error(rc).map(|_| ())
}

/// A listening socket whose accepts are `io_uring` ops.
pub struct UringListener {
    reactor: Arc<Reactor>,
    fd: RawFd,
    buf_size: usize,
}

impl UringListener {
    /// Bind with `SO_REUSEPORT`, so each worker can own an accept queue.
    pub fn bind_reuseport(reactor: Arc<Reactor>, addr: SocketAddr) -> io::Result<Self> {
        let fd = new_socket(addr)?;
        set_opt(fd, libc::SOL_SOCKET, libc::SO_REUSEADDR, true)?;
        set_opt(fd, libc::SOL_SOCKET, libc::SO_REUSEPORT, true)?;
        set_opt(fd, libc::IPPROTO_TCP, libc::TCP_NODELAY, true)?;

        let slot = from_socket_addr(addr);
        // SAFETY: `slot` outlives the call and carries its own length.
        let rc = unsafe {
            libc::bind(
                fd,
                std::ptr::addr_of!(slot.storage).cast::<libc::sockaddr>(),
                slot.len,
            )
        };
        last_error(rc)?;
        // SAFETY: `fd` is a bound stream socket.
        last_error(unsafe { libc::listen(fd, LISTEN_BACKLOG) })?;

        Ok(Self {
            reactor,
            fd,
            buf_size: DEFAULT_BUF,
        })
    }

    /// Accept one connection.
    pub async fn accept(&self) -> io::Result<(UringStream, SocketAddr)> {
        let op = self.reactor.submit_accept(self.fd)?;
        let mut collected = Some(op);
        let (res, payload) = poll_fn(|cx| {
            let op = collected.expect("accept op polled after completion");
            let out = self.reactor.poll_op(op, cx);
            if out.is_ready() {
                collected = None;
            }
            out
        })
        .await;

        let fd = cqe_result(res)? as RawFd;
        let peer = match &payload {
            Payload::Addr(slot) => to_socket_addr(slot),
            _ => None,
        }
        .unwrap_or_else(|| SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::UNSPECIFIED, 0)));

        let stream = UringStream::from_fd(self.reactor.clone(), fd, self.buf_size);
        stream.set_nodelay(true)?;
        Ok((stream, peer))
    }
}

impl Drop for UringListener {
    fn drop(&mut self) {
        // SAFETY: the listener owns `fd`.
        unsafe { libc::close(self.fd) };
    }
}

/// Connect to `addr` through `reactor`.
pub async fn connect(reactor: Arc<Reactor>, addr: SocketAddr) -> io::Result<UringStream> {
    let fd = new_socket(addr)?;
    set_opt(fd, libc::IPPROTO_TCP, libc::TCP_NODELAY, true)?;

    let op = reactor.submit_connect(fd, from_socket_addr(addr))?;
    let mut pending = Some(op);
    let (res, _payload) = poll_fn(|cx| {
        let op = pending.expect("connect op polled after completion");
        let out = reactor.poll_op(op, cx);
        if out.is_ready() {
            pending = None;
        }
        out
    })
    .await;

    if let Err(err) = cqe_result(res) {
        // SAFETY: we own `fd` and no op references it any more.
        unsafe { libc::close(fd) };
        return Err(err);
    }
    Ok(UringStream::from_fd(reactor, fd, DEFAULT_BUF))
}

impl UringListener {
    /// Accept and hand back the raw fd, leaving it open.
    ///
    /// The thread-per-core dispatcher needs this: it decides *after* accepting
    /// which core should own the connection, and a [`UringStream`] would have
    /// bound the socket to the accepting core's ring and closed it on drop.
    pub async fn accept_raw(&self) -> io::Result<(RawFd, SocketAddr)> {
        let op = self.reactor.submit_accept(self.fd)?;
        let mut pending = Some(op);
        let (res, payload) = poll_fn(|cx| {
            let op = pending.expect("accept op polled after completion");
            let out = self.reactor.poll_op(op, cx);
            if out.is_ready() {
                pending = None;
            }
            out
        })
        .await;

        let fd = cqe_result(res)? as RawFd;
        let peer = match &payload {
            Payload::Addr(slot) => to_socket_addr(slot),
            _ => None,
        }
        .unwrap_or_else(|| SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::UNSPECIFIED, 0)));
        set_opt(fd, libc::IPPROTO_TCP, libc::TCP_NODELAY, true)?;
        Ok((fd, peer))
    }

    /// Buffer size handed to streams this listener produces.
    #[must_use]
    pub fn buf_size(&self) -> usize {
        self.buf_size
    }
}
