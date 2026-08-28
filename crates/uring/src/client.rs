//! Hyper client connector that dials upstreams over `io_uring`.
//!
//! Without this the proxy would be half epoll: downstream on the ring,
//! upstream on Tokio's `TcpStream`. The measurement only means something if
//! both hops use the same I/O mechanism.

use crate::net;
use crate::reactor::Reactor;
use crate::stream::UringStream;
use http::Uri;
use hyper_util::client::legacy::connect::{Connected, Connection};
use hyper_util::rt::TokioIo;
use std::future::Future;
use std::net::{SocketAddr, ToSocketAddrs};
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

/// A connected upstream socket, wrapped so Hyper's pool recognises it.
pub struct UringConn(TokioIo<UringStream>);

impl hyper::rt::Read for UringConn {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: hyper::rt::ReadBufCursor<'_>,
    ) -> Poll<std::io::Result<()>> {
        // SAFETY-free projection: `TokioIo` is `Unpin` because `UringStream` is.
        Pin::new(&mut self.get_mut().0).poll_read(cx, buf)
    }
}

impl hyper::rt::Write for UringConn {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<std::io::Result<usize>> {
        Pin::new(&mut self.get_mut().0).poll_write(cx, buf)
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        Pin::new(&mut self.get_mut().0).poll_flush(cx)
    }

    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        Pin::new(&mut self.get_mut().0).poll_shutdown(cx)
    }

    fn is_write_vectored(&self) -> bool {
        self.0.is_write_vectored()
    }

    fn poll_write_vectored(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        bufs: &[std::io::IoSlice<'_>],
    ) -> Poll<std::io::Result<usize>> {
        Pin::new(&mut self.get_mut().0).poll_write_vectored(cx, bufs)
    }
}

impl Connection for UringConn {
    fn connected(&self) -> Connected {
        Connected::new()
    }
}

/// `tower::Service<Uri>` handing Hyper's pooled client `io_uring` sockets.
///
/// The reactor is taken from the calling thread rather than captured, so the
/// same connector works for one shared ring and for one ring per pinned core.
#[derive(Clone, Default)]
pub struct UringConnector {
    /// Fixed reactor, or `None` to use the calling thread's.
    reactor: Option<Arc<Reactor>>,
}

impl UringConnector {
    #[must_use]
    pub fn thread_local() -> Self {
        Self { reactor: None }
    }

    #[must_use]
    pub fn shared(reactor: Arc<Reactor>) -> Self {
        Self {
            reactor: Some(reactor),
        }
    }

    fn reactor(&self) -> Arc<Reactor> {
        self.reactor.clone().unwrap_or_else(Reactor::current)
    }
}

impl tower::Service<Uri> for UringConnector {
    type Response = UringConn;
    type Error = std::io::Error;
    type Future = Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>> + Send>>;

    fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, uri: Uri) -> Self::Future {
        let reactor = self.reactor();
        Box::pin(async move {
            let addr = resolve(&uri)?;
            let stream = net::connect(reactor, addr).await?;
            Ok(UringConn(TokioIo::new(stream)))
        })
    }
}

/// Resolve a URI to one socket address.
///
/// Upstreams in this benchmark are literal `ip:port`, which parses without
/// touching the resolver; anything else falls back to the blocking resolver
/// once per connection, which the pool then amortises.
fn resolve(uri: &Uri) -> std::io::Result<SocketAddr> {
    let host = uri
        .host()
        .ok_or_else(|| std::io::Error::other("upstream URI has no host"))?;
    let port = uri.port_u16().unwrap_or(80);

    if let Ok(ip) = host.parse::<std::net::IpAddr>() {
        return Ok(SocketAddr::new(ip, port));
    }
    (host, port)
        .to_socket_addrs()?
        .next()
        .ok_or_else(|| std::io::Error::other("upstream host resolved to nothing"))
}
