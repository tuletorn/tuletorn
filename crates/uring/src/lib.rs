//! `lb-uring` — an `io_uring` I/O layer that Hyper can run on unchanged.
//!
//! Exists so the benchmark can vary *one* thing at a time. The Hyper candidate
//! is Tokio's work-stealing scheduler on epoll; the Monoio candidate changes
//! both the scheduler and the I/O mechanism at once, so a difference between
//! them cannot be attributed to either. Running the same Hyper HTTP stack over
//! this layer isolates the I/O mechanism, and running it under two different
//! schedulers isolates the scheduler.
//!
//! | Module | Responsibility |
//! | --- | --- |
//! | [`reactor`] | The ring, the in-flight op slab, and buffer ownership |
//! | [`stream`] | `AsyncRead`/`AsyncWrite` over `recv`/`send` ops |
//! | [`net`] | Listener, dialler, address conversion |
//! | [`driver`] | Eventfd-driven completion pump and batched submitter |
//! | [`client`] | Hyper connector so upstream hops use the ring too |

pub mod client;
pub mod driver;
pub mod net;
pub mod reactor;
pub mod stream;

pub use client::{UringConn, UringConnector};
pub use driver::{drive, flush_loop};
pub use net::{UringListener, connect};
pub use reactor::{Reactor, ReactorConfig};
pub use stream::UringStream;
