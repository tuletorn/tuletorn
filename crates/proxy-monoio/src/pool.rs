//! Per-core upstream connection pool.
//!
//! Monoio is thread-per-core and its `TcpStream` is `!Send`, so the pool is
//! deliberately *not* shared between workers: each worker keeps its own
//! connections in a `RefCell`, which needs no locks and no atomics at all.
//!
//! Without this, every request pays a TCP handshake to the upstream. That was
//! the single largest distortion in the original Monoio candidate: it made the
//! benchmark measure connection setup rather than the runtime.

use monoio::net::TcpStream;
use std::cell::RefCell;
use std::collections::HashMap;
use std::rc::Rc;
use std::time::{Duration, Instant};

/// Idle connections retained per upstream address (per core).
const MAX_IDLE_PER_HOST: usize = 8192;
/// Discard idle connections older than this; the upstream may have closed them.
const MAX_IDLE_AGE: Duration = Duration::from_secs(45);

struct Pooled {
    stream: TcpStream,
    parked_at: Instant,
}

/// A thread-local pool of upstream connections.
#[derive(Clone, Default)]
pub struct ConnectionPool {
    inner: Rc<RefCell<HashMap<String, Vec<Pooled>>>>,
}

impl ConnectionPool {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Take an idle connection for `address`, if one is still fresh.
    ///
    /// LIFO: the most recently returned connection is the most likely to still
    /// be open and to have a warm congestion window.
    #[must_use]
    pub fn take(&self, address: &str) -> Option<TcpStream> {
        let mut map = self.inner.borrow_mut();
        let entries = map.get_mut(address)?;
        while let Some(entry) = entries.pop() {
            if entry.parked_at.elapsed() < MAX_IDLE_AGE {
                return Some(entry.stream);
            }
            // Older than the idle window: drop it and try the next one.
        }
        None
    }

    /// Return a connection to the pool for reuse.
    pub fn put(&self, address: &str, stream: TcpStream) {
        let mut map = self.inner.borrow_mut();
        let entries = map.entry(address.to_owned()).or_default();
        if entries.len() < MAX_IDLE_PER_HOST {
            entries.push(Pooled {
                stream,
                parked_at: Instant::now(),
            });
        }
        // At capacity the connection is dropped, which closes it.
    }

    /// Number of idle connections currently held, across all hosts.
    #[must_use]
    pub fn idle_count(&self) -> usize {
        self.inner.borrow().values().map(Vec::len).sum()
    }

    /// Drop every idle connection whose age exceeds the idle window.
    pub fn evict_expired(&self) {
        let mut map = self.inner.borrow_mut();
        for entries in map.values_mut() {
            entries.retain(|e| e.parked_at.elapsed() < MAX_IDLE_AGE);
        }
        map.retain(|_, entries| !entries.is_empty());
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use monoio::net::TcpListener;

    /// Build a connected `TcpStream` pair on a throwaway port.
    async fn connected_pair() -> (TcpStream, TcpStream) {
        let listener = TcpListener::bind("127.0.0.1:0").expect("bind");
        let addr = listener.local_addr().expect("local addr");
        let accept = listener.accept();
        let connect = TcpStream::connect(addr);
        let (accepted, connected) = monoio::join!(accept, connect);
        (accepted.expect("accept").0, connected.expect("connect"))
    }

    #[monoio::test]
    async fn take_returns_none_for_an_unknown_host() {
        let pool = ConnectionPool::new();
        assert!(pool.take("10.0.0.1:80").is_none());
        assert_eq!(pool.idle_count(), 0);
    }

    #[monoio::test]
    async fn put_then_take_reuses_the_connection() {
        let pool = ConnectionPool::new();
        let (_server, client) = connected_pair().await;
        pool.put("backend:80", client);
        assert_eq!(pool.idle_count(), 1);
        assert!(pool.take("backend:80").is_some());
        assert_eq!(pool.idle_count(), 0, "taking must remove it from the pool");
    }

    #[monoio::test]
    async fn connections_are_keyed_by_address() {
        let pool = ConnectionPool::new();
        let (_s, c) = connected_pair().await;
        pool.put("a:80", c);
        assert!(
            pool.take("b:80").is_none(),
            "must not hand out another host's connection"
        );
        assert!(pool.take("a:80").is_some());
    }

    #[monoio::test]
    async fn pool_is_bounded_per_host() {
        let pool = ConnectionPool::new();
        let mut keep_alive = Vec::new();
        for _ in 0..MAX_IDLE_PER_HOST + 8 {
            let (server, client) = connected_pair().await;
            keep_alive.push(server);
            pool.put("backend:80", client);
        }
        assert_eq!(pool.idle_count(), MAX_IDLE_PER_HOST);
    }

    #[monoio::test]
    async fn evict_expired_drops_nothing_while_fresh() {
        let pool = ConnectionPool::new();
        let (_s, c) = connected_pair().await;
        pool.put("backend:80", c);
        pool.evict_expired();
        assert_eq!(pool.idle_count(), 1);
    }
}
