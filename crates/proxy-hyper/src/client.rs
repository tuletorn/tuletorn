//! Connection-pooled upstream client.

use bytes::Bytes;
use http::{Request, Response};
use http_body_util::combinators::BoxBody;
use http_body_util::{BodyExt, Empty, Full};
use hyper_util::client::legacy::Client;
use hyper_util::client::legacy::connect::HttpConnector;
use hyper_util::rt::TokioExecutor;
use std::time::Duration;

pub type BoxedBody = BoxBody<Bytes, hyper::Error>;

/// Tuning for the upstream connection pool.
#[derive(Debug, Clone)]
pub struct ClientConfig {
    /// Idle connections kept per upstream host.
    pub pool_max_idle_per_host: usize,
    /// How long an idle pooled connection survives.
    pub pool_idle_timeout: Duration,
    /// TCP connect timeout.
    pub connect_timeout: Duration,
    /// Speak HTTP/2 to upstreams (prior knowledge, no ALPN).
    pub http2_upstream: bool,
}

impl Default for ClientConfig {
    fn default() -> Self {
        Self {
            // Sized for the 25k-connection sweeps in plan §8: too small and the
            // proxy churns TCP handshakes that the benchmark then attributes to
            // the runtime.
            pool_max_idle_per_host: 8_192,
            pool_idle_timeout: Duration::from_secs(90),
            connect_timeout: Duration::from_secs(5),
            http2_upstream: false,
        }
    }
}

/// Pooled client used to forward requests to upstream backends.
#[derive(Clone)]
pub struct UpstreamClient {
    client: Client<HttpConnector, BoxedBody>,
    http2_upstream: bool,
}

impl Default for UpstreamClient {
    fn default() -> Self {
        Self::new(&ClientConfig::default())
    }
}

impl UpstreamClient {
    #[must_use]
    pub fn new(cfg: &ClientConfig) -> Self {
        let mut connector = HttpConnector::new();
        connector.set_nodelay(true);
        connector.set_keepalive(Some(Duration::from_secs(60)));
        connector.set_connect_timeout(Some(cfg.connect_timeout));
        // Reuse the address so a benchmark that cycles millions of upstream
        // connections does not exhaust the ephemeral port range in TIME_WAIT.
        connector.set_reuse_address(true);

        let mut builder = Client::builder(TokioExecutor::new());
        builder
            .pool_idle_timeout(cfg.pool_idle_timeout)
            .pool_max_idle_per_host(cfg.pool_max_idle_per_host)
            .pool_timer(hyper_util::rt::TokioTimer::new())
            // Deliberately NOT enabling `http1_preserve_header_case` or
            // `http1_title_case_headers`: both force Hyper to keep an
            // original-case side map and re-case every header on write, which
            // is pure overhead on a proxy hot path and is not something the
            // Traefik baseline pays either.
            .http1_max_buf_size(64 * 1024)
            .retry_canceled_requests(true);

        if cfg.http2_upstream {
            builder.http2_only(true);
        }

        Self {
            client: builder.build(connector),
            http2_upstream: cfg.http2_upstream,
        }
    }

    /// Whether this client speaks HTTP/2 to upstreams.
    #[must_use]
    pub fn is_http2_upstream(&self) -> bool {
        self.http2_upstream
    }

    /// Forward an already-addressed request upstream.
    ///
    /// The caller is responsible for having set `req.uri()` to the absolute
    /// upstream URI and `req.version()` to something the pool can serve;
    /// [`crate::server`] does both without allocating.
    pub async fn forward(
        &self,
        req: Request<BoxedBody>,
    ) -> Result<Response<BoxedBody>, hyper_util::client::legacy::Error> {
        let resp = self.client.request(req).await?;
        let (parts, body) = resp.into_parts();
        Ok(Response::from_parts(parts, body.boxed()))
    }
}

/// An empty body.
#[must_use]
pub fn empty_body() -> BoxedBody {
    Empty::<Bytes>::new()
        .map_err(|never| match never {})
        .boxed()
}

/// A body wrapping a fixed buffer.
#[must_use]
pub fn full_body(data: impl Into<Bytes>) -> BoxedBody {
    Full::new(data.into())
        .map_err(|never| match never {})
        .boxed()
}

#[cfg(test)]
mod tests {
    use super::*;
    use http_body_util::BodyExt;

    #[tokio::test]
    async fn empty_body_yields_no_bytes() {
        let collected = empty_body().collect().await.unwrap().to_bytes();
        assert!(collected.is_empty());
    }

    #[tokio::test]
    async fn full_body_round_trips_its_payload() {
        let collected = full_body("hello").collect().await.unwrap().to_bytes();
        assert_eq!(&collected[..], b"hello");
    }

    #[test]
    fn http2_upstream_flag_is_reported() {
        assert!(!UpstreamClient::default().is_http2_upstream());
        let c = UpstreamClient::new(&ClientConfig {
            http2_upstream: true,
            ..Default::default()
        });
        assert!(c.is_http2_upstream());
    }
}
