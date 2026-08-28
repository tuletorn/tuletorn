//! Turning a received request into an upstream-addressed one.
//!
//! Every candidate must do *exactly* this much work per request, or a
//! throughput difference between them measures the handler rather than the
//! runtime. Keeping the whole sequence — route lookup, backend selection, URI
//! reassembly, header hygiene — in one place is what makes the comparison a
//! comparison.
//!
//! It is deliberately free of any runtime or HTTP-library type: it operates on
//! `http::request::Parts`, which Hyper, Pingora and a hand-rolled `io_uring`
//! server can all produce.

use crate::filter::RouteFilters;
use crate::route_table::SharedRouteTable;
use http::uri::{Authority, PathAndQuery, Scheme};
use http::{HeaderMap, StatusCode, Uri, Version, header};
use std::net::IpAddr;

/// What the caller must do after [`prepare`].
pub enum Prepared {
    /// `parts` now addresses the upstream. Apply `filters` to the response.
    Forward {
        filters: RouteFilters,
        /// Backend address, for error reporting only.
        address: String,
    },
    /// Reply with this status; nothing was forwarded.
    Reject(StatusCode),
}

/// How the upstream hop is spoken.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum UpstreamProtocol {
    Http1,
    Http2,
}

/// Rewrite `parts` in place so it addresses the selected backend.
///
/// Returns [`Prepared::Reject`] without touching `parts` further when the
/// request does not route or the chosen backend is unusable.
pub fn prepare(
    parts: &mut http::request::Parts,
    peer_ip: IpAddr,
    routes: &SharedRouteTable,
    forwarded_headers: bool,
    upstream_protocol: UpstreamProtocol,
) -> Prepared {
    // HTTP/2 carries the authority in :authority, HTTP/1.1 in Host.
    let host = parts.uri.host().or_else(|| {
        parts
            .headers
            .get(header::HOST)
            .and_then(|h| h.to_str().ok())
    });
    let path = parts.uri.path();

    // 1. Wait-free route lookup.
    let table = routes.load();
    let Some(action) = table.lookup(host, path) else {
        return Prepared::Reject(StatusCode::NOT_FOUND);
    };

    // 2. Pick a backend.
    let target = action.target_group.select();
    let Some(authority) = target.authority() else {
        tracing::error!(address = %target.address, "backend address is not a valid URI authority");
        return Prepared::Reject(StatusCode::BAD_GATEWAY);
    };
    let authority = authority.clone();
    let address = target.address.clone();
    let filters = action.filters.clone();
    drop(table);

    // 3. Build the upstream URI. With no rewrite the incoming `PathAndQuery`
    //    is reused as-is: `Uri` is `Bytes`-backed, so that is a refcount bump
    //    rather than a format + reparse.
    let path_and_query = if filters.rewrites_path() {
        let rewritten = filters.apply_url_rewrite(parts.uri.path());
        let with_query = match parts.uri.query() {
            Some(q) => {
                let mut s = String::with_capacity(rewritten.len() + q.len() + 1);
                s.push_str(&rewritten);
                s.push('?');
                s.push_str(q);
                s
            }
            None => rewritten.into_owned(),
        };
        match PathAndQuery::try_from(with_query) {
            Ok(pq) => pq,
            Err(err) => {
                tracing::error!(%err, "rewritten path is not a valid URI path");
                return Prepared::Reject(StatusCode::INTERNAL_SERVER_ERROR);
            }
        }
    } else {
        parts
            .uri
            .path_and_query()
            .cloned()
            .unwrap_or_else(|| PathAndQuery::from_static("/"))
    };

    let mut uri_parts = http::uri::Parts::default();
    uri_parts.scheme = Some(Scheme::HTTP);
    uri_parts.authority = Some(authority.clone());
    uri_parts.path_and_query = Some(path_and_query);
    let upstream_uri = match Uri::from_parts(uri_parts) {
        Ok(uri) => uri,
        Err(err) => {
            tracing::error!(%err, "failed to assemble upstream URI");
            return Prepared::Reject(StatusCode::INTERNAL_SERVER_ERROR);
        }
    };

    // 4. Header hygiene (RFC 9110 §7.6.1). Traefik strips these, so a Rust
    //    candidate that does not would be doing strictly less work.
    crate::proxy::strip_hop_by_hop(&mut parts.headers);
    if forwarded_headers {
        crate::proxy::append_forwarded_for(&mut parts.headers, peer_ip);
        crate::proxy::set_forwarded_proto(&mut parts.headers, "http");
    }
    filters.apply_request_headers(&mut parts.headers);

    // The downstream version must not leak upstream: an h2c client would
    // otherwise force HTTP/2 onto an HTTP/1.1 upstream pool and come back 502.
    parts.version = match upstream_protocol {
        UpstreamProtocol::Http1 => Version::HTTP_11,
        UpstreamProtocol::Http2 => Version::HTTP_2,
    };
    set_authority(&mut parts.headers, &authority, parts.version);
    parts.uri = upstream_uri;

    Prepared::Forward { filters, address }
}

/// HTTP/2 has no `Host`; HTTP/1.1 requires one matching the authority.
fn set_authority(headers: &mut HeaderMap, authority: &Authority, version: Version) {
    if version == Version::HTTP_11 {
        if let Ok(value) = header::HeaderValue::from_str(authority.as_str()) {
            headers.insert(header::HOST, value);
        }
    } else {
        headers.remove(header::HOST);
    }
}

/// Response-side hygiene, symmetric with [`prepare`].
pub fn finish_response(headers: &mut HeaderMap, filters: &RouteFilters) {
    crate::proxy::strip_hop_by_hop(headers);
    filters.apply_response_headers(headers);
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::RouteConfig;
    use http::Request;

    fn table(upstream: &str) -> SharedRouteTable {
        SharedRouteTable::from_table(RouteConfig::single_upstream(upstream).compile())
    }

    fn parts_of(uri: &str) -> http::request::Parts {
        Request::builder()
            .uri(uri)
            .header("host", "example.test")
            .header("connection", "keep-alive")
            .header("keep-alive", "timeout=5")
            .body(())
            .unwrap()
            .into_parts()
            .0
    }

    #[test]
    fn rewrites_uri_to_the_backend_and_strips_hop_by_hop() {
        let routes = table("10.0.0.9:9090");
        let mut parts = parts_of("/api/things?page=2");
        let out = prepare(
            &mut parts,
            "203.0.113.7".parse().unwrap(),
            &routes,
            true,
            UpstreamProtocol::Http1,
        );
        assert!(matches!(out, Prepared::Forward { .. }));
        assert_eq!(
            parts.uri.to_string(),
            "http://10.0.0.9:9090/api/things?page=2"
        );
        assert!(parts.headers.get("keep-alive").is_none());
        assert!(parts.headers.get("connection").is_none());
        assert_eq!(parts.headers.get("x-forwarded-for").unwrap(), "203.0.113.7");
        assert_eq!(parts.headers.get("host").unwrap(), "10.0.0.9:9090");
        assert_eq!(parts.version, Version::HTTP_11);
    }

    #[test]
    fn http2_upstream_drops_host_and_sets_the_version() {
        let routes = table("10.0.0.9:9090");
        let mut parts = parts_of("/x");
        let _ = prepare(
            &mut parts,
            "203.0.113.7".parse().unwrap(),
            &routes,
            false,
            UpstreamProtocol::Http2,
        );
        assert!(parts.headers.get("host").is_none());
        assert_eq!(parts.version, Version::HTTP_2);
        assert!(
            parts.headers.get("x-forwarded-for").is_none(),
            "forwarded headers must be opt-in"
        );
    }

    #[test]
    fn an_unroutable_request_is_rejected_without_rewriting() {
        let routes = SharedRouteTable::from_table(RouteConfig::default().compile());
        let mut parts = parts_of("/nope");
        let out = prepare(
            &mut parts,
            "203.0.113.7".parse().unwrap(),
            &routes,
            true,
            UpstreamProtocol::Http1,
        );
        match out {
            Prepared::Reject(status) => assert_eq!(status, StatusCode::NOT_FOUND),
            Prepared::Forward { .. } => panic!("empty table must not route"),
        }
        assert_eq!(parts.uri.to_string(), "/nope");
    }
}
