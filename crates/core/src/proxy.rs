//! HTTP forwarding hygiene shared by every data plane.
//!
//! RFC 9110 §7.6.1 requires an intermediary to remove hop-by-hop headers before
//! forwarding. Passing them through is both a correctness bug (an upstream can
//! see a `Connection: close` meant for the proxy and drop a pooled connection)
//! and a benchmark-fairness bug: Traefik strips them, so a Rust proxy that does
//! not is doing strictly less work per request than its baseline.

use http::HeaderMap;
use http::header::{HeaderName, HeaderValue};

/// `X-Forwarded-For`, added by every candidate so the comparison is like-for-like.
pub const FORWARDED_FOR: HeaderName = HeaderName::from_static("x-forwarded-for");
/// `X-Forwarded-Proto`.
pub const FORWARDED_PROTO: HeaderName = HeaderName::from_static("x-forwarded-proto");

/// Headers that must never be forwarded across a proxy hop.
///
/// `Connection` itself is handled separately because its *value* names further
/// headers that must also be dropped; see [`connection_nominated`].
///
/// Public so that data planes which cannot mutate a `HeaderMap` directly can
/// drive removal through their own API. Pingora is one: its `RequestHeader`
/// keeps a case-preserving side map alongside the `HeaderMap`, and writing to
/// the map behind its back desynchronises the two and trips an assertion.
pub const HOP_BY_HOP: [HeaderName; 7] = [
    HeaderName::from_static("keep-alive"),
    HeaderName::from_static("proxy-authenticate"),
    HeaderName::from_static("proxy-authorization"),
    HeaderName::from_static("proxy-connection"),
    HeaderName::from_static("te"),
    HeaderName::from_static("trailer"),
    HeaderName::from_static("upgrade"),
];

/// Remove hop-by-hop headers from a header map in place.
///
/// Also honours `Connection: <header-name>, ...`, which nominates additional
/// headers as hop-by-hop for this connection only.
///
/// `Transfer-Encoding` is deliberately *not* removed here: the HTTP library
/// owns framing, and stripping it would desynchronise chunked bodies.
pub fn strip_hop_by_hop(headers: &mut HeaderMap) {
    // Collect names listed by `Connection` before removing the header itself.
    let nominated = connection_nominated(headers);
    headers.remove(http::header::CONNECTION);
    for name in HOP_BY_HOP {
        headers.remove(name);
    }
    for name in nominated {
        headers.remove(name);
    }
}

/// Header names nominated as hop-by-hop by a `Connection` header's value.
///
/// `Connection: keep-alive, x-trace-id` makes `x-trace-id` hop-by-hop for this
/// connection only. The `close` and `keep-alive` tokens are connection
/// dispositions, not header names, and are excluded.
#[must_use]
pub fn connection_nominated(headers: &HeaderMap) -> Vec<HeaderName> {
    let mut nominated = Vec::new();
    for value in headers.get_all(http::header::CONNECTION).iter() {
        let Ok(text) = value.to_str() else { continue };
        nominated.extend(nominated_from_value(text));
    }
    nominated
}

/// Parse one `Connection` header value into the header names it nominates.
#[must_use]
pub fn nominated_from_value(value: &str) -> Vec<HeaderName> {
    value
        .split(',')
        .map(str::trim)
        .filter(|token| {
            !token.is_empty()
                && !token.eq_ignore_ascii_case("close")
                && !token.eq_ignore_ascii_case("keep-alive")
        })
        .filter_map(|token| HeaderName::from_bytes(token.as_bytes()).ok())
        .collect()
}

/// Append `peer` to `X-Forwarded-For`, preserving any existing chain.
pub fn append_forwarded_for(headers: &mut HeaderMap, peer: std::net::IpAddr) {
    let mut chain = String::new();
    if let Some(existing) = headers.get(&FORWARDED_FOR).and_then(|v| v.to_str().ok()) {
        chain.push_str(existing);
        chain.push_str(", ");
    }
    chain.push_str(&peer.to_string());
    if let Ok(value) = HeaderValue::from_str(&chain) {
        headers.insert(FORWARDED_FOR, value);
    }
}

/// Set `X-Forwarded-Proto`.
pub fn set_forwarded_proto(headers: &mut HeaderMap, proto: &'static str) {
    headers.insert(FORWARDED_PROTO, HeaderValue::from_static(proto));
}

#[cfg(test)]
mod tests {
    use super::*;

    fn map(pairs: &[(&str, &str)]) -> HeaderMap {
        let mut h = HeaderMap::new();
        for (k, v) in pairs {
            h.append(
                HeaderName::from_bytes(k.as_bytes()).unwrap(),
                HeaderValue::from_str(v).unwrap(),
            );
        }
        h
    }

    #[test]
    fn removes_the_standard_hop_by_hop_set() {
        let mut h = map(&[
            ("connection", "keep-alive"),
            ("keep-alive", "timeout=5"),
            ("proxy-connection", "keep-alive"),
            ("te", "trailers"),
            ("upgrade", "websocket"),
            ("trailer", "expires"),
            ("proxy-authorization", "Basic x"),
            ("proxy-authenticate", "Basic"),
            ("content-type", "application/json"),
        ]);
        strip_hop_by_hop(&mut h);
        assert_eq!(h.len(), 1);
        assert_eq!(h.get("content-type").unwrap(), "application/json");
    }

    #[test]
    fn honours_headers_nominated_by_connection() {
        let mut h = map(&[
            ("connection", "x-custom-hop, x-another"),
            ("x-custom-hop", "1"),
            ("x-another", "2"),
            ("x-keep", "3"),
        ]);
        strip_hop_by_hop(&mut h);
        assert!(h.get("x-custom-hop").is_none());
        assert!(h.get("x-another").is_none());
        assert_eq!(h.get("x-keep").unwrap(), "3");
    }

    #[test]
    fn connection_close_token_is_not_treated_as_a_header_name() {
        let mut h = map(&[("connection", "close"), ("close", "not-a-real-header")]);
        strip_hop_by_hop(&mut h);
        assert_eq!(
            h.get("close").unwrap(),
            "not-a-real-header",
            "the literal 'close' token must not nominate a header"
        );
    }

    #[test]
    fn transfer_encoding_survives_because_the_http_layer_owns_framing() {
        let mut h = map(&[
            ("transfer-encoding", "chunked"),
            ("connection", "keep-alive"),
        ]);
        strip_hop_by_hop(&mut h);
        assert_eq!(h.get("transfer-encoding").unwrap(), "chunked");
    }

    #[test]
    fn forwarded_for_starts_and_extends_a_chain() {
        let mut h = HeaderMap::new();
        append_forwarded_for(&mut h, "10.0.0.1".parse().unwrap());
        assert_eq!(h.get(&FORWARDED_FOR).unwrap(), "10.0.0.1");
        append_forwarded_for(&mut h, "10.0.0.2".parse().unwrap());
        assert_eq!(h.get(&FORWARDED_FOR).unwrap(), "10.0.0.1, 10.0.0.2");
    }

    #[test]
    fn forwarded_proto_is_set() {
        let mut h = HeaderMap::new();
        set_forwarded_proto(&mut h, "http");
        assert_eq!(h.get(&FORWARDED_PROTO).unwrap(), "http");
    }

    #[test]
    fn nominated_names_exclude_connection_dispositions() {
        assert!(nominated_from_value("close").is_empty());
        assert!(nominated_from_value("keep-alive").is_empty());
        assert!(nominated_from_value("").is_empty());
        let names = nominated_from_value("keep-alive, x-trace-id , X-Other");
        let as_str: Vec<&str> = names.iter().map(HeaderName::as_str).collect();
        assert_eq!(as_str, ["x-trace-id", "x-other"]);
    }

    #[test]
    fn hop_by_hop_list_is_exposed_for_data_planes_that_need_it() {
        let hop_by_hop = HOP_BY_HOP;
        let names: Vec<&str> = hop_by_hop.iter().map(HeaderName::as_str).collect();
        assert!(names.contains(&"keep-alive"));
        assert!(names.contains(&"upgrade"));
        assert!(names.contains(&"te"));
        assert!(
            !names.contains(&"connection"),
            "Connection is handled separately because its value nominates others"
        );
    }

    #[test]
    fn empty_map_is_a_no_op() {
        let mut h = HeaderMap::new();
        strip_hop_by_hop(&mut h);
        assert!(h.is_empty());
    }
}
