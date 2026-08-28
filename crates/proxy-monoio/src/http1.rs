//! Minimal, allocation-conscious HTTP/1.1 framing for the Monoio data plane.
//!
//! Monoio's completion-based I/O hands buffers *by value*, so nothing here
//! borrows across an await: a buffer is moved into a read, moved back out, and
//! moved into the next write. That is the whole reason this module exists
//! instead of reusing Hyper — Hyper's `AsyncRead`/`AsyncWrite` model would need
//! a compatibility shim that copies on every hop and would defeat the point of
//! benchmarking a thread-per-core, io_uring-friendly runtime.

use bytes::BytesMut;
use std::fmt::Write as _;

/// Maximum bytes of request head we will buffer before giving up.
pub const MAX_HEAD_SIZE: usize = 64 * 1024;
/// Header slots offered to `httparse`.
pub const MAX_HEADERS: usize = 96;

/// How a message body is framed.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BodyLength {
    /// `Content-Length: n`.
    Fixed(u64),
    /// `Transfer-Encoding: chunked`.
    Chunked,
    /// No length and no chunking: the body ends when the connection closes.
    /// Only legal for responses.
    UntilClose,
    /// Framing guarantees no body (HEAD, 204, 304, 1xx).
    Empty,
}

impl BodyLength {
    /// True when nothing needs to be relayed after the head.
    #[must_use]
    pub fn is_empty(self) -> bool {
        matches!(self, Self::Empty | Self::Fixed(0))
    }
}

/// Everything the proxy needs from a parsed request head.
#[derive(Debug, Clone)]
pub struct RequestHead {
    /// Byte length of the head, including the terminating CRLFCRLF.
    pub head_len: usize,
    pub method: String,
    pub target: String,
    pub host: Option<String>,
    pub body: BodyLength,
    /// Client asked to close after this exchange, or spoke HTTP/1.0 without
    /// `Connection: keep-alive`.
    pub close_requested: bool,
    /// Offsets of the request line within the buffer, so it can be rewritten in
    /// place when a route applies a URL rewrite.
    pub target_span: (usize, usize),
}

/// Everything the proxy needs from a parsed response head.
#[derive(Debug, Clone)]
pub struct ResponseHead {
    pub head_len: usize,
    pub status: u16,
    pub body: BodyLength,
    pub close_requested: bool,
}

/// Outcome of trying to parse a head out of a partially filled buffer.
#[derive(Debug)]
pub enum Parsed<T> {
    /// A complete head.
    Complete(T),
    /// Need more bytes.
    Partial,
    /// Malformed; the connection must be closed.
    Invalid(&'static str),
}

/// Parse a request head.
#[must_use]
pub fn parse_request(buf: &[u8]) -> Parsed<RequestHead> {
    let mut headers = [httparse::EMPTY_HEADER; MAX_HEADERS];
    let mut req = httparse::Request::new(&mut headers);
    let head_len = match req.parse(buf) {
        Ok(httparse::Status::Complete(n)) => n,
        Ok(httparse::Status::Partial) => return Parsed::Partial,
        Err(_) => return Parsed::Invalid("malformed request head"),
    };

    let Some(method) = req.method else {
        return Parsed::Invalid("missing method");
    };
    let Some(target) = req.path else {
        return Parsed::Invalid("missing request target");
    };
    let http_11 = req.version != Some(0);

    // Locate the target inside the request line so it can be rewritten without
    // rebuilding the whole head.
    let Some(target_start) = find_subslice(buf, target.as_bytes()) else {
        return Parsed::Invalid("request target not found in buffer");
    };
    let target_span = (target_start, target_start + target.len());

    let mut host = None;
    let mut content_length: Option<u64> = None;
    let mut chunked = false;
    let mut close_requested = !http_11;

    for h in req.headers.iter() {
        if h.name.eq_ignore_ascii_case("host") {
            host = lb_core::simd::validate_utf8(h.value).map(str::to_owned);
        } else if h.name.eq_ignore_ascii_case("content-length") {
            match lb_core::simd::validate_utf8(h.value).and_then(|v| v.trim().parse::<u64>().ok()) {
                Some(n) => content_length = Some(n),
                None => return Parsed::Invalid("invalid Content-Length"),
            }
        } else if h.name.eq_ignore_ascii_case("transfer-encoding") {
            if let Some(v) = lb_core::simd::validate_utf8(h.value) {
                chunked = v
                    .rsplit(',')
                    .next()
                    .is_some_and(|last| last.trim().eq_ignore_ascii_case("chunked"));
            }
        } else if h.name.eq_ignore_ascii_case("connection")
            && let Some(v) = lb_core::simd::validate_utf8(h.value)
        {
            for token in v.split(',') {
                let token = token.trim();
                if token.eq_ignore_ascii_case("close") {
                    close_requested = true;
                } else if token.eq_ignore_ascii_case("keep-alive") {
                    close_requested = false;
                }
            }
        }
    }

    // Transfer-Encoding wins over Content-Length (RFC 9112 §6.1).
    let body = if chunked {
        BodyLength::Chunked
    } else {
        match content_length {
            Some(0) | None => BodyLength::Empty,
            Some(n) => BodyLength::Fixed(n),
        }
    };

    Parsed::Complete(RequestHead {
        head_len,
        method: method.to_owned(),
        target: target.to_owned(),
        host,
        body,
        close_requested,
        target_span,
    })
}

/// Parse a response head.
///
/// `request_method` is needed because a response to `HEAD` carries the
/// `Content-Length` of the body it *would* have sent but no body at all.
#[must_use]
pub fn parse_response(buf: &[u8], request_method: &str) -> Parsed<ResponseHead> {
    let mut headers = [httparse::EMPTY_HEADER; MAX_HEADERS];
    let mut resp = httparse::Response::new(&mut headers);
    let head_len = match resp.parse(buf) {
        Ok(httparse::Status::Complete(n)) => n,
        Ok(httparse::Status::Partial) => return Parsed::Partial,
        Err(_) => return Parsed::Invalid("malformed response head"),
    };
    let Some(status) = resp.code else {
        return Parsed::Invalid("missing status code");
    };
    let http_11 = resp.version != Some(0);

    let mut content_length: Option<u64> = None;
    let mut chunked = false;
    let mut close_requested = !http_11;

    for h in resp.headers.iter() {
        if h.name.eq_ignore_ascii_case("content-length") {
            content_length =
                lb_core::simd::validate_utf8(h.value).and_then(|v| v.trim().parse::<u64>().ok());
        } else if h.name.eq_ignore_ascii_case("transfer-encoding") {
            if let Some(v) = lb_core::simd::validate_utf8(h.value) {
                chunked = v
                    .rsplit(',')
                    .next()
                    .is_some_and(|last| last.trim().eq_ignore_ascii_case("chunked"));
            }
        } else if h.name.eq_ignore_ascii_case("connection")
            && let Some(v) = lb_core::simd::validate_utf8(h.value)
        {
            for token in v.split(',') {
                let token = token.trim();
                if token.eq_ignore_ascii_case("close") {
                    close_requested = true;
                } else if token.eq_ignore_ascii_case("keep-alive") {
                    close_requested = false;
                }
            }
        }
    }

    // RFC 9112 §6.3: these never carry a body regardless of their headers.
    let bodyless = request_method.eq_ignore_ascii_case("HEAD")
        || status == 204
        || status == 304
        || (100..200).contains(&status);

    let body = if bodyless {
        BodyLength::Empty
    } else if chunked {
        BodyLength::Chunked
    } else {
        match content_length {
            Some(0) => BodyLength::Empty,
            Some(n) => BodyLength::Fixed(n),
            // No framing information: the body runs to end-of-connection, which
            // also means keep-alive is off for this connection.
            None => BodyLength::UntilClose,
        }
    };

    Parsed::Complete(ResponseHead {
        head_len,
        status,
        body,
        close_requested,
    })
}

/// Rewrite a request head for forwarding.
///
/// Copies the request line (with `target` substituted) and every header except
/// the hop-by-hop set, then appends `Host` and `X-Forwarded-For`. Writing into a
/// caller-owned `BytesMut` keeps this allocation-free once the buffer has grown
/// to its steady-state size.
pub fn write_forward_head(
    out: &mut BytesMut,
    original: &[u8],
    head: &RequestHead,
    target: &str,
    upstream_host: &str,
    peer: Option<std::net::IpAddr>,
) {
    out.clear();
    // Request line: METHOD SP target SP HTTP/1.1 CRLF
    out.extend_from_slice(head.method.as_bytes());
    out.extend_from_slice(b" ");
    out.extend_from_slice(target.as_bytes());
    out.extend_from_slice(b" HTTP/1.1\r\n");

    // Skip the original request line; copy the surviving headers verbatim.
    let headers_start = match memchr::memmem::find(&original[..head.head_len], b"\r\n") {
        Some(pos) => pos + 2,
        None => head.head_len,
    };

    // `Connection: keep-alive, x-trace-id` makes `x-trace-id` hop-by-hop for
    // this connection only; those names must be dropped as well as the fixed
    // set, or the proxy leaks a header the client meant to end at this hop.
    let nominated = connection_nominated(&original[headers_start..head.head_len]);

    let mut existing_xff: Option<&[u8]> = None;
    for line in split_header_lines(&original[headers_start..head.head_len]) {
        let Some(colon) = memchr::memchr(b':', line) else {
            continue;
        };
        let name = &line[..colon];
        if is_hop_by_hop(name) || eq_ci(name, b"host") || nominated.iter().any(|n| eq_ci(name, n)) {
            continue;
        }
        if eq_ci(name, b"x-forwarded-for") {
            existing_xff = Some(line[colon + 1..].trim_ascii());
            continue;
        }
        out.extend_from_slice(line);
        out.extend_from_slice(b"\r\n");
    }

    out.extend_from_slice(b"Host: ");
    out.extend_from_slice(upstream_host.as_bytes());
    out.extend_from_slice(b"\r\n");

    if let Some(ip) = peer {
        out.extend_from_slice(b"X-Forwarded-For: ");
        if let Some(prev) = existing_xff {
            out.extend_from_slice(prev);
            out.extend_from_slice(b", ");
        }
        let _ = write!(BytesMutWriter(out), "{ip}");
        out.extend_from_slice(b"\r\n");
    }
    out.extend_from_slice(b"X-Forwarded-Proto: http\r\n");
    out.extend_from_slice(b"\r\n");
}

/// Rewrite a response head for return to the client, dropping hop-by-hop
/// headers and forcing the connection disposition we actually intend to honour.
pub fn write_return_head(
    out: &mut BytesMut,
    original: &[u8],
    head: &ResponseHead,
    keep_alive: bool,
) {
    out.clear();
    let status_line_end = match memchr::memmem::find(&original[..head.head_len], b"\r\n") {
        Some(pos) => pos + 2,
        None => head.head_len,
    };
    out.extend_from_slice(&original[..status_line_end]);

    let nominated = connection_nominated(&original[status_line_end..head.head_len]);
    for line in split_header_lines(&original[status_line_end..head.head_len]) {
        let Some(colon) = memchr::memchr(b':', line) else {
            continue;
        };
        let name = &line[..colon];
        if is_hop_by_hop(name) || nominated.iter().any(|n| eq_ci(name, n)) {
            continue;
        }
        out.extend_from_slice(line);
        out.extend_from_slice(b"\r\n");
    }

    out.extend_from_slice(if keep_alive {
        b"Connection: keep-alive\r\n\r\n".as_slice()
    } else {
        b"Connection: close\r\n\r\n".as_slice()
    });
}

/// A fixed status-line-only response, for errors the proxy generates itself.
#[must_use]
pub fn error_response(status: u16, reason: &str) -> Vec<u8> {
    format!("HTTP/1.1 {status} {reason}\r\nContent-Length: 0\r\nConnection: close\r\n\r\n")
        .into_bytes()
}

// ---------------------------------------------------------------------------
// helpers
// ---------------------------------------------------------------------------

struct BytesMutWriter<'a>(&'a mut BytesMut);
impl std::fmt::Write for BytesMutWriter<'_> {
    fn write_str(&mut self, s: &str) -> std::fmt::Result {
        self.0.extend_from_slice(s.as_bytes());
        Ok(())
    }
}

#[inline]
fn eq_ci(a: &[u8], b: &[u8]) -> bool {
    lb_core::simd::eq_ignore_ascii_case(a.trim_ascii(), b)
}

/// Header names nominated as hop-by-hop by a `Connection` header in `block`.
fn connection_nominated(block: &[u8]) -> Vec<Vec<u8>> {
    let mut out = Vec::new();
    for line in split_header_lines(block) {
        let Some(colon) = memchr::memchr(b':', line) else {
            continue;
        };
        if !eq_ci(&line[..colon], b"connection") {
            continue;
        }
        for token in line[colon + 1..].split(|&b| b == b',') {
            let token = token.trim_ascii();
            if token.is_empty() || eq_ci(token, b"close") || eq_ci(token, b"keep-alive") {
                continue;
            }
            out.push(token.to_ascii_lowercase());
        }
    }
    out
}

fn is_hop_by_hop(name: &[u8]) -> bool {
    const NAMES: [&[u8]; 8] = [
        b"connection",
        b"keep-alive",
        b"proxy-authenticate",
        b"proxy-authorization",
        b"proxy-connection",
        b"te",
        b"trailer",
        b"upgrade",
    ];
    NAMES.iter().any(|n| eq_ci(name, n))
}

/// Split a header block into lines, joining obs-fold continuations onto the
/// previous line rather than dropping them.
fn split_header_lines(block: &[u8]) -> impl Iterator<Item = &[u8]> {
    block
        .split(|&b| b == b'\n')
        .map(|line| line.strip_suffix(b"\r").unwrap_or(line))
        .filter(|line| !line.is_empty())
}

fn find_subslice(haystack: &[u8], needle: &[u8]) -> Option<usize> {
    memchr::memmem::find(haystack, needle)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn req(bytes: &[u8]) -> RequestHead {
        match parse_request(bytes) {
            Parsed::Complete(h) => h,
            other => panic!("expected a complete head, got {other:?}"),
        }
    }

    #[test]
    fn parses_a_simple_get() {
        let h = req(b"GET /api/v1?x=1 HTTP/1.1\r\nHost: example.com\r\n\r\n");
        assert_eq!(h.method, "GET");
        assert_eq!(h.target, "/api/v1?x=1");
        assert_eq!(h.host.as_deref(), Some("example.com"));
        assert_eq!(h.body, BodyLength::Empty);
        assert!(!h.close_requested);
    }

    #[test]
    fn partial_head_is_reported_not_guessed() {
        assert!(matches!(
            parse_request(b"GET / HTTP/1.1\r\nHo"),
            Parsed::Partial
        ));
        assert!(matches!(parse_request(b""), Parsed::Partial));
    }

    #[test]
    fn content_length_and_chunked_framing() {
        assert_eq!(
            req(b"POST / HTTP/1.1\r\nContent-Length: 42\r\n\r\n").body,
            BodyLength::Fixed(42)
        );
        assert_eq!(
            req(b"POST / HTTP/1.1\r\nTransfer-Encoding: chunked\r\n\r\n").body,
            BodyLength::Chunked
        );
        // Transfer-Encoding wins over Content-Length.
        assert_eq!(
            req(b"POST / HTTP/1.1\r\nContent-Length: 9\r\nTransfer-Encoding: chunked\r\n\r\n").body,
            BodyLength::Chunked
        );
    }

    #[test]
    fn invalid_content_length_is_rejected() {
        assert!(matches!(
            parse_request(b"POST / HTTP/1.1\r\nContent-Length: abc\r\n\r\n"),
            Parsed::Invalid(_)
        ));
    }

    #[test]
    fn connection_disposition_is_honoured_in_both_directions() {
        assert!(req(b"GET / HTTP/1.1\r\nConnection: close\r\n\r\n").close_requested);
        assert!(
            req(b"GET / HTTP/1.0\r\n\r\n").close_requested,
            "HTTP/1.0 defaults to close"
        );
        assert!(
            !req(b"GET / HTTP/1.0\r\nConnection: keep-alive\r\n\r\n").close_requested,
            "explicit keep-alive overrides the HTTP/1.0 default"
        );
    }

    #[test]
    fn response_framing_covers_the_bodyless_cases() {
        let parse = |b: &[u8], m: &str| match parse_response(b, m) {
            Parsed::Complete(h) => h,
            other => panic!("expected complete, got {other:?}"),
        };
        assert_eq!(
            parse(b"HTTP/1.1 200 OK\r\nContent-Length: 100\r\n\r\n", "HEAD").body,
            BodyLength::Empty,
            "a HEAD response has no body despite Content-Length"
        );
        assert_eq!(
            parse(b"HTTP/1.1 204 No Content\r\n\r\n", "GET").body,
            BodyLength::Empty
        );
        assert_eq!(
            parse(
                b"HTTP/1.1 304 Not Modified\r\nContent-Length: 5\r\n\r\n",
                "GET"
            )
            .body,
            BodyLength::Empty
        );
        assert_eq!(
            parse(b"HTTP/1.1 200 OK\r\n\r\n", "GET").body,
            BodyLength::UntilClose,
            "no framing headers means read until close"
        );
        assert_eq!(
            parse(b"HTTP/1.1 200 OK\r\nContent-Length: 20000\r\n\r\n", "GET").body,
            BodyLength::Fixed(20_000)
        );
    }

    #[test]
    fn forward_head_strips_hop_by_hop_and_sets_host() {
        let original = b"GET /old HTTP/1.1\r\n\
            Host: client.example.com\r\n\
            Connection: keep-alive\r\n\
            Keep-Alive: timeout=5\r\n\
            Proxy-Connection: keep-alive\r\n\
            Upgrade: h2c\r\n\
            TE: trailers\r\n\
            X-Keep: yes\r\n\r\n";
        let head = req(original);
        let mut out = BytesMut::new();
        write_forward_head(
            &mut out,
            original,
            &head,
            "/new",
            "backend:8080",
            Some("10.0.0.7".parse().unwrap()),
        );
        let text = String::from_utf8(out.to_vec()).unwrap();

        assert!(text.starts_with("GET /new HTTP/1.1\r\n"), "{text}");
        assert!(text.contains("Host: backend:8080\r\n"));
        assert!(text.contains("X-Keep: yes\r\n"));
        assert!(text.contains("X-Forwarded-For: 10.0.0.7\r\n"));
        assert!(text.contains("X-Forwarded-Proto: http\r\n"));
        for banned in [
            "Connection:",
            "Keep-Alive:",
            "Proxy-Connection:",
            "Upgrade:",
            "TE:",
        ] {
            assert!(!text.contains(banned), "{banned} survived: {text}");
        }
        assert!(text.ends_with("\r\n\r\n"));
    }

    /// Regression: `Connection: keep-alive, x-custom-hop` marks `x-custom-hop`
    /// as hop-by-hop for this connection, so it must not reach the upstream.
    #[test]
    fn forward_head_drops_headers_nominated_by_connection() {
        let original = b"GET / HTTP/1.1\r\n\
            Host: client\r\n\
            Connection: keep-alive, X-Custom-Hop\r\n\
            X-Custom-Hop: secret\r\n\
            X-Keep: yes\r\n\r\n";
        let head = req(original);
        let mut out = BytesMut::new();
        write_forward_head(&mut out, original, &head, "/", "b:80", None);
        let text = String::from_utf8(out.to_vec()).unwrap();
        assert!(
            !text.contains("X-Custom-Hop"),
            "nominated header leaked: {text}"
        );
        assert!(text.contains("X-Keep: yes"));
    }

    #[test]
    fn connection_nominated_ignores_dispositions() {
        assert!(connection_nominated(b"Connection: close\r\n").is_empty());
        assert!(connection_nominated(b"Connection: keep-alive\r\n").is_empty());
        assert_eq!(
            connection_nominated(b"Connection: keep-alive, X-Trace , x-b\r\n"),
            vec![b"x-trace".to_vec(), b"x-b".to_vec()]
        );
    }

    #[test]
    fn return_head_drops_headers_nominated_by_connection() {
        let original = b"HTTP/1.1 200 OK\r\n\
            Content-Length: 0\r\n\
            Connection: keep-alive, X-Server-Hop\r\n\
            X-Server-Hop: internal\r\n\r\n";
        let head = match parse_response(original, "GET") {
            Parsed::Complete(h) => h,
            other => panic!("{other:?}"),
        };
        let mut out = BytesMut::new();
        write_return_head(&mut out, original, &head, true);
        let text = String::from_utf8(out.to_vec()).unwrap();
        assert!(!text.contains("X-Server-Hop"), "{text}");
    }

    #[test]
    fn forward_head_extends_an_existing_forwarded_for_chain() {
        let original = b"GET / HTTP/1.1\r\nHost: h\r\nX-Forwarded-For: 1.2.3.4\r\n\r\n";
        let head = req(original);
        let mut out = BytesMut::new();
        write_forward_head(
            &mut out,
            original,
            &head,
            "/",
            "b:80",
            Some("5.6.7.8".parse().unwrap()),
        );
        let text = String::from_utf8(out.to_vec()).unwrap();
        assert!(
            text.contains("X-Forwarded-For: 1.2.3.4, 5.6.7.8\r\n"),
            "{text}"
        );
    }

    #[test]
    fn return_head_sets_the_connection_disposition() {
        let original =
            b"HTTP/1.1 200 OK\r\nContent-Length: 3\r\nConnection: close\r\nKeep-Alive: t=1\r\n\r\n";
        let head = match parse_response(original, "GET") {
            Parsed::Complete(h) => h,
            other => panic!("{other:?}"),
        };
        let mut out = BytesMut::new();
        write_return_head(&mut out, original, &head, true);
        let text = String::from_utf8(out.to_vec()).unwrap();
        assert!(text.starts_with("HTTP/1.1 200 OK\r\n"));
        assert!(text.contains("Content-Length: 3\r\n"));
        assert!(text.contains("Connection: keep-alive\r\n"));
        assert!(!text.contains("Keep-Alive: t=1"));

        out.clear();
        write_return_head(&mut out, original, &head, false);
        assert!(
            String::from_utf8(out.to_vec())
                .unwrap()
                .contains("Connection: close\r\n")
        );
    }

    #[test]
    fn error_response_is_well_formed() {
        let bytes = error_response(502, "Bad Gateway");
        let text = String::from_utf8(bytes).unwrap();
        assert_eq!(
            text,
            "HTTP/1.1 502 Bad Gateway\r\nContent-Length: 0\r\nConnection: close\r\n\r\n"
        );
    }

    #[test]
    fn body_length_empty_predicate() {
        assert!(BodyLength::Empty.is_empty());
        assert!(BodyLength::Fixed(0).is_empty());
        assert!(!BodyLength::Fixed(1).is_empty());
        assert!(!BodyLength::Chunked.is_empty());
    }
}
