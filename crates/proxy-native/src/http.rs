//! Request and response head handling.
//!
//! The proxy never materialises a `Request` object. It parses just enough of
//! the head to route and to know how the body is framed, then writes a
//! rewritten head straight into the outgoing buffer. There is no intermediate
//! representation to allocate, and no header map to build and drop per request.

use lb_core::RouteFilters;
use std::fmt::Write as _;

/// How a message body is delimited.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Body {
    /// No body at all.
    None,
    /// Exactly this many bytes follow the head.
    Length(usize),
    /// `Transfer-Encoding: chunked`.
    Chunked,
    /// Body runs until the peer closes. Only legal on responses.
    UntilClose,
}

/// What a parsed request head tells the proxy.
pub struct RequestHead {
    /// Byte length of the head, including the blank line.
    pub head_len: usize,
    pub method_is_head: bool,
    pub keep_alive: bool,
    pub body: Body,
    /// Offsets of the request target within the source buffer.
    pub path: (usize, usize),
    pub host: Option<(usize, usize)>,
}

/// What a parsed response head tells the proxy.
pub struct ResponseHead {
    pub head_len: usize,
    pub keep_alive: bool,
    pub body: Body,
}

/// Headers a proxy must not forward (RFC 9110 §7.6.1).
///
/// Matched lowercase; `httparse` preserves the wire casing, so comparisons use
/// `eq_ignore_ascii_case`.
const HOP_BY_HOP: [&str; 8] = [
    "connection",
    "keep-alive",
    "proxy-authenticate",
    "proxy-authorization",
    "proxy-connection",
    "te",
    "trailer",
    "upgrade",
];

fn is_hop_by_hop(name: &str) -> bool {
    HOP_BY_HOP.iter().any(|h| name.eq_ignore_ascii_case(h))
}

/// Parse a request head. `Ok(None)` means "need more bytes".
pub fn parse_request(buf: &[u8]) -> Result<Option<RequestHead>, ()> {
    let mut headers = [httparse::EMPTY_HEADER; 64];
    let mut req = httparse::Request::new(&mut headers);
    let head_len = match req.parse(buf) {
        Ok(httparse::Status::Complete(n)) => n,
        Ok(httparse::Status::Partial) => return Ok(None),
        Err(_) => return Err(()),
    };

    let method = req.method.ok_or(())?;
    let target = req.path.ok_or(())?;
    let version_11 = req.version.ok_or(())? == 1;

    // `httparse` hands back subslices of `buf`, so offsets come from pointer
    // arithmetic rather than a search.
    let base = buf.as_ptr() as usize;
    let path_start = target.as_ptr() as usize - base;
    let path = (path_start, target.len());

    let mut host = None;
    let mut keep_alive = version_11;
    let mut content_length = None;
    let mut chunked = false;

    for h in req.headers.iter() {
        if h.name.eq_ignore_ascii_case("host") {
            let start = h.value.as_ptr() as usize - base;
            host = Some((start, h.value.len()));
        } else if h.name.eq_ignore_ascii_case("content-length") {
            content_length = std::str::from_utf8(h.value)
                .ok()
                .and_then(|v| v.trim().parse::<usize>().ok());
        } else if h.name.eq_ignore_ascii_case("transfer-encoding") {
            chunked = h
                .value
                .windows(7)
                .any(|w| w.eq_ignore_ascii_case(b"chunked"));
        } else if h.name.eq_ignore_ascii_case("connection") {
            let v = h.value;
            if v.windows(5).any(|w| w.eq_ignore_ascii_case(b"close")) {
                keep_alive = false;
            } else if v.windows(10).any(|w| w.eq_ignore_ascii_case(b"keep-alive")) {
                keep_alive = true;
            }
        }
    }

    let body = if chunked {
        Body::Chunked
    } else {
        match content_length {
            Some(0) | None => Body::None,
            Some(n) => Body::Length(n),
        }
    };

    Ok(Some(RequestHead {
        head_len,
        method_is_head: method.eq_ignore_ascii_case("HEAD"),
        keep_alive,
        body,
        path,
        host,
    }))
}

/// Parse a response head. `Ok(None)` means "need more bytes".
pub fn parse_response(buf: &[u8], request_was_head: bool) -> Result<Option<ResponseHead>, ()> {
    let mut headers = [httparse::EMPTY_HEADER; 64];
    let mut resp = httparse::Response::new(&mut headers);
    let head_len = match resp.parse(buf) {
        Ok(httparse::Status::Complete(n)) => n,
        Ok(httparse::Status::Partial) => return Ok(None),
        Err(_) => return Err(()),
    };

    let status = resp.code.ok_or(())?;
    let mut keep_alive = resp.version.ok_or(())? == 1;
    let mut content_length = None;
    let mut chunked = false;

    for h in resp.headers.iter() {
        if h.name.eq_ignore_ascii_case("content-length") {
            content_length = std::str::from_utf8(h.value)
                .ok()
                .and_then(|v| v.trim().parse::<usize>().ok());
        } else if h.name.eq_ignore_ascii_case("transfer-encoding") {
            chunked = h
                .value
                .windows(7)
                .any(|w| w.eq_ignore_ascii_case(b"chunked"));
        } else if h.name.eq_ignore_ascii_case("connection")
            && h.value.windows(5).any(|w| w.eq_ignore_ascii_case(b"close"))
        {
            keep_alive = false;
        }
    }

    // RFC 9110 §6.4.1: these carry no body regardless of what the headers say.
    let bodyless =
        request_was_head || status == 204 || status == 304 || (100..200).contains(&status);
    let body = if bodyless {
        Body::None
    } else if chunked {
        Body::Chunked
    } else {
        match content_length {
            Some(0) => Body::None,
            Some(n) => Body::Length(n),
            None => Body::UntilClose,
        }
    };

    Ok(Some(ResponseHead {
        head_len,
        keep_alive,
        body,
    }))
}

/// Write the rewritten request head for the upstream into `out`.
///
/// Copies through every header except the hop-by-hop set and the ones the
/// proxy owns, so the upstream sees the same request the other candidates
/// forward.
pub fn write_request_head(
    out: &mut Vec<u8>,
    src: &[u8],
    head: &RequestHead,
    authority: &str,
    peer_ip: &str,
    filters: &RouteFilters,
    rewritten_path: Option<&str>,
) -> Result<(), ()> {
    let mut headers = [httparse::EMPTY_HEADER; 64];
    let mut req = httparse::Request::new(&mut headers);
    if !matches!(req.parse(src), Ok(httparse::Status::Complete(_))) {
        return Err(());
    }
    let method = req.method.ok_or(())?;
    let path = rewritten_path.unwrap_or_else(|| req.path.unwrap_or("/"));

    out.clear();
    out.extend_from_slice(method.as_bytes());
    out.push(b' ');
    out.extend_from_slice(path.as_bytes());
    out.extend_from_slice(b" HTTP/1.1\r\nhost: ");
    out.extend_from_slice(authority.as_bytes());
    out.extend_from_slice(b"\r\n");

    let mut forwarded_for: Option<&[u8]> = None;
    for h in req.headers.iter() {
        if is_hop_by_hop(h.name)
            || h.name.eq_ignore_ascii_case("host")
            || filters.request_suppresses(h.name)
        {
            continue;
        }
        if h.name.eq_ignore_ascii_case("x-forwarded-for") {
            forwarded_for = Some(h.value);
            continue;
        }
        out.extend_from_slice(h.name.as_bytes());
        out.extend_from_slice(b": ");
        out.extend_from_slice(h.value);
        out.extend_from_slice(b"\r\n");
    }

    out.extend_from_slice(b"x-forwarded-for: ");
    if let Some(existing) = forwarded_for {
        out.extend_from_slice(existing);
        out.extend_from_slice(b", ");
    }
    out.extend_from_slice(peer_ip.as_bytes());
    out.extend_from_slice(b"\r\nx-forwarded-proto: http\r\n");

    filters.write_request_headers(out);

    out.extend_from_slice(b"\r\n");
    let _ = head;
    Ok(())
}

/// Write the rewritten response head for the client into `out`.
pub fn write_response_head(
    out: &mut Vec<u8>,
    src: &[u8],
    keep_alive: bool,
    filters: &RouteFilters,
) -> Result<(), ()> {
    let mut headers = [httparse::EMPTY_HEADER; 64];
    let mut resp = httparse::Response::new(&mut headers);
    if !matches!(resp.parse(src), Ok(httparse::Status::Complete(_))) {
        return Err(());
    }
    let status = resp.code.ok_or(())?;

    out.clear();
    let _ = write!(out_as_fmt(out), "HTTP/1.1 {status} ");
    out.extend_from_slice(resp.reason.unwrap_or("OK").as_bytes());
    out.extend_from_slice(b"\r\n");

    for h in resp.headers.iter() {
        if is_hop_by_hop(h.name) || filters.response_suppresses(h.name) {
            continue;
        }
        out.extend_from_slice(h.name.as_bytes());
        out.extend_from_slice(b": ");
        out.extend_from_slice(h.value);
        out.extend_from_slice(b"\r\n");
    }
    filters.write_response_headers(out);

    out.extend_from_slice(if keep_alive {
        b"connection: keep-alive\r\n\r\n".as_slice()
    } else {
        b"connection: close\r\n\r\n".as_slice()
    });
    Ok(())
}

/// `write!` support for a byte vector.
fn out_as_fmt(out: &mut Vec<u8>) -> impl std::fmt::Write + '_ {
    struct W<'a>(&'a mut Vec<u8>);
    impl std::fmt::Write for W<'_> {
        fn write_str(&mut self, s: &str) -> std::fmt::Result {
            self.0.extend_from_slice(s.as_bytes());
            Ok(())
        }
    }
    W(out)
}

/// A fixed status-line-only response, used for routing failures.
#[must_use]
pub fn status_response(status: u16, reason: &str) -> Vec<u8> {
    format!("HTTP/1.1 {status} {reason}\r\ncontent-length: 0\r\nconnection: keep-alive\r\n\r\n")
        .into_bytes()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_a_simple_get_and_reports_no_body() {
        let raw = b"GET /a?b=1 HTTP/1.1\r\nHost: x.test\r\n\r\n";
        let head = parse_request(raw).unwrap().unwrap();
        assert_eq!(head.head_len, raw.len());
        assert_eq!(head.body, Body::None);
        assert!(head.keep_alive);
        assert_eq!(&raw[head.path.0..head.path.0 + head.path.1], b"/a?b=1");
        let (s, l) = head.host.unwrap();
        assert_eq!(&raw[s..s + l], b"x.test");
    }

    #[test]
    fn a_partial_head_asks_for_more_bytes() {
        assert!(
            parse_request(b"GET / HTTP/1.1\r\nHost: x")
                .unwrap()
                .is_none()
        );
    }

    #[test]
    fn content_length_and_chunked_are_distinguished() {
        let cl = b"POST / HTTP/1.1\r\nHost: x\r\nContent-Length: 12\r\n\r\n";
        assert_eq!(parse_request(cl).unwrap().unwrap().body, Body::Length(12));
        let ch = b"POST / HTTP/1.1\r\nHost: x\r\nTransfer-Encoding: chunked\r\n\r\n";
        assert_eq!(parse_request(ch).unwrap().unwrap().body, Body::Chunked);
    }

    #[test]
    fn connection_close_disables_keep_alive() {
        let raw = b"GET / HTTP/1.1\r\nHost: x\r\nConnection: close\r\n\r\n";
        assert!(!parse_request(raw).unwrap().unwrap().keep_alive);
    }

    #[test]
    fn a_response_without_content_length_runs_until_close() {
        let raw = b"HTTP/1.1 200 OK\r\nServer: t\r\n\r\n";
        assert_eq!(
            parse_response(raw, false).unwrap().unwrap().body,
            Body::UntilClose
        );
    }

    #[test]
    fn head_requests_and_204s_carry_no_body() {
        let raw = b"HTTP/1.1 200 OK\r\nContent-Length: 99\r\n\r\n";
        assert_eq!(parse_response(raw, true).unwrap().unwrap().body, Body::None);
        let raw = b"HTTP/1.1 204 No Content\r\n\r\n";
        assert_eq!(
            parse_response(raw, false).unwrap().unwrap().body,
            Body::None
        );
    }

    #[test]
    fn rewritten_request_drops_hop_by_hop_and_sets_forwarding() {
        let raw = b"GET /p HTTP/1.1\r\nHost: orig\r\nConnection: keep-alive\r\nKeep-Alive: t=5\r\nX-Keep: 1\r\n\r\n";
        let head = parse_request(raw).unwrap().unwrap();
        let mut out = Vec::new();
        write_request_head(
            &mut out,
            raw,
            &head,
            "10.0.0.9:9090",
            "203.0.113.7",
            &RouteFilters::default(),
            None,
        )
        .unwrap();
        let text = String::from_utf8(out).unwrap();
        assert!(text.starts_with("GET /p HTTP/1.1\r\nhost: 10.0.0.9:9090\r\n"));
        assert!(!text.to_lowercase().contains("keep-alive: t=5"));
        assert!(!text.to_lowercase().contains("connection:"));
        assert!(text.contains("x-forwarded-for: 203.0.113.7"));
        assert!(text.contains("X-Keep: 1"));
    }

    #[test]
    fn an_existing_forwarded_chain_is_extended_not_replaced() {
        let raw = b"GET / HTTP/1.1\r\nHost: o\r\nX-Forwarded-For: 1.1.1.1\r\n\r\n";
        let head = parse_request(raw).unwrap().unwrap();
        let mut out = Vec::new();
        write_request_head(
            &mut out,
            raw,
            &head,
            "u:1",
            "2.2.2.2",
            &RouteFilters::default(),
            None,
        )
        .unwrap();
        assert!(
            String::from_utf8(out)
                .unwrap()
                .contains("x-forwarded-for: 1.1.1.1, 2.2.2.2")
        );
    }
}

/// Frames a chunked body as bytes stream past.
///
/// The proxy forwards chunked bodies verbatim, but it still has to know where
/// the body ends: without that, a keep-alive connection would treat the next
/// response's head as body and desynchronise. Scanning for the literal
/// `0\r\n\r\n` is not enough, because those bytes can occur inside chunk data —
/// so this tracks real chunk sizes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ChunkState {
    /// Reading hex chunk-size digits.
    Size,
    /// Skipping a chunk extension up to the CRLF.
    Extension,
    /// Expecting the LF that ends the size line.
    SizeLf,
    /// Passing through `remaining` bytes of chunk data.
    Data,
    /// Expecting the CR that follows chunk data.
    DataCr,
    /// Expecting the LF that follows chunk data.
    DataLf,
    /// After the zero-size chunk: consuming trailers to the final blank line.
    Trailer,
    Done,
    Failed,
}

/// Incremental chunked-body scanner.
#[derive(Debug)]
pub struct ChunkedScan {
    state: ChunkState,
    remaining: u64,
    size_digits: u32,
    /// Consecutive CRLF pairs seen while scanning trailers.
    trailer_crlf: u8,
}

impl Default for ChunkedScan {
    fn default() -> Self {
        Self::new()
    }
}

impl ChunkedScan {
    #[must_use]
    pub fn new() -> Self {
        Self {
            state: ChunkState::Size,
            remaining: 0,
            size_digits: 0,
            trailer_crlf: 0,
        }
    }

    /// Whether the terminal chunk and its trailers have gone past.
    #[must_use]
    pub fn is_complete(&self) -> bool {
        matches!(self.state, ChunkState::Done | ChunkState::Failed)
    }

    /// Feed the next bytes of the body.
    pub fn consume(&mut self, bytes: &[u8]) {
        let mut i = 0;
        while i < bytes.len() {
            match self.state {
                ChunkState::Done | ChunkState::Failed => return,
                ChunkState::Data => {
                    let take = self.remaining.min((bytes.len() - i) as u64) as usize;
                    i += take;
                    self.remaining -= take as u64;
                    if self.remaining == 0 {
                        self.state = ChunkState::DataCr;
                    }
                }
                ChunkState::Size => {
                    let b = bytes[i];
                    match b {
                        b'0'..=b'9' | b'a'..=b'f' | b'A'..=b'F' => {
                            let digit = u64::from((b as char).to_digit(16).unwrap_or(0));
                            // A size this long is malformed, not a huge chunk.
                            if self.size_digits > 16 {
                                self.state = ChunkState::Failed;
                                return;
                            }
                            self.remaining = self.remaining.wrapping_mul(16) + digit;
                            self.size_digits += 1;
                            i += 1;
                        }
                        b';' => {
                            self.state = ChunkState::Extension;
                            i += 1;
                        }
                        b'\r' => {
                            self.state = ChunkState::SizeLf;
                            i += 1;
                        }
                        _ => {
                            self.state = ChunkState::Failed;
                            return;
                        }
                    }
                }
                ChunkState::Extension => {
                    if bytes[i] == b'\r' {
                        self.state = ChunkState::SizeLf;
                    }
                    i += 1;
                }
                ChunkState::SizeLf => {
                    if bytes[i] != b'\n' {
                        self.state = ChunkState::Failed;
                        return;
                    }
                    i += 1;
                    self.size_digits = 0;
                    if self.remaining == 0 {
                        // Zero-size chunk: trailers, then a blank line.
                        self.state = ChunkState::Trailer;
                        self.trailer_crlf = 0;
                    } else {
                        self.state = ChunkState::Data;
                    }
                }
                ChunkState::DataCr => {
                    if bytes[i] != b'\r' {
                        self.state = ChunkState::Failed;
                        return;
                    }
                    i += 1;
                    self.state = ChunkState::DataLf;
                }
                ChunkState::DataLf => {
                    if bytes[i] != b'\n' {
                        self.state = ChunkState::Failed;
                        return;
                    }
                    i += 1;
                    self.state = ChunkState::Size;
                    self.remaining = 0;
                }
                ChunkState::Trailer => {
                    // The body ends at the first empty line, which is a CRLF
                    // arriving with no trailer content between it and the last.
                    match bytes[i] {
                        b'\r' => {}
                        b'\n' => {
                            self.trailer_crlf += 1;
                            if self.trailer_crlf >= 1 {
                                self.state = ChunkState::Done;
                                return;
                            }
                        }
                        _ => self.trailer_crlf = 0,
                    }
                    i += 1;
                }
            }
        }
    }
}

#[cfg(test)]
mod chunked_tests {
    use super::*;

    fn scan_all(parts: &[&[u8]]) -> ChunkedScan {
        let mut s = ChunkedScan::new();
        for p in parts {
            s.consume(p);
        }
        s
    }

    #[test]
    fn a_complete_body_is_detected() {
        let s = scan_all(&[b"5\r\nhello\r\n0\r\n\r\n"]);
        assert!(s.is_complete());
    }

    #[test]
    fn an_incomplete_body_is_not_complete() {
        let s = scan_all(&[b"5\r\nhello\r\n"]);
        assert!(!s.is_complete());
    }

    #[test]
    fn framing_survives_being_split_across_reads() {
        let s = scan_all(&[b"5\r\nhel", b"lo\r\n0\r", b"\n\r\n"]);
        assert!(
            s.is_complete(),
            "chunk framing must not depend on read boundaries"
        );
    }

    #[test]
    fn the_terminator_inside_chunk_data_does_not_end_the_body() {
        // Chunk data that literally contains "0\r\n\r\n"; a naive scan for
        // that byte string would truncate the response here.
        let s = scan_all(&[b"5\r\n0\r\n\r\n\r\n"]);
        assert!(!s.is_complete());
        let mut s = s;
        s.consume(b"0\r\n\r\n");
        assert!(s.is_complete());
    }

    #[test]
    fn chunk_extensions_are_skipped() {
        let s = scan_all(&[b"5;name=value\r\nhello\r\n0\r\n\r\n"]);
        assert!(s.is_complete());
    }

    #[test]
    fn multiple_chunks_are_framed() {
        let s = scan_all(&[b"3\r\nabc\r\n4\r\ndefg\r\n0\r\n\r\n"]);
        assert!(s.is_complete());
    }

    #[test]
    fn malformed_size_stops_the_scan() {
        let s = scan_all(&[b"zz\r\n"]);
        assert!(s.is_complete(), "a failed scan must terminate, not hang");
    }
}
