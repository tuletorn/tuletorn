//! Gateway API request/response filters: header modifiers and URL rewrites.
//!
//! Header names and values are parsed and validated **once, at reconcile time**,
//! into `HeaderName`/`HeaderValue`. The previous implementation called
//! `HeaderName::from_bytes` on every modifier on every request, which is a
//! validating parse plus a potential allocation on the hot path; a route with
//! four header modifiers paid eight of them per request.

use http::HeaderMap;
use http::header::{HeaderName, HeaderValue};
use serde::{Deserialize, Serialize};
use std::borrow::Cow;
use std::sync::Arc;

/// A header modification instruction as it arrives from the control plane.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum HeaderModifier {
    Set { name: String, value: String },
    Add { name: String, value: String },
    Remove { name: String },
}

/// URL rewrite rule.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum UrlRewrite {
    /// Replace a path prefix: `prefix` -> `replacement`.
    Prefix { prefix: String, replacement: String },
    /// Replace the whole path.
    Exact(String),
}

/// A header modifier after validation, ready to apply with no parsing.
#[derive(Debug, Clone)]
enum Compiled {
    Set(HeaderName, HeaderValue),
    Add(HeaderName, HeaderValue),
    Remove(HeaderName),
}

impl Compiled {
    fn compile(modifier: &HeaderModifier) -> Option<Self> {
        let parse = |name: &str, value: &str| {
            Some((
                HeaderName::from_bytes(name.as_bytes()).ok()?,
                HeaderValue::from_str(value).ok()?,
            ))
        };
        match modifier {
            HeaderModifier::Set { name, value } => {
                let (n, v) = parse(name, value)?;
                Some(Self::Set(n, v))
            }
            HeaderModifier::Add { name, value } => {
                let (n, v) = parse(name, value)?;
                Some(Self::Add(n, v))
            }
            HeaderModifier::Remove { name } => {
                Some(Self::Remove(HeaderName::from_bytes(name.as_bytes()).ok()?))
            }
        }
    }

    /// The header name this instruction acts on.
    fn name(&self) -> &HeaderName {
        match self {
            Self::Set(n, _) | Self::Add(n, _) | Self::Remove(n) => n,
        }
    }

    /// Whether an incoming header of this name must not be copied through.
    ///
    /// `Set` replaces, `Remove` drops; `Add` appends alongside the original
    /// and so leaves it in place.
    fn suppresses_original(&self) -> bool {
        matches!(self, Self::Set(_, _) | Self::Remove(_))
    }

    #[inline]
    fn apply(&self, headers: &mut HeaderMap) {
        match self {
            Self::Set(n, v) => {
                headers.insert(n, v.clone());
            }
            Self::Add(n, v) => {
                headers.append(n, v.clone());
            }
            Self::Remove(n) => {
                headers.remove(n);
            }
        }
    }
}

/// Emit `Set`/`Add` instructions as wire header lines. `Remove` contributes
/// nothing: it is honoured by suppressing the original instead.
fn write_lines(compiled: &[Compiled], out: &mut Vec<u8>) {
    for c in compiled {
        let (name, value) = match c {
            Compiled::Set(n, v) | Compiled::Add(n, v) => (n, v),
            Compiled::Remove(_) => continue,
        };
        out.extend_from_slice(name.as_str().as_bytes());
        out.extend_from_slice(b": ");
        out.extend_from_slice(value.as_bytes());
        out.extend_from_slice(b"\r\n");
    }
}

#[derive(Debug, Default)]
struct Pipeline {
    request_headers: Vec<Compiled>,
    response_headers: Vec<Compiled>,
    url_rewrite: Option<UrlRewrite>,
}

/// A route's filter pipeline.
///
/// `None` inside means "no filters", which is the common case and costs a null
/// check rather than iterating empty vectors. Cloning shares the compiled
/// pipeline, so expanding one route into several matchit patterns is free.
#[derive(Debug, Default, Clone)]
pub struct RouteFilters {
    inner: Option<Arc<Pipeline>>,
}

impl RouteFilters {
    /// Compile a set of filters. Modifiers that are not valid HTTP header
    /// names or values are dropped with a warning rather than failing the
    /// reconcile, matching how `build` treats conflicting route patterns.
    #[must_use]
    pub fn new(
        request_headers: &[HeaderModifier],
        response_headers: &[HeaderModifier],
        url_rewrite: Option<UrlRewrite>,
    ) -> Self {
        fn compile_all(mods: &[HeaderModifier]) -> Vec<Compiled> {
            mods.iter()
                .filter_map(|m| {
                    let compiled = Compiled::compile(m);
                    if compiled.is_none() {
                        tracing::warn!(modifier = ?m, "dropping invalid header modifier");
                    }
                    compiled
                })
                .collect()
        }

        let request_headers = compile_all(request_headers);
        let response_headers = compile_all(response_headers);
        if request_headers.is_empty() && response_headers.is_empty() && url_rewrite.is_none() {
            return Self::default();
        }
        Self {
            inner: Some(Arc::new(Pipeline {
                request_headers,
                response_headers,
                url_rewrite,
            })),
        }
    }

    /// True when this route has no filters at all, so callers can skip the
    /// header-map borrow entirely.
    #[inline]
    #[must_use]
    pub fn is_noop(&self) -> bool {
        self.inner.is_none()
    }

    /// True when this route rewrites the request path.
    #[inline]
    #[must_use]
    pub fn rewrites_path(&self) -> bool {
        self.inner.as_ref().is_some_and(|p| p.url_rewrite.is_some())
    }

    /// Apply request header modifications in place.
    #[inline]
    pub fn apply_request_headers(&self, headers: &mut HeaderMap) {
        if let Some(p) = &self.inner {
            for m in &p.request_headers {
                m.apply(headers);
            }
        }
    }

    /// Apply response header modifications in place.
    #[inline]
    pub fn apply_response_headers(&self, headers: &mut HeaderMap) {
        if let Some(p) = &self.inner {
            for m in &p.response_headers {
                m.apply(headers);
            }
        }
    }

    /// Apply the URL rewrite to an incoming path.
    ///
    /// Borrows when there is nothing to rewrite, which is the hot path.
    #[inline]
    #[must_use]
    pub fn apply_url_rewrite<'a>(&'a self, path: &'a str) -> Cow<'a, str> {
        let Some(rewrite) = self.inner.as_ref().and_then(|p| p.url_rewrite.as_ref()) else {
            return Cow::Borrowed(path);
        };
        match rewrite {
            UrlRewrite::Exact(exact) => Cow::Borrowed(exact.as_str()),
            UrlRewrite::Prefix {
                prefix,
                replacement,
            } => match path.strip_prefix(prefix.as_str()) {
                Some(rest) => {
                    let mut out = String::with_capacity(replacement.len() + rest.len() + 1);
                    out.push_str(replacement.trim_end_matches('/'));
                    // "/api" + "/api/v1" -> "/v1"; "/api" + "/api" -> "/".
                    // Both sides are normalised so a replacement of "/" and a
                    // remainder of "/users" cannot produce "//users".
                    if rest.is_empty() {
                        if out.is_empty() {
                            out.push('/');
                        }
                    } else {
                        if !rest.starts_with('/') {
                            out.push('/');
                        }
                        out.push_str(rest);
                    }
                    if out.is_empty() {
                        out.push('/');
                    }
                    Cow::Owned(out)
                }
                None => Cow::Borrowed(path),
            },
        }
    }

    /// Whether a request header arriving with this name must be dropped
    /// rather than copied to the upstream.
    ///
    /// Data planes that rewrite headers as bytes cannot use
    /// [`Self::apply_request_headers`], which needs a `HeaderMap`. They copy
    /// the incoming head through, skipping names this reports, and then append
    /// [`Self::write_request_headers`].
    #[must_use]
    pub fn request_suppresses(&self, name: &str) -> bool {
        self.inner.as_ref().is_some_and(|p| {
            p.request_headers
                .iter()
                .any(|c| c.suppresses_original() && c.name().as_str().eq_ignore_ascii_case(name))
        })
    }

    /// Response-side counterpart of [`Self::request_suppresses`].
    #[must_use]
    pub fn response_suppresses(&self, name: &str) -> bool {
        self.inner.as_ref().is_some_and(|p| {
            p.response_headers
                .iter()
                .any(|c| c.suppresses_original() && c.name().as_str().eq_ignore_ascii_case(name))
        })
    }

    /// Append this filter set's request header lines, CRLF-terminated.
    pub fn write_request_headers(&self, out: &mut Vec<u8>) {
        let Some(pipeline) = self.inner.as_ref() else {
            return;
        };
        write_lines(&pipeline.request_headers, out);
    }

    /// Append this filter set's response header lines, CRLF-terminated.
    pub fn write_response_headers(&self, out: &mut Vec<u8>) {
        let Some(pipeline) = self.inner.as_ref() else {
            return;
        };
        write_lines(&pipeline.response_headers, out);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn set(name: &str, value: &str) -> HeaderModifier {
        HeaderModifier::Set {
            name: name.into(),
            value: value.into(),
        }
    }

    #[test]
    fn noop_filters_touch_nothing() {
        let f = RouteFilters::default();
        assert!(f.is_noop());
        let mut headers = HeaderMap::new();
        headers.insert("x-keep", HeaderValue::from_static("1"));
        f.apply_request_headers(&mut headers);
        assert_eq!(headers.len(), 1);
        assert!(matches!(
            f.apply_url_rewrite("/same"),
            Cow::Borrowed("/same")
        ));
    }

    #[test]
    fn set_replaces_add_appends_remove_deletes() {
        let f = RouteFilters::new(
            &[
                set("x-set", "new"),
                HeaderModifier::Add {
                    name: "x-add".into(),
                    value: "b".into(),
                },
                HeaderModifier::Remove {
                    name: "x-drop".into(),
                },
            ],
            &[],
            None,
        );
        let mut h = HeaderMap::new();
        h.insert("x-set", HeaderValue::from_static("old"));
        h.insert("x-add", HeaderValue::from_static("a"));
        h.insert("x-drop", HeaderValue::from_static("bye"));

        f.apply_request_headers(&mut h);

        assert_eq!(h.get("x-set").unwrap(), "new");
        assert_eq!(h.get_all("x-add").iter().count(), 2);
        assert!(h.get("x-drop").is_none());
    }

    #[test]
    fn response_and_request_pipelines_are_independent() {
        let f = RouteFilters::new(&[set("x-req", "1")], &[set("x-resp", "2")], None);
        let mut req = HeaderMap::new();
        let mut resp = HeaderMap::new();
        f.apply_request_headers(&mut req);
        f.apply_response_headers(&mut resp);
        assert!(req.contains_key("x-req") && !req.contains_key("x-resp"));
        assert!(resp.contains_key("x-resp") && !resp.contains_key("x-req"));
    }

    #[test]
    fn invalid_modifiers_are_dropped_not_fatal() {
        let f = RouteFilters::new(
            &[
                set("invalid header name", "v"),
                set("x-good", "v"),
                set("x-bad-value", "bad\nvalue"),
            ],
            &[],
            None,
        );
        let mut h = HeaderMap::new();
        f.apply_request_headers(&mut h);
        assert_eq!(h.len(), 1);
        assert_eq!(h.get("x-good").unwrap(), "v");
    }

    #[test]
    fn prefix_rewrite_strips_and_replaces() {
        let f = RouteFilters::new(
            &[],
            &[],
            Some(UrlRewrite::Prefix {
                prefix: "/api".into(),
                replacement: "/v2".into(),
            }),
        );
        assert_eq!(f.apply_url_rewrite("/api/users"), "/v2/users");
        assert_eq!(f.apply_url_rewrite("/api"), "/v2");
        assert_eq!(f.apply_url_rewrite("/other"), "/other");
        assert!(f.rewrites_path());
    }

    #[test]
    fn prefix_rewrite_to_root_does_not_double_slash() {
        let f = RouteFilters::new(
            &[],
            &[],
            Some(UrlRewrite::Prefix {
                prefix: "/api".into(),
                replacement: "/".into(),
            }),
        );
        assert_eq!(f.apply_url_rewrite("/api/users"), "/users");
        assert_eq!(f.apply_url_rewrite("/api"), "/");
    }

    #[test]
    fn exact_rewrite_replaces_whole_path() {
        let f = RouteFilters::new(&[], &[], Some(UrlRewrite::Exact("/fixed".into())));
        assert_eq!(f.apply_url_rewrite("/anything/at/all"), "/fixed");
    }

    #[test]
    fn clones_share_the_compiled_pipeline() {
        let f = RouteFilters::new(&[set("x", "1")], &[], None);
        let c = f.clone();
        let (a, b) = (f.inner.as_ref().unwrap(), c.inner.as_ref().unwrap());
        assert!(Arc::ptr_eq(a, b));
    }
}
