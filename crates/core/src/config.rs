//! Declarative route configuration for standalone mode.
//!
//! Plan §4.2 runs the PGO profiling pass with
//! `--config examples/pgo_routes.yaml`, so every proxy binary needs to be able
//! to load a route table from disk without a Kubernetes API server.

use crate::filter::{HeaderModifier, RouteFilters, UrlRewrite};
use crate::route_table::{PathMatch, RouteSpec, RouteTable, RouteTableBuilder};
use crate::target::BackendEndpoint;
use serde::{Deserialize, Serialize};
use std::path::Path;

/// Top-level route config file.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct RouteConfig {
    #[serde(default)]
    pub routes: Vec<RouteEntry>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RouteEntry {
    pub name: String,
    #[serde(default)]
    pub hostname: Option<String>,
    #[serde(default = "default_path")]
    pub path: String,
    #[serde(default)]
    pub path_type: PathType,
    pub backends: Vec<BackendEntry>,
    #[serde(default)]
    pub request_headers: Vec<HeaderModifier>,
    #[serde(default)]
    pub response_headers: Vec<HeaderModifier>,
    #[serde(default)]
    pub rewrite: Option<UrlRewrite>,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "PascalCase")]
pub enum PathType {
    #[default]
    PathPrefix,
    Exact,
}

impl From<PathType> for PathMatch {
    fn from(t: PathType) -> Self {
        match t {
            PathType::PathPrefix => PathMatch::Prefix,
            PathType::Exact => PathMatch::Exact,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BackendEntry {
    pub address: String,
    #[serde(default = "default_weight")]
    pub weight: u32,
}

fn default_path() -> String {
    "/".to_string()
}
const fn default_weight() -> u32 {
    1
}

/// Errors from loading a route config.
#[derive(Debug, thiserror::Error)]
pub enum ConfigError {
    #[error("reading {path}: {source}")]
    Io {
        path: String,
        #[source]
        source: std::io::Error,
    },
    #[error("parsing {path}: {source}")]
    Parse {
        path: String,
        #[source]
        source: serde_yaml::Error,
    },
}

impl RouteConfig {
    /// Load and parse a YAML route config.
    pub fn from_path(path: impl AsRef<Path>) -> Result<Self, ConfigError> {
        let path = path.as_ref();
        let text = std::fs::read_to_string(path).map_err(|source| ConfigError::Io {
            path: path.display().to_string(),
            source,
        })?;
        serde_yaml::from_str(&text).map_err(|source| ConfigError::Parse {
            path: path.display().to_string(),
            source,
        })
    }

    /// Compile into a [`RouteTable`].
    #[must_use]
    pub fn compile(&self) -> RouteTable {
        let mut builder = RouteTableBuilder::new();
        for entry in &self.routes {
            builder.add(RouteSpec {
                hostname: entry.hostname.clone(),
                path: entry.path.clone(),
                path_match: entry.path_type.into(),
                endpoints: entry
                    .backends
                    .iter()
                    .map(|b| BackendEndpoint::new(&b.address, b.weight))
                    .collect(),
                filters: RouteFilters::new(
                    &entry.request_headers,
                    &entry.response_headers,
                    entry.rewrite.clone(),
                ),
                route_name: entry.name.clone(),
            });
        }
        builder.build().unwrap_or_default()
    }

    /// A single catch-all route to `upstream`, used when no config is supplied.
    #[must_use]
    pub fn single_upstream(upstream: &str) -> Self {
        Self {
            routes: vec![RouteEntry {
                name: "default-catchall".into(),
                hostname: None,
                path: "/".into(),
                path_type: PathType::PathPrefix,
                backends: vec![BackendEntry {
                    address: upstream.to_string(),
                    weight: 1,
                }],
                request_headers: Vec::new(),
                response_headers: Vec::new(),
                rewrite: None,
            }],
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const SAMPLE: &str = r#"
routes:
  - name: api-v1
    hostname: api.example.com
    path: /v1
    path_type: PathPrefix
    backends:
      - address: 10.0.0.1:8080
        weight: 3
      - address: 10.0.0.2:8080
        weight: 1
    request_headers:
      - !Set { name: x-gateway, value: lb }
    rewrite: !Prefix { prefix: /v1, replacement: /internal }
  - name: health
    path: /healthz
    path_type: Exact
    backends:
      - address: 127.0.0.1:9090
"#;

    #[test]
    fn parses_and_compiles_a_full_config() {
        let cfg: RouteConfig = serde_yaml::from_str(SAMPLE).expect("parse");
        assert_eq!(cfg.routes.len(), 2);
        let table = cfg.compile();

        let api = table
            .lookup(Some("api.example.com"), "/v1/users")
            .expect("api route");
        assert_eq!(api.route_name, "api-v1");
        assert_eq!(
            api.filters.apply_url_rewrite("/v1/users"),
            "/internal/users"
        );
        assert_eq!(api.target_group.len(), 2);

        assert!(table.lookup(None, "/healthz").is_some());
        assert!(
            table.lookup(None, "/healthz/sub").is_none(),
            "Exact must not match descendants"
        );
    }

    #[test]
    fn defaults_fill_in_optional_fields() {
        let cfg: RouteConfig =
            serde_yaml::from_str("routes:\n  - name: r\n    backends:\n      - address: a:80\n")
                .expect("parse");
        let e = &cfg.routes[0];
        assert_eq!(e.path, "/");
        assert_eq!(e.path_type, PathType::PathPrefix);
        assert_eq!(e.backends[0].weight, 1);
    }

    #[test]
    fn single_upstream_serves_root() {
        let table = RouteConfig::single_upstream("127.0.0.1:9090").compile();
        for path in ["/", "/deep/path"] {
            assert_eq!(
                table
                    .lookup(None, path)
                    .unwrap()
                    .target_group
                    .select()
                    .address,
                "127.0.0.1:9090"
            );
        }
    }

    #[test]
    fn missing_file_is_reported_with_its_path() {
        let err = RouteConfig::from_path("/nonexistent/routes.yaml").unwrap_err();
        assert!(err.to_string().contains("/nonexistent/routes.yaml"));
    }

    #[test]
    fn round_trips_through_yaml() {
        let cfg: RouteConfig = serde_yaml::from_str(SAMPLE).unwrap();
        let text = serde_yaml::to_string(&cfg).unwrap();
        let back: RouteConfig = serde_yaml::from_str(&text).unwrap();
        assert_eq!(back.routes.len(), cfg.routes.len());
        assert_eq!(back.routes[0].name, "api-v1");
    }
}
