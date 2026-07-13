//! TOML configuration for cumulusd (SPEC.md §6.3).

use std::collections::HashMap;
use std::net::SocketAddr;
use std::path::Path;
use std::path::PathBuf;

use serde::Deserialize;
use thiserror::Error;

/// cumulusd configuration, loaded from a single TOML file.
///
/// ```toml
/// listen_addr = "127.0.0.1:8620"
/// data_dir = "/var/lib/cumulusd"
///
/// [auth]
/// tokens = { "some-secret-token" = "alice" }
///
/// [tls]
/// cert = "/etc/cumulusd/cert.pem"
/// key = "/etc/cumulusd/key.pem"
/// ```
#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Config {
    /// Address to listen on, e.g. `127.0.0.1:8620`.
    pub listen_addr: SocketAddr,
    /// Directory holding per-repo stores (under `<data_dir>/repos/<name>/`).
    pub data_dir: PathBuf,
    /// Bearer-token authentication.
    #[serde(default)]
    pub auth: AuthConfig,
    /// Optional TLS via rustls; plaintext h2c if absent.
    pub tls: Option<TlsConfig>,
}

/// `[auth]` section.
#[derive(Clone, Debug, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AuthConfig {
    /// Map from bearer token to user name. Empty means authentication is
    /// disabled (development mode).
    #[serde(default)]
    pub tokens: HashMap<String, String>,
}

/// `[tls]` section.
#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TlsConfig {
    /// PEM-encoded certificate chain.
    pub cert: PathBuf,
    /// PEM-encoded private key.
    pub key: PathBuf,
}

/// Error loading a [`Config`].
#[derive(Debug, Error)]
pub enum ConfigError {
    /// The file could not be read.
    #[error("failed to read config file {path}")]
    Read {
        /// The offending path.
        path: PathBuf,
        /// Underlying I/O error.
        source: std::io::Error,
    },
    /// The file could not be parsed.
    #[error("failed to parse config file {path}")]
    Parse {
        /// The offending path.
        path: PathBuf,
        /// Underlying TOML error.
        source: Box<toml::de::Error>,
    },
}

impl Config {
    /// Loads the configuration from a TOML file.
    pub fn load(path: &Path) -> Result<Self, ConfigError> {
        let text = std::fs::read_to_string(path).map_err(|source| ConfigError::Read {
            path: path.to_path_buf(),
            source,
        })?;
        toml::from_str(&text).map_err(|source| ConfigError::Parse {
            path: path.to_path_buf(),
            source: Box::new(source),
        })
    }
}
