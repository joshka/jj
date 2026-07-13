use std::path::Path;
use std::path::PathBuf;

use cumulus_proto::v1;
use serde::Deserialize;
use serde::Serialize;
use thiserror::Error;

/// Filename persisted beside the backend's `type` marker.
pub const CONFIG_FILE: &str = "cumulus_repo.toml";

/// Persistent connection and repository identity for a Cumulus cache.
#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct CumulusConfig {
    /// Remote repository name.
    pub repo: String,
    /// Remote gRPC endpoint.
    pub url: String,
    /// Optional file containing a bearer token.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub token_file: Option<PathBuf>,
    /// Commit id length advertised by the server.
    pub commit_id_length: usize,
    /// Change id length advertised by the server.
    pub change_id_length: usize,
    /// Root commit id, encoded as lowercase hexadecimal.
    pub root_commit_id: String,
    /// Empty tree id, encoded as lowercase hexadecimal.
    pub empty_tree_id: String,
    /// Wire protocol version.
    pub protocol_version: u32,
}

/// Error loading or validating a persisted Cumulus configuration.
#[derive(Debug, Error)]
#[non_exhaustive]
pub enum CumulusConfigError {
    /// The configuration file could not be read.
    #[error("failed to read Cumulus configuration at {path}")]
    Read {
        /// Configuration path.
        path: PathBuf,
        /// Underlying I/O error.
        #[source]
        source: std::io::Error,
    },
    /// The configuration file was not valid TOML.
    #[error("failed to parse Cumulus configuration at {path}")]
    Parse {
        /// Configuration path.
        path: PathBuf,
        /// Underlying TOML error.
        #[source]
        source: toml::de::Error,
    },
    /// The configuration could not be serialized.
    #[error("failed to serialize Cumulus configuration")]
    Serialize(#[source] toml::ser::Error),
    /// The server supplied incompatible repository metadata.
    #[error("invalid Cumulus repository metadata: {0}")]
    InvalidRepoInfo(String),
    /// A hexadecimal id in the configuration is invalid.
    #[error("invalid hexadecimal {field} in Cumulus configuration")]
    InvalidHex {
        /// Configuration field name.
        field: &'static str,
    },
    /// The configured token file could not be read.
    #[error("failed to read Cumulus token file at {path}")]
    TokenFile {
        /// Token file path.
        path: PathBuf,
        /// Underlying I/O error.
        #[source]
        source: std::io::Error,
    },
}

impl CumulusConfig {
    /// Builds validated persistent metadata from a server response.
    pub fn from_repo_info(
        url: impl Into<String>,
        token_file: Option<PathBuf>,
        info: &v1::RepoInfo,
    ) -> Result<Self, CumulusConfigError> {
        if info.protocol_version != cumulus_proto::PROTOCOL_VERSION {
            return Err(CumulusConfigError::InvalidRepoInfo(format!(
                "server protocol {}, client protocol {}",
                info.protocol_version,
                cumulus_proto::PROTOCOL_VERSION
            )));
        }
        if info.commit_id_length as usize != cumulus_proto::ids::COMMIT_ID_LENGTH
            || info.change_id_length as usize != cumulus_proto::ids::CHANGE_ID_LENGTH
        {
            return Err(CumulusConfigError::InvalidRepoInfo(format!(
                "server id lengths {}/{}, expected {}/{}",
                info.commit_id_length,
                info.change_id_length,
                cumulus_proto::ids::COMMIT_ID_LENGTH,
                cumulus_proto::ids::CHANGE_ID_LENGTH
            )));
        }
        if info.root_commit_id.len() != info.commit_id_length as usize
            || info.empty_tree_id.len() != info.commit_id_length as usize
        {
            return Err(CumulusConfigError::InvalidRepoInfo(
                "root commit or empty tree id has the wrong length".into(),
            ));
        }
        Ok(Self {
            repo: info.name.clone(),
            url: url.into(),
            token_file,
            commit_id_length: info.commit_id_length as usize,
            change_id_length: info.change_id_length as usize,
            root_commit_id: hex_encode(&info.root_commit_id),
            empty_tree_id: hex_encode(&info.empty_tree_id),
            protocol_version: info.protocol_version,
        })
    }

    /// Loads configuration from the backend store directory.
    pub fn load(store_path: &Path) -> Result<Self, CumulusConfigError> {
        let path = store_path.join(CONFIG_FILE);
        let data = std::fs::read_to_string(&path).map_err(|source| CumulusConfigError::Read {
            path: path.clone(),
            source,
        })?;
        toml::from_str(&data).map_err(|source| CumulusConfigError::Parse { path, source })
    }

    /// Persists configuration in the backend store directory.
    pub fn save(&self, store_path: &Path) -> Result<(), CumulusConfigError> {
        let data = toml::to_string_pretty(self).map_err(CumulusConfigError::Serialize)?;
        let path = store_path.join(CONFIG_FILE);
        std::fs::write(&path, data).map_err(|source| CumulusConfigError::Read { path, source })
    }

    /// Resolves the bearer token without persisting it in the repository.
    pub fn token(&self) -> Result<Option<String>, CumulusConfigError> {
        if let Ok(token) = std::env::var("JJ_CUMULUS_TOKEN") {
            return Ok(Some(token));
        }
        let Some(path) = &self.token_file else {
            return Ok(None);
        };
        let token =
            std::fs::read_to_string(path).map_err(|source| CumulusConfigError::TokenFile {
                path: path.clone(),
                source,
            })?;
        Ok(Some(token.trim().to_owned()))
    }

    pub(crate) fn root_commit_bytes(&self) -> Result<Vec<u8>, CumulusConfigError> {
        hex_decode(&self.root_commit_id).ok_or(CumulusConfigError::InvalidHex {
            field: "root_commit_id",
        })
    }

    pub(crate) fn empty_tree_bytes(&self) -> Result<Vec<u8>, CumulusConfigError> {
        hex_decode(&self.empty_tree_id).ok_or(CumulusConfigError::InvalidHex {
            field: "empty_tree_id",
        })
    }
}

fn hex_encode(bytes: &[u8]) -> String {
    use std::fmt::Write as _;
    bytes.iter().fold(String::new(), |mut output, byte| {
        write!(output, "{byte:02x}").unwrap();
        output
    })
}

fn hex_decode(value: &str) -> Option<Vec<u8>> {
    if !value.len().is_multiple_of(2) {
        return None;
    }
    value
        .as_bytes()
        .chunks_exact(2)
        .map(|pair| {
            let text = std::str::from_utf8(pair).ok()?;
            u8::from_str_radix(text, 16).ok()
        })
        .collect()
}
