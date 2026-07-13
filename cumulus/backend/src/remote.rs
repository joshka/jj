use std::path::PathBuf;

use cumulus_proto::v1;
use cumulus_proto::v1::repo_service_client::RepoServiceClient;
use thiserror::Error;
use tonic::Request;
use tonic::metadata::MetadataValue;
use tonic::transport::Endpoint;

use crate::CumulusConfig;

/// Error discovering and validating repository metadata from `cumulusd`.
#[derive(Debug, Error)]
#[non_exhaustive]
pub enum RemoteConfigError {
    /// The client runtime could not be created or joined.
    #[error("Cumulus client runtime failed: {0}")]
    Runtime(String),
    /// The remote endpoint or RPC failed.
    #[error("failed to discover Cumulus repository: {0}")]
    Remote(String),
    /// The server metadata was incompatible.
    #[error(transparent)]
    Config(#[from] crate::CumulusConfigError),
}

/// Creates or opens a remote repository and returns validated local metadata.
pub async fn fetch_remote_config(
    url: String,
    repo: String,
    token_file: Option<PathBuf>,
    create: bool,
) -> Result<CumulusConfig, RemoteConfigError> {
    let token = read_token(token_file.as_deref())?;
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .thread_name("cumulus-discovery")
        .build()
        .map_err(|error| RemoteConfigError::Runtime(error.to_string()))?;
    let request_url = url.clone();
    let request_repo = repo.clone();
    let info = runtime
        .spawn(async move {
            let channel = Endpoint::from_shared(request_url)
                .map_err(|error| RemoteConfigError::Remote(error.to_string()))?
                .connect_lazy();
            let mut client = RepoServiceClient::new(channel);
            if create {
                let request = authorize(
                    v1::CreateRepoRequest { name: request_repo },
                    token.as_deref(),
                )?;
                client
                    .create_repo(request)
                    .await
                    .map(|response| response.into_inner())
                    .map_err(|error| RemoteConfigError::Remote(error.to_string()))
            } else {
                let request = authorize(v1::RepoRef { repo: request_repo }, token.as_deref())?;
                client
                    .get_repo_info(request)
                    .await
                    .map(|response| response.into_inner())
                    .map_err(|error| RemoteConfigError::Remote(error.to_string()))
            }
        })
        .await
        .map_err(|error| RemoteConfigError::Runtime(error.to_string()))??;
    CumulusConfig::from_repo_info(url, token_file, &info).map_err(Into::into)
}

fn read_token(path: Option<&std::path::Path>) -> Result<Option<String>, RemoteConfigError> {
    if let Ok(token) = std::env::var("JJ_CUMULUS_TOKEN") {
        return Ok(Some(token));
    }
    let Some(path) = path else {
        return Ok(None);
    };
    let token = std::fs::read_to_string(path)
        .map_err(|error| RemoteConfigError::Remote(error.to_string()))?;
    Ok(Some(token.trim().to_owned()))
}

fn authorize<T>(message: T, token: Option<&str>) -> Result<Request<T>, RemoteConfigError> {
    let mut request = Request::new(message);
    if let Some(token) = token {
        let value = MetadataValue::try_from(format!("Bearer {token}"))
            .map_err(|error| RemoteConfigError::Remote(error.to_string()))?;
        request.metadata_mut().insert("authorization", value);
    }
    Ok(request)
}
