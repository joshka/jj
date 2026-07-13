use std::sync::Arc;

use cumulus_proto::v1;
use cumulus_proto::v1::repo_service_server::RepoService;
use tonic::Request;
use tonic::Response;
use tonic::Status;

use crate::ServerState;

#[derive(Debug)]
pub(crate) struct RepoApi {
    state: Arc<ServerState>,
}

impl RepoApi {
    pub(crate) fn new(state: Arc<ServerState>) -> Self {
        Self { state }
    }
}

#[tonic::async_trait]
impl RepoService for RepoApi {
    async fn create_repo(
        &self,
        request: Request<v1::CreateRepoRequest>,
    ) -> Result<Response<v1::RepoInfo>, Status> {
        let name = request.into_inner().name;
        let (_store, info) = self.state.repos.create(&name).await?;
        tracing::info!(repo = name, "repo created (or already existed)");
        Ok(Response::new(info))
    }

    async fn get_repo_info(
        &self,
        request: Request<v1::RepoRef>,
    ) -> Result<Response<v1::RepoInfo>, Status> {
        let name = request.into_inner().repo;
        Ok(Response::new(self.state.repos.info(&name).await?))
    }

    async fn list_repos(
        &self,
        _request: Request<v1::Empty>,
    ) -> Result<Response<v1::ListReposResponse>, Status> {
        let repos = self.state.repos.list().await?;
        Ok(Response::new(v1::ListReposResponse { repos }))
    }
}
