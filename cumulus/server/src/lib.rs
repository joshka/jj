//! cumulusd server library (SPEC.md §6).
//!
//! The binary in `main.rs` is a thin wrapper; everything is exposed here so
//! integration tests can run the server in-process on an ephemeral port.

#![warn(missing_docs)]

pub mod auth;
pub mod config;
pub mod repos;
mod services;
mod status;

use std::sync::Arc;

use cumulus_proto::v1::admin_service_server::AdminServiceServer;
use cumulus_proto::v1::index_service_server::IndexServiceServer;
use cumulus_proto::v1::object_service_server::ObjectServiceServer;
use cumulus_proto::v1::op_service_server::OpServiceServer;
use cumulus_proto::v1::repo_service_server::RepoServiceServer;
use tonic::Status;
use tonic::transport::Server;
use tonic::transport::server::Router;

use crate::auth::AuthInterceptor;
use crate::config::Config;
use crate::repos::RepoManager;

/// Shared state behind all services.
#[derive(Debug)]
pub struct ServerState {
    /// Per-repo store handles.
    pub repos: RepoManager,
}

/// Runs a synchronous, possibly-blocking closure on the blocking thread
/// pool. All rusqlite work goes through this (spec §6.3: never hold a
/// connection across awaits — the closure owns the whole store call).
pub(crate) async fn blocking<T: Send + 'static>(
    f: impl FnOnce() -> Result<T, Status> + Send + 'static,
) -> Result<T, Status> {
    tokio::task::spawn_blocking(f)
        .await
        .map_err(|err| Status::internal(format!("blocking task failed: {err}")))?
}

/// Builds the fully-wired tonic router for the given configuration: the
/// four v1 services, the SPEC-ONLY IndexService (UNIMPLEMENTED), and gRPC
/// reflection, all behind the bearer-token interceptor.
pub fn build_router(config: &Config) -> Result<Router, Box<dyn std::error::Error + Send + Sync>> {
    let state = Arc::new(ServerState {
        repos: RepoManager::new(&config.data_dir),
    });
    let auth = AuthInterceptor::new(config.auth.tokens.clone());

    let reflection_v1 = tonic_reflection::server::Builder::configure()
        .register_encoded_file_descriptor_set(cumulus_proto::FILE_DESCRIPTOR_SET)
        .build_v1()?;
    let reflection_v1alpha = tonic_reflection::server::Builder::configure()
        .register_encoded_file_descriptor_set(cumulus_proto::FILE_DESCRIPTOR_SET)
        .build_v1alpha()?;

    let mut builder = Server::builder();
    if let Some(tls) = &config.tls {
        let cert = std::fs::read(&tls.cert)?;
        let key = std::fs::read(&tls.key)?;
        let identity = tonic::transport::Identity::from_pem(cert, key);
        builder =
            builder.tls_config(tonic::transport::ServerTlsConfig::new().identity(identity))?;
    }

    let router = builder
        .add_service(RepoServiceServer::with_interceptor(
            services::repo::RepoApi::new(state.clone()),
            auth.clone(),
        ))
        .add_service(ObjectServiceServer::with_interceptor(
            services::object::ObjectApi::new(state.clone()),
            auth.clone(),
        ))
        .add_service(OpServiceServer::with_interceptor(
            services::op::OpApi::new(state.clone()),
            auth.clone(),
        ))
        .add_service(AdminServiceServer::with_interceptor(
            services::admin::AdminApi::new(state.clone()),
            auth.clone(),
        ))
        .add_service(IndexServiceServer::with_interceptor(
            services::index::IndexApi,
            auth,
        ))
        .add_service(reflection_v1)
        .add_service(reflection_v1alpha);
    Ok(router)
}
