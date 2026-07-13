use std::path::Path;
use std::path::PathBuf;
use std::sync::Arc;

use cumulus_backend::CumulusBackend;
use cumulus_backend::CumulusConfig;
use cumulus_backend::CumulusOpHeadsStore;
use cumulus_backend::CumulusOpStore;
use cumulus_backend::SyncEngine;
use cumulus_backend::fetch_remote_config;
use jj_lib::ref_name::WorkspaceNameBuf;
use jj_lib::repo::ReadonlyRepo;
use jj_lib::repo::Repo as _;
use jj_lib::signing::Signer;
use jj_lib::workspace::Workspace;
use jj_lib::workspace::WorkspaceInitError;
use jj_lib::workspace::default_working_copy_factory;

use crate::cli_util::CommandHelper;
use crate::command_error::CommandError;
use crate::command_error::user_error;
use crate::command_error::user_error_with_message;
use crate::ui::Ui;

pub(super) async fn initialize_workspace(
    ui: &Ui,
    command: &CommandHelper,
    workspace_root: &Path,
    url: String,
    repo: String,
    create: bool,
    workspace_name: WorkspaceNameBuf,
) -> Result<(Workspace, Arc<ReadonlyRepo>, CumulusConfig), CommandError> {
    let (settings, _) = command.settings_for_new_workspace(ui, workspace_root)?;
    let token_file = settings
        .get_string("cumulus.remotes.origin.token-file")
        .ok()
        .map(PathBuf::from);
    let desired_auto_push = settings.get_bool("cumulus.auto-push").unwrap_or(true);
    let mut config = fetch_remote_config(url, repo, token_file, create)
        .await
        .map_err(|error| user_error_with_message("Failed to open Cumulus remote", error))?;
    // Initialization writes several op heads. Keep those foreground-only, then
    // persist the requested setting and do one explicit push after init.
    config.auto_push = false;
    let backend_config = config.clone();
    let backend_initializer = move |_settings: &_, path: &_| {
        Ok(Box::new(CumulusBackend::init(path, backend_config.clone())?) as Box<_>)
    };
    let op_store_initializer = |_settings: &_, path: &_, root_data| {
        Ok(Box::new(CumulusOpStore::init(path, root_data)?) as Box<_>)
    };
    let op_heads_initializer = |_settings: &_, path: &_, root_id: &_| {
        Ok(Box::new(CumulusOpHeadsStore::init(path, root_id)?) as Box<_>)
    };
    let signer = Signer::from_settings(&settings).map_err(WorkspaceInitError::SignInit)?;
    let (workspace, readonly_repo) = Workspace::init_with_factories(
        &settings,
        workspace_root,
        &backend_initializer,
        signer,
        &op_store_initializer,
        &op_heads_initializer,
        ReadonlyRepo::default_index_store_initializer(),
        ReadonlyRepo::default_submodule_store_initializer(),
        &*default_working_copy_factory(),
        workspace_name,
    )
    .await?;
    config.auto_push = desired_auto_push;
    config
        .save(&workspace.repo_path().join("store"))
        .map_err(|error| user_error_with_message("Failed to save Cumulus configuration", error))?;
    Ok((workspace, readonly_repo, config))
}

pub(super) fn sync_engine(
    config: CumulusConfig,
    repo_path: &Path,
) -> Result<SyncEngine, CommandError> {
    SyncEngine::open(config, &repo_path.join("store"))
        .map_err(|error| user_error_with_message("Failed to start Cumulus sync", error))
}

pub(super) fn backend_config(repo: &ReadonlyRepo) -> Result<CumulusConfig, CommandError> {
    let backend = repo
        .store()
        .backend_impl::<CumulusBackend>()
        .ok_or_else(|| user_error("This repository does not use the Cumulus backend"))?;
    Ok(backend.config().clone())
}

pub(super) fn print_staleness_warning(
    ui: &mut Ui,
    engine: &SyncEngine,
) -> Result<(), CommandError> {
    let status = engine
        .status()
        .map_err(|error| user_error_with_message("Failed to read Cumulus status", error))?;
    if status.outbox_depth > 0 {
        writeln!(
            ui.warning_default(),
            "Cumulus has {} queued local update(s); run `jj cumulus sync`.",
            status.outbox_depth
        )?;
    }
    Ok(())
}
