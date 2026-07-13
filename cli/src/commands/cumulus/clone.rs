use std::path::PathBuf;

use jj_lib::object_id::ObjectId as _;
use jj_lib::ref_name::WorkspaceNameBuf;
use jj_lib::repo::Repo as _;
use jj_lib::repo_path::RepoPathBuf;

use super::shared::initialize_workspace;
use super::shared::print_staleness_warning;
use super::shared::sync_engine;
use crate::cli_util::CommandHelper;
use crate::command_error::CommandError;
use crate::command_error::user_error;
use crate::command_error::user_error_with_message;
use crate::ui::Ui;

/// Arguments for `jj cumulus clone`.
#[derive(clap::Args, Clone, Debug)]
pub struct CumulusCloneArgs {
    /// Remote in `<server-url>/<repo>` form.
    #[arg(value_hint = clap::ValueHint::Url)]
    source: String,
    /// Destination directory (defaults to the remote repository name).
    #[arg(value_hint = clap::ValueHint::DirPath)]
    destination: Option<PathBuf>,
    /// Initial sparse path; a trailing `/**` selects the directory subtree.
    #[arg(long)]
    sparse: Option<String>,
}

pub async fn cmd_cumulus_clone(
    ui: &mut Ui,
    command: &CommandHelper,
    args: &CumulusCloneArgs,
) -> Result<(), CommandError> {
    let (url, repo_name) = split_source(&args.source)?;
    let destination = args
        .destination
        .clone()
        .unwrap_or_else(|| PathBuf::from(&repo_name));
    let workspace_root = if destination.is_absolute() {
        destination
    } else {
        command.cwd().join(destination)
    };
    if workspace_root.exists() && !jj_lib::file_util::is_empty_dir(&workspace_root)? {
        return Err(user_error(
            "Destination path exists and is not an empty directory",
        ));
    }
    std::fs::create_dir_all(&workspace_root).map_err(|error| {
        user_error_with_message(
            format!("Failed to create {}", workspace_root.display()),
            error,
        )
    })?;
    let workspace_name = workspace_root
        .file_name()
        .and_then(|name| name.to_str())
        .filter(|name| !name.is_empty())
        .unwrap_or("clone");
    let (mut workspace, _initial_repo, config) = initialize_workspace(
        ui,
        command,
        &workspace_root,
        url,
        repo_name,
        false,
        WorkspaceNameBuf::from(workspace_name),
    )
    .await?;

    if let Some(pattern) = &args.sparse {
        let pattern = pattern.strip_suffix("/**").unwrap_or(pattern);
        let sparse_path = RepoPathBuf::from_relative_path(pattern)
            .map_err(|error| user_error_with_message("Invalid sparse path", error))?;
        let mut locked = workspace.start_working_copy_mutation().await?;
        locked
            .locked_wc()
            .set_sparse_patterns(vec![sparse_path])
            .await
            .map_err(|error| {
                crate::command_error::internal_error_with_message(
                    "Failed to set sparse patterns",
                    error,
                )
            })?;
        let operation_id = locked.locked_wc().old_operation_id().clone();
        locked.finish(operation_id).await?;
    }

    let engine = sync_engine(config, workspace.repo_path())?;
    engine
        .pull()
        .await
        .map_err(|error| user_error_with_message("Failed to pull Cumulus repository", error))?;
    let repo = workspace.repo_loader().load_at_head().await?;
    let local_workspace = workspace.workspace_name();
    let local_wc_id = repo.view().get_wc_commit_id(local_workspace);
    let target_id = repo
        .view()
        .wc_commit_ids()
        .iter()
        .filter(|(name, _)| name.as_str() != local_workspace.as_str())
        .map(|(_, id)| id)
        .min()
        .cloned()
        .or_else(|| {
            repo.view()
                .heads()
                .iter()
                .filter(|id| {
                    *id != repo.store().root_commit_id() && Some(*id) != local_wc_id
                })
                .min()
                .cloned()
        });
    if let Some(target_id) = target_id {
        let target = repo.store().get_commit_async(&target_id).await?;
        let mut workspace_command = command.for_workable_repo(ui, workspace, repo)?;
        let mut tx = workspace_command.start_transaction();
        tx.check_out(&target)?;
        tx.finish(ui, format!("check out Cumulus commit {}", target_id.hex()))
            .await?;
    }
    engine
        .push()
        .await
        .map_err(|error| user_error_with_message("Failed to publish clone operation", error))?;
    print_staleness_warning(ui, &engine)?;
    Ok(())
}

fn split_source(source: &str) -> Result<(String, String), CommandError> {
    let source = source.trim_end_matches('/');
    let (url, repo) = source
        .rsplit_once('/')
        .filter(|(url, repo)| !url.is_empty() && !repo.is_empty())
        .ok_or_else(|| user_error("Cumulus source must be `<server-url>/<repo>`"))?;
    Ok((url.to_owned(), repo.to_owned()))
}
