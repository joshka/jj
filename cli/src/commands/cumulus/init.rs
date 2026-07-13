use std::io::Write as _;

use jj_lib::ref_name::WorkspaceName;

use super::shared::initialize_workspace;
use super::shared::print_staleness_warning;
use super::shared::sync_engine;
use crate::cli_util::CommandHelper;
use crate::command_error::CommandError;
use crate::command_error::user_error_with_message;
use crate::ui::Ui;

/// Arguments for `jj cumulus init`.
#[derive(clap::Args, Clone, Debug)]
pub struct CumulusInitArgs {
    /// Cumulusd gRPC endpoint.
    #[arg(long, value_hint = clap::ValueHint::Url)]
    server: String,
    /// Remote repository name.
    #[arg(long)]
    repo: String,
    /// Create the remote repository if it does not exist.
    #[arg(long)]
    create: bool,
}

pub async fn cmd_cumulus_init(
    ui: &mut Ui,
    command: &CommandHelper,
    args: &CumulusInitArgs,
) -> Result<(), CommandError> {
    let workspace_root = command.cwd().to_owned();
    let (workspace, _repo, config) = initialize_workspace(
        ui,
        command,
        &workspace_root,
        args.server.clone(),
        args.repo.clone(),
        args.create,
        WorkspaceName::DEFAULT.to_owned(),
    )
    .await?;
    let engine = sync_engine(config, workspace.repo_path())?;
    engine
        .push()
        .await
        .map_err(|error| user_error_with_message("Failed to push initial Cumulus state", error))?;
    print_staleness_warning(ui, &engine)?;
    writeln!(
        ui.status(),
        "Initialized Cumulus repository in {}",
        workspace_root.display()
    )?;
    Ok(())
}
