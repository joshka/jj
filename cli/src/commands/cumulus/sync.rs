use super::shared::backend_config;
use super::shared::print_staleness_warning;
use super::shared::sync_engine;
use crate::cli_util::CommandHelper;
use crate::command_error::CommandError;
use crate::command_error::cli_error;
use crate::command_error::user_error_with_message;
use crate::ui::Ui;

/// Arguments for `jj cumulus sync`.
#[derive(clap::Args, Clone, Debug)]
pub struct CumulusSyncArgs {
    /// Push queued local operations without pulling.
    #[arg(long, conflicts_with_all = ["pull_only", "status"])]
    push_only: bool,
    /// Pull remote operations without pushing.
    #[arg(long, conflicts_with_all = ["push_only", "status"])]
    pull_only: bool,
    /// Show local queue and last background-push status without networking.
    #[arg(long, conflicts_with_all = ["push_only", "pull_only"])]
    status: bool,
    /// Suppress normal status output (used by the detached pusher).
    #[arg(long, hide = true)]
    quiet: bool,
}

pub async fn cmd_cumulus_sync(
    ui: &mut Ui,
    command: &CommandHelper,
    args: &CumulusSyncArgs,
) -> Result<(), CommandError> {
    let workspace_command = command.workspace_helper(ui).await?;
    let config = backend_config(workspace_command.repo())?;
    let engine = sync_engine(config, workspace_command.repo_path())?;
    if args.status {
        let status = engine
            .status()
            .map_err(|error| user_error_with_message("Failed to read Cumulus status", error))?;
        writeln!(ui.stdout(), "Outbox: {}", status.outbox_depth)?;
        writeln!(ui.stdout(), "Queued metadata: {}", status.queued_metadata)?;
        writeln!(ui.stdout(), "Queued blobs: {}", status.queued_blobs)?;
        if let Some(error) = status.last_push_error {
            writeln!(ui.stdout(), "Last push error: {error}")?;
        }
        return Ok(());
    }
    let report = if args.push_only {
        engine.push().await
    } else if args.pull_only {
        engine.pull().await
    } else {
        engine.sync().await
    }
    .map_err(|error| user_error_with_message("Cumulus sync failed", error))?;
    if report.push_already_running && std::env::var_os("JJ_CUMULUS_PUSHER").is_some() {
        return Ok(());
    }
    if report.push_already_running && args.push_only {
        return Err(cli_error("A Cumulus push is already running"));
    }
    if report.pulled_operations > 0 {
        command.recover_stale_working_copy(ui).await?;
    }
    if !args.quiet {
        writeln!(
            ui.status(),
            "Cumulus sync: pushed {} operation(s), pulled {} operation(s)",
            report.pushed_operations,
            report.pulled_operations
        )?;
        print_staleness_warning(ui, &engine)?;
    }
    Ok(())
}
use std::io::Write as _;
