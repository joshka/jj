mod clone;
mod init;
mod shared;
mod sync;

use clap::Subcommand;

use self::clone::CumulusCloneArgs;
use self::clone::cmd_cumulus_clone;
use self::init::CumulusInitArgs;
use self::init::cmd_cumulus_init;
use self::sync::CumulusSyncArgs;
use self::sync::cmd_cumulus_sync;
use crate::cli_util::CommandHelper;
use crate::command_error::CommandError;
use crate::ui::Ui;

/// Commands for native lazy Cumulus repositories.
#[derive(Subcommand, Clone, Debug)]
pub enum CumulusCommand {
    /// Clone an existing remote Cumulus repository.
    Clone(CumulusCloneArgs),
    /// Initialize a Cumulus workspace.
    Init(CumulusInitArgs),
    /// Push and pull Mode A operation state.
    Sync(CumulusSyncArgs),
}

pub async fn cmd_cumulus(
    ui: &mut Ui,
    command: &CommandHelper,
    subcommand: &CumulusCommand,
) -> Result<(), CommandError> {
    match subcommand {
        CumulusCommand::Clone(args) => cmd_cumulus_clone(ui, command, args).await,
        CumulusCommand::Init(args) => cmd_cumulus_init(ui, command, args).await,
        CumulusCommand::Sync(args) => cmd_cumulus_sync(ui, command, args).await,
    }
}
