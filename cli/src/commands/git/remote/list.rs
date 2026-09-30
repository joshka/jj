// Copyright 2020-2023 The Jujutsu Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::io::Write as _;

use bstr::BString;
use jj_lib::git;
use jj_lib::ref_name::RemoteName;
use jj_lib::repo::Repo as _;

use crate::cli_util::CommandHelper;
use crate::command_error::CommandError;
use crate::ui::Ui;

/// List Git remotes
#[derive(clap::Args, Clone, Debug)]
pub struct GitRemoteListArgs {}

pub async fn cmd_git_remote_list(
    ui: &mut Ui,
    command: &CommandHelper,
    _args: &GitRemoteListArgs,
) -> Result<(), CommandError> {
    let workspace_command = command.workspace_helper(ui).await?;
    let git_repo = git::get_git_repo(workspace_command.repo().store())?;
    for remote_name in git::configured_remote_names(&git_repo) {
        let remote_name: &RemoteName = &remote_name;
        let Some(remote) = git::try_find_active_remote(&git_repo, remote_name)? else {
            continue; // ignore empty [remote "<name>"] section
        };
        let fetch_url = to_display(remote.fetch_url());
        let push_url = to_display(remote.push_url());
        if fetch_url == push_url {
            writeln!(
                ui.stdout(),
                "{remote_name} {fetch_url}",
                remote_name = remote_name.as_symbol()
            )?;
        } else {
            writeln!(
                ui.stdout(),
                "{remote_name} {fetch_url} (push: {push_url})",
                remote_name = remote_name.as_symbol()
            )?;
        }
    }
    Ok(())
}

fn to_display(url: Option<&bstr::BStr>) -> BString {
    url.map_or_else(|| "<no URL>".into(), BString::from)
}
