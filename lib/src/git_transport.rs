// Copyright 2025 The Jujutsu Authors
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

//! Fetching from and pushing to Git remotes.

use std::io;
use std::num::NonZeroU32;

use bstr::ByteSlice as _;
use thiserror::Error;

use crate::git::GitPushOptions;
use crate::git::GitPushStats;
use crate::git::GitTransportOptions;
use crate::git::NegativeRefSpec;
use crate::git::RefSpec;
use crate::git::RefToPush;
use crate::git_backend::GitBackend;
use crate::ref_name::GitRefNameBuf;
use crate::ref_name::RefNameBuf;
use crate::ref_name::RemoteName;

/// Error from communicating with a Git remote.
#[derive(Error, Debug)]
pub enum GitTransportError {
    /// The remote has no URL, or its repository couldn't be found.
    #[error("Could not find repository at '{0}'")]
    NoSuchRepository(String),
    /// Authentication failed or was cancelled.
    #[error("Authentication failed for '{0}'")]
    Authentication(String),
    /// The remote URL uses a transport that isn't supported.
    #[error("Unsupported transport for remote '{0}'")]
    UnsupportedTransport(String),
    /// The transfer failed.
    #[error(transparent)]
    Transfer(#[from] girt::transfer::TransferError),
}

impl GitTransportError {
    fn from_transfer(
        repo: &girt::Repository,
        remote: &RemoteName,
        error: girt::transfer::TransferError,
    ) -> Self {
        let url = || {
            crate::git::try_find_active_remote(repo, remote)
                .ok()
                .flatten()
                .and_then(|remote| remote.fetch_url().map(|url| url.to_string()))
                .unwrap_or_else(|| remote.as_str().to_owned())
        };
        let source_missing = matches!(
            &error,
            girt::transfer::TransferError::Fetch(error)
                if matches!(
                    error.as_ref(),
                    girt::fetch::FetchWorkflowError::Transfer(girt::fetch::FetchError::Source(_))
                )
        );
        let destination_missing = matches!(
            &error,
            girt::transfer::TransferError::Push(error)
                if matches!(
                    error.as_ref(),
                    girt::push::PushError::NotSent(girt::push::PushFailure::Destination(_))
                )
        );
        if source_missing || destination_missing {
            return Self::NoSuchRepository(url());
        }
        match error {
            girt::transfer::TransferError::NoSuchRemote(_) => Self::NoSuchRepository(url()),
            girt::transfer::TransferError::Authentication(url) => Self::Authentication(url),
            girt::transfer::TransferError::UnsupportedTransport(_) => {
                Self::UnsupportedTransport(remote.as_str().to_owned())
            }
            error => Self::Transfer(error),
        }
    }
}

/// Result of a fetch.
#[derive(Debug, Default)]
pub(crate) struct GitFetchOutcome {
    /// Exact source refs that the remote doesn't have.
    pub missing_sources: Vec<String>,
}

/// Fetches from and pushes to the remotes of a Git repository.
pub(crate) struct GitTransport {
    repo: girt::Repository,
    committer: girt::Signature,
    environment: girt::transfer::Environment,
}

impl GitTransport {
    pub(crate) fn from_git_backend(git_backend: &GitBackend, options: GitTransportOptions) -> Self {
        let mut environment = girt::transfer::Environment::from_process();
        for (name, value) in options.environment {
            environment = environment.with(name, value);
        }
        Self {
            repo: git_backend.git_repo(),
            committer: git_backend.committer_signature(),
            environment,
        }
    }

    /// Fetches `refspecs` (except `negative_refspecs`) from the remote,
    /// pruning remote-tracking refs whose sources were deleted.
    pub(crate) fn fetch(
        &self,
        remote_name: &RemoteName,
        refspecs: &[RefSpec],
        negative_refspecs: &[NegativeRefSpec],
        callback: &mut dyn GitSubprocessCallback,
        depth: Option<NonZeroU32>,
        tag_namespace: &str,
    ) -> Result<GitFetchOutcome, GitTransportError> {
        if refspecs.is_empty() {
            return Ok(GitFetchOutcome::default());
        }
        let mut options = girt::transfer::FetchOptions::new(
            refspecs
                .iter()
                .map(|refspec| refspec.to_git_format())
                .chain(
                    negative_refspecs
                        .iter()
                        .map(|refspec| refspec.to_git_format()),
                ),
        )
        .prune(true)
        .depth(depth)
        .reflog(
            self.committer.clone(),
            format!("fetch {}", remote_name.as_str()),
        );
        let namespace = tag_namespace.trim_end_matches('/');
        if let Ok(namespace) = girt::refs::RefName::new(namespace) {
            options = options.tag_namespace(namespace);
        }
        let mut callbacks = Callbacks::new(callback);
        let outcome = self
            .repo
            .fetch(
                remote_name.as_str(),
                &options,
                &self.environment,
                &mut callbacks,
            )
            .map_err(|error| GitTransportError::from_transfer(&self.repo, remote_name, error))?;
        Ok(GitFetchOutcome {
            missing_sources: outcome
                .missing_sources
                .iter()
                .map(|source| source.to_str_lossy().into_owned())
                .collect(),
        })
    }

    /// Queries the remote's default branch (the branch its HEAD points to).
    pub(crate) fn default_branch(
        &self,
        remote_name: &RemoteName,
    ) -> Result<Option<RefNameBuf>, GitTransportError> {
        let mut callbacks = girt::transfer::NoCallbacks;
        let head = self
            .repo
            .remote_head(remote_name.as_str(), &self.environment, &mut callbacks)
            .map_err(|error| GitTransportError::from_transfer(&self.repo, remote_name, error))?;
        // Like `git remote show`, an unborn HEAD has no default branch.
        let (girt::fetch::RemoteHead::Symbolic { branch, .. }
        | girt::fetch::RemoteHead::Inferred { branch, .. }) = head
        else {
            return Ok(None);
        };
        Ok(branch
            .as_bytes()
            .strip_prefix(b"refs/heads/")
            .and_then(|name| str::from_utf8(name).ok())
            .map(RefNameBuf::from))
    }

    /// Pushes references, each conditional on the remote's current value
    /// matching the expected location (like `--force-with-lease`).
    pub(crate) fn push(
        &self,
        remote_name: &RemoteName,
        references: &[RefToPush],
        callback: &mut dyn GitSubprocessCallback,
        options: &GitPushOptions,
    ) -> Result<GitPushStats, GitTransportError> {
        let format = self.repo.object_format();
        let mut commands = Vec::with_capacity(references.len());
        for reference in references {
            let name = girt::refs::RefName::new(&reference.refspec.destination).map_err(|_| {
                GitTransportError::Transfer(girt::transfer::TransferError::Configuration(format!(
                    "invalid ref name: {}",
                    reference.refspec.destination
                )))
            })?;
            let new = match &reference.refspec.source {
                Some(source) => girt::ObjectId::from_hex(format, source).map_err(|_| {
                    GitTransportError::Transfer(girt::transfer::TransferError::Configuration(
                        format!("invalid push source: {source}"),
                    ))
                })?,
                None => girt::ObjectId::null(format),
            };
            commands.push(girt::push::PushCommand {
                name,
                expected: reference.expected_location.copied(),
                new,
                force: girt::push::ForcePolicy::Allow,
            });
        }
        let mut push_options =
            girt::transfer::PushOptions::default().identity(self.committer.clone());
        for option in &options.remote_push_options {
            push_options = push_options.push_option(option.as_bytes());
        }
        let mut callbacks = Callbacks::new(callback);
        let report = self
            .repo
            .push(
                remote_name.as_str(),
                commands,
                &push_options,
                &self.environment,
                &mut callbacks,
            )
            .map_err(|error| GitTransportError::from_transfer(&self.repo, remote_name, error))?;
        Ok(push_stats_from_report(&report))
    }
}

fn push_stats_from_report(report: &girt::push::PushReport) -> GitPushStats {
    let mut stats = GitPushStats::default();
    let unpack_error = match &report.unpack {
        Some(girt::push::Status::Rejected(reason)) => Some(reason.to_str_lossy().into_owned()),
        _ => None,
    };
    for status in &report.refs {
        let name = GitRefNameBuf::from(status.command.name.as_bytes().to_str_lossy().into_owned());
        match (&status.status, status.rejection_origin) {
            (Some(girt::push::Status::Ok), _) if unpack_error.is_none() => stats.pushed.push(name),
            (Some(girt::push::Status::Ok), _) => {
                stats.remote_rejected.push((name, unpack_error.clone()));
            }
            (
                Some(girt::push::Status::Rejected(_)),
                Some(girt::push::RejectionOrigin::ExpectedValue),
            ) => {
                // The remote ref didn't match the expected location.
                stats.rejected.push((name, Some("stale info".to_owned())));
            }
            (Some(girt::push::Status::Rejected(reason)), _) => {
                let reason = (!reason.is_empty()).then(|| reason.to_str_lossy().into_owned());
                stats.remote_rejected.push((name, reason));
            }
            (None, _) => {
                stats
                    .remote_rejected
                    .push((name, Some("no status reported by the remote".to_owned())));
            }
        }
    }
    stats
}

/// Adapts girt transfer callbacks to jj's progress and sideband callbacks.
struct Callbacks<'a> {
    callback: &'a mut dyn GitSubprocessCallback,
    progress: GitProgress,
    /// Incomplete remote message line.
    remote_line: Vec<u8>,
    /// Incomplete local transport message line.
    local_line: Vec<u8>,
}

impl<'a> Callbacks<'a> {
    fn new(callback: &'a mut dyn GitSubprocessCallback) -> Self {
        Self {
            callback,
            progress: GitProgress::default(),
            remote_line: Vec::new(),
            local_line: Vec::new(),
        }
    }

    fn remote_line(&mut self, line: &[u8]) {
        if update_progress(
            line,
            &mut self.progress.counted_objects,
            b"Counting objects:",
        ) || update_progress(
            line,
            &mut self.progress.compressed_objects,
            b"Compressing objects:",
        ) {
            if self.callback.needs_progress() {
                self.callback.progress(&self.progress).ok();
            }
        } else {
            let (body, term) = trim_sideband_line(line);
            self.callback.remote_sideband(body, term).ok();
        }
    }
}

impl Drop for Callbacks<'_> {
    fn drop(&mut self) {
        // Flush unterminated messages.
        if !self.remote_line.is_empty() {
            let line = std::mem::take(&mut self.remote_line);
            self.remote_line(&line);
        }
        if !self.local_line.is_empty() {
            let (body, term) = trim_sideband_line(&self.local_line);
            self.callback.local_sideband(body, term).ok();
        }
    }
}

/// Splits `buffer` + `data` into complete lines (terminated by CR or LF),
/// leaving any incomplete remainder in `buffer`.
fn split_lines(buffer: &mut Vec<u8>, data: &[u8], mut emit: impl FnMut(&[u8])) {
    buffer.extend_from_slice(data);
    let mut start = 0;
    while let Some(offset) = buffer[start..]
        .iter()
        .position(|&b| matches!(b, b'\r' | b'\n'))
    {
        let end = start + offset + 1;
        emit(&buffer[start..end]);
        start = end;
    }
    buffer.drain(..start);
}

impl girt::transfer::TransferCallbacks for Callbacks<'_> {
    fn remote_message(&mut self, message: &[u8]) {
        let mut buffer = std::mem::take(&mut self.remote_line);
        split_lines(&mut buffer, message, |line| self.remote_line(line));
        self.remote_line = buffer;
    }

    fn transport_message(&mut self, message: &[u8]) {
        let mut buffer = std::mem::take(&mut self.local_line);
        split_lines(&mut buffer, message, |line| {
            let (body, term) = trim_sideband_line(line);
            self.callback.local_sideband(body, term).ok();
        });
        self.local_line = buffer;
    }

    fn credential(
        &mut self,
        url: &str,
        prompt: girt::transfer::CredentialPrompt,
    ) -> Option<Vec<u8>> {
        let prompt = match prompt {
            girt::transfer::CredentialPrompt::Username => GitCredentialPrompt::Username,
            girt::transfer::CredentialPrompt::Password => GitCredentialPrompt::Password,
            _ => return None,
        };
        self.callback
            .credential(url, prompt)
            .map(String::into_bytes)
    }
}

/// A credential value to ask the user for.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum GitCredentialPrompt {
    /// Username for the URL.
    Username,
    /// Password or token for the URL.
    Password,
}

/// Handles Git transfer progress, messages and prompts.
pub trait GitSubprocessCallback {
    /// Whether to request progress information.
    fn needs_progress(&self) -> bool;

    /// Progress of local and remote operations.
    fn progress(&mut self, progress: &GitProgress) -> io::Result<()>;

    /// Single-line message from a local transport process (e.g. SSH).
    fn local_sideband(
        &mut self,
        message: &[u8],
        term: Option<GitSidebandLineTerminator>,
    ) -> io::Result<()>;

    /// Single-line sideband message received from remote.
    fn remote_sideband(
        &mut self,
        message: &[u8],
        term: Option<GitSidebandLineTerminator>,
    ) -> io::Result<()>;

    /// Asks the user for a credential that no configured helper supplied.
    /// Returns `None` to cancel.
    fn credential(&mut self, _url: &str, _prompt: GitCredentialPrompt) -> Option<String> {
        None
    }
}

/// Newline character that terminates sideband message line.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
#[repr(u8)]
pub enum GitSidebandLineTerminator {
    /// CR to remain on the same line.
    Cr = b'\r',
    /// LF to move to the next line.
    Lf = b'\n',
}

impl GitSidebandLineTerminator {
    /// Returns byte representation.
    pub fn as_byte(self) -> u8 {
        self as u8
    }
}

/// Progress of a Git transfer.
#[derive(Clone, Debug, Default)]
pub struct GitProgress {
    /// `(frac, total)` of "Resolving deltas".
    pub deltas: (u64, u64),
    /// `(frac, total)` of "Receiving objects".
    pub objects: (u64, u64),
    /// `(frac, total)` of remote "Counting objects".
    pub counted_objects: (u64, u64),
    /// `(frac, total)` of remote "Compressing objects".
    pub compressed_objects: (u64, u64),
}

// TODO: maybe let callers print each field separately and remove overall()?
impl GitProgress {
    /// Overall progress normalized to 0 to 1 range.
    pub fn overall(&self) -> f32 {
        if self.total() != 0 {
            self.fraction() as f32 / self.total() as f32
        } else {
            0.0
        }
    }

    fn fraction(&self) -> u64 {
        self.objects.0 + self.deltas.0 + self.counted_objects.0 + self.compressed_objects.0
    }

    fn total(&self) -> u64 {
        self.objects.1 + self.deltas.1 + self.counted_objects.1 + self.compressed_objects.1
    }
}

fn update_progress(line: &[u8], progress: &mut (u64, u64), prefix: &[u8]) -> bool {
    if let Some(line) = line.strip_prefix(prefix) {
        if let Some((frac, total)) = read_progress_line(line) {
            *progress = (frac, total);
        }

        true
    } else {
        false
    }
}

/// Read progress lines of the form: `<text> (<frac>/<total>)`
/// Ensures that frac < total
fn read_progress_line(line: &[u8]) -> Option<(u64, u64)> {
    // isolate the part between parenthesis
    let (_prefix, suffix) = line.split_once_str("(")?;
    let (fraction, _suffix) = suffix.split_once_str(")")?;

    // split over the '/'
    let (frac_str, total_str) = fraction.split_once_str("/")?;

    // parse to integers
    let frac = frac_str.to_str().ok()?.parse().ok()?;
    let total = total_str.to_str().ok()?.parse().ok()?;
    (frac <= total).then_some((frac, total))
}

/// Removes trailing spaces from sideband line, which may be padded by the
/// remote in order to clear the previous progress line.
fn trim_sideband_line(line: &[u8]) -> (&[u8], Option<GitSidebandLineTerminator>) {
    let (body, term) = match line {
        [body @ .., b'\r'] => (body, Some(GitSidebandLineTerminator::Cr)),
        [body @ .., b'\n'] => (body, Some(GitSidebandLineTerminator::Lf)),
        _ => (line, None),
    };
    let n = body.iter().rev().take_while(|&&b| b == b' ').count();
    (&body[..body.len() - n], term)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_read_progress_line() {
        assert_eq!(
            read_progress_line(b"Receiving objects: (42/100)\r"),
            Some((42, 100))
        );
        assert_eq!(
            read_progress_line(b"Resolving deltas: (0/1000)\r"),
            Some((0, 1000))
        );
        assert_eq!(read_progress_line(b"Receiving objects: (420/100)\r"), None);
        assert_eq!(
            read_progress_line(b"remote: this is something else\n"),
            None
        );
        assert_eq!(read_progress_line(b"fatal: this is a git error\n"), None);
    }

    #[test]
    fn test_split_lines_keeps_incomplete_remainder() {
        let mut buffer = Vec::new();
        let mut lines = Vec::new();
        split_lines(&mut buffer, b"one\ntw", |line| lines.push(line.to_vec()));
        split_lines(&mut buffer, b"o\rthree", |line| lines.push(line.to_vec()));
        assert_eq!(lines, [b"one\n".to_vec(), b"two\r".to_vec()]);
        assert_eq!(buffer, b"three");
    }

    #[test]
    fn test_initial_overall_progress_is_zero() {
        assert_eq!(GitProgress::default().overall(), 0.0);
    }
}
