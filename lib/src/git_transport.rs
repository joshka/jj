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

#![allow(dead_code)]

use std::io;
use bstr::ByteSlice as _;

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

impl girt::workflow::TransferCallbacks for Callbacks<'_> {
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
        prompt: girt::workflow::CredentialPrompt,
    ) -> Option<Vec<u8>> {
        let prompt = match prompt {
            girt::workflow::CredentialPrompt::Username => GitCredentialPrompt::Username,
            girt::workflow::CredentialPrompt::Password => GitCredentialPrompt::Password,
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
